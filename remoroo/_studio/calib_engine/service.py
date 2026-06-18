"""CalibService — the transport-agnostic dispatch the edge wraps with HTTP. It turns the
Studio's /edge/calib/<verb> requests into CalibSession calls and JSON back. Dependencies
(the Bridge, the kinematic chain, the board, intrinsics) are INJECTED so the whole edge
layer is testable with a FakeBridge + synth (no server, no robot); the real edge passes a
camera-backed bridge + chain_from_urdf + SDK intrinsics.

One calibration at a time (the gate is supervised, one item after another — F2).
"""
from __future__ import annotations

from typing import Callable, List, Optional

import numpy as np

from .geometry import Chain
from .session import CalibSession, build_plan
from .types import BoardModel, PlanItem


def _planitem_json(p: PlanItem) -> dict:
    return {"camera_link": p.camera_link, "optical_frame": p.optical_frame,
            "kind": p.kind, "flange_link": p.flange_link, "arm": p.arm}


class CalibService:
    def __init__(
        self,
        urdf_path: str,
        board: BoardModel,
        K: np.ndarray,
        bridge_factory: Callable[[PlanItem], object],
        chain_provider: Callable[[str], Chain],
        *,
        wh=(1280, 720),
        out_urdf: Optional[str] = None,
        fiducial_obs=None,
    ):
        self.urdf_path = urdf_path
        self.board = board
        self.K = np.asarray(K, float)
        self.bridge_factory = bridge_factory
        self.chain_provider = chain_provider
        self.wh = wh
        self.out_urdf = out_urdf or urdf_path
        self.fiducial_obs = fiducial_obs
        self.plan_items: List[PlanItem] = []
        self.session: Optional[CalibSession] = None

    def handle(self, verb: str, body: Optional[dict] = None) -> dict:
        body = body or {}
        if verb == "plan":
            self.plan_items = build_plan(self.urdf_path)
            return {"type": "plan", "items": [_planitem_json(p) for p in self.plan_items]}

        if verb == "select":
            if not self.plan_items:
                self.plan_items = build_plan(self.urdf_path)
            cam = body.get("camera_link")
            item = next((p for p in self.plan_items if p.camera_link == cam), None)
            if item is None:
                return {"error": f"no camera {cam!r} in plan"}
            chain = self.chain_provider(item.flange_link)
            bridge = self.bridge_factory(item)
            self.session = CalibSession(item, self.board, self.K, chain, bridge, wh=self.wh)
            return {"type": "select", "camera_link": cam, "kind": item.kind, "flange_link": item.flange_link}

        s = self.session
        if s is None:
            return {"error": "no calibration selected — call select first"}

        if verb == "motion_check":  return s.motion_check()
        if verb == "detect":        return s.detect()
        if verb == "suggest_pose":  return s.suggest_pose()
        if verb == "move_to":       return s.move_to(body.get("joints"))
        if verb == "capture":       return s.capture(bool(body.get("held_out", False)))
        if verb == "solve":         return s.solve()
        if verb == "validate":      return s.validate(n_heldout=int(body.get("n_heldout", 6)), fiducial_obs=self.fiducial_obs)
        if verb == "curate":        return s.curate(exclude_samples=body.get("exclude_samples"))
        if verb == "frames":        return s.frames()
        if verb == "frame_detail":  return s.frame_detail(int(body.get("index", -1)))
        if verb == "nudge":         return s.nudge(body.get("x_new"))
        if verb == "accept":        return s.accept(self.urdf_path, out_path=self.out_urdf, provenance=body.get("provenance", "measured"))
        return {"error": f"unknown verb {verb!r}"}
