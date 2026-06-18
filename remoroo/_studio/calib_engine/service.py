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
from .session import BaseToBaseSession, CalibSession, build_plan
from .types import BoardModel, CalibResult, PlanItem


def _planitem_json(p: PlanItem) -> dict:
    return {"camera_link": p.camera_link, "optical_frame": p.optical_frame,
            "kind": p.kind, "flange_link": p.flange_link, "arm": p.arm,
            "board_source": p.board_source,
            "partner_camera": p.partner_camera, "secondary_camera": p.secondary_camera,
            # the camera's NOMINAL flange->optical (from the URDF) — the "before" pose the
            # stage slides FROM as calibration refines it toward the solved X.
            "nominal_T": np.asarray(p.nominal_T, float).round(6).tolist()}


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
        calib_dir: Optional[str] = None,
        accept_heldout_px: float = 1.5,
        accept_tip_mm: float = 3.0,
    ):
        self.urdf_path = urdf_path
        self.board = board
        self.K = np.asarray(K, float)
        self.bridge_factory = bridge_factory
        self.chain_provider = chain_provider
        self.wh = wh
        self.out_urdf = out_urdf or urdf_path
        self.fiducial_obs = fiducial_obs
        self.calib_dir = calib_dir
        # accept gate from cell.yaml (the operator's tuned thresholds), not a hardcoded default
        self.accept_heldout_px = accept_heldout_px
        self.accept_tip_mm = accept_tip_mm
        self.plan_items: List[PlanItem] = []
        self.session: Optional[CalibSession] = None
        self.b2b: Optional[BaseToBaseSession] = None
        self.accepted: dict = {}   # camera_link -> CalibResult (this Studio session)

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
            if item.kind == "base_to_base":
                return self._select_b2b(item)
            self.b2b = None
            # A world-fixed camera with a HANDHELD board has no moving chain (the flange is
            # the world root) — use an empty Chain; the static path never touches it.
            static = item.kind == "eye_to_hand" and getattr(item, "board_source", "handheld") != "arm"
            chain = Chain([], []) if static else self.chain_provider(item.flange_link)
            bridge = self.bridge_factory(item)
            self.session = CalibSession(item, self.board, self.K, chain, bridge, wh=self.wh,
                                        accept_heldout_px=self.accept_heldout_px,
                                        accept_tip_mm=self.accept_tip_mm)
            return {"type": "select", "camera_link": cam, "kind": item.kind, "flange_link": item.flange_link}

        # base_to_base verbs run against the dedicated dual-arm session
        if verb in ("b2b_capture", "b2b_solve", "b2b_accept"):
            if self.b2b is None:
                return {"error": "select the base_to_base item first"}
            if verb == "b2b_capture":  return self.b2b.capture()
            if verb == "b2b_solve":    return self.b2b.solve()
            return self.b2b.accept(self.calib_dir or ".")

        s = self.session
        if s is None:
            return {"error": "no calibration selected — call select first"}

        if verb == "motion_check":  return s.motion_check()
        if verb == "detect":        return s.detect()
        if verb == "suggest_pose":  return s.suggest_pose()
        if verb == "move_to":       return s.move_to(body.get("joints"))
        if verb == "capture":       return s.capture(bool(body.get("held_out", False)))
        if verb == "observe":       return s.observe()
        if verb == "solve":         return s.solve()
        if verb == "validate":      return s.validate(n_heldout=int(body.get("n_heldout", 6)),
                                                       fiducial_obs=self.fiducial_obs,
                                                       recollect=bool(body.get("recollect", True)))
        if verb == "curate":        return s.curate(exclude_samples=body.get("exclude_samples"),
                                                     exclude_corners=body.get("exclude_corners"))
        if verb == "resnap":        return s.resnap(int(body.get("sample_id", -1)),
                                                     int(body.get("corner_id", -1)), body.get("uv"))
        if verb == "tip_test":      return s.tip_test()
        if verb == "frames":        return s.frames()
        if verb == "frame_detail":  return s.frame_detail(int(body.get("index", -1)))
        if verb == "nudge":         return s.nudge(body.get("x_new"))
        if verb == "accept":
            out = s.accept(self.urdf_path, out_path=self.out_urdf,
                           provenance=body.get("provenance", "measured"), calib_dir=self.calib_dir)
            self.accepted[s.item.camera_link] = s.result   # remember X for base_to_base
            return out
        return {"error": f"unknown verb {verb!r}"}

    def _select_b2b(self, item: PlanItem) -> dict:
        """Build the dual-arm base_to_base session — needs BOTH wrist cameras already
        calibrated (accepted) this session, so we have each arm's X."""
        ra = self.accepted.get(item.partner_camera)
        rb = self.accepted.get(item.secondary_camera)
        if ra is None or rb is None:
            missing = [c for c, r in ((item.partner_camera, ra), (item.secondary_camera, rb)) if r is None]
            return {"error": f"calibrate + accept these cameras first: {', '.join(missing)}"}
        pa = next(p for p in self.plan_items if p.camera_link == item.partner_camera)
        pb = next(p for p in self.plan_items if p.camera_link == item.secondary_camera)
        self.session = None
        self.b2b = BaseToBaseSession(
            item, self.board, self.K,
            self.chain_provider(pa.flange_link), self.chain_provider(pb.flange_link),
            self.bridge_factory(pa), self.bridge_factory(pb),
            ra.T_optical, rb.T_optical,
        )
        return {"type": "select", "camera_link": item.camera_link, "kind": "base_to_base",
                "flange_link": item.flange_link, "partner_camera": item.partner_camera,
                "secondary_camera": item.secondary_camera}
