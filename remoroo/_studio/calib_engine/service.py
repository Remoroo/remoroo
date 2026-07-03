"""CalibService — the transport-agnostic dispatch the edge wraps with HTTP. It turns the
Studio's /edge/calib/<verb> requests into CalibSession calls and JSON back. Dependencies
(the Bridge, the kinematic chain, the board, intrinsics) are INJECTED so the whole edge
layer is testable with a FakeBridge + synth (no server, no robot); the real edge passes a
camera-backed bridge + chain_from_urdf + SDK intrinsics.

One calibration at a time (the gate is supervised, one item after another — F2).
"""
from __future__ import annotations

from typing import Callable, Dict, List, Optional

import numpy as np

from . import steps
from .geometry import Chain
from .session import BaseToBaseSession, CalibSession, build_plan
from .types import BoardModel, CalibResult, PlanItem, Target


def _planitem_json(p: PlanItem) -> dict:
    return {"camera_link": p.camera_link, "optical_frame": p.optical_frame,
            "kind": p.kind, "flange_link": p.flange_link, "arm": p.arm,
            "board_source": p.board_source,
            "partner_camera": p.partner_camera, "secondary_camera": p.secondary_camera,
            # authored-pipeline identity: the step id (rail key + dep target), which named
            # target it binds, and the step ids that must be accepted before it can run.
            "id": p.id or p.camera_link, "target_id": p.target_id,
            "depends_on": list(p.depends_on),
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
        accept_rot_sigma_deg: float = 0.5,
        accept_trans_sigma_mm: float = 2.0,
        plan_items: Optional[List[PlanItem]] = None,
        targets: Optional[Dict[str, Target]] = None,
        intrinsics: Optional[Dict[str, tuple]] = None,
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
        self.accept_rot_sigma_deg = accept_rot_sigma_deg
        self.accept_trans_sigma_mm = accept_trans_sigma_mm
        # The AUTHORED pipeline (resolved PlanItems + named Targets), injected by the edge.
        # When absent (older tests), fall back to deriving the plan from the URDF. The engine
        # consumes the agent's steps; it does not guess them when they're authored.
        self._authored: Optional[List[PlanItem]] = list(plan_items) if plan_items else None
        self.targets: Dict[str, Target] = dict(targets or {})
        # Per-camera (K, wh): a multi-camera rig solves each camera with ITS OWN intrinsics.
        self.intrinsics: Dict[str, tuple] = dict(intrinsics or {})
        self.plan_items: List[PlanItem] = []
        self.session: Optional[CalibSession] = None
        self.b2b: Optional[BaseToBaseSession] = None
        self.accepted: dict = {}   # camera_link -> CalibResult (this Studio session)

    def _resolve_plan(self) -> List[PlanItem]:
        """The authored pipeline if the agent provided one, else the URDF-derived fallback."""
        return list(self._authored) if self._authored is not None else build_plan(self.urdf_path)

    def handle(self, verb: str, body: Optional[dict] = None) -> dict:
        body = body or {}
        if verb in ("plan", "pipeline"):
            self.plan_items = self._resolve_plan()
            return {"type": "pipeline", "items": [_planitem_json(p) for p in self.plan_items]}

        if verb == "select":
            if not self.plan_items:
                self.plan_items = self._resolve_plan()
            # The Studio selects a step by its authored ID (e.g. "arm1_cam1"); older callers
            # pass the camera link. Match on EITHER so both work.
            sel = body.get("camera_link")
            item = next((p for p in self.plan_items
                         if (p.id or p.camera_link) == sel or p.camera_link == sel), None)
            if item is None:
                return {"error": f"no step {sel!r} in pipeline"}
            # Build the executor for this step's KIND from the registry (no `if kind == ...`);
            # the family decides which verb-slot it lives in. StepError (unknown kind, or a
            # base-to-base whose partners aren't accepted yet) surfaces as an honest {error}.
            try:
                executor, family = steps.make_executor(item, self._step_ctx(item))
            except steps.StepError as e:
                return {"error": str(e)}
            if family == "b2b":
                self.session = None
                self.b2b = executor
                return {"type": "select", "camera_link": item.camera_link, "kind": item.kind,
                        "flange_link": item.flange_link, "partner_camera": item.partner_camera,
                        "secondary_camera": item.secondary_camera}
            self.b2b = None
            self.session = executor
            return {"type": "select", "camera_link": item.camera_link, "kind": item.kind,
                    "flange_link": item.flange_link, "id": item.id or item.camera_link}

        # base_to_base verbs run against the dedicated dual-arm session
        if verb in ("b2b_capture", "b2b_observe", "b2b_solve", "b2b_validate", "b2b_verify", "b2b_accept"):
            if self.b2b is None:
                return {"error": "select the base_to_base item first"}
            if verb == "b2b_capture":   return self.b2b.capture()
            if verb == "b2b_observe":   return self.b2b.observe()
            if verb == "b2b_solve":     return self.b2b.solve()
            if verb == "b2b_validate":  return self.b2b.validate(n_heldout=int(body.get("n_heldout", 0)))
            if verb == "b2b_verify":    return self.b2b.verify()
            return self.b2b.accept(self.calib_dir or ".", urdf_path=self.urdf_path,
                                   out_path=self.out_urdf, force=bool(body.get("force", False)))

        s = self.session
        if s is None:
            return {"error": "no calibration selected — call select first"}

        if verb == "motion_check":  return s.motion_check()
        if verb == "detect":        return s.detect()
        # ── supervised visual seed → confirm → verify (the operator places + trusts X) ──
        if verb == "seed":          return s.seed()
        if verb == "seed_nudge":    return s.seed_nudge(body.get("x_new"))
        if verb == "seed_wiggle":   return s.seed_wiggle(float(body.get("deg", 3.0)))
        if verb == "confirm_seed":  return s.confirm_seed()
        if verb == "overlay":       return s.predicted_overlay()
        if verb == "verify":        return s.verify(n_poses=int(body.get("n_poses", 4)))
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
        # recorded-routine replay (automatic recalibration along the human-traversed path)
        if verb == "routine_info":  return s.routine_info(self.calib_dir or ".")
        if verb == "replay_start":  return s.replay_start(self.calib_dir or ".")
        if verb == "replay_step":   return s.replay_step()
        if verb == "replay_abort":  return s.replay_abort()
        if verb == "frames":        return s.frames()
        if verb == "frame_detail":  return s.frame_detail(int(body.get("index", -1)))
        if verb == "nudge":         return s.nudge(body.get("x_new"))
        if verb == "accept":
            out = s.accept(self.urdf_path, out_path=self.out_urdf,
                           provenance=body.get("provenance", "measured"), calib_dir=self.calib_dir,
                           force=bool(body.get("force", False)))
            if out.get("ok"):
                self.accepted[s.item.camera_link] = s.result   # remember X for base_to_base
            return out
        return {"error": f"unknown verb {verb!r}"}

    def _step_ctx(self, item: PlanItem) -> steps.StepContext:
        """Inject what the step factories need. The target is the one THIS step bound
        (`item.target_id` into the authored targets), falling back to the single default
        board for an un-authored / single-target run."""
        target = self.targets.get(item.target_id) or self.board
        # the bound camera's own intrinsics (multi-cam); else the service default K.
        kv = self.intrinsics.get(item.camera_link)
        K = kv[0] if kv and kv[0] is not None else self.K
        wh = tuple(kv[1]) if kv and kv[1] is not None else self.wh
        return steps.StepContext(
            target=target, K=K, chain_provider=self.chain_provider,
            bridge_factory=self.bridge_factory, wh=wh,
            accept_heldout_px=self.accept_heldout_px, accept_tip_mm=self.accept_tip_mm,
            accept_rot_sigma_deg=self.accept_rot_sigma_deg,
            accept_trans_sigma_mm=self.accept_trans_sigma_mm,
            accepted=self.accepted, plan_items=self.plan_items,
        )
