"""The supervised calibration protocol (P2) — a transport-agnostic state machine the
edge wraps with SSE. It calls a Bridge (the cell's `primitives.py`, or the test
FakeBridge) for motion/capture/joints, and the shipped engine for the math. Every verb
returns a JSON-friendly event dict; the edge just serialises them.

Verbs:  build_plan -> motion_check -> detect -> (suggest_pose -> move_to -> capture)*
        -> solve -> validate -> curate -> accept

No cv2, no robot: drive it with FakeBridge + synth to prove the whole flow off-robot.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Protocol, Sequence, Tuple

import numpy as np

from . import urdf_io
from .curate import flag_suspect_corners, homography_resnap, resnap_corner, solve_curated
from .geometry import Chain, inv_T, transform_points
from .metrics import (
    consensus_spread,
    fiducial_consistency_mm,
    held_out_reprojection,
    observability,
    reprojection_detail,
    stereo_consistency,
    tip_landing_error,
    weak_rotation_axis,
)
from .posegen import suggest_next_pose
from .solve import _estimate_target_pose, solve_eye_in_hand, solve_static_camera
from .types import BoardModel, CalibResult, CaptureSample, PlanItem


# --------------------------------------------------------------------------- #
# The Bridge contract the cell's primitives.py must satisfy (engine calls it). #
# --------------------------------------------------------------------------- #
class BridgeProtocol(Protocol):
    def read_joints(self) -> np.ndarray: ...
    def read_pose(self) -> np.ndarray: ...            # reported base->flange (4x4)
    def move_to_joints(self, joints: np.ndarray) -> None: ...
    def capture(self) -> Tuple[np.ndarray, np.ndarray]: ...  # (corner_ids, corners_px)
    def estop_ok(self) -> bool: ...
    # ---- optional (probed with getattr; absent on the off-robot FakeBridge) ----
    # capture_image() -> (H,W,3) the retained frame for curation (E / A / image overlay)
    # capture_right() -> (corner_ids, corners_px) from the RIGHT stereo lens (F9 self-check)
    # sdk_T_lr()      -> 4x4 factory left->right extrinsic (the stereo baseline to check)
    # feasible(joints)-> bool cuRobo collision/in-envelope filter (7.7), checked at suggest
    # move_tcp_to_world(p_base) -> None drive the TCP to a base-frame point (tip test, 7.1)
    # marker_in_cam() -> 4x4 camera->shared-marker (base-to-base, dual-arm)
    # motion_probe() -> {"joint": int, "delta": float} the RIG-SPECIFIC safe test motion for
    #   the pre-flight check, AUTHORED BY THE AGENT for THIS cell (which joint is safe to
    #   twitch, how far). Absent -> the engine computes the least-flange-effect joint.


def _T(x) -> list:
    return np.asarray(x, float).round(6).tolist()


def build_plan(urdf_path: str) -> List[PlanItem]:
    """Derive the calibration plan from the rig (Pillar B): one item per detected camera.
    A camera on a moving flange -> eye_in_hand; one anchored to the world -> eye_to_hand.
    Two arms each with a wrist camera -> append a base_to_base item (the shared-marker
    transform between their bases), which runs after both eye-in-hand items are accepted."""
    root_names = _world_links(urdf_path)
    items: List[PlanItem] = []
    for cam in urdf_io.find_camera_links(urdf_path):
        flange = urdf_io.find_flange_link(urdf_path, cam)
        flange_body = urdf_io.link_chain_transform(urdf_path, flange, cam)
        body_optical = urdf_io.read_nominal_optical(urdf_path, cam)  # identity if absent
        kind = "eye_to_hand" if flange in root_names else "eye_in_hand"
        items.append(PlanItem(
            camera_link=cam, optical_frame=f"{cam}_optical_frame", kind=kind,
            flange_link=flange, nominal_flange_body=flange_body,
            nominal_T=flange_body @ body_optical, arm=flange,
        ))
    # Multi-arm: link the FIRST arm's wrist camera to EACH other arm's wrist camera — a
    # spanning set of N-1 base_to_base items for N arms (2 arms -> 1, 3 arms -> 2, a
    # humanoid's several limbs -> one per limb). Other base pairs follow transitively. One
    # representative eye-in-hand camera per distinct flange (arm). Each runs after both its
    # eye-in-hand calibrations are accepted.
    by_flange: dict = {}
    for p in items:
        if p.kind == "eye_in_hand":
            by_flange.setdefault(p.flange_link, p)
    reps = list(by_flange.values())
    if len(reps) >= 2:
        a = reps[0]
        for b in reps[1:]:
            items.append(PlanItem(
                camera_link=f"base_to_base[{a.arm}|{b.arm}]", optical_frame="", kind="base_to_base",
                flange_link=a.flange_link, nominal_flange_body=np.eye(4), nominal_T=np.eye(4),
                arm=a.arm, partner_camera=a.camera_link, secondary_camera=b.camera_link,
            ))
    return items


def _world_links(urdf_path: str) -> set:
    import xml.etree.ElementTree as ET
    root = ET.parse(urdf_path).getroot()
    children = {j.find("child").get("link") for j in root.findall("joint") if j.find("child") is not None}
    return {l.get("name") for l in root.findall("link")} - children  # links that are never a child


class CalibSession:
    def __init__(
        self,
        item: PlanItem,
        board: BoardModel,
        K: np.ndarray,
        chain: Chain,
        bridge: BridgeProtocol,
        *,
        wh: Tuple[int, int] = (1280, 720),
        nominal_joints: Optional[np.ndarray] = None,
        min_corners: Optional[int] = None,
        seed: int = 0,
        accept_heldout_px: float = 1.5,
        accept_tip_mm: float = 3.0,
    ):
        self.item = item
        self.board = board
        self.K = np.asarray(K, float)
        self.chain = chain
        self.bridge = bridge
        self.wh = wh
        self.nominal_joints = np.zeros(chain.n) if nominal_joints is None else np.asarray(nominal_joints, float)
        # The minimum usable view is a property of the TARGET, not a hardcoded 6 (a single
        # marker has 4 corners, a board many) — derive it unless explicitly overridden.
        self.min_corners = int(board.min_points) if min_corners is None else int(min_corners)
        self.rng = np.random.default_rng(seed)
        self.accept_heldout_px = accept_heldout_px
        self.accept_tip_mm = accept_tip_mm

        # Resolve the SOLVE kind from the plan item:
        #   moving-link camera            -> "eye_in_hand"
        #   world-fixed, handheld board   -> "static"  (operator moves the board, NO robot motion)
        #   world-fixed, arm presents it  -> "eye_to_hand" (the arm-presented bundle)
        if item.kind == "static":
            self.kind = "static"
        elif item.kind == "eye_to_hand":
            self.kind = "eye_to_hand" if getattr(item, "board_source", "handheld") == "arm" else "static"
        else:
            self.kind = "eye_in_hand"
        self.static = self.kind == "static"
        self.samples: List[CaptureSample] = []
        self.right_samples: List[CaptureSample] = []     # F9 stereo right-lens captures
        self.heldout: List[CaptureSample] = []           # cached held-out set (7.8)
        self.result: Optional[CalibResult] = None
        self.result_right: Optional[CalibResult] = None  # right-lens solve for T_L_R check
        self.X_est = np.asarray(item.nominal_T, float)   # seed for visibility prediction
        self.T_board_est: Optional[np.ndarray] = None
        self.motion_ok = False
        # optional bridge capabilities (absent on the off-robot FakeBridge)
        self._feasible = getattr(bridge, "feasible", None)
        self._capture_image = getattr(bridge, "capture_image", None)
        self._capture_right = getattr(bridge, "capture_right", None)
        self._sdk_T_lr = getattr(bridge, "sdk_T_lr", None)
        self._move_tcp = getattr(bridge, "move_tcp_to_world", None)
        self._motion_probe = getattr(bridge, "motion_probe", None)  # agent-authored {joint,delta}

    # ── C.0 pre-flight motion check (gates everything) ──────────────────────
    def motion_check(self, joint: Optional[int] = None, delta: float = 0.05, tol: float = 0.02) -> dict:
        q0 = self.bridge.read_joints()
        # WHICH joint to twitch + how much is RIG-SPECIFIC, so the agent owns it: if the
        # cell's Bridge authors `motion_probe()` (it knows what's safe to move on THIS rig —
        # arm, humanoid, gantry, near the operator) we use its {joint, delta}. Otherwise we
        # COMPUTE the safest joint (the one that moves the flange origin the least) from the
        # chain — never a hardcoded index, never "the last joint is the wrist". The engine
        # only owns the universal part: nudge → read back → confirm it moved + E-stop.
        probe = self._motion_probe() if callable(getattr(self, "_motion_probe", None)) else None
        if joint is not None:
            j = int(joint)
        elif probe and probe.get("joint") is not None:
            j = int(probe["joint"])
        else:
            j = self.chain.least_effect_joint(q0)
        if probe and probe.get("delta") is not None:
            delta = float(probe["delta"])
        j = max(0, min(j, self.chain.n - 1))
        step = float(delta)
        lim = self.chain.limits[j] if j < len(self.chain.limits) else None
        if lim is not None:
            lo, hi = lim
            if q0[j] + step > hi - 1e-3:        # near the upper limit → nudge down instead
                step = -abs(delta)
            target = float(np.clip(q0[j] + step, lo, hi))
            step = target - q0[j]
        q1 = q0.copy()
        q1[j] += step
        self.bridge.move_to_joints(q1)
        obs = self.bridge.read_joints() - q0
        self.bridge.move_to_joints(q0)                    # return to start
        cmd = q1 - q0
        max_err = float(np.max(np.abs(obs - cmd)))
        estop = bool(self.bridge.estop_ok())
        # need a real commanded motion (a clamped-to-zero nudge can't prove anything)
        self.motion_ok = (max_err < tol) and estop and (abs(step) > 1e-4)
        return {"type": "motion_check", "ok": self.motion_ok, "max_err_rad": round(max_err, 5),
                "joint": j, "estop_ok": estop, "commanded": _T(cmd), "observed": _T(obs)}

    # ── detect (board seen?) + bootstrap the board pose estimate ────────────
    def detect(self) -> dict:
        ids, uv = self.bridge.capture()
        seen = len(ids) >= self.min_corners
        if seen and self.T_board_est is None and not self.static:
            s = CaptureSample(id=-1, joints=self.bridge.read_joints(),
                              fk_pose=self.bridge.read_pose(), corner_ids=ids, corners=uv)
            T_c_board = _estimate_target_pose(s, self.board.points, self.K)
            self.T_board_est = s.fk_pose @ self.X_est @ T_c_board
        return {"type": "detect", "seen": seen, "n_corners": int(len(ids))}

    # ── static-camera capture (world-fixed cam, handheld board, NO robot motion) ─
    def capture_static(self, held_out: bool = False) -> dict:
        """One view of the operator-placed board for a world-fixed camera. No joints, no FK,
        no motion — just the camera's detected corners (+ retained frame)."""
        ids, uv = self.bridge.capture()
        img = None
        if not held_out and self._capture_image is not None:
            try:
                img = np.asarray(self._capture_image())
            except Exception:  # noqa: BLE001
                img = None
        bucket = self.heldout if held_out else self.samples
        s = CaptureSample(id=(2000 if held_out else 0) + len(bucket), joints=np.zeros(0),
                          fk_pose=np.eye(4), corner_ids=ids, corners=uv, image=img)
        accepted = len(ids) >= self.min_corners
        if accepted:
            bucket.append(s)
        return {"type": "capture", "index": s.id, "n_corners": int(len(ids)),
                "accepted": accepted, "held_out": held_out, "has_image": img is not None,
                "collected": len(self.samples), "static": True}

    # ── suggest -> move -> capture loop ─────────────────────────────────────
    def suggest_pose(self) -> dict:
        if self.T_board_est is None:
            return {"type": "suggest_pose", "feasible": False, "reason": "no board yet"}
        # Information gain (7.6): once we have a solve, steer the next pose toward the
        # least-observed rotation axis; collision/in-envelope filtered by cuRobo if wired (7.7).
        weak = weak_rotation_axis(self.result) if self.result is not None else None
        q, score = suggest_next_pose(
            self.chain, self.X_est, self.T_board_est, self.board, self.K, self.wh,
            [s.joints for s in self.samples], rng=self.rng, nominal_joints=self.nominal_joints,
            feasible=self._feasible, weak_axis=weak,
        )
        self._pending = q
        return {"type": "suggest_pose", "feasible": q is not None,
                "joints": _T(q) if q is not None else None,
                "ghost_pose": _T(self.chain.fk(q) @ self.X_est) if q is not None else None,
                "diversity_gain_deg": round(float(np.degrees(score)), 2)}

    def move_to(self, joints: Optional[Sequence[float]] = None) -> dict:
        if not self.motion_ok:
            return {"type": "move_to", "ok": False, "reason": "motion check not passed"}
        q = np.asarray(joints, float) if joints is not None else self._pending
        self.bridge.move_to_joints(q)
        return {"type": "move_to", "ok": True, "joints": _T(q)}

    def capture(self, held_out: bool = False) -> dict:
        if self.static:
            return self.capture_static(held_out)
        ids, uv = self.bridge.capture()
        joints = self.bridge.read_joints()
        fk_pose = self.bridge.read_pose()
        bucket = self.heldout if held_out else self.samples
        img = None
        if not held_out and self._capture_image is not None:
            try:
                img = np.asarray(self._capture_image())     # retain the frame for curation (E)
            except Exception:  # noqa: BLE001 — image is best-effort; the solve doesn't need it
                img = None
        s = CaptureSample(id=(2000 if held_out else 0) + len(bucket),
                          joints=joints, fk_pose=fk_pose, corner_ids=ids, corners=uv, image=img)
        accepted = len(ids) >= self.min_corners
        if accepted:
            bucket.append(s)
            # F9: also grab the RIGHT stereo lens (same instant) for the left/right self-check.
            if not held_out and self._capture_right is not None:
                try:
                    rids, ruv = self._capture_right()
                    if len(rids) >= self.min_corners:
                        self.right_samples.append(CaptureSample(
                            id=s.id, joints=joints, fk_pose=fk_pose,
                            corner_ids=rids, corners=ruv, cam="right"))
                except Exception:  # noqa: BLE001
                    pass
        return {"type": "capture", "index": s.id, "n_corners": int(len(ids)),
                "accepted": accepted, "held_out": held_out, "has_image": img is not None,
                "collected": len(self.samples)}

    # ── solve (reprojection bundle, or static PnP) ──────────────────────────
    def solve(self) -> dict:
        if self.static:
            # world-fixed camera: robust single-pose PnP over the fixed-board views. No
            # kinematics, no FK correction, no stereo (one lens localizes the board).
            self.result = solve_static_camera(self.samples, self.board.points, self.K)
            self.X_est = self.result.T_optical
            return {"type": "solve", "train_rms_px": round(self.result.residual_px, 3),
                    "scale": 1.0, "X": _T(self.result.T_optical), "board": _T(np.eye(4)),
                    "samples": self.result.samples_used, "kind": "static", "t_lr_err_deg": None}
        self.result = solve_eye_in_hand(self.samples, self.board.points, self.K, self.chain, mode=self.kind)
        self.X_est = self.result.T_optical
        self.T_board_est = self.result.T_board
        # Stereo F9: solve the right lens on its own captures and check inv(X_L)@X_R
        # against the SDK left->right baseline. eye-in-hand only (right lens shares the flange).
        t_lr_err = None
        sdk = self._sdk_T_lr() if callable(self._sdk_T_lr) else None
        if self.kind == "eye_in_hand" and len(self.right_samples) >= self.min_corners and sdk is not None:
            try:
                self.result_right = solve_eye_in_hand(self.right_samples, self.board.points, self.K, self.chain, mode="eye_in_hand")
                t_lr_err = stereo_consistency(self.result.T_optical, self.result_right.T_optical, np.asarray(sdk, float))
                self.result.t_lr_err_deg = t_lr_err
            except Exception:  # noqa: BLE001 — the self-check is a bonus, never blocks the solve
                t_lr_err = None
        return {"type": "solve", "train_rms_px": round(self.result.residual_px, 3),
                "scale": round(self.result.board_scale, 5), "X": _T(self.result.T_optical),
                "board": _T(self.result.T_board), "samples": self.result.samples_used,
                "kind": self.kind,
                "t_lr_err_deg": None if t_lr_err is None else round(float(t_lr_err), 3)}

    def _fiducial_obs(self) -> List[dict]:
        """The fiducial for the (HW) tip metric is the board ORIGIN itself: across the
        held-out poses the camera re-sights one fixed world point, and a good calibration
        maps every sighting to the same base position. No extra target / ground truth."""
        obs = []
        for s in self.heldout:
            P = _estimate_target_pose(s, self.board.points, self.K)  # board->camera
            obs.append({"joints": s.joints, "p_cam": P[:3, 3]})       # board origin in cam
        return obs

    # ── validate on held-out poses + tip-landing (the accept gate) ──────────
    def validate(self, n_heldout: int = 6, fiducial_obs: Optional[Sequence[dict]] = None,
                 recollect: bool = True) -> dict:
        """Accept gate (F7): RMS reprojection on poses NOT in the fit + a task-space tip
        metric, never the training residual. `recollect=False` re-SCORES the cached held-out
        set instead of re-driving the arm (7.8) — used after curation so a curate edit costs
        no extra motion."""
        assert self.result is not None, "solve before validate"
        if self.static:
            # No robot motion: split the captured views — fit on a subset, score the rest.
            # If the board moved between views (it must stay put), held-out blows up — the
            # honest signal. tip/observability/stereo/consensus don't apply to a fixed cam.
            n = len(self.samples)
            k = max(1, min(n_heldout, n // 3))
            train, test = self.samples[: n - k], self.samples[n - k:]
            sub = solve_static_camera(train, self.board.points, self.K) if len(train) >= 1 else self.result
            heldout_px = held_out_reprojection(sub, test, self.board.points, self.K, self.chain) if test else float("nan")
            passed = heldout_px == heldout_px and heldout_px < self.accept_heldout_px
            return {"type": "validate", "heldout_px": round(float(heldout_px), 3), "tip_mm": None,
                    "rot_worst_deg": 0.0, "observability": None, "t_lr_err_deg": None,
                    "consensus_deg": 0.0, "n_heldout": len(test), "pass": bool(passed)}
        if recollect or not self.heldout:
            self.heldout = []
            for _ in range(n_heldout):
                self.suggest_pose()
                if getattr(self, "_pending", None) is None:
                    continue
                self.move_to()
                self.capture(held_out=True)
        heldout_px = held_out_reprojection(self.result, self.heldout, self.board.points, self.K, self.chain) \
            if self.heldout else float("nan")
        # tip metric: explicit ground-truth obs (synth) if given, else the ground-truth-free
        # fiducial-consistency over the held-out board sightings (the on-robot path).
        if fiducial_obs:
            tip_mm = tip_landing_error(self.result, fiducial_obs, self.chain)
        else:
            tip_mm = fiducial_consistency_mm(self.result, self._fiducial_obs(), self.chain)
            if tip_mm != tip_mm:  # NaN (need >=2 held-out)
                tip_mm = None
        obs = observability(self.result)
        cons = consensus_spread(self.samples, self.board.points, self.K, seed=1)
        passed = (heldout_px < self.accept_heldout_px) and (tip_mm is None or tip_mm < self.accept_tip_mm)
        return {"type": "validate", "heldout_px": round(float(heldout_px), 3),
                "tip_mm": None if tip_mm is None else round(float(tip_mm), 3),
                "rot_worst_deg": round(obs["rot_worst_deg"], 4),
                "observability": {k: round(float(v), 4) for k, v in obs.items()},
                "t_lr_err_deg": None if self.result.t_lr_err_deg is None else round(float(self.result.t_lr_err_deg), 3),
                "consensus_deg": round(cons, 3), "n_heldout": len(self.heldout), "pass": bool(passed)}

    # ── curate: exclude bad images/corners (accumulated), re-solve, must improve held-out ──
    def curate(self, exclude_samples=None, exclude_corners=None) -> dict:
        assert self.result is not None
        ex_c = {int(k): list(v) for k, v in (exclude_corners or {}).items()}
        before = held_out_reprojection(self.result, self.heldout, self.board.points, self.K, self.chain) \
            if self.heldout else float("nan")
        new = solve_curated(self.samples, self.board.points, self.K, self.chain, mode=self.kind,
                            exclude_samples=exclude_samples, exclude_corners=ex_c)
        after = held_out_reprojection(new, self.heldout, self.board.points, self.K, self.chain) \
            if self.heldout else float("nan")
        improved = bool(after < before) if self.heldout else True
        if improved:
            self.result = new
            self.X_est = new.T_optical
            self.T_board_est = new.T_board
        return {"type": "curate", "excluded_samples": list(exclude_samples or []),
                "heldout_before": round(float(before), 3), "heldout_after": round(float(after), 3),
                "improved": improved}

    # ── observe: live observability DURING collection (6.2) — a cheap solve, no motion ──
    def observe(self) -> dict:
        """Solve on the running set so the operator sees the per-DOF observability meter
        WHILE collecting (which axes are still weak → what motion to add). Cheap, no arm
        motion; needs a few poses to be meaningful."""
        if len(self.samples) < max(6, self.min_corners):
            return {"type": "observe", "ready": False, "collected": len(self.samples)}
        r = solve_eye_in_hand(self.samples, self.board.points, self.K, self.chain, mode=self.kind)
        self.result = r                       # so the next suggest_pose can target the weak axis
        self.X_est = r.T_optical
        self.T_board_est = r.T_board
        obs = observability(r)
        return {"type": "observe", "ready": True, "collected": len(self.samples),
                "train_rms_px": round(r.residual_px, 3),
                "observability": {k: round(float(v), 4) for k, v in obs.items()}}

    # ── resnap: lock a coarse human corner drag to the truth (A) ─────────────
    def resnap(self, sample_id: int, corner_id: int, approx_uv) -> dict:
        """The operator dragged a gross-outlier corner; snap it sub-pixel. Homography refit
        (board geometry, always) then cornerSubPix on the retained frame (if present), update
        the corner, re-solve, report held-out so a bad edit is visible, never silent (F8)."""
        s = next((x for x in self.samples if x.id == sample_id), None)
        if s is None:
            return {"type": "resnap", "error": f"no sample {sample_id}"}
        try:
            uv = homography_resnap(s, self.board.points, int(corner_id))
        except Exception:  # noqa: BLE001 — fall back to the operator's click
            uv = np.asarray(approx_uv, float)
        if s.image is not None:
            try:
                uv = resnap_corner(s.image, uv)   # cv2 sub-pixel lock on the real frame
            except Exception:  # noqa: BLE001
                pass
        ci = list(s.corner_ids.astype(int)).index(int(corner_id))
        s.corners[ci] = uv
        self.result = solve_eye_in_hand(self.samples, self.board.points, self.K, self.chain, mode=self.kind)
        self.X_est, self.T_board_est = self.result.T_optical, self.result.T_board
        heldout = held_out_reprojection(self.result, self.heldout, self.board.points, self.K, self.chain) \
            if self.heldout else None
        return {"type": "resnap", "sample_id": sample_id, "corner_id": int(corner_id),
                "uv": [round(float(uv[0]), 2), round(float(uv[1]), 2)],
                "train_rms_px": round(self.result.residual_px, 3),
                "heldout_px": None if heldout is None else round(float(heldout), 3)}

    # ── tip_test: physically drive the TCP to a predicted fiducial (7.1, HW) ─
    def tip_test(self) -> dict:
        """Drive the TCP to where the calibration predicts the fiducial (board origin) sits
        in the base — the operator watches whether the tip lands on the mark (the signal a
        clueless operator trusts). Needs the Bridge's move_tcp_to_world; the software tip
        metric is the held-out fiducial-consistency in validate()."""
        if self.result is None or not self.heldout:
            return {"type": "tip_test", "moved": False, "reason": "validate first"}
        obs = self._fiducial_obs()
        if not obs:
            return {"type": "tip_test", "moved": False, "reason": "no fiducial sighting"}
        X, fk = self.result.T_optical, self.result.fk_offsets
        pts = [transform_points(self.chain.fk(np.asarray(o["joints"], float) + fk) @ X,
                                np.asarray(o["p_cam"], float).reshape(1, 3))[0] for o in obs]
        p_base = np.mean(pts, axis=0)
        moved = False
        if self._move_tcp is not None:
            try:
                self._move_tcp(p_base); moved = True
            except Exception:  # noqa: BLE001
                moved = False
        return {"type": "tip_test", "moved": moved, "predicted_base": _T(p_base),
                "spread_mm": round(float(fiducial_consistency_mm(self.result, obs, self.chain)), 3)}

    # ── frames: per-capture reprojection for the contact-sheet + overlay (P4) ─
    def frames(self, px_thresh: float = 2.0) -> dict:
        assert self.result is not None, "solve before frames"
        detail = reprojection_detail(self.result, self.samples, self.board.points, self.K, self.chain)
        flagged = flag_suspect_corners(self.result, self.samples, self.board.points, self.K, self.chain, px_thresh=px_thresh)
        summ = [{"id": d["id"], "n_corners": len(d["corner_ids"]),
                 "mean_px": round(d["mean_px"], 3), "max_px": round(d["max_px"], 3),
                 "flagged": len(flagged.get(d["id"], []))} for d in detail]
        return {"type": "frames", "frames": summ}

    def frame_detail(self, index: int) -> dict:
        """Full per-corner detected/predicted/err for ONE capture (the overlay)."""
        assert self.result is not None
        for d in reprojection_detail(self.result, self.samples, self.board.points, self.K, self.chain):
            if d["id"] == index:
                return {"type": "frame_detail", **d}
        return {"type": "frame_detail", "id": index, "corner_ids": [], "detected": [],
                "predicted": [], "err_px": [], "mean_px": 0.0, "max_px": 0.0}

    # ── nudge: operator repositions the optical frame -> optimizer re-snaps (P4) ─
    def nudge(self, x_new) -> dict:
        """Re-solve the bundle SEEDED at the operator's nudged flange->optical transform
        (4x4). The human picks the basin; the optimizer snaps to the nearest reprojection
        minimum. Held-out is re-reported so a worse nudge is visible, never silent."""
        assert self.samples, "collect + solve before nudging"
        Xn = np.asarray(x_new, float).reshape(4, 4)
        self.result = solve_eye_in_hand(self.samples, self.board.points, self.K, self.chain, X_init=Xn)
        self.X_est = self.result.T_optical
        self.T_board_est = self.result.T_board
        heldout = held_out_reprojection(self.result, self.heldout, self.board.points, self.K, self.chain) \
            if self.heldout else None
        return {"type": "nudge", "train_rms_px": round(self.result.residual_px, 3),
                "X": _T(self.result.T_optical),
                "heldout_px": None if heldout is None else round(float(heldout), 3)}

    # ── accept: write body->optical back to the URDF + the calibration json ──
    def accept(self, urdf_path: str, out_path: Optional[str] = None, provenance: str = "measured",
               calib_dir: Optional[str] = None) -> dict:
        assert self.result is not None
        body_optical = inv_T(self.item.nominal_flange_body) @ self.result.T_optical
        dst = urdf_io.write_calibrated_optical(urdf_path, self.item.camera_link, body_optical,
                                               out_path=out_path, provenance=provenance)
        # Persist the CalibResult json the seed promises (so the agent + base_to_base can read
        # each camera's X without re-running the solve).
        json_path = None
        if calib_dir is not None:
            json_path = _write_calib_json(calib_dir, self.item.camera_link, self.result, provenance)
        return {"type": "accept", "urdf": dst, "camera": self.item.camera_link,
                "optical_frame": self.item.optical_frame, "provenance": provenance,
                "calib_json": json_path, "body_to_optical": _T(body_optical)}


def _write_calib_json(calib_dir: str, camera: str, result: CalibResult, provenance: str) -> str:
    import json
    Path(calib_dir).mkdir(parents=True, exist_ok=True)
    safe = camera.replace("/", "_").replace("[", "_").replace("]", "_").replace("|", "_")
    dst = str(Path(calib_dir) / f"{safe}.json")
    Path(dst).write_text(json.dumps({
        "camera": camera, "kind": result.kind, "provenance": provenance,
        "T_optical": _T(result.T_optical), "T_board": _T(result.T_board),
        "fk_offsets": [round(float(v), 8) for v in result.fk_offsets],
        "board_scale": round(float(result.board_scale), 6),
        "residual_px": round(float(result.residual_px), 4),
        "t_lr_err_deg": result.t_lr_err_deg, "samples_used": result.samples_used,
    }, indent=2), encoding="utf-8")
    return dst


class BaseToBaseSession:
    """Dual-arm base-to-base (Pillar/variant 3). Both arms' wrist cams are already
    calibrated eye-in-hand (X_a, X_b); the operator places ONE board both cams can see, and
    for each capture both arms view it. solve_base_to_base recovers T_baseA_baseB from the
    shared sightings — matrix algebra, no nonlinear solve. Reuses the per-arm bridges."""

    def __init__(self, item: PlanItem, board: BoardModel, K: np.ndarray,
                 chain_a: Chain, chain_b: Chain, bridge_a, bridge_b,
                 X_a: np.ndarray, X_b: np.ndarray):
        self.item = item
        self.board = board
        self.K = np.asarray(K, float)
        self.chain_a, self.chain_b = chain_a, chain_b
        self.bridge_a, self.bridge_b = bridge_a, bridge_b
        self.X_a, self.X_b = np.asarray(X_a, float), np.asarray(X_b, float)
        self.obs: List[dict] = []
        self.result: Optional[np.ndarray] = None
        self.min_corners = int(board.min_points)

    def capture(self) -> dict:
        """Both arms view the shared board once; store (joints, camera->board) for each."""
        ida, uva = self.bridge_a.capture()
        idb, uvb = self.bridge_b.capture()
        if len(ida) < self.min_corners or len(idb) < self.min_corners:
            return {"type": "b2b_capture", "accepted": False, "n_a": int(len(ida)), "n_b": int(len(idb)),
                    "collected": len(self.obs)}
        sa = CaptureSample(id=0, joints=self.bridge_a.read_joints(), fk_pose=self.bridge_a.read_pose(),
                           corner_ids=ida, corners=uva)
        sb = CaptureSample(id=0, joints=self.bridge_b.read_joints(), fk_pose=self.bridge_b.read_pose(),
                           corner_ids=idb, corners=uvb)
        self.obs.append({
            "joints_a": sa.joints, "T_ca_marker": _estimate_target_pose(sa, self.board.points, self.K),
            "joints_b": sb.joints, "T_cb_marker": _estimate_target_pose(sb, self.board.points, self.K),
        })
        return {"type": "b2b_capture", "accepted": True, "n_a": int(len(ida)), "n_b": int(len(idb)),
                "collected": len(self.obs)}

    def solve(self) -> dict:
        if len(self.obs) < 2:
            return {"type": "b2b_solve", "error": "need >=2 shared-board captures"}
        from .solve import solve_base_to_base
        self.result = solve_base_to_base(self.obs, self.X_a, self.X_b, self.chain_a, self.chain_b)
        # spread of the per-obs transforms = a self-consistency quality (deg)
        ts = [solve_base_to_base([o], self.X_a, self.X_b, self.chain_a, self.chain_b) for o in self.obs]
        from .geometry import transform_geodesic
        spread = max((transform_geodesic(self.result, t)[0] for t in ts), default=0.0)
        return {"type": "b2b_solve", "T_base_to_base": _T(self.result),
                "captures": len(self.obs), "consensus_deg": round(float(spread), 3)}

    def accept(self, calib_dir: str) -> dict:
        import json
        assert self.result is not None
        Path(calib_dir).mkdir(parents=True, exist_ok=True)
        dst = str(Path(calib_dir) / "base_to_base.json")
        Path(dst).write_text(json.dumps({
            "kind": "base_to_base", "T_base_to_base": _T(self.result),
            "arm_a": self.item.partner_camera, "arm_b": self.item.secondary_camera,
            "captures": len(self.obs),
        }, indent=2), encoding="utf-8")
        return {"type": "b2b_accept", "path": dst, "T_base_to_base": _T(self.result)}
