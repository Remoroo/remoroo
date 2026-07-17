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
from .geometry import Chain, inv_T, make_T, project, rodrigues, rotation_angle, transform_points
from .metrics import (
    consensus_spread,
    fiducial_consistency_mm,
    held_out_reprojection,
    heldout_interarm_agreement_mm,
    interarm_agreement_mm,
    observability,
    reprojection_detail,
    stereo_consistency,
    tip_landing_error,
    weak_rotation_axis,
    well_observed,
)
from .posegen import suggest_next_pose
from .routine import HOP_TOL_RAD, ReplayCursor, Routine, RoutineRecorder
from .solve import (
    _estimate_target_pose,
    solve_base_to_base_bundle,
    solve_eye_in_hand,
    solve_static_camera,
)
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
    # move_direct(joints) -> None a DIRECT (planner-free) joint move — routine REPLAY uses it
    #   to re-follow the recorded human-traversed path in tiny hops; absent -> move_to_joints.


def _T(x) -> list:
    return np.asarray(x, float).round(6).tolist()


def _image_coverage(samples, wh, grid=(6, 4)) -> dict:
    """Coarse grid of WHERE the board's detected corners landed in the image across all views —
    the static-camera placement map (the interactive-calibration coverage idea). Empty cells are
    regions the board never covered, so the operator knows to fill the frame corners + add tilt.
    Returns {grid:[gx,gy], cells:[[...]], filled, total} for the Studio heatmap."""
    W, H = (int(wh[0]) or 1, int(wh[1]) or 1)
    gx, gy = grid
    cells = np.zeros((gy, gx), int)
    for s in samples:
        for u, v in np.asarray(getattr(s, "corners", np.zeros((0, 2))), float):
            cx = min(gx - 1, max(0, int(u / W * gx)))
            cy = min(gy - 1, max(0, int(v / H * gy)))
            cells[cy, cx] += 1
    return {"grid": [gx, gy], "cells": cells.tolist(),
            "filled": int((cells > 0).sum()), "total": int(gx * gy)}


def _chain_columns(routine, chain_names: Sequence[str], n_chain: int):
    """Which columns of a (possibly WHOLE-BODY) routine belong to this step's chain. By NAME
    when both sides carry names; [] when the routine is already chain-shaped (legacy/off-robot,
    width == chain DOF); None when the chain can't be found in the routine (rig changed)."""
    rn = [str(n) for n in (routine.joint_names or [])]
    cn = [str(n) for n in (chain_names or [])]
    if rn and cn and set(cn) <= set(rn):
        cols = [rn.index(n) for n in cn]
        return [] if cols == list(range(len(rn))) else cols
    W = np.asarray(routine.waypoints, float)
    if W.ndim == 2 and W.shape[1] == n_chain:
        return []                                   # already chain-shaped
    return None


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
        accept_rot_sigma_deg: float = 0.5,
        accept_trans_sigma_mm: float = 2.0,
        accept_static_depth_mm: float = 8.0,
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
        # Observability accept gate (the fix for the silent 2x-translation failure): X is only
        # trustworthy if every DOF is pinned below these 1σ limits (deg / mm). Threaded from
        # cell.yaml by the service; defaults are sane for a wrist-cam eye-in-hand.
        self.accept_rot_sigma_deg = float(accept_rot_sigma_deg)
        self.accept_trans_sigma_mm = float(accept_trans_sigma_mm)
        # ACTIVE-REFINE targets — deliberately much tighter than the accept gate. The accept
        # gate (0.5°/2mm) is the MINIMUM-trust floor for saving at all; the refine loop chases
        # the statistical floor the rig can actually deliver (ChArUco corner noise ~0.1-0.3px
        # over ~20 poses ⇒ σ well under 0.1°/0.2mm in the Fisher model). Stopping the loop at
        # the accept σ would declare "converged" the moment a decent replay lands. σ is
        # PRECISION — the held-out validate stays the accuracy anchor (FK bias never shows in σ).
        self.refine_rot_sigma_deg = 0.05
        self.refine_trans_sigma_mm = 0.15
        # A WORLD-FIXED camera has no hand-eye rotation coupling, so its weak DOF is the optical-axis
        # (depth) translation — a near-frontal / single placement leaves it under-constrained. This is
        # the hard depth-σ limit (mm) the static accept refuses past (unless the operator forces it).
        self.accept_static_depth_mm = float(accept_static_depth_mm)
        # The supervised visual phase: seed (place + confirm the hand-eye seed) → collect
        # (look-at orbit) → verify (drift tracking) → done. Static cams skip seed (no chain).
        self.phase: str = "seed"
        self._last_Tc_board: Optional[np.ndarray] = None   # PnP board->camera at the last detect
        self._seed_sample: Optional[CaptureSample] = None  # the detect capture that seeded X

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
        # ROUTINE recording (arm-motion kinds only): passively logs the dense joint trajectory
        # while the operator collects, so the whole calibration can later be REPLAYED (direct
        # hops along the human-traversed path — never a planner). The edge feeds tick(); off-robot
        # tests call it directly. Armed at confirm_seed (eye-in-hand) / detect (eye-to-hand).
        # NAME-LESS on purpose: the joint stream ADOPTS the whole body's joint names on its
        # first full sample (a commissioned rig is ONE body — record all so replay/goto can
        # restore all). Off-robot (no stream) the vector path records the chain, as before.
        self.recorder: Optional[RoutineRecorder] = None if self.static else RoutineRecorder()
        self._replay: Optional[ReplayCursor] = None
        self._marks: Optional[dict] = None      # PLANNED replay state (cuRobo drives; edge owns motion)
        self._active: Optional[dict] = None     # ACTIVE NBV collection state (edge owns motion)
        # POST-MOTION SETTLE (automatic captures only): the arm RINGS after a streamed
        # trajectory stops — structural oscillation the joint encoders don't see but sub-pixel
        # corners do. Capturing immediately smears the fit (the measured reason the first
        # planned recalibration scored worse than manual — a human's click delay was a natural
        # settle). Wait until the IMAGE is stable: consecutive detections must agree.
        self.settle_max_wait_s = 2.0
        self.motion_wait_s = 4.0        # motion-check arrival wait (async cell primitives)
        self.settle_interval_s = 0.15
        self.settle_drift_px = 0.8   # ringing is >1px; detection noise ~0.2–0.5px sits below
        # Engine-COMMANDED motion happens with the bridge lock held for the whole move, so the
        # edge's poll ticker is blind during it. The bridge reports each commanded waypoint via
        # this hook — the commanded path IS the traversed path, so the routine records the full
        # safe passage instead of a blind jump the replay guard would refuse.
        if self.recorder is not None:
            rec = self.recorder
            chain_names = list(getattr(bridge, "joint_names", []) or [])
            try:
                # commanded=True: an engine-commanded hop is a straight joint move by
                # construction, so the recorder may densify it at any excursion. The hop is
                # CHAIN-scoped; the recorder merges it BY NAME into the whole-body state (a
                # commissioned rig is one body — record all so replay can restore all).
                bridge.on_move = lambda q: rec.tick_named(chain_names, q, commanded=True)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001 — a hook-less bridge just relies on the stream
                pass

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
            step = float(target - q0[j])     # q0[j] is np.float64 → keep step a python float
        q1 = q0.copy()
        q1[j] += step
        map0 = self._joints_map()                         # full-rig snapshot (foreign-motion diag)
        self.bridge.move_to_joints(q1)
        # WAIT FOR ARRIVAL, never an instant read: a cell move primitive may return before the
        # motion completes (async controller) — judging from one immediate sample fails a move
        # that is still happening ("arm didn't move" while the operator watches it move).
        obs_q = self._wait_until_reached(q1, tol, timeout_s=self.motion_wait_s)
        obs = obs_q - q0
        cmd = q1 - q0
        max_err = float(np.max(np.abs(obs - cmd)))
        # THE crossed-binding diagnosis: our chain read no motion — did OTHER joints move? The
        # command routes by the authored arm LABEL (primitives.py), the measurement by the
        # authored group's chain (cell.yaml tip_links); if those two point at different arms,
        # the moved joints show up here BY NAME instead of a dead-end "didn't move".
        moved_elsewhere = {}
        if max_err >= tol and map0:
            map1 = self._joints_map()
            ours = set(getattr(self.bridge, "joint_names", []) or [])
            moved_elsewhere = {n: round(float(map1[n] - map0[n]), 4)
                               for n in map1 if n in map0 and n not in ours
                               and abs(float(map1[n] - map0[n])) > tol}
        self.bridge.move_to_joints(q0)                    # return to start
        estop = bool(self.bridge.estop_ok())
        # need a real commanded motion (a clamped-to-zero nudge can't prove anything)
        self.motion_ok = bool((max_err < tol) and estop and (abs(step) > 1e-4))
        out = {"type": "motion_check", "ok": self.motion_ok, "max_err_rad": round(max_err, 5),
               "joint": j, "estop_ok": estop, "commanded": _T(cmd), "observed": _T(obs)}
        if moved_elsewhere:
            out["moved_elsewhere"] = moved_elsewhere
            out["error"] = ("this step's chain read NO motion but these joints moved: "
                            + ", ".join(f"{n} ({d:+.3f} rad)" for n, d in moved_elsewhere.items())
                            + " — the step's arm label and its chain point at DIFFERENT arms "
                              "(cell.yaml groups tip_links vs primitives.py arm routing); fix "
                              "the authored mapping, don't retry")
        return out

    def _joints_map(self) -> dict:
        """The WHOLE rig's joints by name (not just this step's chain) — optional bridge hook;
        {} when the bridge can't provide it (off-robot fakes without it lose only the diag)."""
        fn = getattr(self.bridge, "read_joints_map", None)
        if not callable(fn):
            return {}
        try:
            return {str(k): float(v) for k, v in (fn() or {}).items()}
        except Exception:  # noqa: BLE001 — a diag helper must never break the check
            return {}

    def _wait_until_reached(self, target: np.ndarray, tol: float,
                            timeout_s: float = 4.0, interval_s: float = 0.1) -> np.ndarray:
        """Poll until the joints arrive at `target` (within tol) or timeout — zero extra
        latency for a synchronous primitive (the first read is already there)."""
        import time as _t
        t0 = _t.time()
        cur = self.bridge.read_joints()
        while float(np.max(np.abs(cur - target))) >= tol and _t.time() - t0 < timeout_s:
            _t.sleep(interval_s)
            cur = self.bridge.read_joints()
        return cur

    # ── detect (board seen?) + bootstrap the board pose estimate ────────────
    def detect(self) -> dict:
        ids, uv = self.bridge.capture()
        seen = len(ids) >= self.min_corners
        # An eye-to-hand (arm-presented) step has no seed-confirm phase — recording starts the
        # moment the fixed camera sees the board (the operator begins hand-posing from here).
        if seen and self.recorder is not None and self.kind == "eye_to_hand":
            self.recorder.armed = True
        if seen and self.T_board_est is None and not self.static:
            s = CaptureSample(id=-1, joints=self.bridge.read_joints(),
                              fk_pose=self.bridge.read_pose(), corner_ids=ids, corners=uv)
            T_c_board = _estimate_target_pose(s, self.board.points, self.K)
            self.T_board_est = s.fk_pose @ self.X_est @ T_c_board
            # Center pose suggestions on the CONFIGURATION WHERE THE BOARD IS SEEN — the operator
            # aimed the arm here. Sampling around home (zeros, the default) swings the camera away
            # from the board and proposes large, irrelevant, often-unsafe motions.
            self.nominal_joints = np.asarray(s.joints, float)
        return {"type": "detect", "seen": seen, "n_corners": int(len(ids))}

    # ── VISUAL SEED → CONFIRM (the operator places + trusts the hand-eye seed) ──────
    def _pnp_candidates(self, sample: CaptureSample) -> List[np.ndarray]:
        """Board->camera candidates for ONE view. For a multi-point board PnP is unique; for a
        SINGLE marker (4 coplanar points) there is a 2-fold flip — so we LM-refine the canonical
        solution AND restarts from 180° flips about the board's in-plane axes, then dedupe by
        rotation distance. `_estimate_target_pose_robust` picks among these across poses."""
        base = _estimate_target_pose(sample, self.board.points, self.K)
        cands = [base]
        for axis in ([np.pi, 0.0, 0.0], [0.0, np.pi, 0.0]):
            seed = base.copy()
            seed[:3, :3] = base[:3, :3] @ rodrigues(axis)
            from .solve import refine_target_pose
            ref, _ = refine_target_pose(sample, self.board.points, self.K, seed)
            if ref[2, 3] > 0 and all(rotation_angle(ref[:3, :3].T @ c[:3, :3]) > np.radians(10)
                                     for c in cands):
                cands.append(ref)
        return cands

    def _estimate_target_pose_robust(self, samples: Sequence[CaptureSample]) -> np.ndarray:
        """Board->camera for samples[0], choosing the planar-PnP flip CONSISTENT across the
        given views (the board is one fixed object in the base, so every view's implied
        board-in-base must agree). With a single view it's the canonical multi-start PnP."""
        import itertools
        cand = [self._pnp_candidates(s) for s in samples]
        if len(samples) == 1:
            return cand[0][0]
        best, best_err = (0,) * len(samples), float("inf")
        for combo in itertools.product(*[range(len(c)) for c in cand]):
            bases = [s.fk_pose @ self.X_est @ cand[i][combo[i]] for i, s in enumerate(samples)]
            err = max(float(np.linalg.norm(bases[i][:3, 3] - bases[j][:3, 3]))
                      for i in range(len(bases)) for j in range(i + 1, len(bases)))
            if err < best_err:
                best_err, best = err, combo
        return cand[0][best[0]]

    def _fovx_deg(self) -> float:
        return round(float(np.degrees(2 * np.arctan(self.wh[0] / (2 * self.K[0, 0])))), 2)

    def seed(self) -> dict:
        """Place the hand-eye seed for VISUAL confirmation (the original idea: use the URDF +
        optical-centre nominal as X, locate the marker by PnP, render both in 3D, let the
        operator confirm it points at the marker). Detect the marker, set X_est = nominal_T,
        T_board_est = fk @ X_est @ (board->camera). Returns the camera optical pose, the board
        pose (base), the flange pose (base) + FOV so the Studio draws the frustum + marker."""
        if self.static:
            return {"type": "seed", "static": True, "seen": False}
        ids, uv = self.bridge.capture()
        if len(ids) < self.min_corners:
            return {"type": "seed", "seen": False, "n_corners": int(len(ids))}
        q = np.asarray(self.bridge.read_joints(), float)
        s = CaptureSample(id=-1, joints=q, fk_pose=self.bridge.read_pose(),
                          corner_ids=ids, corners=uv)
        self._seed_sample = s
        self.X_est = np.asarray(self.item.nominal_T, float)        # the URDF + optical seed
        self._last_Tc_board = self._estimate_target_pose_robust([s])
        self.T_board_est = s.fk_pose @ self.X_est @ self._last_Tc_board
        self.nominal_joints = q                                    # centre the IK seed here
        self.phase = "seed"
        return {"type": "seed", "seen": True, "n_corners": int(len(ids)),
                "X": _T(self.X_est), "T_board": _T(self.T_board_est),
                "T_flange": _T(s.fk_pose), "wh": list(self.wh), "fovx_deg": self._fovx_deg()}

    def seed_nudge(self, x_new) -> dict:
        """The operator dragged the seed camera frame in 3D → a new flange->optical X. The
        marker is rigidly tied to the camera by the fixed PnP, so it moves with the frame;
        the operator drags until the rendered marker overlaps the physical one."""
        self.X_est = np.asarray(x_new, float).reshape(4, 4)
        if self._last_Tc_board is not None and self._seed_sample is not None:
            self.T_board_est = self._seed_sample.fk_pose @ self.X_est @ self._last_Tc_board
        return {"type": "seed_nudge", "X": _T(self.X_est),
                "T_board": None if self.T_board_est is None else _T(self.T_board_est)}

    def seed_wiggle(self, deg: float = 3.0) -> dict:
        """The seed-confirm signal a clueless operator trusts: jog the least-effect joint a few
        degrees and compare the PREDICTED marker corners (under the seed X) to the DETECTED
        ones at the jogged pose. A good seed tracks (small drift); a wrong seed drifts. The two
        views also disambiguate the single-marker PnP flip (the board is fixed in base)."""
        if self.T_board_est is None or self._seed_sample is None:
            return {"type": "seed_wiggle", "ok": False, "reason": "seed first"}
        q0 = np.asarray(self.bridge.read_joints(), float)
        j = self.chain.least_effect_joint(q0)
        step = float(np.radians(deg))
        lim = self.chain.limits[j] if j < len(self.chain.limits) else None
        if lim is not None and q0[j] + step > lim[1] - 1e-3:
            step = -step
        q1 = q0.copy(); q1[j] += step
        self.bridge.move_to_joints(q1)
        ids1, uv1_det = self.bridge.capture()
        q1r = np.asarray(self.bridge.read_joints(), float)
        wig = CaptureSample(id=-2, joints=q1r, fk_pose=self.bridge.read_pose(),
                            corner_ids=ids1, corners=uv1_det)
        # correct the flip using both views, then re-place the board with the agreed PnP
        self._last_Tc_board = self._estimate_target_pose_robust([self._seed_sample, wig])
        self.T_board_est = self._seed_sample.fk_pose @ self.X_est @ self._last_Tc_board
        _, uv1_pred = self._predict_uv(q1r)
        self.bridge.move_to_joints(q0)                            # return to start
        drift = self._corner_drift(ids1, uv1_det, uv1_pred)
        return {"type": "seed_wiggle", "ok": True, "joint": int(j),
                "drift_px": None if drift is None else round(drift, 2),
                "T_board": _T(self.T_board_est)}

    def confirm_seed(self) -> dict:
        """The operator confirmed the seed visually → enter the collect phase (the look-at
        orbit refines X from here). The current X_est is frozen as the collection seed."""
        self.phase = "collect"
        if self.recorder is not None:          # start logging the trajectory for future replay
            self.recorder.armed = True
        return {"type": "confirm_seed", "phase": self.phase, "X": _T(self.X_est)}

    def _predict_uv(self, joints) -> Tuple[np.ndarray, np.ndarray]:
        """Predicted (ids, pixels) of the target's points under the CURRENT X_est/T_board_est
        at `joints` — the amber overlay the operator compares to the live detection. Mode-aware
        (eye_in_hand / eye_to_hand / static), mirroring the solved-result reprojection model."""
        ids = self.board.point_ids
        # Predict with the EXACT fitted model: the solved board scale + REFINED intrinsics (and the
        # per-joint FK correction below). Mismatching any of these against the solve shows up as a
        # constant overlay drift the operator would misread as a bad calibration.
        r = self.result
        scale = float(r.board_scale) if r is not None else 1.0
        K = r.K if (r is not None and getattr(r, "K", None) is not None) else self.K
        pb = self.board.points * scale
        if self.static:
            pc = transform_points(self.X_est, pb)
        else:
            q = np.asarray(joints, float)
            if r is not None and np.asarray(r.fk_offsets).size == self.chain.n:
                q = q + np.asarray(r.fk_offsets, float)
            T_fk = self.chain.fk(q)
            if self.kind == "eye_to_hand":
                pc = transform_points(inv_T(self.X_est), transform_points(T_fk @ self.T_board_est, pb))
            else:
                pc = transform_points(inv_T(T_fk @ self.X_est), transform_points(self.T_board_est, pb))
        return ids, project(K, pc)

    @staticmethod
    def _corner_drift(ids_det, uv_det, uv_pred_full) -> Optional[float]:
        """Mean pixel distance between detected and predicted corners on the SHARED ids
        (predicted is indexed by point id; detected carries its own ids)."""
        if len(ids_det) == 0:
            return None
        idx = np.asarray(ids_det, int)
        d = uv_pred_full[idx] - np.asarray(uv_det, float)
        return float(np.mean(np.linalg.norm(d, axis=1)))

    def predicted_overlay(self) -> dict:
        """The live amber overlay (G16): predict the marker corners under the current X at the
        LIVE joints, return them with the green detection + the px drift between them. Drives
        the live-cam overlay + the 3D marker during collect/verify."""
        if self.T_board_est is None:
            return {"type": "overlay", "ok": False, "reason": "seed first"}
        q = np.asarray(self.bridge.read_joints(), float)
        ids_p, uv_p = self._predict_uv(q)
        try:
            ids_d, uv_d = self.bridge.capture()
        except Exception:  # noqa: BLE001 — overlay is best-effort; no detection still shows predicted
            ids_d, uv_d = np.zeros(0, int), np.zeros((0, 2))
        drift = self._corner_drift(ids_d, uv_d, uv_p)
        return {"type": "overlay", "ok": True,
                "predicted": np.asarray(uv_p, float).round(1).tolist(),
                "predicted_ids": np.asarray(ids_p, int).tolist(),
                "detected": np.asarray(uv_d, float).round(1).tolist(),
                "detected_ids": np.asarray(ids_d, int).tolist(),
                "drift_px": None if drift is None else round(drift, 2),
                "T_board": _T(self.T_board_est), "X": _T(self.X_est)}

    def verify(self, n_poses: int = 4, px_thresh: float = 3.0) -> dict:
        """The visual accept check (alongside held-out): drive to a few orbit poses and confirm
        the predicted marker overlay TRACKS the real marker at each (median drift < px_thresh).
        A correct X tracks everywhere; an under-observed one drifts off as the arm moves. Returns
        the arm to start. `tracks` plus the observability gate are required to accept."""
        if self.static or self.T_board_est is None:
            return {"type": "verify", "ok": False, "reason": "arm calibration only"}
        q_home = np.asarray(self.bridge.read_joints(), float)
        drifts: List[float] = []
        for _ in range(n_poses):
            sp = self.suggest_pose()
            if not sp.get("feasible"):
                continue
            if not self.motion_ok:
                break
            self.move_to()
            ov = self.predicted_overlay()
            if ov.get("drift_px") is not None:
                drifts.append(float(ov["drift_px"]))
        self.bridge.move_to_joints(q_home)                       # return to where we started
        self.phase = "verify"
        if not drifts:
            return {"type": "verify", "ok": False, "reason": "no poses tracked",
                    "tracks": False, "n_poses": 0}
        med = float(np.median(drifts))
        return {"type": "verify", "ok": True, "tracks": bool(med < px_thresh),
                "drift_px": round(med, 2), "worst_pose_px": round(float(np.max(drifts)), 2),
                "px_thresh": px_thresh, "n_poses": len(drifts),
                **self._obs_json(self.result)}

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
        # Cartesian look-at orbit: seed the IK from the CURRENT joints (where the operator
        # aimed the arm), and rank by next-best-view information gain once a solve exists
        # (7.6) — collision/in-envelope filtered by cuRobo if wired (7.7).
        try:
            q_seed = np.asarray(self.bridge.read_joints(), float)
        except Exception:  # noqa: BLE001 — fall back to the centred viewing config
            q_seed = self.nominal_joints
        weak = weak_rotation_axis(self.result) if self.result is not None else None
        q, score, diag = suggest_next_pose(
            self.chain, self.X_est, self.T_board_est, self.board, self.K, self.wh,
            [s.joints for s in self.samples], rng=self.rng, nominal_joints=self.nominal_joints,
            q_seed=q_seed, feasible=self._feasible, weak_axis=weak, result=self.result,
        )
        self._pending = q
        return {"type": "suggest_pose", "feasible": q is not None,
                "joints": _T(q) if q is not None else None,
                "ghost_pose": _T(self.chain.fk(q) @ self.X_est) if q is not None else None,
                "diversity_gain_deg": round(float(np.degrees(score)), 2),
                "posegen": diag}

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
            if self.recorder is not None:      # tie this capture to the recorded trajectory
                self.recorder.tick_named(list(getattr(self.bridge, "joint_names", []) or []),
                                         joints, mark=True)
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

    def _note_observability(self, result) -> bool:
        """Compute the observability accept gate for `result` and stash it on its metrics so
        solve/observe/verify/accept all read the same numbers. Returns whether X is well
        observed (every DOF pinned below the σ limits)."""
        try:
            ok, det = well_observed(result, rot_sigma_deg=self.accept_rot_sigma_deg,
                                    trans_sigma_mm=self.accept_trans_sigma_mm)
        except Exception:  # noqa: BLE001 — a near-degenerate covariance must not crash the solve
            ok, det = False, {"observable": False, "worst_rot_sigma_deg": None,
                              "worst_trans_sigma_mm": None}
        result.metrics.update(det)
        return ok

    @staticmethod
    def _obs_json(result) -> dict:
        m = getattr(result, "metrics", {}) or {}
        return {"observable": bool(m.get("observable", False)),
                "worst_rot_sigma_deg": m.get("worst_rot_sigma_deg"),
                "worst_trans_sigma_mm": m.get("worst_trans_sigma_mm")}

    def _static_obs_json(self, result) -> dict:
        """Static-camera feedback: the per-DOF camera-pose 1σ (same shape eye-in-hand emits, so the
        Studio meter reads it unchanged), the DEPTH CAVEAT (the optical-axis translation σ dominates —
        a near-frontal / single placement), and the image-coverage map (where to place the board next)."""
        try:
            o = observability(result)
            obs = {k: round(float(v), 4) for k, v in o.items()}
            tz = float(o["tz_mm"]); txy = max(float(o["tx_mm"]), float(o["ty_mm"]))
            depth_caveat = bool(len(self.samples) < 2 or (tz > 2.0 and tz > 2.0 * max(txy, 1e-6)))
            observable = bool(tz < self.accept_static_depth_mm and len(self.samples) >= 2)
        except Exception:  # noqa: BLE001 — a near-degenerate covariance must not crash the solve
            obs, depth_caveat, observable = None, True, False
        return {"observability": obs, "depth_caveat": depth_caveat, "observable": observable,
                "coverage": _image_coverage(self.samples, self.wh)}

    def _intrinsics_diag(self) -> dict:
        """Loud target/intrinsics diagnostics for the operator: the board-SCALE fit (far from 1.0
        ⇒ the printed marker size or the camera intrinsics are wrong), whether K was REFINED and by
        how much, and whether the target is a flip-ambiguous SINGLE marker (discouraged). These are
        the signals that say 'it's the target/K, not the pose count'."""
        r = self.result
        single = int(getattr(self.board, "n", 0)) <= 4
        out = {"board_points": int(getattr(self.board, "n", 0)), "single_marker": bool(single),
               "intrinsics_refined": False, "focal_change_pct": None, "principal_shift_px": None}
        if r is not None and getattr(r, "K", None) is not None and not np.allclose(r.K, self.K):
            out["intrinsics_refined"] = True
            out["focal_change_pct"] = round(float((r.K[0, 0] / self.K[0, 0] - 1.0) * 100.0), 2)
            out["principal_shift_px"] = round(float(np.hypot(r.K[0, 2] - self.K[0, 2],
                                                             r.K[1, 2] - self.K[1, 2])), 1)
        return out

    # ── solve (reprojection bundle, or static PnP) ──────────────────────────
    def solve(self) -> dict:
        if self.static:
            # world-fixed camera: robust single-pose PnP over the fixed-board views. No
            # kinematics, no FK correction, no stereo (one lens localizes the board).
            self.result = solve_static_camera(self.samples, self.board.points, self.K)
            self.X_est = self.result.T_optical
            return {"type": "solve", "train_rms_px": round(self.result.residual_px, 3),
                    "scale": 1.0, "X": _T(self.result.T_optical), "board": _T(np.eye(4)),
                    "samples": self.result.samples_used, "kind": "static", "t_lr_err_deg": None,
                    **self._static_obs_json(self.result)}
        # Over-parametrisation guard (10.7): the per-joint FK correction + board scale are only
        # identifiable with enough rotation-diverse poses. Fitting them on a thin set overfits
        # (absorbs noise into FK), so gate them on the sample count — a few poses solve X alone.
        # INTRINSICS (K) are refined jointly only for a MULTI-POINT board (>=8 points, not a
        # single 4-corner marker, where K is degenerate with scale/distance) over enough views —
        # this is the v3 fix for a wrong factory K / unrectified image (the 9%-scale failure).
        self.result = self._gated_solve()
        self.X_est = self.result.T_optical
        self.T_board_est = self.result.T_board
        self._note_observability(self.result)
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
                "t_lr_err_deg": None if t_lr_err is None else round(float(t_lr_err), 3),
                **self._obs_json(self.result), **self._intrinsics_diag()}

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
            out = {"type": "validate", "heldout_px": round(float(heldout_px), 3), "tip_mm": None,
                   "rot_worst_deg": 0.0, "t_lr_err_deg": None,
                   "consensus_deg": 0.0, "n_heldout": len(test), "pass": bool(passed)}
            # Surface the per-DOF camera-pose σ (+ depth caveat + coverage) so the static flow has the
            # SAME observability meter as eye-in-hand. Held-out px stays the accept gate.
            out.update(self._static_obs_json(self.result))
            return out
        if self.kind == "eye_to_hand":
            # FIXED camera, board PRESENTED by the arm (operator hand-moves it). Like the static
            # cam, we hold out a SPLIT of the collected views rather than driving the arm to new
            # poses (the look-at orbit assumes a MOVING camera, which this isn't). The eye-to-hand
            # bundle re-fits on the train split; held-out reprojection on the rest is the honest gate.
            n = len(self.samples)
            k = max(1, min(n_heldout, n // 3))
            train, test = self.samples[: n - k], self.samples[n - k:]
            sub = solve_eye_in_hand(train, self.board.points, self.K, self.chain, mode="eye_to_hand") \
                if len(train) >= self.min_corners else self.result
            heldout_px = held_out_reprojection(sub, test, self.board.points, self.K, self.chain) \
                if test else float("nan")
            obs = observability(self.result)
            passed = heldout_px == heldout_px and heldout_px < self.accept_heldout_px
            return {"type": "validate", "heldout_px": round(float(heldout_px), 3), "tip_mm": None,
                    "rot_worst_deg": round(obs["rot_worst_deg"], 4),
                    "observability": {kk: round(float(v), 4) for kk, v in obs.items()},
                    "t_lr_err_deg": None, "consensus_deg": 0.0, "n_heldout": len(test),
                    "pass": bool(passed), **self._obs_json(self.result)}
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

    def _gated_solve(self, **kw):
        """THE eye-in-hand estimator policy — the over-parametrisation guard in ONE place:
        FK correction + board scale only with enough poses (thin sets overfit — absorbing
        noise into FK shrinks the covariance and lies to the convergence check), K refined
        only for a multi-point board over enough views. solve(), observe() AND every mid-loop
        meter (replay/active) go through here, so the refine loop can never converge on a σ
        the final displayed solve won't reproduce (the live 0.148-meter-vs-0.17-panel bug)."""
        n = len(self.samples)
        many_points = int(getattr(self.board, "n", 0)) >= 8
        return solve_eye_in_hand(self.samples, self.board.points, self.K, self.chain,
                                 mode=self.kind, estimate_fk=(n >= 10),
                                 estimate_scale=(n >= 10),
                                 estimate_intrinsics=(many_points and n >= 12), **kw)

    # ── observe: live observability DURING collection (6.2) — a cheap solve, no motion ──
    def observe(self) -> dict:
        """Solve on the running set so the operator sees the per-DOF observability meter
        WHILE collecting (which axes are still weak → what motion to add). Cheap, no arm
        motion; needs a few poses to be meaningful."""
        if self.static:
            # World-fixed camera: a pooled PnP gives the per-DOF camera-pose σ + the coverage map
            # live, so the operator sees the depth caveat clear as they add varied placements.
            if len(self.samples) < 2:
                return {"type": "observe", "ready": False, "collected": len(self.samples)}
            r = solve_static_camera(self.samples, self.board.points, self.K)
            self.result = r
            self.X_est = r.T_optical
            return {"type": "observe", "ready": True, "collected": len(self.samples),
                    "train_rms_px": round(r.residual_px, 3), **self._static_obs_json(r)}
        if len(self.samples) < max(6, self.min_corners):
            return {"type": "observe", "ready": False, "collected": len(self.samples)}
        r = self._gated_solve()
        self.result = r                       # so the next suggest_pose can target the weak axis
        self.X_est = r.T_optical
        self.T_board_est = r.T_board
        ok, det = well_observed(r, rot_sigma_deg=self.accept_rot_sigma_deg,
                                trans_sigma_mm=self.accept_trans_sigma_mm)
        r.metrics.update(det)                 # one covariance computation feeds gate + meter
        meter = ("Rx_deg", "Ry_deg", "Rz_deg", "tx_mm", "ty_mm", "tz_mm", "rot_worst_deg")
        return {"type": "observe", "ready": True, "collected": len(self.samples),
                "train_rms_px": round(r.residual_px, 3),
                "observability": {k: round(float(det[k]), 4) for k in meter if k in det},
                **self._obs_json(r)}

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
        self.result = self._gated_solve()
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
        self.result = self._gated_solve(X_init=Xn)
        self.X_est = self.result.T_optical
        self.T_board_est = self.result.T_board
        heldout = held_out_reprojection(self.result, self.heldout, self.board.points, self.K, self.chain) \
            if self.heldout else None
        return {"type": "nudge", "train_rms_px": round(self.result.residual_px, 3),
                "X": _T(self.result.T_optical),
                "heldout_px": None if heldout is None else round(float(heldout), 3)}

    # ── recorded ROUTINE: save on accept, replay for automatic recalibration ─
    def _routine_path(self, calib_dir: str) -> str:
        name = self.item.id or self.item.camera_link
        safe = name.replace("/", "_").replace("[", "_").replace("]", "_").replace("|", "_")
        return str(Path(calib_dir) / "routines" / f"{safe}.json")

    def routine_info(self, calib_dir: str) -> dict:
        """Does a saved routine exist for THIS step (→ the Auto-recalibrate card), and is the
        recorder currently logging (→ the '● recording' chip)?"""
        out = {"type": "routine_info", "exists": False,
               "recording": bool(self.recorder is not None and self.recorder.armed),
               "recorded_waypoints": 0 if self.recorder is None else self.recorder.n_waypoints,
               "recorded_captures": 0 if self.recorder is None else len(self.recorder.capture_marks)}
        p = self._routine_path(calib_dir)
        if Path(p).exists():
            try:
                r = Routine.load(p)
                out.update({"exists": True, "waypoints": int(np.asarray(r.waypoints).shape[0]),
                            "captures": len(r.capture_marks), "created_at": r.created_at})
            except Exception as e:  # noqa: BLE001 — a corrupt file reads as absent, loudly
                out.update({"exists": False, "error": f"routine unreadable: {e}"})
        return out

    def routine_start_pose(self, calib_dir: str) -> dict:
        """The saved routine's START configuration + identity — what the edge's drive-to-start
        (the commission-proven motion stack) needs: the joint vector, its NAMES (the group is
        resolved from the joint SET, never the arm label), and how far the live arm is from it.
        Pure read; no motion, no motion_engine import (the edge owns that fusion)."""
        p = self._routine_path(calib_dir)
        if not Path(p).exists():
            return {"type": "routine_start_pose", "ok": False, "error": "no recorded routine for this step"}
        r = Routine.load(p).densified()
        W = np.asarray(r.waypoints, float)
        if W.ndim != 2 or W.shape[0] < 1:
            return {"type": "routine_start_pose", "ok": False, "error": "routine has no trajectory"}
        out = {"type": "routine_start_pose", "ok": True, "joints": _T(W[0]),
               "joint_names": list(r.joint_names) or list(getattr(self.bridge, "joint_names", []) or []),
               "arm": r.arm or self.item.arm}
        try:
            chain = _chain_columns(r, getattr(self.bridge, "joint_names", []) or [], self.chain.n)
            if chain is not None:
                w0 = W[0][chain] if len(chain) else W[0]
                d = np.abs(np.asarray(self.bridge.read_joints(), float).reshape(-1) - w0)
                out["deltas_rad"] = d.round(4).tolist()
                out["at_start"] = bool(d.max() <= HOP_TOL_RAD)
        except Exception:  # noqa: BLE001 — proximity is advisory here; replay_start re-gates it
            pass
        return out

    def replay_start(self, calib_dir: str) -> dict:
        """Begin an automatic recalibration by REPLAYING the recorded routine — direct joint
        hops along the path the arm already physically traversed under supervision, NEVER a
        planner. Guards: motion-check passed, E-stop live, joints/schema/hop validation, and
        the arm must already be jogged NEAR the routine's first waypoint (we don't plan an
        approach). Resets the sample set — this is a fresh calibration."""
        if self.static:
            return {"type": "replay_start", "ok": False, "error": "arm-motion steps only"}
        if not self.motion_ok:
            return {"type": "replay_start", "ok": False, "error": "run the motion check first"}
        if not self.bridge.estop_ok():
            return {"type": "replay_start", "ok": False, "error": "E-stop not OK"}
        p = self._routine_path(calib_dir)
        if not Path(p).exists():
            return {"type": "replay_start", "ok": False, "error": "no recorded routine for this step"}
        # HEAL on load: bridge bounded-excursion recording gaps by joint interpolation (real
        # rigs sample joints far below the arm's rate when the stream is degraded), THEN apply
        # the guards. Only an excursion beyond the safe-interpolation bound is refused.
        r = Routine.load(p).densified()
        cols = _chain_columns(r, getattr(self.bridge, "joint_names", []) or [], self.chain.n)
        if cols is None:
            return {"type": "replay_start", "ok": False,
                    "error": "routine joints don't cover this step's chain — the rig changed; re-record"}
        if len(cols):
            # whole-body recording → the tape drives THIS chain's columns; the rest of the body
            # was restored to its recorded start by drive-to-start (a commissioned rig is one body)
            r = Routine(step_id=r.step_id, camera=r.camera, arm=r.arm,
                        joint_names=[r.joint_names[i] for i in cols],
                        waypoints=np.asarray(r.waypoints, float)[:, cols], dt=r.dt,
                        capture_marks=list(r.capture_marks), target=r.target,
                        created_at=r.created_at, schema=r.schema)
        err = r.validate_against(getattr(self.bridge, "joint_names", []) or [], self.chain.n)
        if err:
            return {"type": "replay_start", "ok": False, "error": err}
        cursor = ReplayCursor(r)
        ok, deltas = cursor.start_check(self.bridge.read_joints())
        if not ok:
            return {"type": "replay_start", "ok": False,
                    "error": "arm is not at the routine's start pose — jog it there first",
                    "start_joints": _T(np.asarray(r.waypoints)[0]), "deltas_rad": deltas}
        # fresh recalibration: clear the collection; don't re-record while replaying
        self.samples, self.heldout, self.result = [], [], None
        if self.recorder is not None:
            self.recorder.armed = False
        self._replay = cursor
        return {"type": "replay_start", "ok": True, "segments": len(cursor.segments),
                "captures": len(r.capture_marks)}

    def replay_step(self) -> dict:
        """Execute ONE segment: hop through its dense waypoints (move_direct when the bridge
        offers it — the planner-free path — else the plain joint move), then capture at the
        mark. Every 4th capture is held out so validate(recollect=False) scores it with NO new
        motion. Short per call (HTTP-friendly); the Studio loop drives it with a Stop button."""
        cur = self._replay
        if cur is None:
            return {"type": "replay_step", "error": "replay not started"}
        import time as _time
        _t0 = _time.time()
        seg = cur.next_segment()
        if seg is None:
            self._replay = None
            return {"type": "replay_step", "done": True, "collected": len(self.samples),
                    "heldout": len(self.heldout)}
        W, dts = seg
        move = getattr(self.bridge, "move_direct", None) or self.bridge.move_to_joints
        for q, dt in zip(W, dts):
            if not self.bridge.estop_ok():
                self._replay = None
                return {"type": "replay_step", "error": "E-stop tripped — replay aborted",
                        "segment": cur.i, "collected": len(self.samples)}
            move(q)
            if dt > 0:
                import time
                time.sleep(min(float(dt), 0.25))    # roughly the recorded pacing, capped
        _t_moved = _time.time()
        settle = self._settle_for_capture()          # the arm rings after motion — capture still
        held_out = (cur.i - 1) % 4 == 3             # every 4th capture → the held-out set
        cap = self.capture(held_out=held_out)
        print(f"[replay] segment {cur.i}/{len(cur.segments)} · {len(W)} hops moved in "
              f"{_t_moved - _t0:.2f}s · capture {'ok' if cap.get('accepted') else 'MISSED'} in "
              f"{_time.time() - _t_moved:.2f}s", flush=True)
        out = {"type": "replay_step", "segment": cur.i, "segments": len(cur.segments),
               "accepted": bool(cap.get("accepted")), "held_out": held_out, "settle": settle,
               "collected": len(self.samples), "heldout": len(self.heldout),
               "done": cur.done}
        if cur.done:
            self._replay = None
        # live meter: a cheap running solve once enough poses (same payload observe() gives)
        if len(self.samples) >= max(6, self.min_corners):
            try:
                r = self._gated_solve()
                self.result, self.X_est, self.T_board_est = r, r.T_optical, r.T_board
                self._note_observability(r)
                out["observability"] = {k: round(float(v), 4) for k, v in observability(r).items()}
            except Exception:  # noqa: BLE001 — the meter is best-effort mid-replay
                pass
        return out

    # ── PLANNED replay (post-commission): the routine's CAPTURE MARKS are the pose set; the
    #    EDGE drives each with the commissioned stack (one smooth streamed trajectory per pose)
    #    instead of walking the dense tape (hundreds of ramped micro-hops — shaky and slow).
    #    The tape verbs above remain the planner-free fallback. The engine never moves here:
    #    it only serves goals and captures — motion is the edge's fusion (like drive-to-start).
    def replay_start_planned(self, calib_dir: str) -> dict:
        if self.static:
            return {"type": "replay_start_planned", "ok": False, "error": "arm-motion steps only"}
        if not self.bridge.estop_ok():
            return {"type": "replay_start_planned", "ok": False, "error": "E-stop not OK"}
        p = self._routine_path(calib_dir)
        if not Path(p).exists():
            return {"type": "replay_start_planned", "ok": False, "error": "no recorded routine for this step"}
        r = Routine.load(p)                      # marks only — no tape densify needed
        if not r.joint_names:
            return {"type": "replay_start_planned", "ok": False,
                    "error": "routine carries no joint names (older chain-only recording) — planned "
                             "replay plans the WHOLE body; use tape replay or re-record once"}
        W = np.asarray(r.waypoints, float)
        marks = [i for i in sorted(set(int(m) for m in r.capture_marks)) if 0 <= i < W.shape[0]]
        if len(marks) < 2:
            return {"type": "replay_start_planned", "ok": False, "error": "routine has fewer than 2 capture marks — re-record"}
        # fresh recalibration: clear the collection; don't re-record while replaying
        self.samples, self.heldout, self.result = [], [], None
        if self.recorder is not None:
            self.recorder.armed = False
        self._marks = {"rows": [W[m] for m in marks], "names": list(r.joint_names),
                       "idx": 0, "awaiting": False, "skipped": []}
        return {"type": "replay_start_planned", "ok": True, "marks": len(marks),
                "joint_names": list(r.joint_names)}

    def replay_next_goal(self) -> dict:
        """The next capture mark's WHOLE-BODY goal (the edge plans+drives it), or done."""
        st = self._marks
        if st is None:
            return {"type": "replay_next_goal", "error": "planned replay not started"}
        if st["idx"] >= len(st["rows"]):
            self._marks = None
            return {"type": "replay_next_goal", "done": True,
                    "collected": len(self.samples), "heldout": len(self.heldout),
                    "skipped_marks": len(st.get("skipped") or [])}
        st["awaiting"] = True
        return {"type": "replay_next_goal", "done": False,
                "mark": st["idx"] + 1, "marks": len(st["rows"]),
                "held_out": st["idx"] % 4 == 3,
                "joints": _T(st["rows"][st["idx"]]), "joint_names": list(st["names"])}

    def _settle_for_capture(self) -> dict:
        """Wait until the IMAGE is stable before an AUTOMATIC capture: detect twice per interval
        and require the common corners to drift < settle_drift_px. Gives up (capture proceeds,
        flagged UNSETTLED) after settle_max_wait_s — a noisy scene must not hang the run.
        Manual captures don't need this: the human's click delay is the settle."""
        import time as _t
        t0 = _t.time()
        try:
            ids0, uv0 = self.bridge.capture()
        except Exception:  # noqa: BLE001 — no detection yet; capture() will judge for real
            return {"settled": False, "waited_s": 0.0, "drift_px": None}
        last = {int(i): np.asarray(u, float) for i, u in zip(ids0, uv0)}
        drift = None
        while True:
            _t.sleep(self.settle_interval_s)
            try:
                ids1, uv1 = self.bridge.capture()
            except Exception:  # noqa: BLE001
                ids1, uv1 = np.zeros(0, int), np.zeros((0, 2))
            cur = {int(i): np.asarray(u, float) for i, u in zip(ids1, uv1)}
            common = set(last) & set(cur)
            if len(common) >= max(4, self.min_corners // 2):
                drift = float(np.mean([np.linalg.norm(cur[i] - last[i]) for i in common]))
                if drift < self.settle_drift_px:
                    return {"settled": True, "waited_s": round(_t.time() - t0, 2),
                            "drift_px": round(drift, 2)}
            if _t.time() - t0 > self.settle_max_wait_s:
                return {"settled": False, "waited_s": round(_t.time() - t0, 2),
                        "drift_px": None if drift is None else round(drift, 2)}
            last = cur

    def replay_skip_mark(self, reason: str = "") -> dict:
        """A mark whose goal the CURRENT world refuses (a new obstacle sits on a recorded pose)
        is SKIPPED, not fatal: log it, advance, calibrate from the marks that remain. The run
        only dies for execution failures (the arm was moving) — the edge decides which is which
        via MoveResult.executed. Every-4th held-out keeps using the mark INDEX, so the split
        stays deterministic whatever gets skipped."""
        st = self._marks
        if st is None or not st.get("awaiting"):
            return {"type": "replay_skip_mark", "error": "no pending mark — call replay_next_goal first"}
        st["awaiting"] = False
        st.setdefault("skipped", []).append({"mark": st["idx"] + 1, "reason": str(reason)[:200]})
        st["idx"] += 1
        return {"type": "replay_skip_mark", "mark": st["idx"], "marks": len(st["rows"]),
                "skipped_marks": len(st["skipped"]), "collected": len(self.samples),
                "heldout": len(self.heldout), "done": st["idx"] >= len(st["rows"])}

    def replay_capture_mark(self) -> dict:
        """Capture at the mark the edge just drove to — after the SETTLE gate (the arm rings
        when a streamed trajectory stops; capture only once the image is still). Every 4th
        capture held out (the no-motion validate split) + the live observability payload."""
        st = self._marks
        if st is None or not st.get("awaiting"):
            return {"type": "replay_capture_mark", "error": "no pending mark — call replay_next_goal first"}
        settle = self._settle_for_capture()
        held_out = st["idx"] % 4 == 3
        cap = self.capture(held_out=held_out)
        st["awaiting"] = False
        st["idx"] += 1
        done = st["idx"] >= len(st["rows"])
        out = {"type": "replay_capture_mark", "mark": st["idx"], "marks": len(st["rows"]),
               "accepted": bool(cap.get("accepted")), "held_out": held_out, "settle": settle,
               "collected": len(self.samples), "heldout": len(self.heldout), "done": done,
               "skipped_marks": len(st.get("skipped") or [])}
        # _marks is cleared by replay_next_goal's done (both exit styles stay valid)
        if len(self.samples) >= max(6, self.min_corners):
            try:
                r = self._gated_solve()
                self.result, self.X_est, self.T_board_est = r, r.T_optical, r.T_board
                self._note_observability(r)
                out["observability"] = {k: round(float(v), 4) for k, v in observability(r).items()}
            except Exception:  # noqa: BLE001 — the meter is best-effort mid-replay
                pass
        return out

    def replay_abort(self) -> dict:
        """Stop in place (no retreat motion) and forget the cursor (tape, planned AND active)."""
        self._replay = None
        self._marks = None
        self._active = None
        return {"type": "replay_abort", "ok": True, "collected": len(self.samples)}

    # ── ACTIVE calibration (post-commission): close the loop — beyond the first N poses,
    #    KEEP SAMPLING next-best-view poses (posegen's predicted-σ ranking, board-size-aware
    #    framing) until the covariance CONVERGES. Same engine/edge boundary as planned replay:
    #    the session serves goals + captures, the EDGE plans+drives each with the commissioned
    #    stack. Stops on: every DOF of X under the accept σ targets, a σ plateau (more poses
    #    stopped helping), or the pose budget.
    def active_start(self, max_poses: int = 24) -> dict:
        if self.static:
            return {"type": "active_start", "ok": False, "error": "arm-motion steps only"}
        if not self.bridge.estop_ok():
            return {"type": "active_start", "ok": False, "error": "E-stop not OK"}
        if len(self.samples) < max(6, self.min_corners):
            return {"type": "active_start", "ok": False,
                    "error": "not enough poses collected yet — active refinement EXTENDS an "
                             "existing set (collect or replay first)"}
        names = list(getattr(self.bridge, "joint_names", []) or [])
        if not names:
            return {"type": "active_start", "ok": False,
                    "error": "bridge exposes no joint names — planned motion needs named joints"}
        try:  # a fresh solve so the NBV ranks against the CURRENT covariance
            r = self._gated_solve()
            self.result, self.X_est, self.T_board_est = r, r.T_optical, r.T_board
            self._note_observability(r)                      # accept-gate meter (unchanged)
            converged = self._refine_converged(r)            # the loop chases REFINE targets
        except Exception as e:  # noqa: BLE001
            return {"type": "active_start", "ok": False, "error": f"solve failed: {e}"}
        worst = r.metrics.get("worst_trans_sigma_mm")
        self._active = {"added": 0, "max": int(max_poses), "awaiting": False,
                        "names": names, "sigma_hist": [float(worst) if worst is not None else float("inf")]}
        return {"type": "active_start", "ok": True, "converged": bool(converged),
                "max_poses": int(max_poses),
                "targets": {"rot_sigma_deg": self.refine_rot_sigma_deg,
                            "trans_sigma_mm": self.refine_trans_sigma_mm},
                "observability": {k: round(float(v), 4) for k, v in observability(r).items()}}

    def _refine_converged(self, result) -> bool:
        """Every DOF of X under the REFINE σ targets (tighter than the accept gate — see
        __init__). A degenerate covariance reads as not-converged, never as a crash."""
        if result is None:
            return False
        try:
            ok, _ = well_observed(result, rot_sigma_deg=self.refine_rot_sigma_deg,
                                  trans_sigma_mm=self.refine_trans_sigma_mm)
            return bool(ok)
        except Exception:  # noqa: BLE001
            return False

    def active_next(self) -> dict:
        """Converged/budget/plateau check, then the next NBV goal for the edge — or done."""
        st = self._active
        if st is None:
            return {"type": "active_next", "error": "active collection not started"}
        st["awaiting"] = False
        done_reason = None
        if self._refine_converged(self.result):
            done_reason = "converged"                     # every DOF of X under the REFINE σ targets
        elif st["added"] >= st["max"]:
            done_reason = "budget"
        else:
            h = st["sigma_hist"]                          # more poses stopped helping (<2% over 3)
            if len(h) >= 4 and h[-4] - h[-1] < 0.02 * h[-4]:
                done_reason = "plateau"
        if done_reason is not None:
            self._active = None
            return {"type": "active_next", "done": True, "reason": done_reason,
                    "converged": done_reason == "converged", "added": st["added"],
                    "collected": len(self.samples), "heldout": len(self.heldout)}
        sp = self.suggest_pose()                          # NBV: predicted-σ ranked, size-aware
        if not sp.get("feasible"):
            self._active = None
            return {"type": "active_next", "done": True, "reason": "no reachable informative pose",
                    "converged": False, "added": st["added"],
                    "collected": len(self.samples), "heldout": len(self.heldout),
                    "posegen": sp.get("posegen")}
        st["awaiting"] = True
        return {"type": "active_next", "done": False, "added": st["added"], "max": st["max"],
                "joints": sp["joints"], "joint_names": list(st["names"]),
                "held_out": st["added"] % 4 == 3,
                "gain_deg": sp.get("diversity_gain_deg")}

    def active_goto_failed(self, reason: str = "") -> dict:
        """The edge couldn't PLAN to the pending suggestion (a new obstacle boxes it in) — not
        fatal: drop it and let active_next pick a different candidate (the rng advances, the
        feasibility filter culls config-collisions; a path-blocked pose won't recur verbatim).
        Three REFUSALS IN A ROW (no capture between) means the workspace is too constrained to
        keep trying — end the loop honestly instead of thrashing. Execution failures never come
        here: the edge stops the run for those (the arm was moving)."""
        st = self._active
        if st is None or not st.get("awaiting"):
            return {"type": "active_goto_failed", "error": "no pending pose — call active_next first"}
        st["awaiting"] = False
        st["goto_fails"] = int(st.get("goto_fails", 0)) + 1
        if st["goto_fails"] >= 3:
            self._active = None
            return {"type": "active_goto_failed", "done": True, "reason": "blocked",
                    "converged": False, "added": st["added"],
                    "collected": len(self.samples), "heldout": len(self.heldout),
                    "detail": f"3 consecutive suggestions unplannable — last: {str(reason)[:200]}"}
        return {"type": "active_goto_failed", "done": False, "retry": True,
                "fails": st["goto_fails"], "added": st["added"], "max": st["max"]}

    def active_capture(self) -> dict:
        """Capture at the pose the edge just drove to (settle-gated), re-solve, and report the
        meter — the loop's observe step. Every 4th ADDED pose feeds the held-out set."""
        st = self._active
        if st is None or not st.get("awaiting"):
            return {"type": "active_capture", "error": "no pending pose — call active_next first"}
        settle = self._settle_for_capture()
        held_out = st["added"] % 4 == 3
        cap = self.capture(held_out=held_out)
        st["awaiting"] = False
        st["goto_fails"] = 0                     # a reached pose ends any refusal streak
        if cap.get("accepted"):
            st["added"] += 1
        out = {"type": "active_capture", "accepted": bool(cap.get("accepted")),
               "held_out": held_out, "settle": settle, "added": st["added"], "max": st["max"],
               "collected": len(self.samples), "heldout": len(self.heldout)}
        try:
            r = self._gated_solve()
            self.result, self.X_est, self.T_board_est = r, r.T_optical, r.T_board
            self._note_observability(r)                  # accept-gate meter (unchanged)
            out["converged"] = self._refine_converged(r)
            worst = r.metrics.get("worst_trans_sigma_mm")
            if cap.get("accepted"):
                st["sigma_hist"].append(float(worst) if worst is not None else float("inf"))
            out["observability"] = {k: round(float(v), 4) for k, v in observability(r).items()}
        except Exception:  # noqa: BLE001 — the meter is best-effort mid-loop
            pass
        return out

    # ── accept: write body->optical back to the URDF + the calibration json ──
    def accept(self, urdf_path: str, out_path: Optional[str] = None, provenance: str = "measured",
               calib_dir: Optional[str] = None, force: bool = False) -> dict:
        assert self.result is not None
        # OBSERVABILITY ACCEPT GATE: an arm-driven hand-eye is refused unless every DOF of X is
        # pinned (the fix for silently accepting 2x-translation garbage). A static camera has no
        # hand-eye rotation coupling — its gate is held-out reprojection alone. `force` is the
        # operator OVERRIDE (the "Save calibration" action): write it anyway and report the
        # observability as a WARNING instead of refusing — saving a result is the operator's call,
        # NOT the same as finalizing the gate.
        warning = None
        if not self.static:
            observable = self.result.metrics.get("observable")
            if observable is None:
                observable = self._note_observability(self.result)
            if not observable:
                if not force:
                    return {"type": "accept", "ok": False, "camera": self.item.camera_link,
                            "error": "calibration is not observable — collect more rotation-diverse "
                                     "poses (translation under-constrained)", **self._obs_json(self.result)}
                warning = ("saved while UNDER-OBSERVED — the translation is under-constrained; "
                           "collect more rotation-diverse poses before relying on it")
        else:
            # A world-fixed camera's weak DOF is DEPTH (the optical-axis translation): a near-frontal
            # or single placement leaves it under-constrained. Refuse past the depth-σ limit (unless
            # forced), the static analog of the rotation-diversity gate above.
            sj = self._static_obs_json(self.result)
            if not sj.get("observable"):
                if not force:
                    return {"type": "accept", "ok": False, "camera": self.item.camera_link,
                            "error": "static camera pose is under-constrained along the optical axis "
                                     "(depth) — capture the board at more varied DISTANCES and TILTS",
                            "observability": sj.get("observability"), "depth_caveat": sj.get("depth_caveat")}
                warning = ("saved while DEPTH-UNDER-CONSTRAINED — the camera's distance is uncertain; "
                           "add board placements at more varied distances/tilts before relying on it")
        body_optical = inv_T(self.item.nominal_flange_body) @ self.result.T_optical
        dst = urdf_io.write_calibrated_optical(urdf_path, self.item.camera_link, body_optical,
                                               out_path=out_path, provenance=provenance)
        # Persist the CalibResult json the seed promises (so the agent + base_to_base can read
        # each camera's X without re-running the solve), PLUS the operator-readable hand_eye.yaml
        # (accumulating per camera) + report.md — the named artifacts the cell + gate-check expect.
        json_path = None
        if calib_dir is not None:
            json_path = _write_calib_json(calib_dir, self.item.camera_link, self.result, provenance)
            _write_hand_eye_summary(calib_dir, self.item.camera_link, self.item.flange_link,
                                    self.kind, self.result.T_optical, provenance, self.result)
            # An ACCEPTED calibration's recorded trajectory is worth keeping: save it as the
            # step's ROUTINE so recalibration can replay it automatically (latest accepted wins).
            # A replay run records nothing (recorder disarmed) → the original routine survives.
            if self.recorder is not None and len(self.recorder.capture_marks) >= 2:
                try:
                    self.recorder.to_routine(
                        step_id=self.item.id or self.item.camera_link, camera=self.item.camera_link,
                        arm=self.item.arm, target={"type": getattr(self.board, "type", "")},
                    ).save(self._routine_path(calib_dir))
                except Exception:  # noqa: BLE001 — never block the accept on the routine write
                    pass
        out = {"type": "accept", "ok": True, "urdf": dst, "camera": self.item.camera_link,
               "optical_frame": self.item.optical_frame, "provenance": provenance,
               "calib_json": json_path, "body_to_optical": _T(body_optical),
               "X": _T(self.result.T_optical), "flange": self.item.flange_link,
               **self._obs_json(self.result)}
        if warning:
            out["warning"] = warning
        return out


def safe_camera_name(camera: str) -> str:
    """The persisted-calibration filename for a camera link — THE one sanitize rule, shared by
    the writer, the cross-session loader and the saved-flag stamping (a drifted copy = a
    silently-invisible saved calibration)."""
    return camera.replace("/", "_").replace("[", "_").replace("]", "_").replace("|", "_")


def _write_calib_json(calib_dir: str, camera: str, result: CalibResult, provenance: str) -> str:
    import json
    Path(calib_dir).mkdir(parents=True, exist_ok=True)
    dst = str(Path(calib_dir) / f"{safe_camera_name(camera)}.json")
    Path(dst).write_text(json.dumps({
        "camera": camera, "kind": result.kind, "provenance": provenance,
        "T_optical": _T(result.T_optical), "T_board": _T(result.T_board),
        "fk_offsets": [round(float(v), 8) for v in result.fk_offsets],
        "board_scale": round(float(result.board_scale), 6),
        "residual_px": round(float(result.residual_px), 4),
        # the REFINED camera intrinsics (when K was estimated) — downstream (depth, reprojection)
        # must use these, not the stale factory K that produced the bad scale.
        "K": _T(result.K) if getattr(result, "K", None) is not None else None,
        "t_lr_err_deg": result.t_lr_err_deg, "samples_used": result.samples_used,
    }, indent=2), encoding="utf-8")
    return dst


def _write_hand_eye_summary(calib_dir: str, camera: str, flange: str, kind: str,
                            X: np.ndarray, provenance: str, result: CalibResult) -> None:
    """Update `calibration/hand_eye.yaml` (per-camera, ACCUMULATING across steps) + regenerate the
    human-readable `report.md`. These are the named artifacts the cell + the Studio gate-check read.
    Saving one camera here does NOT finalize the calibrate gate — other cameras may still follow."""
    import time
    try:
        import yaml  # type: ignore
    except Exception:  # noqa: BLE001
        yaml = None
    Path(calib_dir).mkdir(parents=True, exist_ok=True)
    t = np.asarray(X, float)[:3, 3]
    m = result.metrics or {}
    entry = {
        "flange": flange, "kind": kind, "provenance": provenance,
        "saved_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "T_cam_flange": _T(X),                       # camera optical centre in the flange frame (4x4)
        "translation_mm": [round(float(t[i]) * 1000.0, 3) for i in range(3)],
        "offset_mm": round(float(np.linalg.norm(t) * 1000.0), 3),
        "board_scale": round(float(result.board_scale), 6),
        "train_rms_px": round(float(result.residual_px), 4),
        "samples": int(result.samples_used),
        "observable": bool(m.get("observable", False)),
        "worst_rot_sigma_deg": m.get("worst_rot_sigma_deg"),
        "worst_trans_sigma_mm": m.get("worst_trans_sigma_mm"),
    }
    he = Path(calib_dir) / "hand_eye.yaml"
    data: dict = {}
    if yaml is not None and he.exists():
        try:
            data = yaml.safe_load(he.read_text(encoding="utf-8")) or {}
        except Exception:  # noqa: BLE001
            data = {}
    cams = data.get("cameras") if isinstance(data.get("cameras"), dict) else {}
    cams[camera] = entry
    if yaml is not None:
        he.write_text(yaml.safe_dump({"cameras": cams}, sort_keys=False), encoding="utf-8")
    # regenerate the markdown report from ALL saved cameras
    lines = ["# Calibration report", ""]
    for cam, e in cams.items():
        tr = e.get("translation_mm", [0, 0, 0])
        obs = ("observable ✓" if e.get("observable") else "UNDER-OBSERVED ✗")
        if e.get("worst_rot_sigma_deg") is not None:
            obs += f" (worst rot σ {e['worst_rot_sigma_deg']}°, worst trans σ {e['worst_trans_sigma_mm']} mm)"
        lines += [
            f"## {cam}  ({e.get('kind', '?')})",
            f"- camera optical centre vs flange `{e.get('flange', '?')}`: "
            f"[{tr[0]}, {tr[1]}, {tr[2]}] mm  (‖offset‖ {e.get('offset_mm', '?')} mm)",
            f"- board scale {e.get('board_scale', '?')} · train RMS {e.get('train_rms_px', '?')} px · "
            f"{e.get('samples', '?')} samples",
            f"- observability: {obs}",
            f"- provenance: {e.get('provenance', '?')} · saved {e.get('saved_at', '?')}",
            "",
        ]
    (Path(calib_dir) / "report.md").write_text("\n".join(lines), encoding="utf-8")


def _b2b_obs_json(result: CalibResult) -> dict:
    """The per-DOF 1σ of T_baseA_baseB (the SAME shape eye-in-hand emits, so the Studio's
    ObservabilityMeter + mapObs read it unchanged) + which DOF is least observed."""
    try:
        o = observability(result)
    except Exception:  # noqa: BLE001 — a near-degenerate covariance must not crash the solve
        return {"observability": None, "weak_rot_axis": None, "worst_trans_axis": None}
    trans = {"x": o["tx_mm"], "y": o["ty_mm"], "z": o["tz_mm"]}
    worst_trans = max(trans, key=trans.get)
    try:
        weak = np.asarray(weak_rotation_axis(result), float).round(3).tolist()
    except Exception:  # noqa: BLE001
        weak = None
    return {"observability": {k: round(float(v), 4) for k, v in o.items()},
            "weak_rot_axis": weak, "worst_trans_axis": worst_trans}


class BaseToBaseSession:
    """Dual-arm base-to-base (Pillar/variant 3). Both arms' wrist cams are already
    calibrated eye-in-hand (X_a, X_b); the operator places ONE board both cams can see, and
    for each capture both arms view it. `solve_base_to_base_bundle` recovers T_baseA_baseB
    AND its covariance from the shared sightings, so this flow has the SAME error feedback as
    eye-in-hand: an inter-arm-agreement (mm) headline, a per-DOF observability meter, held-out
    generalization, a fresh-placement verify, and an observability accept gate. Reuses the
    per-arm bridges."""

    def __init__(self, item: PlanItem, board: BoardModel, K: np.ndarray,
                 chain_a: Chain, chain_b: Chain, bridge_a, bridge_b,
                 X_a: np.ndarray, X_b: np.ndarray, *,
                 fk_a: Optional[np.ndarray] = None, fk_b: Optional[np.ndarray] = None,
                 accept_agreement_mm: float = 3.0,
                 accept_rot_sigma_deg: float = 0.5, accept_trans_sigma_mm: float = 2.0):
        self.item = item
        self.board = board
        self.K = np.asarray(K, float)
        self.chain_a, self.chain_b = chain_a, chain_b
        self.bridge_a, self.bridge_b = bridge_a, bridge_b
        self.X_a, self.X_b = np.asarray(X_a, float), np.asarray(X_b, float)
        # Each hand-eye X was solved JOINTLY with per-joint FK corrections (estimate_fk) — X
        # and its offsets are a matched pair. Evaluating X on the NOMINAL chain reintroduces
        # exactly the FK bias the bundle removed (a chunk of the live 35mm inter-arm error).
        # Zeros when the hand-eye solved without corrections or the DOF doesn't line up.
        def _fk(v, n):
            a = np.asarray(v, float).ravel() if v is not None else np.zeros(0)
            return a if a.size == n else np.zeros(n)
        self.fk_a, self.fk_b = _fk(fk_a, chain_a.n), _fk(fk_b, chain_b.n)
        # PER-CAMERA intrinsics (latent bug: both wrist cams shared one K) — the bridges carry
        # each camera's real K on the edge; the service default is only the off-robot fallback.
        ka, kb = getattr(bridge_a, "K", None), getattr(bridge_b, "K", None)
        self.K_a = np.asarray(ka, float) if ka is not None else self.K
        self.K_b = np.asarray(kb, float) if kb is not None else self.K
        self.obs: List[dict] = []
        self.last_report: Optional[dict] = None   # solve policy report (method/per-view/offsets)
        self.result: Optional[CalibResult] = None
        self.min_corners = int(board.min_points)
        self.accept_agreement_mm = float(accept_agreement_mm)
        self.accept_rot_sigma_deg = float(accept_rot_sigma_deg)
        self.accept_trans_sigma_mm = float(accept_trans_sigma_mm)

    def _capture_obs(self) -> Optional[dict]:
        """Both arms view the shared board once → (joints, camera->marker) per arm, or None if
        either camera misses it. Shared by capture / verify."""
        import time as _t
        _t0 = _t.time()
        ida, uva = self.bridge_a.capture()
        idb, uvb = self.bridge_b.capture()
        _t_grab = _t.time()
        if len(ida) < self.min_corners or len(idb) < self.min_corners:
            print(f"[b2b] capture REJECTED: grabs+detect {_t_grab - _t0:.2f}s "
                  f"· corners {len(ida)}/{len(idb)} (min {self.min_corners})", flush=True)
            return None
        qa, qb = self.bridge_a.read_joints(), self.bridge_b.read_joints()
        from .solve import _planar_pose_candidates
        P = self.board.points
        _t_det = _t.time()

        def _side_from(cands):
            """Best-branch pose + the P2 ambiguity signal: the view is AMBIGUITY-PRONE when
            the OTHER flip branch explains the pixels nearly as well (the formal
            weak-perspective condition — small/distant/near-fronto-parallel board) or the
            board is near-fronto-parallel. Frozen commitment to such a view is what produced
            the live 35mm agreement failure; the capture WARNS instead of silently storing."""
            T = cands[0][0]
            ambiguous = bool(len(cands) == 2 and np.isfinite(cands[1][1])
                             and cands[1][1] < 3.0 * max(cands[0][1], 1e-6))
            view = T[:3, 3] / (np.linalg.norm(T[:3, 3]) + 1e-12)
            tilt = float(np.degrees(np.arccos(np.clip(abs(float(T[:3, 2] @ view)), 0.0, 1.0))))
            return T, ambiguous, tilt

        ca = _planar_pose_candidates(P[np.asarray(ida, int)], np.asarray(uva, float), self.K_a)
        cb = _planar_pose_candidates(P[np.asarray(idb, int)], np.asarray(uvb, float), self.K_b)
        Ta, amb_a, tilt_a = _side_from(ca)
        Tb, amb_b, tilt_b = _side_from(cb)
        print(f"[b2b] capture: camera grabs+detect {_t_grab - _t0:.2f}s · joints {_t_det - _t_grab:.2f}s "
              f"· branch fits {_t.time() - _t_det:.2f}s · corners {len(ida)}/{len(idb)}", flush=True)
        return {
            "_cands": (ca, cb),   # branch fits are per-capture work — never redone downstream

            "joints_a": qa, "T_ca_marker": Ta,
            "joints_b": qb, "T_cb_marker": Tb,
            # the RAW measurement — the pixel bundle re-estimates the board pose per view,
            # so per-view PnP error (incl. the flip ambiguity) is absorbed, never frozen in
            "ids_a": np.asarray(ida, int), "uv_a": np.asarray(uva, float), "K_a": self.K_a,
            "ids_b": np.asarray(idb, int), "uv_b": np.asarray(uvb, float), "K_b": self.K_b,
            "_n_a": int(len(ida)), "_n_b": int(len(idb)),
            "_amb_a": amb_a, "_amb_b": amb_b, "_tilt_a": tilt_a, "_tilt_b": tilt_b,
        }

    def capture(self) -> dict:
        o = self._capture_obs()
        if o is None:
            return {"type": "b2b_capture", "accepted": False, "n_a": 0, "n_b": 0,
                    "collected": len(self.obs)}
        n_a, n_b = o.pop("_n_a"), o.pop("_n_b")
        amb_a, amb_b = o.pop("_amb_a"), o.pop("_amb_b")
        tilt_a, tilt_b = o.pop("_tilt_a"), o.pop("_tilt_b")
        self.obs.append(o)
        out = {"type": "b2b_capture", "accepted": True, "n_a": n_a, "n_b": n_b,
               "collected": len(self.obs),
               "ambiguous_a": amb_a, "ambiguous_b": amb_b,
               "tilt_a_deg": round(tilt_a, 1), "tilt_b_deg": round(tilt_b, 1)}
        if amb_a or amb_b:
            side = "both cameras" if (amb_a and amb_b) else ("camera A" if amb_a else "camera B")
            out["warning"] = (f"this view is AMBIGUITY-PRONE for {side} (board too small/"
                              f"distant/fronto-parallel — its pose has two near-equal "
                              f"interpretations). Bring the board CLOSER and TILT it more; "
                              f"such views add little and can mislead the legacy metric.")
        return out

    def _bundle(self, obs: List[dict], full: bool = False) -> CalibResult:
        """The solve policy (see solve_base_to_base_auto): branch-aware selection + the
        two-camera PIXEL bundle when corner obs exist (legacy 3D bundle otherwise); `full`
        additionally runs the held-out-guarded joint-offsets trial (solve/validate — too
        heavy for the per-capture observe meter)."""
        import time as _t
        _t0 = _t.time()
        from .solve import solve_base_to_base_auto
        # the per-capture METER runs a bounded-iteration bundle (a live gauge doesn't need
        # full convergence on an Orin-class CPU); Solve/validate get the full budget
        r, report = solve_base_to_base_auto(
            obs, self.X_a, self.X_b, self.chain_a, self.chain_b, self.board.points,
            fk_a=self.fk_a, fk_b=self.fk_b,
            min_views_for_offsets=20 if full else 10 ** 9,
            max_nfev=400 if full else 100)
        self.last_report = report
        print(f"[b2b] solve({'full' if full else 'meter'}): {r.metrics.get('method')} "
              f"· {len(obs)} views · {_t.time() - _t0:.2f}s "
              f"· px_rms {r.metrics.get('pixel_rms_px')} "
              f"· flips {r.metrics.get('flipped_views', 0)}"
              + (f" · offsets {report.get('offsets')}" if full else ""), flush=True)
        return r

    def observe(self) -> dict:
        """Live feedback during collection (the b2b analog of CalibSession.observe): once ≥2
        shared views exist, bundle-solve and report inter-arm agreement (mm) + the per-DOF
        observability meter + the weakest DOF — so the operator watches the error shrink and
        knows what placement to add. Cheap, no arm motion."""
        if len(self.obs) < 2:
            return {"type": "b2b_observe", "ready": False, "collected": len(self.obs)}
        self.result = self._bundle(self.obs)
        return {"type": "b2b_observe", "ready": True, "collected": len(self.obs),
                "agreement_mm": round(float(self.result.metrics.get("agreement_mm", float("nan"))), 3),
                **_b2b_obs_json(self.result)}

    def solve(self) -> dict:
        if len(self.obs) < 2:
            return {"type": "b2b_solve", "error": "need >=2 shared-board captures"}
        from .geometry import transform_geodesic
        from .solve import solve_base_to_base
        self.result = self._bundle(self.obs, full=True)   # incl. the offsets trial when earned
        T_AB = self.result.T_optical
        agreement_mm = float(self.result.metrics.get("agreement_mm", float("nan")))
        # Self-consistency: spread of the per-obs closed-form solves vs the bundle (rotation deg
        # AND translation mm) — a free quality signal alongside the covariance.
        ts = [solve_base_to_base([o], self.X_a, self.X_b, self.chain_a, self.chain_b,
                                 fk_a=self.fk_a, fk_b=self.fk_b) for o in self.obs]
        spreads = [transform_geodesic(T_AB, t) for t in ts]
        cons_deg = max((s[0] for s in spreads), default=0.0)
        cons_trans_mm = max((s[1] for s in spreads), default=0.0) * 1000.0
        obsj = _b2b_obs_json(self.result)
        observable = bool(obsj.get("observability") and self._observable())
        rep = self.last_report or {}
        return {"type": "b2b_solve", "T_base_to_base": _T(T_AB), "captures": len(self.obs),
                "agreement_mm": round(agreement_mm, 3), "consensus_deg": round(float(cons_deg), 3),
                "consensus_trans_mm": round(float(cons_trans_mm), 3), "observable": observable,
                "method": self.result.metrics.get("method"),
                "flipped_views": self.result.metrics.get("flipped_views", 0),
                "pixel_rms_px": self.result.metrics.get("pixel_rms_px"),
                "per_view": rep.get("per_view"), "offsets": rep.get("offsets"),
                **obsj}

    def _observable(self) -> bool:
        if self.result is None:
            return False
        try:
            ok, _ = well_observed(self.result, rot_sigma_deg=self.accept_rot_sigma_deg,
                                  trans_sigma_mm=self.accept_trans_sigma_mm)
            return bool(ok)
        except Exception:  # noqa: BLE001
            return False

    def validate(self, n_heldout: int = 0) -> dict:
        """Accept gate: inter-arm agreement on shared views HELD OUT of the fit (generalization,
        not the training residual) + the observability gate. NaN held-out (too few views) falls
        back to the training agreement so a small set still reports honestly."""
        if len(self.obs) < 2:
            return {"type": "b2b_validate", "error": "need >=2 shared-board captures"}
        if self.result is None:
            self.result = self._bundle(self.obs)
        # Held-out through the SAME policy as the solve (branch-aware + pixel bundle when
        # corners exist): fit on the train split, score UNSEEN views with flip branches
        # picked under the fitted T_AB — the honest generalization number.
        n = len(self.obs)
        held = float("nan")
        if n >= 4:
            from .solve import _b2b_heldout_mm
            k = max(1, n // 3) if n_heldout <= 0 else int(n_heldout)
            train, test = list(self.obs[: n - k]), list(self.obs[n - k:])
            try:
                rtr = self._bundle(train)
                dq = rtr.fk_offsets
                dqa = dq[: self.chain_a.n] if dq.size else np.zeros(self.chain_a.n)
                dqb = dq[self.chain_a.n:] if dq.size else np.zeros(self.chain_b.n)
                held = _b2b_heldout_mm(test, rtr.T_optical, self.X_a, self.X_b,
                                       self.chain_a, self.chain_b, self.board.points,
                                       self.fk_a + dqa, self.fk_b + dqb)
            except Exception:  # noqa: BLE001 — fall back to the training agreement below
                held = float("nan")
        train_mm = float(self.result.metrics.get("agreement_mm", float("nan")))
        score = held if held == held else train_mm   # NaN-safe: fall back to training agreement
        observable = self._observable()
        passed = bool(score == score and score < self.accept_agreement_mm and observable)
        return {"type": "b2b_validate",
                "heldout_agreement_mm": None if held != held else round(float(held), 3),
                "agreement_mm": round(train_mm, 3), "observable": observable,
                "n_heldout": (max(1, len(self.obs) // 3) if n_heldout <= 0 else n_heldout),
                "pass": passed, **_b2b_obs_json(self.result)}

    def verify(self) -> dict:
        """The fresh-placement check (the b2b analog of the visual verify): the operator moves
        the board to a NEW spot both cams see; we capture it WITHOUT adding it to the fit and
        report the inter-arm agreement (mm) there. A correct T_AB agrees everywhere; an
        under-observed one drifts at the unseen placement."""
        if self.result is None:
            return {"type": "b2b_verify", "ok": False, "reason": "solve first", "tracks": False}
        o = self._capture_obs()
        if o is None:
            return {"type": "b2b_verify", "ok": False, "reason": "both cameras must see the board",
                    "tracks": False}
        for k in ("_n_a", "_n_b", "_amb_a", "_amb_b", "_tilt_a", "_tilt_b"):
            o.pop(k, None)
        # fresh-placement agreement with the flip branch picked UNDER the solved T_AB — a
        # single view can't vote its own branch (that's the frozen-PnP failure)
        from .solve import _b2b_heldout_mm
        mm = _b2b_heldout_mm([o], self.result.T_optical, self.X_a, self.X_b,
                             self.chain_a, self.chain_b, self.board.points,
                             self.fk_a, self.fk_b)
        return {"type": "b2b_verify", "ok": True, "tracks": bool(mm < self.accept_agreement_mm),
                "agreement_mm": round(float(mm), 3), "thresh_mm": self.accept_agreement_mm}

    def accept(self, calib_dir: str, urdf_path: Optional[str] = None,
               out_path: Optional[str] = None, force: bool = False) -> dict:
        import json
        assert self.result is not None
        T_AB = self.result.T_optical
        # OBSERVABILITY ACCEPT GATE: refuse an under-observed base-to-base (the dual-arm analog of
        # the eye-in-hand 2x-translation guard) unless `force` (the operator SAVE override), which
        # writes it with a warning instead.
        warning = None
        observable = self._observable()
        if not observable:
            if not force:
                return {"type": "b2b_accept", "ok": False, "error": "base-to-base is not observable "
                        "— place the shared marker at more varied positions/orientations in the "
                        "overlap (the transform is under-constrained)", **_b2b_obs_json(self.result)}
            warning = ("saved while UNDER-OBSERVED — the inter-arm transform is under-constrained; "
                       "collect more varied shared placements before relying on it")
        Path(calib_dir).mkdir(parents=True, exist_ok=True)
        dst = str(Path(calib_dir) / "base_to_base.json")
        Path(dst).write_text(json.dumps({
            "kind": "base_to_base", "T_base_to_base": _T(T_AB),
            "arm_a": self.item.partner_camera, "arm_b": self.item.secondary_camera,
            "captures": len(self.obs),
            "agreement_mm": round(float(self.result.metrics.get("agreement_mm", float("nan"))), 4),
        }, indent=2), encoding="utf-8")
        # BAKE it into the URDF too: place arm B's base at its CALIBRATED pose relative to arm A, so the
        # motion planner's two arms are in their true relative pose (the JSON alone is consumed nowhere).
        urdf = out_path or urdf_path
        baked = None
        if urdf:
            try:
                from .bake import apply_base_to_base
                baked = apply_base_to_base(urdf, self.item.partner_camera, self.item.secondary_camera,
                                           T_AB)
            except Exception as e:  # noqa: BLE001 — never block the save on the URDF write; report it
                baked = {"error": f"{type(e).__name__}: {e}"}
        out = {"type": "b2b_accept", "ok": True, "path": dst, "T_base_to_base": _T(T_AB),
               "urdf": baked, **_b2b_obs_json(self.result)}
        if warning:
            out["warning"] = warning
        return out
