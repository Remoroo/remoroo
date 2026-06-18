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
from .curate import flag_suspect_corners, solve_curated
from .geometry import Chain, inv_T, transform_points
from .metrics import (
    consensus_spread,
    held_out_reprojection,
    observability,
    reprojection_detail,
    tip_landing_error,
)
from .posegen import suggest_next_pose
from .solve import _estimate_target_pose, solve_eye_in_hand
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


def _T(x) -> list:
    return np.asarray(x, float).round(6).tolist()


def build_plan(urdf_path: str) -> List[PlanItem]:
    """Derive the calibration plan from the rig (Pillar B): one item per detected camera.
    A camera on a moving flange -> eye_in_hand; one anchored to the world -> eye_to_hand."""
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
        min_corners: int = 6,
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
        self.min_corners = min_corners
        self.rng = np.random.default_rng(seed)
        self.accept_heldout_px = accept_heldout_px
        self.accept_tip_mm = accept_tip_mm

        self.samples: List[CaptureSample] = []
        self.heldout: List[CaptureSample] = []
        self.result: Optional[CalibResult] = None
        self.X_est = np.asarray(item.nominal_T, float)   # seed for visibility prediction
        self.T_board_est: Optional[np.ndarray] = None
        self.motion_ok = False

    # ── C.0 pre-flight motion check (gates everything) ──────────────────────
    def motion_check(self, joint: int = 0, delta: float = 0.05, tol: float = 0.02) -> dict:
        q0 = self.bridge.read_joints()
        q1 = q0.copy()
        q1[joint] += delta
        self.bridge.move_to_joints(q1)
        obs = self.bridge.read_joints() - q0
        self.bridge.move_to_joints(q0)                    # return to start
        cmd = q1 - q0
        max_err = float(np.max(np.abs(obs - cmd)))
        estop = bool(self.bridge.estop_ok())
        self.motion_ok = (max_err < tol) and estop
        return {"type": "motion_check", "ok": self.motion_ok, "max_err_rad": round(max_err, 5),
                "estop_ok": estop, "commanded": _T(cmd), "observed": _T(obs)}

    # ── detect (board seen?) + bootstrap the board pose estimate ────────────
    def detect(self) -> dict:
        ids, uv = self.bridge.capture()
        seen = len(ids) >= self.min_corners
        if seen and self.T_board_est is None:
            s = CaptureSample(id=-1, joints=self.bridge.read_joints(),
                              fk_pose=self.bridge.read_pose(), corner_ids=ids, corners=uv)
            T_c_board = _estimate_target_pose(s, self.board.points, self.K)
            self.T_board_est = s.fk_pose @ self.X_est @ T_c_board
        return {"type": "detect", "seen": seen, "n_corners": int(len(ids))}

    # ── suggest -> move -> capture loop ─────────────────────────────────────
    def suggest_pose(self) -> dict:
        if self.T_board_est is None:
            return {"type": "suggest_pose", "feasible": False, "reason": "no board yet"}
        q, score = suggest_next_pose(
            self.chain, self.X_est, self.T_board_est, self.board, self.K, self.wh,
            [s.joints for s in self.samples], rng=self.rng, nominal_joints=self.nominal_joints,
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
        ids, uv = self.bridge.capture()
        bucket = self.heldout if held_out else self.samples
        s = CaptureSample(id=(2000 if held_out else 0) + len(bucket),
                          joints=self.bridge.read_joints(), fk_pose=self.bridge.read_pose(),
                          corner_ids=ids, corners=uv)
        accepted = len(ids) >= self.min_corners
        if accepted:
            bucket.append(s)
        return {"type": "capture", "index": s.id, "n_corners": int(len(ids)),
                "accepted": accepted, "held_out": held_out,
                "collected": len(self.samples)}

    # ── solve (reprojection bundle) ─────────────────────────────────────────
    def solve(self) -> dict:
        self.result = solve_eye_in_hand(self.samples, self.board.points, self.K, self.chain)
        self.X_est = self.result.T_optical
        self.T_board_est = self.result.T_board
        return {"type": "solve", "train_rms_px": round(self.result.residual_px, 3),
                "scale": round(self.result.board_scale, 5), "X": _T(self.result.T_optical),
                "board": _T(self.result.T_board), "samples": self.result.samples_used}

    # ── validate on FRESH held-out poses + tip-landing (the accept gate) ────
    def validate(self, n_heldout: int = 6, fiducial_obs: Optional[Sequence[dict]] = None) -> dict:
        assert self.result is not None, "solve before validate"
        self.heldout = []
        for _ in range(n_heldout):
            self.suggest_pose()
            if getattr(self, "_pending", None) is None:
                continue
            self.move_to()
            self.capture(held_out=True)
        heldout_px = held_out_reprojection(self.result, self.heldout, self.board.points, self.K, self.chain) \
            if self.heldout else float("nan")
        tip_mm = tip_landing_error(self.result, fiducial_obs, self.chain) if fiducial_obs else None
        obs = observability(self.result)
        cons = consensus_spread(self.samples, self.board.points, self.K, seed=1)
        passed = (heldout_px < self.accept_heldout_px) and (tip_mm is None or tip_mm < self.accept_tip_mm)
        return {"type": "validate", "heldout_px": round(float(heldout_px), 3),
                "tip_mm": None if tip_mm is None else round(tip_mm, 3),
                "rot_worst_deg": round(obs["rot_worst_deg"], 4),
                "consensus_deg": round(cons, 3), "n_heldout": len(self.heldout), "pass": bool(passed)}

    # ── curate: exclude bad images/corners, re-solve, must improve held-out ──
    def curate(self, exclude_samples=None, exclude_corners=None, fiducial_obs=None) -> dict:
        assert self.result is not None
        before = held_out_reprojection(self.result, self.heldout, self.board.points, self.K, self.chain) \
            if self.heldout else float("nan")
        new = solve_curated(self.samples, self.board.points, self.K, self.chain,
                            exclude_samples=exclude_samples, exclude_corners=exclude_corners)
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
    def accept(self, urdf_path: str, out_path: Optional[str] = None, provenance: str = "measured") -> dict:
        assert self.result is not None
        body_optical = inv_T(self.item.nominal_flange_body) @ self.result.T_optical
        dst = urdf_io.write_calibrated_optical(urdf_path, self.item.camera_link, body_optical,
                                               out_path=out_path, provenance=provenance)
        return {"type": "accept", "urdf": dst, "camera": self.item.camera_link,
                "optical_frame": self.item.optical_frame, "provenance": provenance,
                "body_to_optical": _T(body_optical)}
