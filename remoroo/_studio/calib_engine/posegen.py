"""Observability-guided next-pose suggestion. Propose a joint configuration that (a)
keeps the whole board in view from the predicted camera pose and (b) is maximally
*informative* — here approximated by rotation diversity vs already-collected poses,
which is the dominant driver of hand-eye observability. Pure numpy.

On the robot a cuRobo collision/in-envelope filter wraps this (the `feasible` hook);
off-robot (FakeBridge) we skip it. A full Fisher-information ranking can replace the
diversity proxy later without changing the call site.
"""
from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

import numpy as np

from .geometry import Chain, inv_T, project, rotation_angle, rotvec_from_R, transform_points
from .types import BoardModel


def _board_fully_visible(T_bc: np.ndarray, T_board: np.ndarray, board: BoardModel,
                         K: np.ndarray, wh: Tuple[int, int], margin_frac: float = 0.10,
                         max_oblique_deg: float = 70.0) -> bool:
    """The camera must FACE the target with slack. `margin_frac` keeps the target inside the
    central (1 - 2*margin) of the frame — NOT at the very edge — so the nominal-hand-eye seed's
    error can't push it out of the real image. `max_oblique_deg` rejects grazing views (target
    seen near edge-on, where its corners are undetectable anyway)."""
    W, H = wh
    pc = transform_points(inv_T(T_bc), transform_points(T_board, board.points))
    if np.any(pc[:, 2] <= 0.1):
        return False
    # facing check: the target's normal (its z-axis in the camera frame) vs the viewing ray to
    # the target centre. |cos| → 1 is fronto-parallel, → 0 is edge-on.
    normal_cam = (inv_T(T_bc) @ T_board)[:3, 2]
    view = pc.mean(0); view = view / (np.linalg.norm(view) + 1e-9)
    if np.degrees(np.arccos(np.clip(abs(float(normal_cam @ view)), 0.0, 1.0))) > max_oblique_deg:
        return False
    uv = project(K, pc)
    mx, my = margin_frac * W, margin_frac * H
    return bool(uv[:, 0].min() >= mx and uv[:, 0].max() < W - mx
                and uv[:, 1].min() >= my and uv[:, 1].max() < H - my)


def _centering(T_bc: np.ndarray, T_board: np.ndarray, board: BoardModel,
               K: np.ndarray, wh: Tuple[int, int]) -> float:
    """1.0 when the target's centroid is at image centre, → 0 toward the edge. A centred
    prediction has the most slack against hand-eye-seed error, so it's the SAFEST to suggest."""
    pc = transform_points(inv_T(T_bc), transform_points(T_board, board.points))
    cen = project(K, pc).mean(0)
    off = float(np.linalg.norm(cen - np.array([wh[0] / 2.0, wh[1] / 2.0])) / (0.5 * min(wh)))
    return max(0.0, 1.0 - off)


def suggest_next_pose(
    chain: Chain,
    X_est: np.ndarray,
    T_board_est: np.ndarray,
    board: BoardModel,
    K: np.ndarray,
    wh: Tuple[int, int],
    collected_joints: Sequence[np.ndarray],
    *,
    rng: np.random.Generator,
    nominal_joints: Optional[np.ndarray] = None,
    n_cand: int = 300,
    wrist_range: float = 0.6,
    base_range: float = 0.12,
    feasible=None,
    weak_axis: Optional[np.ndarray] = None,
) -> Tuple[Optional[np.ndarray], float]:
    """Return (joints, score) for the next pose, or (None, 0) if nothing feasible was
    found. `score` is the information-gain estimate (rad) — higher is more informative.

    Information gain (7.6): when `weak_axis` is given (the least-observed rotation axis of
    X, the top eigenvector of the rotation-covariance block from `observability`), a
    candidate is rewarded for rotating ABOUT that axis — it directly excites the DOF the
    solve is still uncertain in, instead of just being far from prior poses. With no
    `weak_axis` (first poses, before a solve) it falls back to the rotation-DIVERSITY
    proxy (min angle to collected poses). `feasible(joints)->bool` is the optional
    collision/in-envelope filter (cuRobo, robot-side)."""
    base = np.zeros(chain.n) if nominal_joints is None else np.asarray(nominal_joints, float)
    collected_R = [(chain.fk(q) @ X_est)[:3, :3] for q in collected_joints]
    wa = None
    if weak_axis is not None:
        wa = np.asarray(weak_axis, float)
        n = np.linalg.norm(wa)
        wa = wa / n if n > 1e-9 else None
    best_q: Optional[np.ndarray] = None
    best_score = -1.0
    R_ref = (chain.fk(base) @ X_est)[:3, :3]
    lims = getattr(chain, "limits", None) or [None] * chain.n

    # BEFORE a solve exists, the hand-eye seed is the URDF NOMINAL — which can be wrong by tens
    # of degrees, so any prediction of "where the camera points" is unreliable. The one thing
    # that is NOT seed-dependent is joint-motion MAGNITUDE: a small joint move from the operator's
    # viewing pose physically can't swing a small marker out of frame, whatever the seed. So
    # explore SMALL until a solve (weak_axis) refines X; then widen for orientation diversity.
    if weak_axis is None:
        wrist_range, base_range = wrist_range * 0.4, base_range * 0.4

    # Per-joint sampling range, COMPUTED from each joint's effect on the camera's VIEWING
    # DIRECTION — NOT from the joint index. A joint that mostly ROLLS the camera in place
    # (small optical-axis tilt) keeps the board in frame, so sample it WIDE for orientation
    # diversity; a joint that PITCHES/YAWS the view (large tilt) swings the board out of
    # frame, so sample it NARROW. This is the rig-agnostic generalisation of the old
    # "first 3 = positioning, last 3 = wrist" split — it holds for any DOF count, a 7-DOF
    # arm, a humanoid limb, etc. The chosen range is then intersected with the URDF limit.
    z0 = (chain.fk(base) @ X_est)[:3, 2]
    span = []
    for i in range(chain.n):
        dq = base.copy(); dq[i] += 1e-3
        z = (chain.fk(dq) @ X_est)[:3, 2]
        tilt = float(np.arccos(np.clip(float(z0 @ z), -1.0, 1.0)) / 1e-3)  # view-tilt rad per rad
        span.append(wrist_range if tilt < 0.3 else base_range)

    def _sample() -> np.ndarray:
        q = base.copy()
        for i in range(chain.n):
            lo, hi = base[i] - span[i], base[i] + span[i]
            lim = lims[i] if i < len(lims) else None
            if lim is not None:                          # never leave the URDF joint limits
                m = 0.02 * (lim[1] - lim[0])
                lo, hi = max(lo, lim[0] + m), min(hi, lim[1] - m)
            q[i] = rng.uniform(lo, hi) if hi > lo else base[i]
        return q

    for _ in range(n_cand):
        q = _sample()
        T_bc = chain.fk(q) @ X_est
        if not _board_fully_visible(T_bc, T_board_est, board, K, wh):
            continue
        if feasible is not None and not feasible(q):
            continue
        diversity = min((rotation_angle(R.T @ T_bc[:3, :3]) for R in collected_R), default=float(np.pi))
        if wa is not None and collected_R:
            # rotation axis of this candidate vs the reference, in the camera frame; reward
            # alignment with the under-observed axis (|cos| since ± about the axis both help).
            rv = rotvec_from_R(R_ref.T @ T_bc[:3, :3])
            mag = float(np.linalg.norm(rv))
            align = abs(float(rv @ wa)) / mag if mag > 1e-6 else 0.0
            score = diversity * (0.3 + 0.7 * align)
        else:
            score = diversity
        # Bias toward poses that keep the target CENTRED: among equally-diverse candidates the
        # centred one survives hand-eye-seed error, so we don't suggest the edge case that drifts
        # the marker out of frame the moment the arm moves there.
        score = score * (0.4 + 0.6 * _centering(T_bc, T_board_est, board, K, wh))
        if score > best_score:
            best_score, best_q = score, q
    return best_q, max(0.0, best_score)
