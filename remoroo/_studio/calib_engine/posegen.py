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

from .geometry import Chain, inv_T, project, rotation_angle, transform_points
from .types import BoardModel


def _board_fully_visible(T_bc: np.ndarray, T_board: np.ndarray, board: BoardModel,
                         K: np.ndarray, wh: Tuple[int, int], margin: float = 0.0) -> bool:
    W, H = wh
    pc = transform_points(inv_T(T_bc), transform_points(T_board, board.points))
    if np.any(pc[:, 2] <= 0.1):
        return False
    uv = project(K, pc)
    return bool(uv[:, 0].min() >= margin and uv[:, 0].max() < W - margin
                and uv[:, 1].min() >= margin and uv[:, 1].max() < H - margin)


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
) -> Tuple[Optional[np.ndarray], float]:
    """Return (joints, score) for the next pose, or (None, 0) if nothing feasible was
    found. `score` is the rotation-diversity gain (rad) — higher is more informative.
    `feasible(joints)->bool` is an optional collision/in-envelope filter (robot-side)."""
    base = np.zeros(chain.n) if nominal_joints is None else np.asarray(nominal_joints, float)
    collected_R = [(chain.fk(q) @ X_est)[:3, :3] for q in collected_joints]
    best_q: Optional[np.ndarray] = None
    best_score = -1.0
    for _ in range(n_cand):
        q = base.copy()
        q[:3] = q[:3] + rng.uniform(-base_range, base_range, 3)
        q[3:] = q[3:] + rng.uniform(-wrist_range, wrist_range, chain.n - 3)
        T_bc = chain.fk(q) @ X_est
        if not _board_fully_visible(T_bc, T_board_est, board, K, wh):
            continue
        if feasible is not None and not feasible(q):
            continue
        if collected_R:
            score = min(rotation_angle(R.T @ T_bc[:3, :3]) for R in collected_R)
        else:
            score = float(np.pi)  # first pose: anything visible is fine
        if score > best_score:
            best_score, best_q = score, q
    return best_q, max(0.0, best_score)
