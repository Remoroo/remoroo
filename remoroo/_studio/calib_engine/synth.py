"""THE TEST BACKBONE — generate a fully-known calibration world (no cv2, no robot) so
the whole math pipeline is provable in CI against ground truth.

We pick a true hand-eye X, a revolute Chain, a static board, intrinsics, and a set of
diverse joint configurations. Crucially we inject a known **per-joint FK offset**: the
robot *reports* joints `theta`, but its true physical angles are `theta + dtheta_true`,
so observations are generated from the TRUE pose while samples carry the REPORTED pose.
A solver that trusts the reported FK (closed form) is biased; the bundle that estimates
`dtheta` recovers the truth. We also inject board-scale error, corner noise, and a
controllable fraction of outlier corners (for the curation tests).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np

from .geometry import Chain, inv_T, make_T, project, rodrigues, transform_points
from .types import BoardModel, CaptureSample


@dataclass
class SynthTruth:
    X: np.ndarray                  # true flange->camera
    T_board: np.ndarray            # true board->base
    chain: Chain
    K: np.ndarray
    board: BoardModel
    dtheta_true: np.ndarray        # injected per-joint offset (rad)
    scale_true: float              # injected board-scale error
    train: List[CaptureSample]
    test: List[CaptureSample]
    fiducial_obs: List[dict]       # held-out tip-landing observations
    fiducial_base: np.ndarray      # true fiducial location in base


def default_chain() -> Chain:
    """A fixed 6-DOF revolute chain (pure-translation origins, alternating axes). Its
    exact geometry doesn't matter — we place the board in front of the nominal camera
    by construction, so visibility is guaranteed; wrist joints give rotation diversity."""
    origins = [make_T(np.eye(3), [0, 0, 0.15]) for _ in range(6)]
    axes = [
        [0, 0, 1], [0, 1, 0], [0, 1, 0],   # positioning
        [0, 0, 1], [0, 1, 0], [0, 0, 1],   # wrist (roll/pitch/roll)
    ]
    return Chain(origins, axes)


def default_board(rows: int = 5, cols: int = 7, square: float = 0.03) -> BoardModel:
    xs = (np.arange(cols) - (cols - 1) / 2.0) * square
    ys = (np.arange(rows) - (rows - 1) / 2.0) * square
    gx, gy = np.meshgrid(xs, ys)
    pts = np.stack([gx.ravel(), gy.ravel(), np.zeros(gx.size)], axis=1)
    return BoardModel(points=pts, rows=rows, cols=cols, square_m=square)


def default_K(w: int = 1280, h: int = 720, f: float = 900.0) -> np.ndarray:
    return np.array([[f, 0, w / 2.0], [0, f, h / 2.0], [0, 0, 1.0]], float)


def make_dataset(
    *,
    seed: int = 0,
    n_train: int = 16,
    n_test: int = 6,
    n_fiducial: int = 6,
    dtheta_deg: float = 0.4,
    scale_err: float = 1.01,
    noise_px: float = 0.3,
    outlier_frac: float = 0.0,
    outlier_px: float = 25.0,
    estimate_fk_world: bool = True,
    wrist_range: float = 0.5,
    base_range: float = 0.12,
    chain: Optional[Chain] = None,
) -> SynthTruth:
    rng = np.random.default_rng(seed)
    chain = chain if chain is not None else default_chain()
    board = default_board()
    K = default_K()
    J = chain.n

    # True hand-eye: a modest rotation + a wrist-mounted offset.
    X = make_T(rodrigues(np.deg2rad([4.0, -3.0, 6.0])), [0.04, -0.02, 0.06])

    # Inject the systematic FK errors on the IDENTIFIABLE (interior) joints only, 1..J-2.
    # The first joint's offset is a gauge freedom with the board pose and the last with X,
    # so injecting them would just shift those by unobservable constants and muddy the test.
    pattern = np.zeros(J)
    interior = np.array([-1.0, 0.6, -0.8, 0.7, 0.9, -0.5])
    pattern[1 : J - 1] = interior[: max(0, J - 2)]
    dtheta_true = np.deg2rad(dtheta_deg) * pattern if estimate_fk_world else np.zeros(J)
    scale_true = scale_err

    # Place the board ~0.5 m in front of the nominal camera, facing it.
    theta0 = np.zeros(J)
    T_bc0 = chain.fk(theta0 + dtheta_true) @ X
    cam_z = T_bc0[:3, 2]
    board_center = T_bc0[:3, 3] + 0.5 * cam_z
    # board frame: x,y span the plane (camera x,y), z = -cam_z (normal faces camera)
    bx = T_bc0[:3, 0]
    by = T_bc0[:3, 1]
    bz = np.cross(bx, by)
    R_board = np.stack([bx, by, bz], axis=1)
    T_board = make_T(R_board, board_center)

    # A fixed fiducial near the board (for the tip-landing test).
    fiducial_base = board_center + 0.05 * bx + 0.03 * by

    def sample_pose() -> Optional[np.ndarray]:
        """Diverse reported joints whose TRUE camera sees the whole board (all z>0)."""
        for _ in range(80):
            th = np.zeros(J)
            th[:3] = rng.uniform(-base_range, base_range, 3)
            th[3:] = rng.uniform(-wrist_range, wrist_range, J - 3)
            T_bc = chain.fk(th + dtheta_true) @ X
            pc = transform_points(inv_T(T_bc), transform_points(T_board, board.points * scale_true))
            if np.all(pc[:, 2] > 0.1):
                return th
        return None

    def make_sample(sid: int, th: np.ndarray) -> CaptureSample:
        T_bc_true = chain.fk(th + dtheta_true) @ X      # TRUE camera pose
        pc = transform_points(inv_T(T_bc_true), transform_points(T_board, board.points * scale_true))
        uv = project(K, pc)
        uv = uv + rng.normal(0.0, noise_px, uv.shape)
        if outlier_frac > 0:
            mask = rng.random(uv.shape[0]) < outlier_frac
            uv[mask] += rng.normal(0.0, outlier_px, (int(mask.sum()), 2))
        ids = np.arange(board.n)
        fk_reported = chain.fk(th)                       # REPORTED pose (no offset -> biased)
        return CaptureSample(id=sid, joints=th, fk_pose=fk_reported, corner_ids=ids, corners=uv)

    train, test = [], []
    sid = 0
    while len(train) < n_train:
        th = sample_pose()
        if th is not None:
            train.append(make_sample(sid, th)); sid += 1
    while len(test) < n_test:
        th = sample_pose()
        if th is not None:
            test.append(make_sample(sid, th)); sid += 1

    # Tip-landing observations: the camera measures the fixed fiducial in its frame.
    fiducial_obs: List[dict] = []
    for _ in range(n_fiducial):
        th = sample_pose()
        if th is None:
            continue
        T_bc_true = chain.fk(th + dtheta_true) @ X
        p_cam = transform_points(inv_T(T_bc_true), fiducial_base.reshape(1, 3))[0]
        fiducial_obs.append({"joints": th, "p_cam": p_cam, "p_base_true": fiducial_base})

    return SynthTruth(
        X=X, T_board=T_board, chain=chain, K=K, board=board,
        dtheta_true=dtheta_true, scale_true=scale_true,
        train=train, test=test, fiducial_obs=fiducial_obs, fiducial_base=fiducial_base,
    )
