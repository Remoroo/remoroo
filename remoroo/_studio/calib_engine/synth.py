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


# --------------------------------------------------------------------------- #
# Variants: eye-to-hand, stereo right lens, dual-arm base-to-base, image render #
# --------------------------------------------------------------------------- #
def _look_at(eye: np.ndarray, target: np.ndarray, up=(0, 0, 1)) -> np.ndarray:
    """A camera pose T_base_cam looking from `eye` toward `target` (+z = view direction)."""
    z = target - eye
    z = z / np.linalg.norm(z)
    x = np.cross(np.asarray(up, float), z)
    if np.linalg.norm(x) < 1e-6:
        x = np.array([1.0, 0.0, 0.0])
    x = x / np.linalg.norm(x)
    y = np.cross(z, x)
    return make_T(np.stack([x, y, z], axis=1), eye)


def make_eye_to_hand_dataset(*, seed: int = 0, n_train: int = 16, n_test: int = 6,
                             dtheta_deg: float = 0.4, noise_px: float = 0.3) -> SynthTruth:
    """Static camera in the base; board mounted on the gripper (X = base->camera,
    T_board = board->flange). Mirrors make_dataset but with the eye-to-hand observation
    model, so test_eye_to_hand can recover X with the eye_to_hand solve."""
    rng = np.random.default_rng(seed)
    chain = default_chain()
    board = default_board()
    K = default_K()
    J = chain.n

    Tb = make_T(rodrigues(np.deg2rad([3.0, -2.0, 4.0])), [0.0, 0.0, 0.05])  # board->flange
    pattern = np.zeros(J)
    interior = np.array([-1.0, 0.6, -0.8, 0.7, 0.9, -0.5])
    pattern[1:J - 1] = interior[: max(0, J - 2)]
    dtheta_true = np.deg2rad(dtheta_deg) * pattern

    board_center0 = (chain.fk(dtheta_true) @ Tb)[:3, 3]
    X = _look_at(board_center0 + np.array([0.0, -0.6, 0.25]), board_center0)  # base->camera

    def visible(th):
        bw = transform_points(chain.fk(th + dtheta_true) @ Tb, board.points)
        pc = transform_points(inv_T(X), bw)
        if np.any(pc[:, 2] < 0.1):
            return None
        uv = project(K, pc)
        if uv[:, 0].min() < 0 or uv[:, 0].max() >= K[0, 2] * 2 or uv[:, 1].min() < 0 or uv[:, 1].max() >= K[1, 2] * 2:
            return None
        return uv

    def sample():
        for _ in range(200):
            th = np.zeros(J)
            th[:3] = rng.uniform(-0.08, 0.08, 3)
            th[3:] = rng.uniform(-0.35, 0.35, J - 3)
            uv = visible(th)
            if uv is not None:
                return th, uv
        return None

    def make(sid):
        got = sample()
        if got is None:
            return None
        th, uv = got
        uv = uv + rng.normal(0.0, noise_px, uv.shape)
        return CaptureSample(id=sid, joints=th, fk_pose=chain.fk(th),
                             corner_ids=np.arange(board.n), corners=uv)

    train, test, sid, tries = [], [], 0, 0
    while len(train) < n_train and tries < 4000:
        tries += 1
        s = make(sid)
        if s: train.append(s); sid += 1
    while len(test) < n_test and tries < 8000:
        tries += 1
        s = make(sid)
        if s: test.append(s); sid += 1
    if len(train) < n_train or len(test) < n_test:
        raise RuntimeError(f"eye_to_hand synth could not place enough visible poses ({len(train)}/{len(test)})")
    return SynthTruth(X=X, T_board=Tb, chain=chain, K=K, board=board,
                      dtheta_true=dtheta_true, scale_true=1.0,
                      train=train, test=test, fiducial_obs=[], fiducial_base=board_center0)


def add_right_lens(truth: SynthTruth, baseline_m: float = 0.063, noise_px: float = 0.3,
                   seed: int = 7) -> Tuple[List[CaptureSample], np.ndarray]:
    """Generate the RIGHT-lens captures for an eye-in-hand `truth`, offset from the left by
    a known stereo baseline. Returns (right_samples aligned to truth.train, sdk_T_LR =
    left->right). The two independently-solved hand-eyes must agree with sdk_T_LR (F9)."""
    rng = np.random.default_rng(seed)
    T_lr = make_T(np.eye(3), [baseline_m, 0.0, 0.0])      # left-optical -> right-optical
    X_r = truth.X @ T_lr                                  # flange -> right optical
    out = []
    for s in truth.train:
        T_bc = truth.chain.fk(s.joints + truth.dtheta_true) @ X_r
        pc = transform_points(inv_T(T_bc), transform_points(truth.T_board, truth.board.points * truth.scale_true))
        uv = project(truth.K, pc) + rng.normal(0.0, noise_px, (truth.board.n, 2))
        out.append(CaptureSample(id=s.id, joints=s.joints, fk_pose=s.fk_pose,
                                 corner_ids=np.arange(truth.board.n), corners=uv, cam="right"))
    return out, T_lr


def make_base_to_base_dataset(*, seed: int = 0, n_obs: int = 8):
    """Two arms (A at the origin, B offset by a known T_AB), each with its own eye-in-hand
    X, both wrist cams viewing ONE shared marker fixed in A's base. Returns everything
    solve_base_to_base needs + the true T_AB to assert against."""
    rng = np.random.default_rng(seed)
    chain = default_chain()
    K = default_K()
    X_a = make_T(rodrigues(np.deg2rad([4.0, -3.0, 6.0])), [0.04, -0.02, 0.06])
    X_b = make_T(rodrigues(np.deg2rad([-5.0, 2.0, -4.0])), [-0.03, 0.05, 0.05])
    T_AB = make_T(rodrigues(np.deg2rad([2.0, 1.0, 30.0])), [0.8, 0.1, 0.0])   # baseA->baseB
    marker_in_A = make_T(rodrigues(np.deg2rad([10.0, 5.0, -8.0])), [0.4, 0.0, 0.3])
    marker_in_B = inv_T(T_AB) @ marker_in_A

    def view(chain_, X, marker):
        """Diverse joints + the EXACT camera->marker pose (no projection/PnP needed — the
        base-to-base solve is matrix algebra on the marker poses, so visibility is moot)."""
        th = np.zeros(chain_.n)
        th[:3] = rng.uniform(-0.15, 0.15, 3)
        th[3:] = rng.uniform(-0.5, 0.5, chain_.n - 3)
        return th, inv_T(chain_.fk(th) @ X) @ marker            # camera -> marker

    obs = []
    for _ in range(n_obs):
        ja, Tca = view(chain, X_a, marker_in_A)
        jb, Tcb = view(chain, X_b, marker_in_B)
        obs.append({"joints_a": ja, "T_ca_marker": Tca, "joints_b": jb, "T_cb_marker": Tcb})
    return {"obs": obs, "X_a": X_a, "X_b": X_b, "chain_a": chain, "chain_b": chain, "T_AB": T_AB}


def render_corners_image(uv: np.ndarray, wh: Tuple[int, int] = (1280, 720)) -> np.ndarray:
    """A minimal synthetic grayscale frame: bright squares at the corner pixels on a mid-gray
    background. Enough for a cv2 cornerSubPix smoke test + the frame_image serving path; NOT
    a realistic board render."""
    W, H = wh
    img = np.full((H, W), 128, np.uint8)
    for u, v in np.asarray(uv, float):
        ui, vi = int(round(u)), int(round(v))
        if 2 <= ui < W - 2 and 2 <= vi < H - 2:
            img[vi - 2:vi + 3, ui - 2:ui + 3] = 255
    return img
