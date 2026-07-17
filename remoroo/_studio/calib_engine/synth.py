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


def seven_dof_chain() -> Chain:
    """A 7-DOF redundant arm — proves the engine makes no '6 joints' assumption (the gauge
    rule estimates interior joints 1..J-2 for whatever J the URDF gives)."""
    origins = [make_T(np.eye(3), [0, 0, 0.13]) for _ in range(7)]
    axes = [[0, 0, 1], [0, 1, 0], [0, 1, 0], [0, 0, 1], [0, 1, 0], [0, 0, 1], [0, 1, 0]]
    return Chain(origins, axes)


def prismatic_chain() -> Chain:
    """A gantry-style chain: two PRISMATIC axes (x, z) + four revolute wrist joints. Proves
    the FK + solver are joint-TYPE agnostic (Chain.fk translates along a prismatic axis), not
    'revolute arm' only."""
    origins = [make_T(np.eye(3), [0, 0, 0.12]) for _ in range(6)]
    axes = [[1, 0, 0], [0, 0, 1], [0, 0, 1], [0, 1, 0], [0, 1, 0], [0, 0, 1]]
    types = ["prismatic", "prismatic", "revolute", "revolute", "revolute", "revolute"]
    return Chain(origins, axes, types=types)


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


def make_base_to_base_pixel_dataset(
    *,
    seed: int = 0,
    n_obs: int = 12,
    noise_px: float = 0.3,
    ambiguous_frac: float = 0.0,
    fk_true_a: Optional[np.ndarray] = None,
    fk_true_b: Optional[np.ndarray] = None,
    wh: Tuple[int, int] = (1280, 720),
):
    """PIXEL-LEVEL dual-arm base-to-base ground truth — the validation harness for the
    branch-aware / pixel-bundle solvers. Per shared view it emits what the REAL capture
    stores: reported joints for both arms + detected corner PIXELS per camera (+ each
    camera's K), plus every truth needed to score an estimator (T_AB, per-view board poses,
    injected joint offsets).

    Geometry is CONTROLLED per view on camera A (the operator-facing knob):
      * normal views: close (0.35-0.6 m) and well tilted (25-50°) — perspective decidable;
      * `ambiguous_frac` of views: FAR (1.3-1.8 m) and near-fronto-parallel (2-7°) — the
        formal weak-perspective regime where the planar flip ambiguity bites (Collins &
        Bartoli). Camera B's joints are rejection-sampled until it actually sees the board.
    Reported joints are BIASED by fk_true (the controller doesn't know its own offsets):
    truth uses q_true = q_reported + fk_true, mirroring the FakeBridge convention."""
    rng = np.random.default_rng(seed)
    chain = default_chain()
    n = chain.n
    board = default_board()
    P = board.points
    K_a, K_b = default_K(), default_K(f=850.0)          # distinct intrinsics per camera
    W, H = wh
    fa = np.zeros(n) if fk_true_a is None else np.asarray(fk_true_a, float)
    fb = np.zeros(n) if fk_true_b is None else np.asarray(fk_true_b, float)
    X_a = make_T(rodrigues(np.deg2rad([4.0, -3.0, 6.0])), [0.04, -0.02, 0.06])
    X_b = make_T(rodrigues(np.deg2rad([-5.0, 2.0, -4.0])), [-0.03, 0.05, 0.05])
    T_AB = make_T(rodrigues(np.deg2rad([2.0, 1.0, 30.0])), [0.8, 0.1, 0.0])

    ctr = P.mean(0)

    def cam_to_board(dist, tilt_deg, roll):
        """Camera→board pose: board centre `dist` ahead on the optical axis, plane tilted
        `tilt_deg` off fronto-parallel, rolled in plane."""
        Rt = rodrigues(np.deg2rad(tilt_deg) * np.array([1.0, 0.0, 0.0]))
        Rr = rodrigues(roll * np.array([0.0, 0.0, 1.0]))
        T = make_T(Rr @ Rt, [0.0, 0.0, dist])
        T[:3, 3] -= T[:3, :3] @ ctr - np.array([0.0, 0.0, 0.0])   # centre the BOARD CENTRE on axis
        T[:3, 3] += np.array([0.0, 0.0, 0.0])
        return T

    def corners(K, T_cam_board, npx=noise_px):
        pc = transform_points(T_cam_board, P)
        if np.any(pc[:, 2] < 0.05):
            return None, None
        uv = project(K, pc)
        ok = (uv[:, 0] >= 0) & (uv[:, 0] < W) & (uv[:, 1] >= 0) & (uv[:, 1] < H)
        if ok.sum() < max(8, P.shape[0] // 2):
            return None, None
        ids = np.arange(P.shape[0])[ok]
        return ids, uv[ok] + rng.normal(0.0, npx, (int(ok.sum()), 2))

    obs, truth_views = [], []
    n_amb = int(round(ambiguous_frac * n_obs))
    for i in range(n_obs):
        ambiguous = i < n_amb
        for _attempt in range(200):
            if ambiguous:
                # TRUE weak perspective with a CONSEQUENTIAL flip: far (board ~60-90px
                # across, both branches explain the pixels) at MODERATE tilt — a committed
                # wrong branch is then a 2×tilt ≈ 12-28° rotation error, the regime that
                # produced the live tens-of-mm agreement failure
                Ca = cam_to_board(rng.uniform(2.0, 2.8), rng.uniform(6.0, 14.0),
                                  rng.uniform(-0.4, 0.4))
            else:
                Ca = cam_to_board(rng.uniform(0.35, 0.6), rng.uniform(25.0, 50.0),
                                  rng.uniform(-0.8, 0.8))
            qa = rng.uniform(-0.6, 0.6, n)
            ids_a, uv_a = corners(K_a, Ca, npx=(2.0 * noise_px if ambiguous else noise_px))
            if ids_a is None:
                continue
            # the board's WORLD pose follows from arm A's TRUE kinematics
            M_A = chain.fk(qa + fa) @ X_a @ Ca                     # board in baseA
            M_B = inv_T(T_AB) @ M_A                                # board in baseB
            qb = rng.uniform(-0.6, 0.6, n)
            Cb = inv_T(chain.fk(qb + fb) @ X_b) @ M_B              # camera B → board (whatever falls out)
            ids_b, uv_b = corners(K_b, Cb)
            if ids_b is None:
                continue
            # legacy frozen-PnP fields come from the SAME single-pose estimator the live
            # pipeline used — so the legacy method is scored with its real failure modes
            from .solve import _estimate_target_pose
            sa = CaptureSample(id=i, joints=qa, fk_pose=chain.fk(qa), corner_ids=ids_a, corners=uv_a)
            sb = CaptureSample(id=i, joints=qb, fk_pose=chain.fk(qb), corner_ids=ids_b, corners=uv_b)
            obs.append({
                "joints_a": qa, "joints_b": qb,
                "T_ca_marker": _estimate_target_pose(sa, P, K_a),
                "T_cb_marker": _estimate_target_pose(sb, P, K_b),
                "ids_a": ids_a, "uv_a": uv_a, "K_a": K_a,
                "ids_b": ids_b, "uv_b": uv_b, "K_b": K_b,
            })
            truth_views.append({"C_a": Ca, "C_b": Cb, "ambiguous": ambiguous})
            break
        else:
            raise RuntimeError(f"could not sample a visible shared view (i={i})")
    return {"obs": obs, "X_a": X_a, "X_b": X_b, "chain_a": chain, "chain_b": chain,
            "T_AB": T_AB, "fk_true_a": fa, "fk_true_b": fb, "board": board,
            "K_a": K_a, "K_b": K_b, "views": truth_views}


def make_static_camera_views(*, seed: int = 0, n_views: int = 8, noise_px: float = 0.3):
    """A WORLD-FIXED camera and a FIXED board (operator placed it at the reference). Returns
    (views, T_optical_true, board, K): N captures of the same board from the fixed camera, so
    solve_static_camera must recover T_optical = board->camera. The camera doesn't move."""
    rng = np.random.default_rng(seed)
    board = default_board()
    K = default_K()
    # camera looks at the board from ~0.6 m, off to one side (a plausible overhead/static cam)
    T_cam_board = _look_at(np.array([0.1, -0.6, 0.4]), np.zeros(3))  # camera pose in board frame
    T_optical = inv_T(T_cam_board)                                   # board->camera
    views: List[CaptureSample] = []
    for i in range(n_views):
        pc = transform_points(T_optical, board.points)
        uv = project(K, pc) + rng.normal(0.0, noise_px, (board.n, 2))
        views.append(CaptureSample(id=i, joints=np.zeros(0), fk_pose=np.eye(4),
                                   corner_ids=np.arange(board.n), corners=uv))
    return views, T_optical, board, K


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
