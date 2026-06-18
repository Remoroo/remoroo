"""The solve — eye-in-hand hand-eye calibration.

Two layers:
  * `solve_handeye_closed` — a Park/Martin-style closed form (Kabsch rotation + linear
    translation) from AX=XB. Fast, but it *assumes the reported flange pose is exact*,
    so it inherits the robot's FK bias. Used only as the bundle initialiser + as the
    baseline the bundle must beat.
  * `solve_eye_in_hand` — the v2 deliverable: a robust reprojection-error bundle that
    jointly fits X (flange->camera), the static board pose, a small per-joint FK
    correction, and the board scale. This removes systematic FK/scale bias instead of
    averaging around it.

Pure numpy + scipy (no cv2): runs in the dev `.venv` and CI.
"""
from __future__ import annotations

from typing import Callable, List, Optional, Sequence, Tuple

import numpy as np
from scipy.optimize import least_squares

from .geometry import (
    Chain,
    inv_T,
    make_T,
    project,
    rodrigues,
    rotvec_from_R,
    transform_points,
)
from .types import CalibResult, CaptureSample


# --------------------------------------------------------------------------- #
# Closed form (initialiser / baseline)                                        #
# --------------------------------------------------------------------------- #
def _solve_AXXB(As: Sequence[np.ndarray], Bs: Sequence[np.ndarray], *, min_angle: float = 0.05) -> np.ndarray:
    """Core closed form for A X = X B over a set of relative-motion pairs (A, B). Rotation
    by Park/Martin (SVD of sum beta alpha^T); translation by stacked least-squares. Shared
    by eye-in-hand, eye-to-hand, and base-to-base (each just builds different A,B pairs)."""
    alphas: List[np.ndarray] = []
    betas: List[np.ndarray] = []
    A_keep: List[np.ndarray] = []
    B_keep: List[np.ndarray] = []
    for A, B in zip(As, Bs):
        a = rotvec_from_R(A[:3, :3])
        b = rotvec_from_R(B[:3, :3])
        if np.linalg.norm(a) < min_angle or np.linalg.norm(b) < min_angle:
            continue  # near-pure-translation pair is degenerate for rotation
        alphas.append(a); betas.append(b); A_keep.append(A); B_keep.append(B)
    if len(alphas) < 2:
        raise ValueError("not enough rotation-diverse pose pairs for a closed-form solve")

    M = np.zeros((3, 3))
    for a, b in zip(alphas, betas):
        M += np.outer(b, a)
    U, _, Vt = np.linalg.svd(M)
    V = Vt.T
    d = np.sign(np.linalg.det(V @ U.T))
    Rx = V @ np.diag([1.0, 1.0, d]) @ U.T

    C, e = [], []
    for A, B in zip(A_keep, B_keep):
        C.append(A[:3, :3] - np.eye(3))
        e.append(Rx @ B[:3, 3] - A[:3, 3])
    tx, *_ = np.linalg.lstsq(np.vstack(C), np.concatenate(e), rcond=None)
    return make_T(Rx, tx)


def solve_handeye_closed(
    T_bg_list: Sequence[np.ndarray],
    T_ct_list: Sequence[np.ndarray],
    *,
    min_angle: float = 0.05,
) -> np.ndarray:
    """Closed-form X = flange->camera (eye-in-hand) from base->flange (T_bg) and
    camera->target (T_ct) pairs: A = inv(T_bg_j) T_bg_i, B = T_ct_j inv(T_ct_i)."""
    As, Bs = [], []
    n = len(T_bg_list)
    for i in range(n):
        for j in range(i + 1, n):
            As.append(inv_T(T_bg_list[j]) @ T_bg_list[i])
            Bs.append(T_ct_list[j] @ inv_T(T_ct_list[i]))
    return _solve_AXXB(As, Bs, min_angle=min_angle)


def _closed_form_eye_to_hand(
    samples: Sequence[CaptureSample], board_points: np.ndarray, K: np.ndarray, chain: Chain,
) -> Tuple[np.ndarray, np.ndarray]:
    """Closed-form init for eye-to-hand (static camera, board on the gripper). Returns
    (X = base->camera, Tb = board->flange). With T_ct = camera->board (PnP) and fk =
    base->flange: fk_j^-1 fk_i = Tb (T_ct_j^-1 T_ct_i) Tb^-1  ->  solve A Tb = Tb B for Tb,
    then X = average_i( fk_i Tb T_ct_i^-1 )."""
    fk = [s.fk_pose for s in samples]
    T_ct = [_estimate_target_pose(s, board_points, K) for s in samples]
    As, Bs = [], []
    n = len(samples)
    for i in range(n):
        for j in range(i + 1, n):
            As.append(inv_T(fk[j]) @ fk[i])
            Bs.append(inv_T(T_ct[j]) @ T_ct[i])
    Tb = _solve_AXXB(As, Bs)                              # board->flange
    Xs = [fk[i] @ Tb @ inv_T(T_ct[i]) for i in range(n)]  # base->camera per sample
    from .geometry import average_transforms
    return average_transforms(Xs), Tb


def _closed_form_from_samples(
    samples: Sequence[CaptureSample], board_points: np.ndarray, K: np.ndarray
) -> np.ndarray:
    """Build the AX=XB inputs from samples by PnP-free target poses: we don't have a
    PnP here, so estimate T_ct (camera->target) per sample by a quick planar solve
    using the reported flange pose is impossible without X. Instead use the analytic
    target pose from the *reported* geometry is also impossible. So we estimate T_ct
    via a minimal linear PnP from the known board points + observed pixels."""
    T_bg = [s.fk_pose for s in samples]
    T_ct = [_estimate_target_pose(s, board_points, K) for s in samples]
    return solve_handeye_closed(T_bg, T_ct)


def _planar_pose_init(pts: np.ndarray, uv: np.ndarray, K: np.ndarray) -> np.ndarray:
    """Board->camera pose for a PLANAR target (z=0) from a homography decomposition — a
    correct basin for ANY board orientation (the identity-rotation seed only works when the
    board faces the camera, i.e. eye-in-hand). Fit H: board(x,y)->pixel (DLT), then
    Hn = K^-1 H gives [r1 r2 t] up to scale; r3 = r1 x r2, orthonormalised via SVD."""
    src, dst = pts[:, :2], np.asarray(uv, float)
    A = []
    for (x, y), (u, v) in zip(src, dst):
        A.append([-x, -y, -1, 0, 0, 0, u * x, u * y, u])
        A.append([0, 0, 0, -x, -y, -1, v * x, v * y, v])
    _, _, Vt = np.linalg.svd(np.asarray(A, float))
    H = Vt[-1].reshape(3, 3)
    Hn = np.linalg.inv(K) @ H
    if Hn[2, 2] < 0:
        Hn = -Hn
    lam = 1.0 / np.linalg.norm(Hn[:, 0])
    r1, r2, t = Hn[:, 0] * lam, Hn[:, 1] * lam, Hn[:, 2] * lam
    R = np.stack([r1, r2, np.cross(r1, r2)], axis=1)
    U, _, Vt2 = np.linalg.svd(R)
    R = U @ np.diag([1.0, 1.0, float(np.sign(np.linalg.det(U @ Vt2)))]) @ Vt2
    if t[2] < 0:                          # board must sit in front of the camera
        t = -t
        R = R @ np.diag([-1.0, -1.0, 1.0])
    return make_T(R, t)


def _estimate_target_pose(sample: CaptureSample, board_points: np.ndarray, K: np.ndarray) -> np.ndarray:
    """Board->camera pose from observed corners. MULTI-START: the planar-homography pose
    has a two-fold (flip) ambiguity, so we LM-refine from both the homography seed AND the
    facing-camera seed and keep the lowest-residual solution. This makes PnP robust for an
    arbitrarily-oriented board (eye-to-hand) WITHOUT regressing the face-on case (eye-in-
    hand). (cv2.solvePnP on-robot; numpy here for CI.)"""
    pts = board_points[sample.corner_ids]            # (M,3) board frame
    uv = sample.corners                              # (M,2)

    def resid(x):
        T = make_T(rodrigues(x[:3]), x[3:])
        return (project(K, transform_points(T, pts)) - uv).ravel()

    seeds = [np.array([0.0, 0.0, 0.0, 0.0, 0.0, 0.5])]  # board facing the camera
    try:
        T0 = _planar_pose_init(pts, uv, K)
        seeds.insert(0, np.concatenate([rotvec_from_R(T0[:3, :3]), T0[:3, 3]]))
    except Exception:  # noqa: BLE001
        pass
    best, best_cost = None, np.inf
    for x0 in seeds:
        res = least_squares(resid, x0, method="lm", max_nfev=200)
        if res.cost < best_cost:
            best, best_cost = res.x, res.cost
    return make_T(rodrigues(best[:3]), best[3:])


# --------------------------------------------------------------------------- #
# The bundle                                                                  #
# --------------------------------------------------------------------------- #
class _Problem:
    """Packs/unpacks the parameter vector and builds the stacked reprojection
    residual. Shared by the solver and by metrics (covariance / observability)."""

    def __init__(
        self,
        samples: Sequence[CaptureSample],
        board_points: np.ndarray,
        K: np.ndarray,
        chain: Chain,
        *,
        estimate_fk: bool,
        estimate_scale: bool,
        weights: Optional[Sequence[np.ndarray]],
        mode: str = "eye_in_hand",
    ):
        self.samples = list(samples)
        self.board_points = np.asarray(board_points, float)
        self.K = np.asarray(K, float)
        self.chain = chain
        self.estimate_fk = estimate_fk
        self.estimate_scale = estimate_scale
        self.mode = mode
        self.J = chain.n
        # Both ENDS of the chain are gauge freedoms, not real DOF:
        #   * the LAST joint's offset is rigidly distal to the flange -> indistinguishable
        #     from the hand-eye X (correctly absorbed into X, which is what we write back);
        #   * the FIRST (base) joint's offset rotates the whole arm in the base frame ->
        #     indistinguishable from the static board pose (absorbed into T_board).
        # Only the INTERIOR joints (1..J-2) carry identifiable FK error, so we estimate
        # exactly those.
        self.fk_lo = 1
        self.n_fk = max(0, self.J - 2) if estimate_fk else 0
        # per-corner sqrt-weights (0 excludes a corner); default all ones
        if weights is None:
            self.w = [np.ones(len(s.corner_ids)) for s in self.samples]
        else:
            self.w = [np.asarray(w, float) for w in weights]
        # parameter layout
        self.i_X = slice(0, 6)
        self.i_board = slice(6, 12)
        self.i_fk = slice(12, 12 + self.n_fk)
        base = 12 + self.n_fk
        self.i_scale = base if estimate_scale else None
        self.nparams = base + (1 if estimate_scale else 0)

    def unpack(self, x: np.ndarray):
        X = make_T(rodrigues(x[self.i_X][:3]), x[self.i_X][3:])
        Tb = make_T(rodrigues(x[self.i_board][:3]), x[self.i_board][3:])
        fk = np.zeros(self.J)
        if self.estimate_fk and self.n_fk > 0:
            fk[self.fk_lo : self.fk_lo + self.n_fk] = x[self.i_fk]
        scale = float(x[self.i_scale]) if self.estimate_scale else 1.0
        return X, Tb, fk, scale

    def residual(self, x: np.ndarray) -> np.ndarray:
        X, Tb, fk, scale = self.unpack(x)
        out = []
        for s, w in zip(self.samples, self.w):
            T_fk = self.chain.fk(s.joints + fk)
            pb = self.board_points[s.corner_ids] * scale
            if self.mode == "eye_to_hand":
                # static camera (X = base->camera), board on the flange (Tb = board->flange):
                # board->base = fk @ Tb; camera point = inv(X) @ board->base.
                pw = transform_points(T_fk @ Tb, pb)        # board->base
                pc = transform_points(inv_T(X), pw)         # base->camera
            else:
                # eye-in-hand: camera on the flange (X = flange->camera), board fixed (Tb = board->base).
                pw = transform_points(Tb, pb)               # board->base
                pc = transform_points(inv_T(T_fk @ X), pw)  # base->camera
            uv = project(self.K, pc)
            r = (uv - s.corners) * w[:, None]
            out.append(r.ravel())
        return np.concatenate(out) if out else np.zeros(0)

    def x0_from(self, X: np.ndarray, Tb: np.ndarray) -> np.ndarray:
        x = np.zeros(self.nparams)
        x[self.i_X] = np.concatenate([rotvec_from_R(X[:3, :3]), X[:3, 3]])
        x[self.i_board] = np.concatenate([rotvec_from_R(Tb[:3, :3]), Tb[:3, 3]])
        if self.estimate_scale:
            x[self.i_scale] = 1.0
        return x


def solve_eye_in_hand(
    samples: Sequence[CaptureSample],
    board_points: np.ndarray,
    K: np.ndarray,
    chain: Chain,
    *,
    estimate_fk: bool = True,
    estimate_scale: bool = True,
    weights: Optional[Sequence[np.ndarray]] = None,
    robust: bool = True,
    f_scale: float = 1.5,
    X_init: Optional[np.ndarray] = None,
    Tboard_init: Optional[np.ndarray] = None,
    mode: str = "eye_in_hand",
) -> CalibResult:
    """Robust reprojection-error bundle. `mode` selects the observation model:
    eye_in_hand (camera on flange, board fixed) or eye_to_hand (static camera, board on
    the gripper). Returns a CalibResult; the `_Problem` is stashed on it (`._problem`,
    `._x`) so metrics can reuse it."""
    prob = _Problem(
        samples, board_points, K, chain,
        estimate_fk=estimate_fk, estimate_scale=estimate_scale, weights=weights, mode=mode,
    )

    # Initialise from the mode's closed form. A caller MAY pin X_init (the eye-nudge gives
    # the bundle the operator's basin); when it does and Tboard_init is None, the board init
    # must be made CONSISTENT WITH THAT X (not the closed-form X) so the human-picked basin
    # is honoured — otherwise the bundle starts at an inconsistent (X, board) pair.
    s0 = samples[0]

    def _board_from_X(X: np.ndarray) -> np.ndarray:
        P = _estimate_target_pose(s0, board_points, K)        # board->camera (PnP)
        if mode == "eye_to_hand":                              # Tb = inv(fk) @ X @ P
            return inv_T(s0.fk_pose) @ X @ P
        return s0.fk_pose @ X @ P                              # board->base = fk @ X @ P

    if X_init is None:
        if mode == "eye_to_hand":
            X_init, Tbc = _closed_form_eye_to_hand(samples, board_points, K, chain)
        else:
            X_init = _closed_form_from_samples(samples, board_points, K)
            Tbc = _board_from_X(X_init)
        Tboard_init = Tboard_init if Tboard_init is not None else Tbc
    elif Tboard_init is None:
        Tboard_init = _board_from_X(X_init)

    x0 = prob.x0_from(X_init, Tboard_init)
    res = least_squares(
        prob.residual, x0,
        loss="huber" if robust else "linear", f_scale=f_scale,
        method="trf", max_nfev=400, x_scale="jac",
    )
    X, Tb, fk, scale = prob.unpack(res.x)
    r = res.fun
    rms = float(np.sqrt(np.sum(r ** 2) / r.size)) if r.size else 0.0

    result = CalibResult(
        T_optical=X, T_board=Tb, fk_offsets=fk, board_scale=scale,
        residual_px=rms, samples_used=len(samples), kind=mode,
        converged=bool(res.success), message=str(res.message),
        metrics={"train_rms_px": rms},
    )
    # Stash the problem + solution for metrics (covariance/observability/held-out).
    result._problem = prob   # type: ignore[attr-defined]
    result._x = res.x        # type: ignore[attr-defined]
    return result


def solve_base_to_base(
    obs: Sequence[dict],
    X_a: np.ndarray,
    X_b: np.ndarray,
    chain_a: Chain,
    chain_b: Chain,
    *,
    fk_a: Optional[np.ndarray] = None,
    fk_b: Optional[np.ndarray] = None,
) -> np.ndarray:
    """Dual-arm base-to-base: the transform T_baseA_baseB from a shared marker BOTH wrist
    cams observe, once each arm's eye-in-hand X is solved (no extra nonlinear solve — the
    design's "simple matrix transforms"). Each obs = {joints_a, joints_b, T_ca_marker,
    T_cb_marker} (camera->marker from PnP). The marker is one physical point:
        marker_in_baseA = fk_a(joints_a) @ X_a @ T_ca_marker
        marker_in_baseB = fk_b(joints_b) @ X_b @ T_cb_marker
        T_baseA_baseB   = marker_in_baseA @ inv(marker_in_baseB),  averaged over obs.
    `fk_a/fk_b` are optional per-joint corrections to add to the reported joints."""
    fa = np.zeros(chain_a.n) if fk_a is None else np.asarray(fk_a, float)
    fb = np.zeros(chain_b.n) if fk_b is None else np.asarray(fk_b, float)
    Ts = []
    for o in obs:
        mA = chain_a.fk(np.asarray(o["joints_a"], float) + fa) @ X_a @ np.asarray(o["T_ca_marker"], float)
        mB = chain_b.fk(np.asarray(o["joints_b"], float) + fb) @ X_b @ np.asarray(o["T_cb_marker"], float)
        Ts.append(mA @ inv_T(mB))
    if not Ts:
        raise ValueError("base_to_base needs at least one shared-marker observation")
    from .geometry import average_transforms
    return average_transforms(Ts)
