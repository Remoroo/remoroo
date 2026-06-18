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
def solve_handeye_closed(
    T_bg_list: Sequence[np.ndarray],
    T_ct_list: Sequence[np.ndarray],
    *,
    min_angle: float = 0.05,
) -> np.ndarray:
    """Closed-form X = flange->camera from base->flange (T_bg) and camera->target
    (T_ct) pairs, solving A X = X B with A = inv(T_bg_j) T_bg_i, B = T_ct_j inv(T_ct_i).
    """
    n = len(T_bg_list)
    alphas: List[np.ndarray] = []
    betas: List[np.ndarray] = []
    As: List[np.ndarray] = []
    Bs: List[np.ndarray] = []
    for i in range(n):
        for j in range(i + 1, n):
            A = inv_T(T_bg_list[j]) @ T_bg_list[i]
            B = T_ct_list[j] @ inv_T(T_ct_list[i])
            a = rotvec_from_R(A[:3, :3])
            b = rotvec_from_R(B[:3, :3])
            if np.linalg.norm(a) < min_angle or np.linalg.norm(b) < min_angle:
                continue  # near-pure-translation pair is degenerate for rotation
            alphas.append(a)
            betas.append(b)
            As.append(A)
            Bs.append(B)
    if len(alphas) < 2:
        raise ValueError("not enough rotation-diverse pose pairs for a closed-form solve")

    # Rotation: maximise tr(R_x M), M = sum beta alpha^T  ->  R_x = V diag(1,1,det) U^T.
    M = np.zeros((3, 3))
    for a, b in zip(alphas, betas):
        M += np.outer(b, a)
    U, _, Vt = np.linalg.svd(M)
    V = Vt.T
    d = np.sign(np.linalg.det(V @ U.T))
    Rx = V @ np.diag([1.0, 1.0, d]) @ U.T

    # Translation: (R_A - I) t_x = R_x t_B - t_A, stacked least-squares.
    C = []
    e = []
    for A, B in zip(As, Bs):
        C.append(A[:3, :3] - np.eye(3))
        e.append(Rx @ B[:3, 3] - A[:3, 3])
    tx, *_ = np.linalg.lstsq(np.vstack(C), np.concatenate(e), rcond=None)
    return make_T(Rx, tx)


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


def _estimate_target_pose(sample: CaptureSample, board_points: np.ndarray, K: np.ndarray) -> np.ndarray:
    """A small Levenberg-Marquardt PnP: camera->board pose from observed corners.
    (cv2.solvePnP would do this on-robot; here we keep it numpy-only for CI.)"""
    pts = board_points[sample.corner_ids]            # (M,3) board frame
    uv = sample.corners                              # (M,2)

    def resid(x):
        T = make_T(rodrigues(x[:3]), x[3:])
        pc = transform_points(T, pts)
        return (project(K, pc) - uv).ravel()

    # init: board a bit in front of the camera, facing it.
    x0 = np.array([0.0, 0.0, 0.0, 0.0, 0.0, 0.5])
    res = least_squares(resid, x0, method="lm", max_nfev=200)
    return make_T(rodrigues(res.x[:3]), res.x[3:])


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
    ):
        self.samples = list(samples)
        self.board_points = np.asarray(board_points, float)
        self.K = np.asarray(K, float)
        self.chain = chain
        self.estimate_fk = estimate_fk
        self.estimate_scale = estimate_scale
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
            T_bg = self.chain.fk(s.joints + fk)
            T_bc = T_bg @ X                       # base->camera
            T_cb = inv_T(T_bc)                    # camera->base
            pb = self.board_points[s.corner_ids] * scale
            pw = transform_points(Tb, pb)          # board->base
            pc = transform_points(T_cb, pw)        # base->camera
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
) -> CalibResult:
    """Robust reprojection-error bundle for eye-in-hand. Returns a CalibResult; attach
    the `_Problem` on the result (`._problem`, `._x`) so metrics can reuse it."""
    prob = _Problem(
        samples, board_points, K, chain,
        estimate_fk=estimate_fk, estimate_scale=estimate_scale, weights=weights,
    )

    # Initialise from the closed form (+ a target-pose estimate for the board).
    if X_init is None:
        X_init = _closed_form_from_samples(samples, board_points, K)
    if Tboard_init is None:
        # board->base via the first sample: T_b_board = T_bg @ X @ T_c_board
        s0 = samples[0]
        T_c_board = _estimate_target_pose(s0, board_points, K)
        Tboard_init = s0.fk_pose @ X_init @ T_c_board

    x0 = prob.x0_from(X_init, Tboard_init)
    res = least_squares(
        prob.residual, x0,
        loss="huber" if robust else "linear", f_scale=f_scale,
        method="trf", max_nfev=400, x_scale="jac",
    )
    X, Tb, fk, scale = prob.unpack(res.x)
    r = res.fun
    n_obs = max(1, r.size // 2)
    rms = float(np.sqrt(np.sum(r ** 2) / r.size)) if r.size else 0.0

    result = CalibResult(
        T_optical=X, T_board=Tb, fk_offsets=fk, board_scale=scale,
        residual_px=rms, samples_used=len(samples),
        converged=bool(res.success), message=str(res.message),
        metrics={"train_rms_px": rms},
    )
    # Stash the problem + solution for metrics (covariance/observability/held-out).
    result._problem = prob   # type: ignore[attr-defined]
    result._x = res.x        # type: ignore[attr-defined]
    return result
