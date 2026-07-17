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


def refine_target_pose(sample: CaptureSample, board_points: np.ndarray, K: np.ndarray,
                       T_seed: np.ndarray) -> Tuple[np.ndarray, float]:
    """LM-refine a board->camera pose from a GIVEN seed (not the canonical seeds). Used to
    explore the planar-PnP flip neighbourhood for a single marker: seed from a flipped pose
    and refine, so `_estimate_target_pose_robust` can pick the flip consistent across poses.
    Returns (board->camera, residual cost)."""
    pts = board_points[sample.corner_ids]
    uv = sample.corners

    def resid(x):
        T = make_T(rodrigues(x[:3]), x[3:])
        return (project(K, transform_points(T, pts)) - uv).ravel()

    x0 = np.concatenate([rotvec_from_R(T_seed[:3, :3]), T_seed[:3, 3]])
    res = least_squares(resid, x0, method="lm", max_nfev=200)
    return make_T(rodrigues(res.x[:3]), res.x[3:]), float(res.cost)


def _flip_seed(T: np.ndarray) -> np.ndarray:
    """The SECOND branch of the planar two-fold ambiguity (Collins & Bartoli, IJCV'14): the
    board plane's normal reflected about the camera→board viewing direction, with minimal
    in-plane change. Refining from this seed lands in the other basin when the view is
    weak-perspective (small/distant/near-fronto-parallel board) — the regime where the
    lowest-residual pick is wrong ~50% of the time."""
    R, t = T[:3, :3], T[:3, 3]
    v = t / (np.linalg.norm(t) + 1e-12)              # camera → board centre (optical ray)
    n = R[:, 2]
    n2 = 2.0 * float(n @ v) * v - n                  # reflect the plane normal about the ray
    axis = np.cross(n, n2)
    s_ = np.linalg.norm(axis)
    c_ = float(np.clip(n @ n2, -1.0, 1.0))
    if s_ < 1e-9:                                    # already fronto-parallel → no distinct flip
        return T.copy()
    R_align = rodrigues((axis / s_) * np.arctan2(s_, c_))
    return make_T(R_align @ R, t)


def _planar_pose_candidates(pts: np.ndarray, uv: np.ndarray, K: np.ndarray):
    """BOTH ambiguity branches of the planar board pose, LM-refined, sorted by cost:
    [(T, rms_px), ...] (1-2 entries; near-duplicates merged). The single-pose
    `_estimate_target_pose` keeps the lowest-cost one (unchanged behaviour); the base-to-base
    solve keeps BOTH and lets cross-view/cross-camera consistency pick the branch — the
    published fix for frozen-PnP flip commitment (IPPE returns both for the same reason)."""
    def resid(x):
        T = make_T(rodrigues(x[:3]), x[3:])
        pc = transform_points(T, pts)
        z = np.maximum(pc[:, 2], 1e-3)      # depth-clamped: a diverging flip-basin LM must
        u = pc[:, 0] / z * K[0, 0] + K[0, 2]  # yield large finite residuals, not inf/NaN
        v = pc[:, 1] / z * K[1, 1] + K[1, 2]
        return (np.stack([u, v], axis=1) - uv).ravel()

    seeds = [np.array([0.0, 0.0, 0.0, 0.0, 0.0, 0.5])]  # board facing the camera
    try:
        T0 = _planar_pose_init(pts, uv, K)
        seeds.insert(0, np.concatenate([rotvec_from_R(T0[:3, :3]), T0[:3, 3]]))
        seeds.insert(1, np.concatenate([rotvec_from_R(_flip_seed(T0)[:3, :3]), _flip_seed(T0)[:3, 3]]))
    except Exception:  # noqa: BLE001
        pass
    sols = []
    for x0 in seeds:
        res = least_squares(resid, x0, method="lm", max_nfev=200)
        T = make_T(rodrigues(res.x[:3]), res.x[3:])
        rms = float(np.sqrt(np.mean(res.fun ** 2))) if res.fun.size else float("inf")
        sols.append((T, rms))
    sols.sort(key=lambda tr: tr[1])
    kept = []
    for T, rms in sols:                              # merge same-basin refinements
        from .geometry import transform_geodesic
        if all(transform_geodesic(T, Tk)[0] > 5.0 or transform_geodesic(T, Tk)[1] > 0.01
               for Tk, _ in kept) or not kept:
            kept.append((T, rms))
        if len(kept) == 2:
            break
    return kept


def _estimate_target_pose(sample: CaptureSample, board_points: np.ndarray, K: np.ndarray) -> np.ndarray:
    """Board->camera pose from observed corners — the LOWEST-COST branch of
    `_planar_pose_candidates` (unchanged single-pose behaviour; see it for the ambiguity
    story). (cv2.solvePnP on-robot; numpy here for CI.)"""
    cands = _planar_pose_candidates(board_points[sample.corner_ids], sample.corners, K)
    return cands[0][0]


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
        estimate_intrinsics: bool = False,
    ):
        self.samples = list(samples)
        self.board_points = np.asarray(board_points, float)
        self.K = np.asarray(K, float)
        self.chain = chain
        self.estimate_fk = estimate_fk
        # Board SCALE and the focal length are degenerate (a bigger board / longer focal / nearer
        # board all rescale the image the same way), so we never estimate both: when refining the
        # intrinsics we FIX scale=1 (the board's printed geometry is the metric anchor that makes
        # the focal length identifiable across diverse views — exactly how camera calibration works).
        self.estimate_intrinsics = estimate_intrinsics
        self.estimate_scale = estimate_scale and not estimate_intrinsics
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
        # parameter layout: [ X(6) | board(6) | fk(n_fk) | scale(0|1) | intrinsics(0|4) ]
        self.i_X = slice(0, 6)
        self.i_board = slice(6, 12)
        self.i_fk = slice(12, 12 + self.n_fk)
        base = 12 + self.n_fk
        self.i_scale = base if self.estimate_scale else None
        base += 1 if self.estimate_scale else 0
        self.i_intr = slice(base, base + 4) if self.estimate_intrinsics else None
        self.nparams = base + (4 if self.estimate_intrinsics else 0)

    def unpack(self, x: np.ndarray):
        X = make_T(rodrigues(x[self.i_X][:3]), x[self.i_X][3:])
        Tb = make_T(rodrigues(x[self.i_board][:3]), x[self.i_board][3:])
        fk = np.zeros(self.J)
        if self.estimate_fk and self.n_fk > 0:
            fk[self.fk_lo : self.fk_lo + self.n_fk] = x[self.i_fk]
        scale = float(x[self.i_scale]) if self.estimate_scale else 1.0
        if self.estimate_intrinsics:
            fx, fy, cx, cy = x[self.i_intr]
            K = np.array([[fx, 0.0, cx], [0.0, fy, cy], [0.0, 0.0, 1.0]])
        else:
            K = self.K
        return X, Tb, fk, scale, K

    def residual(self, x: np.ndarray) -> np.ndarray:
        X, Tb, fk, scale, K = self.unpack(x)
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
            uv = project(K, pc)
            r = (uv - s.corners) * w[:, None]
            out.append(r.ravel())
        return np.concatenate(out) if out else np.zeros(0)

    def x0_from(self, X: np.ndarray, Tb: np.ndarray) -> np.ndarray:
        x = np.zeros(self.nparams)
        x[self.i_X] = np.concatenate([rotvec_from_R(X[:3, :3]), X[:3, 3]])
        x[self.i_board] = np.concatenate([rotvec_from_R(Tb[:3, :3]), Tb[:3, 3]])
        if self.estimate_scale:
            x[self.i_scale] = 1.0
        if self.estimate_intrinsics:
            x[self.i_intr] = [self.K[0, 0], self.K[1, 1], self.K[0, 2], self.K[1, 2]]
        return x


def solve_eye_in_hand(
    samples: Sequence[CaptureSample],
    board_points: np.ndarray,
    K: np.ndarray,
    chain: Chain,
    *,
    estimate_fk: bool = True,
    estimate_scale: bool = True,
    estimate_intrinsics: bool = False,
    weights: Optional[Sequence[np.ndarray]] = None,
    robust: bool = True,
    f_scale: float = 1.5,
    X_init: Optional[np.ndarray] = None,
    Tboard_init: Optional[np.ndarray] = None,
    mode: str = "eye_in_hand",
) -> CalibResult:
    """Robust reprojection-error bundle. `mode` selects the observation model:
    eye_in_hand (camera on flange, board fixed) or eye_to_hand (static camera, board on
    the gripper). With `estimate_intrinsics`, the camera K (fx,fy,cx,cy) is refined JOINTLY
    (board scale held fixed — they're degenerate), turning this into a full camera+hand-eye
    calibration; only sound with a multi-point board over diverse views. Returns a CalibResult
    (`.K` = the K actually used/refined); the `_Problem` is stashed on it for metrics."""
    prob = _Problem(
        samples, board_points, K, chain,
        estimate_fk=estimate_fk, estimate_scale=estimate_scale, weights=weights, mode=mode,
        estimate_intrinsics=estimate_intrinsics,
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
    X, Tb, fk, scale, K_used = prob.unpack(res.x)
    r = res.fun
    rms = float(np.sqrt(np.sum(r ** 2) / r.size)) if r.size else 0.0

    result = CalibResult(
        T_optical=X, T_board=Tb, fk_offsets=fk, board_scale=scale,
        residual_px=rms, samples_used=len(samples), kind=mode,
        converged=bool(res.success), message=str(res.message),
        K=np.asarray(K_used, float),
        metrics={"train_rms_px": rms},
    )
    # Stash the problem + solution for metrics (covariance/observability/held-out).
    result._problem = prob   # type: ignore[attr-defined]
    result._x = res.x        # type: ignore[attr-defined]
    return result


class _StaticProblem:
    """A world-fixed camera localized by pooled PnP: params are [rotvec(3), trans(3)] of the
    board->camera pose, residual is the reprojection over ALL pooled corners (px). Stashed on
    the result so `metrics.observability` gives the per-DOF camera-pose σ for free — the
    optical-axis (depth) translation is the loud weak DOF when the board is near-frontal /
    one placement. The [rotvec, trans] layout is the one the covariance metrics slice."""

    def __init__(self, pts: np.ndarray, uv: np.ndarray, K: np.ndarray):
        self.pts = np.asarray(pts, float)
        self.uv = np.asarray(uv, float)
        self.K = np.asarray(K, float)
        self.nparams = 6

    def residual(self, x: np.ndarray) -> np.ndarray:
        T = make_T(rodrigues(x[:3]), x[3:])
        return (project(self.K, transform_points(T, self.pts)) - self.uv).ravel()

    def x0_from(self, T: np.ndarray) -> np.ndarray:
        return np.concatenate([rotvec_from_R(T[:3, :3]), T[:3, 3]])


def solve_static_camera(
    views: Sequence[CaptureSample],
    board_points: np.ndarray,
    K: np.ndarray,
    *,
    robust: bool = True,
    f_scale: float = 1.5,
) -> CalibResult:
    """A WORLD-FIXED camera, operator-hand-moved board (eye-to-hand, handheld). The board is
    placed where the camera should be ANCHORED (the world/reference frame) and a few views
    are captured with NO robot motion; the camera is localized by a robust single-pose PnP
    that pools ALL views' corners (the board is one fixed placement, so every view shares the
    same board->camera). Result `T_optical` = board->camera = world->optical (board = world
    origin). If the operator moved the board between views, held-out reprojection blows up —
    the honest signal to keep it fixed. No kinematics, no FK correction. The bundle problem is
    stashed on the result so the covariance/observability meter works (the depth caveat)."""
    pts = np.concatenate([np.asarray(board_points)[v.corner_ids] for v in views])
    uv = np.concatenate([np.asarray(v.corners, float) for v in views])
    T0 = _estimate_target_pose(views[0], board_points, K)     # PnP init from the first view
    prob = _StaticProblem(pts, uv, K)
    x0 = prob.x0_from(T0)
    res = least_squares(prob.residual, x0, loss="huber" if robust else "linear", f_scale=f_scale,
                        method="trf", max_nfev=400)
    T = make_T(rodrigues(res.x[:3]), res.x[3:])
    rms = float(np.sqrt(np.mean(res.fun ** 2))) if res.fun.size else 0.0
    result = CalibResult(
        T_optical=T, T_board=np.eye(4), fk_offsets=np.zeros(0), board_scale=1.0,
        residual_px=rms, samples_used=len(views), kind="static",
        converged=bool(res.success), message=str(res.message),
        K=np.asarray(K, float), metrics={"train_rms_px": rms},
    )
    # Stash the problem + solution for the covariance metrics (per-DOF camera-pose σ).
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


# --------------------------------------------------------------------------- #
# Base-to-base: branch-aware selection (P0) + two-camera pixel bundle (P1/P3). #
# Research grounding (2026-07 deep-research, all claims 3-0 verified):         #
#  - frozen per-view PnP poses can commit to the WRONG planar-ambiguity branch #
#    at low pixel residual (Collins&Bartoli IJCV'14; Ch'ng ICRA'20) — the      #
#    published mechanism matching the live 35.7mm-at-σ1mm failure;             #
#  - SOTA estimators are joint image-space bundles insensitive to per-view PnP #
#    error (Koide&Menegatti ICRA'19; Ali et al. Sensors'19);                   #
#  - joint kinematic offsets belong in the bundle only with enough views and a #
#    good init (Pradeep'10; Stepanova RCIM'22 local-minima warning).           #
# --------------------------------------------------------------------------- #
def _view_candidates(o: dict, board_points: np.ndarray):
    """Per-camera branch candidates for ONE shared view: BOTH planar-ambiguity branches from
    the stored corners (new-style obs), or the single frozen pose (legacy obs without
    corners). Returns ([(T, rms_px)...]_a, [...]_b)."""
    P = np.asarray(board_points, float)
    out = []
    for side in ("a", "b"):
        ids, uv, K = o.get(f"ids_{side}"), o.get(f"uv_{side}"), o.get(f"K_{side}")
        if ids is not None and uv is not None and K is not None and len(ids) >= 4:
            out.append(_planar_pose_candidates(P[np.asarray(ids, int)],
                                               np.asarray(uv, float), np.asarray(K, float)))
        else:
            out.append([(np.asarray(o[f"T_c{side}_marker"], float), float("nan"))])
    return out[0], out[1]


def select_b2b_branches(
    obs: Sequence[dict], X_a: np.ndarray, X_b: np.ndarray, chain_a: Chain, chain_b: Chain,
    board_points: np.ndarray, *, fk_a: Optional[np.ndarray] = None,
    fk_b: Optional[np.ndarray] = None, iters: int = 5,
) -> Tuple[List[dict], List[dict], np.ndarray]:
    """P0 — branch-AWARE base-to-base: never commit a view's planar-flip branch in isolation.
    Alternate (1) per view, pick the branch combination minimising THAT view's inter-arm
    corner disagreement under the current T_AB, (2) re-solve T_AB on the selected branches —
    until the selection is stable. Returns (obs with the selected poses substituted,
    per-view diagnostics, T_AB). Legacy obs (no corners) degrade to their single pose."""
    fa = np.zeros(chain_a.n) if fk_a is None else np.asarray(fk_a, float)
    fb = np.zeros(chain_b.n) if fk_b is None else np.asarray(fk_b, float)
    P = np.asarray(board_points, float)
    cands = [_view_candidates(o, P) for o in obs]
    TA_base = [chain_a.fk(np.asarray(o["joints_a"], float) + fa) @ X_a for o in obs]
    TB_base = [chain_b.fk(np.asarray(o["joints_b"], float) + fb) @ X_b for o in obs]

    def obs_with(sel):
        out = []
        for o, (ia, ib), (ca, cb) in zip(obs, sel, cands):
            oo = dict(o)
            oo["T_ca_marker"], oo["T_cb_marker"] = ca[ia][0], cb[ib][0]
            out.append(oo)
        return out

    def view_cost(k, ia, ib, T_AB):
        PA = transform_points(TA_base[k] @ cands[k][0][ia][0], P)
        PB = transform_points(TB_base[k] @ cands[k][1][ib][0], P)
        d = PA - transform_points(T_AB, PB)
        return float(np.sqrt(np.mean(np.sum(d ** 2, axis=1))))

    sel = [(0, 0)] * len(obs)                        # start at each view's lowest-cost branch
    T_AB = solve_base_to_base(obs_with(sel), X_a, X_b, chain_a, chain_b, fk_a=fa, fk_b=fb)
    for _ in range(max(1, iters)):
        new_sel = []
        for k, (ca, cb) in enumerate(cands):
            combos = [(ia, ib) for ia in range(len(ca)) for ib in range(len(cb))]
            new_sel.append(min(combos, key=lambda c: view_cost(k, c[0], c[1], T_AB)))
        changed = new_sel != sel
        sel = new_sel
        T_AB = solve_base_to_base_bundle(obs_with(sel), X_a, X_b, chain_a, chain_b, P,
                                         fk_a=fa, fk_b=fb).T_optical
        if not changed:
            break
    diag = []
    for k, ((ca, cb), (ia, ib)) in enumerate(zip(cands, sel)):
        # ambiguity-proneness: the OTHER branch explains the pixels nearly as well (the formal
        # weak-perspective condition) — flag it even when the selection is right
        def _amb(cl):
            return bool(len(cl) == 2 and np.isfinite(cl[1][1])
                        and cl[1][1] < 3.0 * max(cl[0][1], 1e-6))
        diag.append({"view": k, "agreement_mm": round(view_cost(k, ia, ib, T_AB) * 1000.0, 3),
                     "flipped": bool((ia, ib) != (0, 0)),
                     "ambiguous_a": _amb(ca), "ambiguous_b": _amb(cb)})
    return obs_with(sel), diag, T_AB


class _B2BPixelProblem:
    """P1 — the joint TWO-CAMERA reprojection bundle (the SOTA formulation): parameters
    x = [xi_AB(6) | board-in-baseA pose per view (6·V) | dq_a(na), dq_b(nb) (optional P3)],
    residuals = PIXEL reprojection of every shared corner in BOTH wrist cameras (Huber at the
    caller). Per-view board poses are PARAMETERS, so per-view PnP error (incl. the flip
    ambiguity) is absorbed instead of frozen in; xi_AB FIRST means the existing
    σ²(JᵀJ)⁻¹ machinery hands back the MARGINALISED T_AB covariance with no extra code."""

    def __init__(self, obs, X_a, X_b, chain_a, chain_b, board_points, fk_a, fk_b,
                 estimate_offsets: bool):
        self.obs = list(obs)
        self.X_a, self.X_b = np.asarray(X_a, float), np.asarray(X_b, float)
        self.chain_a, self.chain_b = chain_a, chain_b
        self.P = np.asarray(board_points, float)
        self.fa = np.asarray(fk_a, float) if fk_a is not None else np.zeros(chain_a.n)
        self.fb = np.asarray(fk_b, float) if fk_b is not None else np.zeros(chain_b.n)
        self.est_off = bool(estimate_offsets)
        self.V = len(self.obs)
        self.na, self.nb = chain_a.n, chain_b.n
        # GAUGE FIX: each arm's FIRST joint offset is degenerate with T_AB (a base-joint
        # offset is exactly a rotation of every flange pose about that arm's base axis,
        # absorbable by T_AB + the free per-view board poses) — estimating it would let the
        # bundle trade a real transform error for a fictitious offset. Joints 1..n-1 only.
        self.noff_a = max(0, self.na - 1) if self.est_off else 0
        self.noff_b = max(0, self.nb - 1) if self.est_off else 0
        self.nparams = 6 + 6 * self.V + self.noff_a + self.noff_b

    def unpack(self, x):
        T_AB = make_T(rodrigues(x[:3]), x[3:6])
        boards = [make_T(rodrigues(x[6 + 6 * i:9 + 6 * i]), x[9 + 6 * i:12 + 6 * i])
                  for i in range(self.V)]
        dqa, dqb = np.zeros(self.na), np.zeros(self.nb)
        if self.est_off:
            o0 = 6 + 6 * self.V
            dqa[1:] = x[o0:o0 + self.noff_a]
            dqb[1:] = x[o0 + self.noff_a:o0 + self.noff_a + self.noff_b]
        return T_AB, boards, dqa, dqb

    @staticmethod
    def _proj(K, pc, uv):
        """Projection residual with the depth CLAMPED away from zero — TRF explores poses
        that put corners at z≈0 mid-iteration; a finite (large) residual keeps the solver on
        a smooth barrier instead of inf/NaN wrecking the step."""
        z = np.maximum(pc[:, 2], 1e-3)
        u = pc[:, 0] / z * K[0, 0] + K[0, 2]
        v = pc[:, 1] / z * K[1, 1] + K[1, 2]
        return (np.stack([u, v], axis=1) - uv).ravel()

    def residual(self, x):
        T_AB, boards, dqa, dqb = self.unpack(x)
        T_BA = inv_T(T_AB)
        out = []
        for i, o in enumerate(self.obs):
            Ta = self.chain_a.fk(np.asarray(o["joints_a"], float) + self.fa + dqa) @ self.X_a
            Tb = self.chain_b.fk(np.asarray(o["joints_b"], float) + self.fb + dqb) @ self.X_b
            bA = boards[i]
            pa = transform_points(inv_T(Ta) @ bA, self.P[np.asarray(o["ids_a"], int)])
            out.append(self._proj(np.asarray(o["K_a"], float), pa, np.asarray(o["uv_a"], float)))
            pb = transform_points(inv_T(Tb) @ (T_BA @ bA), self.P[np.asarray(o["ids_b"], int)])
            out.append(self._proj(np.asarray(o["K_b"], float), pb, np.asarray(o["uv_b"], float)))
        return np.concatenate(out) if out else np.zeros(0)

    def sparsity(self):
        """Residual-block structure for TRF's grouped finite differences: view i's pixels
        depend only on xi_AB, board_i, and the offsets — this is what keeps the bundle fast
        at MANY poses (the whole point of the many-poses regime)."""
        from scipy.sparse import lil_matrix
        rows = []
        for o in self.obs:
            rows.append(2 * len(o["ids_a"]) + 2 * len(o["ids_b"]))
        S = lil_matrix((int(sum(rows)), self.nparams), dtype=np.uint8)
        r0 = 0
        for i, nr in enumerate(rows):
            S[r0:r0 + nr, 0:6] = 1
            S[r0:r0 + nr, 6 + 6 * i:12 + 6 * i] = 1
            if self.est_off:
                S[r0:r0 + nr, 6 + 6 * self.V:] = 1
            r0 += nr
        return S


def solve_base_to_base_pixel(
    obs: Sequence[dict], X_a: np.ndarray, X_b: np.ndarray, chain_a: Chain, chain_b: Chain,
    board_points: np.ndarray, *, fk_a: Optional[np.ndarray] = None,
    fk_b: Optional[np.ndarray] = None, estimate_joint_offsets: bool = False,
    robust: bool = True, f_scale_px: float = 2.0, max_offset_rad: float = 0.03,
) -> CalibResult:
    """P1 (+P3) — solve T_baseA_baseB by the joint two-camera PIXEL bundle. Requires
    corner-bearing obs (ids/uv/K per camera) — raises ValueError otherwise so callers fall
    back to the legacy 3D bundle. Init = P0 branch-aware selection (certified start before
    the nonlinear bundle, per the local-minima literature). With `estimate_joint_offsets`,
    per-joint corrections of BOTH arms join the parameter vector, bounded to ±max_offset_rad
    so they can never absorb gross pose error."""
    if not obs:
        raise ValueError("no observations")
    for o in obs:
        for k in ("ids_a", "uv_a", "K_a", "ids_b", "uv_b", "K_b"):
            if o.get(k) is None:
                raise ValueError("pixel bundle needs corner-bearing obs (ids/uv/K per camera)")
    P = np.asarray(board_points, float)
    sel_obs, diag, T_AB0 = select_b2b_branches(obs, X_a, X_b, chain_a, chain_b, P,
                                               fk_a=fk_a, fk_b=fk_b)
    fa = np.asarray(fk_a, float) if fk_a is not None else np.zeros(chain_a.n)
    fb = np.asarray(fk_b, float) if fk_b is not None else np.zeros(chain_b.n)
    prob = _B2BPixelProblem(obs, X_a, X_b, chain_a, chain_b, P, fa, fb, estimate_joint_offsets)
    x0 = np.zeros(prob.nparams)
    x0[:3], x0[3:6] = rotvec_from_R(T_AB0[:3, :3]), T_AB0[:3, 3]
    for i, o in enumerate(sel_obs):
        bA = (chain_a.fk(np.asarray(o["joints_a"], float) + fa) @ X_a
              @ np.asarray(o["T_ca_marker"], float))
        x0[6 + 6 * i:9 + 6 * i] = rotvec_from_R(bA[:3, :3])
        x0[9 + 6 * i:12 + 6 * i] = bA[:3, 3]
    lo = np.full(prob.nparams, -np.inf)
    hi = np.full(prob.nparams, np.inf)
    if estimate_joint_offsets:
        lo[6 + 6 * prob.V:], hi[6 + 6 * prob.V:] = -max_offset_rad, max_offset_rad
    res = least_squares(prob.residual, x0, loss="huber" if robust else "linear",
                        f_scale=f_scale_px, method="trf", jac_sparsity=prob.sparsity(),
                        bounds=(lo, hi), max_nfev=400)
    T_AB, _boards, dqa, dqb = prob.unpack(res.x)
    # the task-space headline, computed the SAME way as the legacy path (comparable numbers):
    # frozen selected branches vs the final T_AB, with the estimated offsets folded in
    PA, PB = _b2b_corner_pairs(sel_obs, X_a, X_b, chain_a, chain_b, P,
                               fk_a=fa + dqa, fk_b=fb + dqb)
    d = PA - transform_points(T_AB, PB) if PA.shape[0] else np.zeros((0, 3))
    agreement_mm = float(np.sqrt(np.mean(np.sum(d ** 2, axis=1))) * 1000.0) if d.shape[0] else float("nan")
    pix = prob.residual(res.x)
    result = CalibResult(
        T_optical=T_AB, T_board=np.eye(4),
        fk_offsets=(np.concatenate([dqa, dqb]) if estimate_joint_offsets else np.zeros(0)),
        board_scale=1.0, residual_px=float(np.sqrt(np.mean(pix ** 2))) if pix.size else 0.0,
        samples_used=len(obs), kind="base_to_base", converged=bool(res.success),
        message=str(res.message),
        metrics={"agreement_mm": agreement_mm, "method": "pixel_bundle",
                 "pixel_rms_px": round(float(np.sqrt(np.mean(pix ** 2))), 3) if pix.size else 0.0,
                 "offsets_estimated": bool(estimate_joint_offsets),
                 "flipped_views": int(sum(1 for dv in diag if dv["flipped"]))},
    )
    result._problem = prob   # type: ignore[attr-defined]
    result._x = res.x        # type: ignore[attr-defined]
    result._per_view = diag  # type: ignore[attr-defined]
    return result


B2B_OFFSETS_MIN_VIEWS = 20   # joint offsets join the bundle only with this many shared views
                             # (identifiability + the local-minima literature; the held-out
                             # guard below still has to APPROVE them)


def _b2b_heldout_mm(test_obs, T_AB, X_a, X_b, chain_a, chain_b, board_points, fk_a, fk_b):
    """Task-space agreement (mm) of `T_AB` on UNSEEN shared views — per-view flip branches are
    picked under the GIVEN T_AB (a handful of test views can't stably vote alone)."""
    P = np.asarray(board_points, float)
    tot, n = 0.0, 0
    for o in test_obs:
        ca, cb = _view_candidates(o, P)
        TA = chain_a.fk(np.asarray(o["joints_a"], float) + fk_a) @ X_a
        TB = chain_b.fk(np.asarray(o["joints_b"], float) + fk_b) @ X_b
        best = np.inf
        for Tca, _ in ca:
            PA = transform_points(TA @ Tca, P)
            for Tcb, _ in cb:
                PB = transform_points(TB @ Tcb, P)
                d = PA - transform_points(T_AB, PB)
                best = min(best, float(np.mean(np.sum(d ** 2, axis=1))))
        tot += best * P.shape[0]
        n += P.shape[0]
    return float(np.sqrt(tot / max(n, 1)) * 1000.0)


def solve_base_to_base_auto(
    obs: Sequence[dict], X_a: np.ndarray, X_b: np.ndarray, chain_a: Chain, chain_b: Chain,
    board_points: np.ndarray, *, fk_a: Optional[np.ndarray] = None,
    fk_b: Optional[np.ndarray] = None, min_views_for_offsets: int = B2B_OFFSETS_MIN_VIEWS,
) -> Tuple[CalibResult, dict]:
    """THE base-to-base solve policy, in one place. Corner-bearing obs → P0 branch-aware
    selection + P1 pixel bundle; ≥min_views_for_offsets views → ALSO try P3 joint offsets,
    kept only when HELD-OUT agreement improves (never blind trust in extra parameters).
    Legacy obs (no corners) → branch-aware selection over whatever candidates exist + the
    3D bundle (old behaviour, still flip-hardened where corners allow). Returns
    (result, report) — the report carries method / per-view diagnostics / the offsets trial."""
    fa = np.asarray(fk_a, float) if fk_a is not None else np.zeros(chain_a.n)
    fb = np.asarray(fk_b, float) if fk_b is not None else np.zeros(chain_b.n)
    P = np.asarray(board_points, float)
    have_px = bool(obs) and all(
        all(o.get(k) is not None for k in ("ids_a", "uv_a", "K_a", "ids_b", "uv_b", "K_b"))
        for o in obs)
    if not have_px:
        sel_obs, diag, _ = select_b2b_branches(obs, X_a, X_b, chain_a, chain_b, P,
                                               fk_a=fa, fk_b=fb)
        r = solve_base_to_base_bundle(sel_obs, X_a, X_b, chain_a, chain_b, P, fk_a=fa, fk_b=fb)
        r.metrics["method"] = "3d_bundle_branchaware"
        r.metrics["flipped_views"] = int(sum(1 for dv in diag if dv["flipped"]))
        r._per_view = diag   # type: ignore[attr-defined]
        return r, {"method": r.metrics["method"], "per_view": diag,
                   "offsets": {"tried": False, "kept": False}}

    r = solve_base_to_base_pixel(obs, X_a, X_b, chain_a, chain_b, P, fk_a=fa, fk_b=fb,
                                 estimate_joint_offsets=False)
    report = {"method": "pixel_bundle", "per_view": r._per_view,
              "offsets": {"tried": False, "kept": False}}
    if len(obs) >= int(min_views_for_offsets):
        k = max(3, len(obs) // 3)
        train, test = list(obs[:-k]), list(obs[-k:])
        try:
            rb = solve_base_to_base_pixel(train, X_a, X_b, chain_a, chain_b, P,
                                          fk_a=fa, fk_b=fb, estimate_joint_offsets=False)
            ro = solve_base_to_base_pixel(train, X_a, X_b, chain_a, chain_b, P,
                                          fk_a=fa, fk_b=fb, estimate_joint_offsets=True)
            dqa, dqb = ro.fk_offsets[:chain_a.n], ro.fk_offsets[chain_a.n:]
            h0 = _b2b_heldout_mm(test, rb.T_optical, X_a, X_b, chain_a, chain_b, P, fa, fb)
            h1 = _b2b_heldout_mm(test, ro.T_optical, X_a, X_b, chain_a, chain_b, P,
                                 fa + dqa, fb + dqb)
            report["offsets"] = {"tried": True, "kept": bool(h1 < h0),
                                 "heldout_base_mm": round(h0, 3),
                                 "heldout_offsets_mm": round(h1, 3)}
            if h1 < h0:                          # offsets EARNED their place on unseen views
                r = solve_base_to_base_pixel(obs, X_a, X_b, chain_a, chain_b, P,
                                             fk_a=fa, fk_b=fb, estimate_joint_offsets=True)
                report["method"] = "pixel_bundle+joint_offsets"
                report["per_view"] = r._per_view
        except Exception as e:  # noqa: BLE001 — the offsets trial must never sink the solve
            report["offsets"] = {"tried": True, "kept": False, "error": f"{type(e).__name__}: {e}"}
    r.metrics["method"] = report["method"]
    return r, report


# --------------------------------------------------------------------------- #
# Base-to-base bundle (covariance-bearing) — the dual-arm analog of            #
# solve_eye_in_hand: refine T_AB and carry the Fisher information for metrics. #
# --------------------------------------------------------------------------- #
def _b2b_corner_pairs(
    obs: Sequence[dict], X_a: np.ndarray, X_b: np.ndarray, chain_a: Chain, chain_b: Chain,
    board_points: np.ndarray, *, fk_a: Optional[np.ndarray] = None, fk_b: Optional[np.ndarray] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """The shared marker's board CORNERS reconstructed in BOTH arms' base frames. Returns
    (PA, PB), each (N,3): PA[k] is corner k as arm A's wrist cam saw it mapped into baseA,
    PB[k] the SAME physical corner as arm B's cam saw it mapped into baseB. A correct
    T_baseA_baseB maps every PB onto its PA, so `PA - T_AB·PB` is the inter-arm disagreement
    (the task-space metric AND the bundle residual). Using the marker's CORNERS (the full
    pose, a spanning plane) — not just its origin — pins T_AB's rotation from one placement."""
    fa = np.zeros(chain_a.n) if fk_a is None else np.asarray(fk_a, float)
    fb = np.zeros(chain_b.n) if fk_b is None else np.asarray(fk_b, float)
    P = np.asarray(board_points, float)
    PA: List[np.ndarray] = []
    PB: List[np.ndarray] = []
    for o in obs:
        TA = chain_a.fk(np.asarray(o["joints_a"], float) + fa) @ X_a @ np.asarray(o["T_ca_marker"], float)
        TB = chain_b.fk(np.asarray(o["joints_b"], float) + fb) @ X_b @ np.asarray(o["T_cb_marker"], float)
        PA.append(transform_points(TA, P))
        PB.append(transform_points(TB, P))
    if not PA:
        return np.zeros((0, 3)), np.zeros((0, 3))
    return np.concatenate(PA), np.concatenate(PB)


class _B2BProblem:
    """The base-to-base bundle: parameters are T_baseA_baseB as [rotvec(3), trans(3)] (the
    layout `metrics.observability` slices into deg/mm), residual is the 3D disagreement
    `PA - T_AB·PB` over every shared corner (metres). Stashed on the result so the covariance
    metrics (observability / well_observed / weak_rotation_axis) read it with no changes."""

    def __init__(self, PA: np.ndarray, PB: np.ndarray):
        self.PA = np.asarray(PA, float)
        self.PB = np.asarray(PB, float)
        self.nparams = 6

    def residual(self, x: np.ndarray) -> np.ndarray:
        T = make_T(rodrigues(x[:3]), x[3:])
        return (self.PA - transform_points(T, self.PB)).ravel()

    def x0_from(self, T: np.ndarray) -> np.ndarray:
        return np.concatenate([rotvec_from_R(T[:3, :3]), T[:3, 3]])


def solve_base_to_base_bundle(
    obs: Sequence[dict], X_a: np.ndarray, X_b: np.ndarray, chain_a: Chain, chain_b: Chain,
    board_points: np.ndarray, *, fk_a: Optional[np.ndarray] = None,
    fk_b: Optional[np.ndarray] = None, robust: bool = True, f_scale: float = 0.005,
) -> CalibResult:
    """Refine T_baseA_baseB with a small bundle that minimises the shared marker's corner
    disagreement across both arms (init from the closed-form average). Returns a CalibResult
    (kind 'base_to_base', `T_optical` = T_AB) carrying the bundle problem for the covariance
    metrics — the dual-arm analog of solve_eye_in_hand. `metrics['agreement_mm']` is the RMS
    inter-arm point error (the headline task-space number)."""
    T0 = solve_base_to_base(obs, X_a, X_b, chain_a, chain_b, fk_a=fk_a, fk_b=fk_b)
    PA, PB = _b2b_corner_pairs(obs, X_a, X_b, chain_a, chain_b, board_points, fk_a=fk_a, fk_b=fk_b)
    prob = _B2BProblem(PA, PB)
    x0 = prob.x0_from(T0)
    if PA.shape[0] >= 1:
        res = least_squares(prob.residual, x0, loss="huber" if robust else "linear",
                            f_scale=f_scale, method="trf", max_nfev=200)
        x, r, success, msg = res.x, res.fun, bool(res.success), str(res.message)
    else:
        x, r, success, msg = x0, np.zeros(0), False, "no observations"
    T_AB = make_T(rodrigues(x[:3]), x[3:])
    d = PA - transform_points(T_AB, PB) if PA.shape[0] else np.zeros((0, 3))
    agreement_mm = float(np.sqrt(np.mean(np.sum(d ** 2, axis=1))) * 1000.0) if d.shape[0] else float("nan")
    result = CalibResult(
        T_optical=T_AB, T_board=np.eye(4), fk_offsets=np.zeros(0), board_scale=1.0,
        residual_px=0.0, samples_used=len(obs), kind="base_to_base",
        converged=success, message=msg, metrics={"agreement_mm": agreement_mm},
    )
    result._problem = prob   # type: ignore[attr-defined]
    result._x = x            # type: ignore[attr-defined]
    return result
