"""Honest quality metrics — the accept gate is built on these, NEVER on the training
residual. Pure numpy + scipy.

  * held_out_reprojection  — RMS px on poses NOT used in the fit (generalisation).
  * tip_landing_error      — task-space mm: predict a world fiducial from a camera view
                             via the calibration; compare to truth (the physical tip test).
  * observability          — per-DOF std of X from the parameter covariance (which axes
                             are pinned; flags unobserved DOF so collection is guided).
  * consensus_spread       — disagreement across bootstrap closed-form solves (deg).
"""
from __future__ import annotations

from typing import Dict, List, Sequence

import numpy as np

from .geometry import Chain, inv_T, project, transform_points
from .solve import _Problem, solve_handeye_closed, _estimate_target_pose
from .types import CalibResult, CaptureSample


def predict_uv(
    result: CalibResult, s: CaptureSample, board_points: np.ndarray, K: np.ndarray, chain: Chain,
) -> np.ndarray:
    """Predicted pixels for a sample's corners under the calibration — the single
    mode-aware reprojection used by held-out, the contact-sheet, and curation flagging
    (so eye-to-hand is never scored with the eye-in-hand model)."""
    X, Tb, fk, scale = result.T_optical, result.T_board, result.fk_offsets, result.board_scale
    pb = board_points[s.corner_ids] * scale
    if result.kind == "static":
        # world-fixed camera, board at the world/reference origin: X = board->camera, so the
        # board points project straight through X. No kinematics (the camera doesn't move).
        return project(K, transform_points(X, pb))
    T_fk = chain.fk(s.joints + fk)
    if result.kind == "eye_to_hand":
        pc = transform_points(inv_T(X), transform_points(T_fk @ Tb, pb))
    else:
        pc = transform_points(inv_T(T_fk @ X), transform_points(Tb, pb))
    return project(K, pc)


def held_out_reprojection(
    result: CalibResult,
    test_samples: Sequence[CaptureSample],
    board_points: np.ndarray,
    K: np.ndarray,
    chain: Chain,
) -> float:
    """RMS reprojection (px) of the calibration on poses it never saw."""
    sq = 0.0
    n = 0
    for s in test_samples:
        d = predict_uv(result, s, board_points, K, chain) - s.corners
        sq += float(np.sum(d ** 2))
        n += d.size
    return float(np.sqrt(sq / n)) if n else 0.0


def stereo_consistency(X_left: np.ndarray, X_right: np.ndarray, sdk_T_lr: np.ndarray) -> float:
    """Stereo self-check (F9): the two independently-solved hand-eye results must agree
    with the SDK's factory left->right extrinsic. Returns the rotation disagreement (deg)
    between inv(X_left) @ X_right and the SDK `T_L_R` — a free sanity check on a ZED."""
    from .geometry import transform_geodesic
    measured = inv_T(X_left) @ X_right
    ang, _ = transform_geodesic(measured, np.asarray(sdk_T_lr, float))
    return float(ang)


def tip_landing_error(
    result: CalibResult,
    fiducial_obs: Sequence[dict],
    chain: Chain,
) -> float:
    """Task-space accuracy in mm. Each obs = {joints, p_cam, p_base_true}: the camera
    measured a fixed fiducial at `p_cam` (camera frame) from configuration `joints`;
    the calibration predicts it in base as FK(joints+fk) @ X @ p_cam, which is where
    the arm would drive the TCP. Compare to the true base location. Returns RMS mm."""
    X, fk = result.T_optical, result.fk_offsets
    sq = 0.0
    n = 0
    for o in fiducial_obs:
        T_bc = chain.fk(np.asarray(o["joints"], float) + fk) @ X
        p_pred = transform_points(T_bc, np.asarray(o["p_cam"], float).reshape(1, 3))[0]
        d = p_pred - np.asarray(o["p_base_true"], float)
        sq += float(np.dot(d, d))
        n += 1
    return float(np.sqrt(sq / n) * 1000.0) if n else 0.0


def fiducial_consistency_mm(
    result: CalibResult, fiducial_obs: Sequence[dict], chain: Chain,
) -> float:
    """Task-space accuracy WITHOUT ground truth (the on-robot tip metric, 7.1): the camera
    sees ONE fixed world point from several poses; each obs = {joints, p_cam}. A correct
    calibration maps them all to the SAME base point — so the RMS spread of the predicted
    base positions (mm) is a real tip-landing accuracy, runnable on hardware with no known
    target. (tip_landing_error needs p_base_true; this needs only the repeated sighting.)"""
    X, fk = result.T_optical, result.fk_offsets
    pts = []
    for o in fiducial_obs:
        T_bc = chain.fk(np.asarray(o["joints"], float) + fk) @ X
        pts.append(transform_points(T_bc, np.asarray(o["p_cam"], float).reshape(1, 3))[0])
    if len(pts) < 2:
        return float("nan")
    P = np.asarray(pts, float)
    return float(np.sqrt(np.mean(np.sum((P - P.mean(0)) ** 2, axis=1))) * 1000.0)


def reprojection_detail(
    result: CalibResult,
    samples: Sequence[CaptureSample],
    board_points: np.ndarray,
    K: np.ndarray,
    chain: Chain,
) -> List[Dict]:
    """Per-capture, per-corner reprojection: detected vs predicted pixels + the error.
    Powers the Studio's held-out overlay + the curate contact-sheet (corners coloured by
    error) so the operator inspects only the handful that matter."""
    out: List[Dict] = []
    for s in samples:
        uv = predict_uv(result, s, board_points, K, chain)
        err = np.linalg.norm(uv - s.corners, axis=1)
        out.append({
            "id": int(s.id),
            "corner_ids": s.corner_ids.astype(int).tolist(),
            "detected": np.round(s.corners, 2).tolist(),
            "predicted": np.round(uv, 2).tolist(),
            "err_px": np.round(err, 3).tolist(),
            "mean_px": float(err.mean()) if err.size else 0.0,
            "max_px": float(err.max()) if err.size else 0.0,
        })
    return out


def _clean(v):
    return np.nan_to_num(v, nan=0.0, posinf=0.0, neginf=0.0)


def _residual_jacobian(prob: _Problem, x: np.ndarray) -> np.ndarray:
    """Finite-difference the reprojection residual Jacobian (m residuals x p params) at x."""
    with np.errstate(all="ignore"):
        r0 = _clean(prob.residual(x))
        m, p = r0.size, x.size
        Jm = np.zeros((m, p))
        eps = 1e-6
        for k in range(p):
            dx = np.zeros(p)
            dx[k] = eps
            Jm[:, k] = (_clean(prob.residual(x + dx)) - r0) / eps
    return np.clip(_clean(Jm), -1e6, 1e6)


def _reprojection_information(result: CalibResult):
    """The Fisher information of the solve: (JᵀJ, sigma², prob, x). Cov = sigma² (JᵀJ)⁻¹.
    Shared by `parameter_covariance` and the next-best-view `predicted_sigma_after`."""
    prob: _Problem = result._problem   # type: ignore[attr-defined]
    x: np.ndarray = result._x          # type: ignore[attr-defined]
    Jm = _residual_jacobian(prob, x)
    r0 = _clean(prob.residual(x))
    with np.errstate(all="ignore"):
        JtJ = _clean(Jm.T @ Jm)
    dof = max(1, r0.size - x.size)
    sigma2 = float(np.sum(r0 ** 2) / dof)
    return JtJ, sigma2, prob, x


def _cov_from_info(JtJ: np.ndarray, sigma2: float) -> np.ndarray:
    try:
        return np.linalg.inv(JtJ) * sigma2
    except np.linalg.LinAlgError:
        return np.linalg.pinv(JtJ) * sigma2


def parameter_covariance(result: CalibResult) -> np.ndarray:
    """Covariance of the full parameter vector at the solution, via a finite-difference
    Jacobian of the reprojection residual: Cov = sigma^2 (J^T J)^-1."""
    JtJ, sigma2, _, _ = _reprojection_information(result)
    return _cov_from_info(JtJ, sigma2)


def well_observed(result: CalibResult, *, rot_sigma_deg: float = 0.5,
                  trans_sigma_mm: float = 2.0):
    """The OBSERVABILITY ACCEPT GATE (the fix for the silent 2x-translation failure): a
    calibration is only trustworthy if EVERY DOF of X is pinned. Returns (ok, detail) where
    `ok` requires the worst rotation 1σ < `rot_sigma_deg` AND the worst translation 1σ <
    `trans_sigma_mm`. A low-rotation-diversity collection leaves translation unobservable →
    huge trans σ → ok=False, so the gate refuses it instead of accepting garbage."""
    obs = observability(result)
    worst_rot = float(obs["rot_worst_deg"])
    worst_trans = float(max(obs["tx_mm"], obs["ty_mm"], obs["tz_mm"]))
    ok = bool(worst_rot < rot_sigma_deg and worst_trans < trans_sigma_mm)
    detail = {"observable": ok, "worst_rot_sigma_deg": round(worst_rot, 4),
              "worst_trans_sigma_mm": round(worst_trans, 4),
              "rot_sigma_deg_limit": rot_sigma_deg, "trans_sigma_mm_limit": trans_sigma_mm}
    detail.update({k: round(float(v), 4) for k, v in obs.items()})
    return ok, detail


def predicted_sigma_after(result: CalibResult, candidate: CaptureSample,
                          board_points: np.ndarray, K: np.ndarray, chain: Chain) -> float:
    """Next-best-view information gain (rigorous, not a heuristic): the worst translation 1σ
    (mm) of X that WOULD result if `candidate` (a hypothetical capture, corners predicted
    under the current X) were added — by appending its residual Jacobian to the Fisher
    information JᵀJ and re-inverting. Posegen ranks candidates by how much this DROPS the
    worst σ vs the current solve, i.e. which pose most pins the under-observed DOF."""
    JtJ, sigma2, prob, x = _reprojection_information(result)
    prob1 = _Problem([candidate], board_points, K, chain,
                     estimate_fk=prob.estimate_fk, estimate_scale=prob.estimate_scale,
                     weights=None, mode=prob.mode)
    Jc = _residual_jacobian(prob1, x)
    with np.errstate(all="ignore"):
        cov = _cov_from_info(JtJ + _clean(Jc.T @ Jc), sigma2)
    std = np.sqrt(np.clip(np.diag(cov), 0.0, None))
    return float(np.max(std[3:6]) * 1000.0)   # worst translation σ in mm


def observability(result: CalibResult) -> Dict[str, float]:
    """Per-DOF 1-sigma of the hand-eye X (rotation in deg, translation in mm). Larger =
    less observed. The first 3 params are the X rotation vector, next 3 the translation."""
    cov = parameter_covariance(result)
    std = np.sqrt(np.clip(np.diag(cov), 0.0, None))
    rot_deg = np.degrees(std[0:3])
    trans_mm = std[3:6] * 1000.0
    return {
        "Rx_deg": float(rot_deg[0]), "Ry_deg": float(rot_deg[1]), "Rz_deg": float(rot_deg[2]),
        "tx_mm": float(trans_mm[0]), "ty_mm": float(trans_mm[1]), "tz_mm": float(trans_mm[2]),
        "rot_worst_deg": float(np.max(rot_deg)),
    }


def weak_rotation_axis(result: CalibResult) -> np.ndarray:
    """The least-observed rotation axis of X (camera frame): the top eigenvector of the
    rotation block of the parameter covariance. Feeds posegen's information-gain so the
    next pose excites the DOF the solve is still uncertain in (7.6)."""
    cov = parameter_covariance(result)
    w, V = np.linalg.eigh(cov[0:3, 0:3])
    return V[:, int(np.argmax(w))]


def consensus_spread(
    samples: Sequence[CaptureSample],
    board_points: np.ndarray,
    K: np.ndarray,
    *,
    n_boot: int = 6,
    subset: float = 0.7,
    seed: int = 0,
) -> float:
    """Run the closed form on bootstrap subsets; return the worst pairwise rotation
    disagreement (deg). Tight cluster = trustworthy; wide = collect more/better data."""
    from .geometry import transform_geodesic

    rng = np.random.default_rng(seed)
    sols = []
    idx = np.arange(len(samples))
    k = max(3, int(len(samples) * subset))
    for _ in range(n_boot):
        pick = rng.choice(idx, size=k, replace=False)
        sub = [samples[i] for i in pick]
        T_bg = [s.fk_pose for s in sub]
        T_ct = [_estimate_target_pose(s, board_points, K) for s in sub]
        try:
            sols.append(solve_handeye_closed(T_bg, T_ct))
        except ValueError:
            continue
    worst = 0.0
    for i in range(len(sols)):
        for j in range(i + 1, len(sols)):
            ang, _ = transform_geodesic(sols[i], sols[j])
            worst = max(worst, ang)
    return float(worst)
