"""Cartesian look-at next-pose generation — the fix for the "99% of suggestions miss the
marker" failure. The OLD generator perturbed JOINTS around the viewing config and hoped the
camera still saw the board; with a wrong hand-eye seed it usually didn't, and small joint
moves also killed the rotation diversity the solve needs (→ unobservable translation → 2x
garbage). The NEW generator works in CARTESIAN space, the way a human (and MoveIt) reasons:

  1. orbit the camera on a hemisphere over the MARKER, every pose LOOKING AT the marker
     (so the marker is in frame BY CONSTRUCTION, regardless of seed error);
  2. convert each desired camera pose -> flange pose via the current hand-eye X, then to
     joints by the engine's damped-least-squares IK (`Chain.ik`);
  3. keep only poses that are reachable (IK ok), keep the whole board in frame from the
     ACHIEVED pose, and pass the optional cuRobo feasibility filter;
  4. RANK by next-best-view information gain — the pose that most pins the under-observed
     DOF of X (`metrics.predicted_sigma_after`) — falling back, before any solve exists, to
     rotation diversity x centering.

Pure numpy (+ the engine IK). No cv2, no robot.
"""
from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

import numpy as np

from .geometry import (
    Chain,
    inv_T,
    make_T,
    project,
    rotation_angle,
    transform_points,
)
from .types import BoardModel, CaptureSample


def look_at(eye, target, up=(0.0, 0.0, 1.0)) -> np.ndarray:
    """Base->camera transform for a camera at `eye` whose +z (optical axis) points at
    `target`. Camera x = up x z, y = z x x (orthonormal). If `up` is ~parallel to the view
    direction, fall back to a different up so the basis stays well-conditioned."""
    eye = np.asarray(eye, float).reshape(3)
    target = np.asarray(target, float).reshape(3)
    z = target - eye
    nz = np.linalg.norm(z)
    if nz < 1e-9:
        z = np.array([0.0, 0.0, 1.0])
    else:
        z = z / nz
    up = np.asarray(up, float).reshape(3)
    if abs(float(up @ z)) > 0.95:                 # up nearly along the view → pick another
        up = np.array([1.0, 0.0, 0.0]) if abs(z[0]) < 0.9 else np.array([0.0, 1.0, 0.0])
    x = np.cross(up, z)
    x = x / (np.linalg.norm(x) + 1e-12)
    y = np.cross(z, x)
    R = np.stack([x, y, z], axis=1)
    return make_T(R, eye)


def _roll(T_cam: np.ndarray, roll: float) -> np.ndarray:
    """Rotate a camera pose about its own optical (+z) axis by `roll` rad — in-plane variety
    that adds rotation diversity without changing where the camera points."""
    c, s = np.cos(roll), np.sin(roll)
    Rz = np.array([[c, -s, 0.0], [s, c, 0.0], [0.0, 0.0, 1.0]])
    out = T_cam.copy()
    out[:3, :3] = T_cam[:3, :3] @ Rz
    return out


def _orbit_camera_poses(
    T_board: np.ndarray, normal_side: np.ndarray, *, n: int, dist_range: Tuple[float, float],
    el_max_deg: float, roll_max_deg: float, rng: np.random.Generator,
) -> List[np.ndarray]:
    """`n` camera poses on a hemisphere over the marker, each LOOKING AT the marker centre.
    `normal_side` is the unit direction (base frame) the visible face points toward (oriented
    toward the operator's camera side, §10.4), so we never orbit BEHIND the marker. Elevation
    is the polar angle off that normal (0 = head-on), azimuth sweeps around it, distance and
    roll are sampled in range — a spread that excites all three rotation axes of X."""
    c = T_board[:3, 3]
    n_hat = normal_side / (np.linalg.norm(normal_side) + 1e-12)
    # a tangent frame (t1, t2) spanning the plane perpendicular to n_hat
    ref = np.array([1.0, 0.0, 0.0]) if abs(n_hat[0]) < 0.9 else np.array([0.0, 1.0, 0.0])
    t1 = np.cross(n_hat, ref); t1 /= np.linalg.norm(t1) + 1e-12
    t2 = np.cross(n_hat, t1)
    el_max = np.radians(el_max_deg)
    roll_max = np.radians(roll_max_deg)
    poses: List[np.ndarray] = []
    for _ in range(n):
        el = el_max * np.sqrt(rng.random())                    # area-uniform on the cap
        az = rng.uniform(0.0, 2.0 * np.pi)
        d = np.cos(el) * n_hat + np.sin(el) * (np.cos(az) * t1 + np.sin(az) * t2)
        dist = rng.uniform(*dist_range)
        eye = c + dist * d
        poses.append(_roll(look_at(eye, c), rng.uniform(-roll_max, roll_max)))
    return poses


def _board_fully_visible(T_bc: np.ndarray, T_board: np.ndarray, board: BoardModel,
                         K: np.ndarray, wh: Tuple[int, int], margin_frac: float = 0.08,
                         max_oblique_deg: float = 70.0) -> bool:
    """The camera must FACE the target with slack: the whole target inside the central
    (1 - 2*margin) of the frame, in front (z>0), and not edge-on. Guards the ACHIEVED pose
    after IK (the look-at aims the centre; this confirms every corner actually fits)."""
    W, H = wh
    pc = transform_points(inv_T(T_bc), transform_points(T_board, board.points))
    if np.any(pc[:, 2] <= 0.05):
        return False
    normal_cam = (inv_T(T_bc) @ T_board)[:3, 2]
    view = pc.mean(0); view = view / (np.linalg.norm(view) + 1e-9)
    if np.degrees(np.arccos(np.clip(abs(float(normal_cam @ view)), 0.0, 1.0))) > max_oblique_deg:
        return False
    uv = project(K, pc)
    mx, my = margin_frac * W, margin_frac * H
    return bool(uv[:, 0].min() >= mx and uv[:, 0].max() < W - mx
                and uv[:, 1].min() >= my and uv[:, 1].max() < H - my)


def size_aware_dist_range(board: BoardModel, K: np.ndarray, wh: Tuple[int, int],
                          *, fill_lo: float = 0.35, fill_hi: float = 0.75) -> Tuple[float, float]:
    """The camera-to-board DISTANCE range derived from the BOARD'S PHYSICAL SIZE and the
    camera optics — the board should span fill_lo..fill_hi of the frame's minor dimension
    (large board in frame = sub-pixel corners worth capturing). Pixels spanned at distance d
    ≈ f·extent/d, so d = f·extent/(fill·minor). Replaces anchoring on wherever the camera
    happened to be (a far seed pose kept every candidate far → a small, information-poor board)."""
    pts = np.asarray(board.points, float)
    extent = max(float(np.max(pts.max(0) - pts.min(0))), 1e-3)
    f = float(min(K[0, 0], K[1, 1]))
    minor = float(min(wh))
    return f * extent / (fill_hi * minor), f * extent / (fill_lo * minor)


def _fill_frac(T_bc: np.ndarray, T_board: np.ndarray, board: BoardModel,
               K: np.ndarray, wh: Tuple[int, int]) -> float:
    """How much of the frame's minor dimension the projected board spans (0..~1) — bigger
    board = better corner signal-to-noise, so candidates that SEE MORE of it score higher."""
    pc = transform_points(inv_T(T_bc), transform_points(T_board, board.points))
    if np.any(pc[:, 2] <= 1e-6):
        return 0.0
    uv = project(K, pc)
    span = max(float(uv[:, 0].max() - uv[:, 0].min()), float(uv[:, 1].max() - uv[:, 1].min()))
    return span / float(min(wh))


def _centering(T_bc: np.ndarray, T_board: np.ndarray, board: BoardModel,
               K: np.ndarray, wh: Tuple[int, int]) -> float:
    """1.0 when the target centroid is at image centre, → 0 toward the edge (the safest
    framing against residual seed error)."""
    pc = transform_points(inv_T(T_bc), transform_points(T_board, board.points))
    cen = project(K, pc).mean(0)
    off = float(np.linalg.norm(cen - np.array([wh[0] / 2.0, wh[1] / 2.0])) / (0.5 * min(wh)))
    return max(0.0, 1.0 - off)


def _predict_corners(chain: Chain, X: np.ndarray, T_board: np.ndarray, board: BoardModel,
                     K: np.ndarray, q: np.ndarray) -> np.ndarray:
    pc = transform_points(inv_T(chain.fk(q) @ X), transform_points(T_board, board.points))
    return project(K, pc)


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
    q_seed: Optional[np.ndarray] = None,
    feasible=None,
    weak_axis: Optional[np.ndarray] = None,       # accepted for back-compat; NBV uses `result`
    result=None,
    n_cand: int = 32,
    max_keep: int = 18,
    el_max_deg: float = 50.0,
    roll_max_deg: float = 35.0,
    visible_margin: float = 0.18,
) -> Tuple[Optional[np.ndarray], float]:
    """Return (joints, score) for the next pose, or (None, 0.0) if nothing reachable+visible
    was found. `score` is the chosen pose's rotation diversity vs the collected set (rad), so
    the UI's "next-pose gain" stays meaningful in BOTH selection regimes.

    Selection: with a solve (`result`), pick the pose MINIMISING the predicted worst
    translation σ of X (true next-best-view, `metrics.predicted_sigma_after`); before any
    solve, pick the pose maximising rotation-diversity × centring. Either way the marker is in
    frame by construction (look-at) and the pose is IK-reachable + feasibility-filtered."""
    q_seed = (np.asarray(nominal_joints, float) if q_seed is None and nominal_joints is not None
              else np.zeros(chain.n) if q_seed is None else np.asarray(q_seed, float))
    c = T_board_est[:3, 3]
    cam0 = (chain.fk(q_seed) @ X_est)[:3, 3]
    dist0 = float(np.linalg.norm(cam0 - c))
    dist0 = dist0 if dist0 > 0.05 else 0.4
    # SIZE-AWARE distance: derived from the board's physical extent + optics (the board fills
    # a healthy fraction of the frame), NOT from wherever the camera happens to be. The seed
    # anchor survives only as the fallback when nothing in the size-aware band is reachable.
    # The band depends on how much we trust X: with a solve in hand (NBV mode) the look-at
    # miss is small, so go CLOSE — board large, corners sharp, capped by the visibility
    # margin; before any solve the seed may be degrees wrong, so keep a wide buffer (the
    # 12°-wrong-seed regression test pins this).
    if result is not None:
        fill_band = (0.35, max(0.40, 1.0 - 2.0 * visible_margin - 0.06))
        # Hand-eye information grows with ROTATION magnitude and axis diversity (the classic
        # Tsai-Lenz error bounds; the NBV literature's main lever) — and roll about the optical
        # axis is FREE (the marker stays framed). With a trusted X, sample a wider rotation
        # envelope; IK + visibility + the collision filter still cull what the rig can't do.
        el_max_deg = max(el_max_deg, 60.0)
        roll_max_deg = max(roll_max_deg, 60.0)
    else:
        fill_band = (0.20, 0.40)
    dist_size = size_aware_dist_range(board, K, wh, fill_lo=fill_band[0], fill_hi=fill_band[1])
    # the visible face points toward the camera side (never orbit behind the marker)
    normal = T_board_est[:3, 2].copy()
    if float(normal @ (cam0 - c)) < 0:
        normal = -normal
    collected_R = [(chain.fk(np.asarray(q, float)) @ X_est)[:3, :3] for q in collected_joints]

    def _gather(dist_range):
        cam_poses = _orbit_camera_poses(T_board_est, normal, n=n_cand, dist_range=dist_range,
                                        el_max_deg=el_max_deg, roll_max_deg=roll_max_deg, rng=rng)
        out = []   # (q, T_bc, diversity_rad)
        for T_cam_des in cam_poses:
            T_flange_des = T_cam_des @ inv_T(X_est)        # camera = flange ∘ X  ->  flange = cam ∘ X⁻¹
            q, ok = chain.ik(T_flange_des, q_seed, iters=45, pos_tol=2e-4, rot_tol=3e-3)
            if not ok:
                continue
            T_bc = chain.fk(q) @ X_est                     # ACHIEVED camera pose (under the seed)
            # A GENEROUS margin here is deliberate: the pose is checked against the (possibly
            # wrong) seed X, so a wide buffer keeps the marker in frame under the TRUE X too —
            # the residual miss is bounded by f·tan(seed_error), so we leave room for it.
            if not _board_fully_visible(T_bc, T_board_est, board, K, wh, margin_frac=visible_margin):
                continue
            if feasible is not None and not feasible(q):
                continue
            div = min((rotation_angle(R.T @ T_bc[:3, :3]) for R in collected_R), default=float(np.pi))
            out.append((q, T_bc, div))
            if len(out) >= max_keep:                       # enough to rank — stop IK'ing
                break
        return out

    candidates = _gather(dist_size)
    if not candidates:                                     # size-band unreachable → seed anchor
        candidates = _gather((0.85 * dist0, 1.2 * dist0))
    if not candidates:
        return None, 0.0

    if result is not None:
        # NBV: choose the candidate that most pins the under-observed DOF of X.
        from .metrics import predicted_sigma_after
        best = None
        best_sigma = float("inf")
        for q, T_bc, div in candidates:
            cand = CaptureSample(id=-1, joints=q, fk_pose=chain.fk(q),
                                 corner_ids=board.point_ids,
                                 corners=_predict_corners(chain, X_est, T_board_est, board, K, q))
            sigma = predicted_sigma_after(result, cand, board.points, K, chain)
            # tiny tiebreaks so near-equal candidates prefer a safe framing AND a LARGE board
            # (bigger board in frame = better corner signal-to-noise)
            score = (sigma - 0.01 * _centering(T_bc, T_board_est, board, K, wh)
                     - 0.01 * min(_fill_frac(T_bc, T_board_est, board, K, wh) / 0.6, 1.0))
            if score < best_sigma:
                best_sigma, best = score, (q, div)
        return best[0], float(best[1])

    # pre-solve: rotation diversity × centring × FILL (see most of the board, large)
    best_q, best_score, best_div = None, -1.0, 0.0
    for q, T_bc, div in candidates:
        fill = min(_fill_frac(T_bc, T_board_est, board, K, wh) / 0.6, 1.0)
        s = div * (0.4 + 0.6 * _centering(T_bc, T_board_est, board, K, wh)) * (0.5 + 0.5 * fill)
        if s > best_score:
            best_score, best_q, best_div = s, q, div
    return best_q, float(best_div)
