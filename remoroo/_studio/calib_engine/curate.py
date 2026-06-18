"""Operator corner curation — the proven high-accuracy path (Bouguet/photogrammetry).

Two safe operations:
  * exclude  — drop whole images or individual corners (robust weighting). Always safe;
               directly attacks the "one bad detection swings the closed form" failure.
  * re-snap  — a coarse human drag is locked sub-pixel by cornerSubPix (needs cv2, so
               it's a thin guarded wrapper, exercised on-robot / in the cv2 CI image).

`solve_curated` re-solves with the operator's exclusions; the caller compares HELD-OUT
error before/after so a curation that doesn't help the *unseen* poses is rejected
(keeps the human from overfitting the training residual).
"""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Sequence, Set

import numpy as np

from .geometry import Chain
from .solve import solve_eye_in_hand
from .types import CalibResult, CaptureSample


def build_weights(
    samples: Sequence[CaptureSample],
    *,
    exclude_samples: Optional[Iterable[int]] = None,
    exclude_corners: Optional[Dict[int, Iterable[int]]] = None,
) -> List[np.ndarray]:
    """Per-corner {0,1} weights honouring the operator's exclusions. `exclude_samples`
    is a set of sample ids; `exclude_corners` maps sample id -> set of corner ids."""
    ex_s: Set[int] = set(exclude_samples or [])
    ex_c: Dict[int, Set[int]] = {k: set(v) for k, v in (exclude_corners or {}).items()}
    weights: List[np.ndarray] = []
    for s in samples:
        w = np.ones(len(s.corner_ids))
        if s.id in ex_s:
            w[:] = 0.0
        for ci, cid in enumerate(s.corner_ids):
            if int(cid) in ex_c.get(s.id, set()):
                w[ci] = 0.0
        weights.append(w)
    return weights


def solve_curated(
    samples: Sequence[CaptureSample],
    board_points: np.ndarray,
    K: np.ndarray,
    chain: Chain,
    *,
    exclude_samples: Optional[Iterable[int]] = None,
    exclude_corners: Optional[Dict[int, Iterable[int]]] = None,
    **solve_kw,
) -> CalibResult:
    weights = build_weights(samples, exclude_samples=exclude_samples, exclude_corners=exclude_corners)
    return solve_eye_in_hand(samples, board_points, K, chain, weights=weights, **solve_kw)


def flag_suspect_corners(
    result: CalibResult,
    samples: Sequence[CaptureSample],
    board_points: np.ndarray,
    K: np.ndarray,
    chain: Chain,
    *,
    px_thresh: float = 2.0,
) -> Dict[int, List[int]]:
    """Per-image, the corner ids whose reprojection error exceeds `px_thresh` — the
    handful the Studio pre-flags so the operator only inspects what matters."""
    from .metrics import predict_uv

    flagged: Dict[int, List[int]] = {}
    for s in samples:
        uv = predict_uv(result, s, board_points, K, chain)
        err = np.linalg.norm(uv - s.corners, axis=1)
        bad = [int(s.corner_ids[i]) for i in np.where(err > px_thresh)[0]]
        if bad:
            flagged[s.id] = bad
    return flagged


def homography_resnap(sample: CaptureSample, board_points: np.ndarray, corner_id: int) -> np.ndarray:
    """Re-derive ONE corner's pixel from the planar structure of the board: fit the
    homography board(x,y) -> pixels from the OTHER (good) corners of this image (DLT), then
    map the target corner's board point through it. Pure numpy — the geometric truth from
    the board plane, available without cv2 and without the image. cornerSubPix (below)
    then locks it to the image gradient when cv2 + the frame are present."""
    ids = list(sample.corner_ids.astype(int))
    if corner_id not in ids:
        raise ValueError(f"corner {corner_id} not in sample {sample.id}")
    ti = ids.index(corner_id)
    src = board_points[sample.corner_ids][:, :2]      # (M,2) board plane
    dst = np.asarray(sample.corners, float)           # (M,2) pixels
    keep = [i for i in range(len(ids)) if i != ti]
    if len(keep) < 4:
        raise ValueError("need >=4 other corners to fit a homography")
    H = _fit_homography(src[keep], dst[keep])
    p = H @ np.array([src[ti, 0], src[ti, 1], 1.0])
    return p[:2] / p[2]


def _fit_homography(src: np.ndarray, dst: np.ndarray) -> np.ndarray:
    """Direct linear transform: 3x3 H with dst ~ H @ [src;1], from >=4 correspondences."""
    A = []
    for (x, y), (u, v) in zip(src, dst):
        A.append([-x, -y, -1, 0, 0, 0, u * x, u * y, u])
        A.append([0, 0, 0, -x, -y, -1, v * x, v * y, v])
    _, _, Vt = np.linalg.svd(np.asarray(A, float))
    H = Vt[-1].reshape(3, 3)
    return H / H[2, 2]


def resnap_corner(image: np.ndarray, approx_uv) -> "np.ndarray":
    """Lock a coarse human click to the true sub-pixel corner (cv2.cornerSubPix).
    Guarded import: only available where cv2 is (robot / cv2 CI image)."""
    import cv2  # noqa: PLC0415  (intentional guarded import)

    gray = image if image.ndim == 2 else cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    pt = np.array([[approx_uv]], dtype=np.float32)
    term = (cv2.TERM_CRITERIA_EPS + cv2.TERM_CRITERIA_MAX_ITER, 40, 1e-3)
    cv2.cornerSubPix(gray, pt, (7, 7), (-1, -1), term)
    return pt[0, 0]
