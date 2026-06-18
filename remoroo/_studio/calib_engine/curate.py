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
    from .geometry import inv_T, project, transform_points

    X, Tb, fk, scale = result.T_optical, result.T_board, result.fk_offsets, result.board_scale
    flagged: Dict[int, List[int]] = {}
    for s in samples:
        T_cb = inv_T(chain.fk(s.joints + fk) @ X)
        pw = transform_points(Tb, board_points[s.corner_ids] * scale)
        uv = project(K, transform_points(T_cb, pw))
        err = np.linalg.norm(uv - s.corners, axis=1)
        bad = [int(s.corner_ids[i]) for i in np.where(err > px_thresh)[0]]
        if bad:
            flagged[s.id] = bad
    return flagged


def resnap_corner(image: np.ndarray, approx_uv) -> "np.ndarray":
    """Lock a coarse human click to the true sub-pixel corner (cv2.cornerSubPix).
    Guarded import: only available where cv2 is (robot / cv2 CI image)."""
    import cv2  # noqa: PLC0415  (intentional guarded import)

    gray = image if image.ndim == 2 else cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    pt = np.array([[approx_uv]], dtype=np.float32)
    term = (cv2.TERM_CRITERIA_EPS + cv2.TERM_CRITERIA_MAX_ITER, 40, 1e-3)
    cv2.cornerSubPix(gray, pt, (7, 7), (-1, -1), term)
    return pt[0, 0]
