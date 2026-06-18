"""Version-robust ChArUco/ArUco detection (P1) — the ONLY module needing cv2, so it is
imported lazily by callers (never at package import). OpenCV >=4.7 moved detection into
`ArucoDetector`/`CharucoDetector` classes and removed the free `detectMarkers`; this
module hides that behind one stable API so the agent never guesses it.

Returns (corner_ids, corners_px) aligned arrays — the same shape `CaptureSample` wants.
Tested in the cv2 CI image / on the robot, not in the numpy-only `.venv`.
"""
from __future__ import annotations

from typing import Tuple

import numpy as np


def _aruco():
    import cv2  # guarded
    return cv2, cv2.aruco


def detect_charuco(image: np.ndarray, *, squares_x: int, squares_y: int,
                   square_len: float, marker_len: float, dict_name: str = "DICT_5X5_1000"
                   ) -> Tuple[np.ndarray, np.ndarray]:
    """Detect ChArUco interior corners. Works across OpenCV >=4.7 (new classes) and the
    legacy free-function API (<4.7)."""
    cv2, aruco = _aruco()
    gray = image if image.ndim == 2 else cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    adict = aruco.getPredefinedDictionary(getattr(aruco, dict_name))
    board = _make_board(aruco, squares_x, squares_y, square_len, marker_len, adict)

    if hasattr(aruco, "CharucoDetector"):                 # OpenCV >= 4.7
        det = aruco.CharucoDetector(board)
        ch_corners, ch_ids, _, _ = det.detectBoard(gray)
    else:                                                 # OpenCV < 4.7 fallback
        m_corners, m_ids, _ = aruco.detectMarkers(gray, adict)
        if m_ids is None or len(m_ids) == 0:
            return np.empty(0, int), np.empty((0, 2), float)
        _, ch_corners, ch_ids = aruco.interpolateCornersCharuco(m_corners, m_ids, gray, board)

    if ch_ids is None or len(ch_ids) == 0:
        return np.empty(0, int), np.empty((0, 2), float)
    return ch_ids.ravel().astype(int), ch_corners.reshape(-1, 2).astype(float)


def charuco_board_points(*, squares_x: int, squares_y: int, square_len: float,
                         marker_len: float, dict_name: str = "DICT_5X5_1000") -> np.ndarray:
    """The 3D interior-corner coordinates (board frame, z=0) in cv2's id order, so the
    engine's BoardModel indexes corners exactly as detect_charuco returns them."""
    cv2, aruco = _aruco()
    adict = aruco.getPredefinedDictionary(getattr(aruco, dict_name))
    board = _make_board(aruco, squares_x, squares_y, square_len, marker_len, adict)
    corners = board.getChessboardCorners()
    return np.asarray(corners, float).reshape(-1, 3)


def _make_board(aruco, sx, sy, sl, ml, adict):
    if hasattr(aruco, "CharucoBoard"):
        try:
            return aruco.CharucoBoard((sx, sy), sl, ml, adict)      # >= 4.7 signature
        except TypeError:
            return aruco.CharucoBoard_create(sx, sy, sl, ml, adict)  # legacy signature
    return aruco.CharucoBoard_create(sx, sy, sl, ml, adict)
