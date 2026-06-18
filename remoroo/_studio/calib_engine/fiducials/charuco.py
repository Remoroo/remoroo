"""ChArUco target — the version-robust detector ported from the old `detect.py`.

OpenCV >=4.7 moved detection into `CharucoDetector`; <4.7 used free functions. This hides
both behind the `FiducialDetector` contract so the engine never names cv2's shifting API.
The interior chessboard corners are the points; cv2's id order is the point order, so the
solver's `point_xyz[ids]` matches the detection exactly.
"""
from __future__ import annotations

from typing import Tuple

import numpy as np

from .base import Target


def _aruco():
    import cv2  # guarded — only imported on the robot / cv2 CI, never at library import
    return cv2, cv2.aruco


def _make_board(aruco, sx, sy, sl, ml, adict):
    if hasattr(aruco, "CharucoBoard"):
        try:
            return aruco.CharucoBoard((sx, sy), sl, ml, adict)      # >= 4.7 signature
        except TypeError:
            return aruco.CharucoBoard_create(sx, sy, sl, ml, adict)  # legacy signature
    return aruco.CharucoBoard_create(sx, sy, sl, ml, adict)


class CharucoDetector:
    def __init__(self, squares_x: int, squares_y: int, square_len: float,
                 marker_len: float, dict: str = "DICT_5X5_1000"):
        self.squares_x = int(squares_x)
        self.squares_y = int(squares_y)
        self.square_len = float(square_len)
        self.marker_len = float(marker_len)
        self.dict_name = str(dict)

    def _board(self):
        cv2, aruco = _aruco()
        adict = aruco.getPredefinedDictionary(getattr(aruco, self.dict_name))
        return cv2, aruco, adict, _make_board(aruco, self.squares_x, self.squares_y,
                                              self.square_len, self.marker_len, adict)

    def detect(self, image: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        cv2, aruco, adict, board = self._board()
        gray = image if image.ndim == 2 else cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
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

    def board_points(self) -> np.ndarray:
        """The interior-corner 3D coords (board frame, z=0) in cv2's id order."""
        _, _, _, board = self._board()
        return np.asarray(board.getChessboardCorners(), float).reshape(-1, 3)


def build(*, squares_x: int, squares_y: int, square_len: float, marker_len: float,
          dict: str = "DICT_5X5_1000", min_points: int = 6) -> Target:
    det = CharucoDetector(squares_x, squares_y, square_len, marker_len, dict)
    return Target(point_xyz=det.board_points(), detector=det,
                  min_points=int(min_points), planar=True, type="charuco")
