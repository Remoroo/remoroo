"""Classic checkerboard target — `cv2.findChessboardCorners` over the inner corners. The
object points are the canonical OpenCV ordering (`mgrid[0:cols,0:rows]`), so the detected
corner order matches `point_xyz` row-for-row. cv2 returns the whole board or nothing, so a
usable view needs every inner corner (min_points = cols*rows).

`build()` is cv2-free (the points are a numpy grid); only `detect()` touches cv2.
"""
from __future__ import annotations

from typing import Tuple

import numpy as np

from .base import Target


def board_points(cols: int, rows: int, square_len: float) -> np.ndarray:
    objp = np.zeros((rows * cols, 3), float)
    objp[:, :2] = np.mgrid[0:cols, 0:rows].T.reshape(-1, 2) * float(square_len)
    return objp


class CheckerboardDetector:
    def __init__(self, cols: int, rows: int):
        self.cols, self.rows = int(cols), int(rows)

    def detect(self, image: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        import cv2  # guarded
        gray = image if image.ndim == 2 else cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        found, corners = cv2.findChessboardCorners(gray, (self.cols, self.rows))
        if not found or corners is None:
            return np.empty(0, int), np.empty((0, 2), float)
        term = (cv2.TERM_CRITERIA_EPS + cv2.TERM_CRITERIA_MAX_ITER, 40, 1e-3)
        cv2.cornerSubPix(gray, corners, (7, 7), (-1, -1), term)
        uv = np.asarray(corners, float).reshape(-1, 2)
        return np.arange(uv.shape[0]), uv


def build(*, cols: int, rows: int, square_len: float, min_points: int = 0) -> Target:
    pts = board_points(cols, rows, square_len)
    return Target(point_xyz=pts, detector=CheckerboardDetector(cols, rows),
                  min_points=int(min_points) or cols * rows, planar=True, type="checkerboard")
