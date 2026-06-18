"""ArUco grid-board target — an `m_x × m_y` printed marker grid (`cv2.aruco.GridBoard`).
Many markers ⇒ many corners ⇒ a strong solve, while staying just-data: the board supplies
the object points, detection maps each seen marker's four corners to their point ids.

`build()` reads the grid geometry from cv2 (it needs the board layout), so it runs on the
robot / cv2 CI; synth builds simpler targets directly. Point id for marker index m,
corner c is `m*4 + c`, matching the concatenation order of `point_xyz`.
"""
from __future__ import annotations

from typing import Tuple

import numpy as np

from .base import Target


def _board(markers_x, markers_y, marker_len, marker_sep, dict_name):
    import cv2  # guarded
    aruco = cv2.aruco
    adict = aruco.getPredefinedDictionary(getattr(aruco, dict_name))
    if hasattr(aruco, "GridBoard") and not hasattr(aruco, "GridBoard_create"):
        board = aruco.GridBoard((markers_x, markers_y), marker_len, marker_sep, adict)  # >=4.7
    else:
        board = aruco.GridBoard_create(markers_x, markers_y, marker_len, marker_sep, adict)
    return cv2, aruco, adict, board


class ArucoGridDetector:
    def __init__(self, markers_x, markers_y, marker_len, marker_sep, dict):
        self.mx, self.my = int(markers_x), int(markers_y)
        self.marker_len, self.marker_sep = float(marker_len), float(marker_sep)
        self.dict_name = str(dict)

    def detect(self, image: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        cv2, aruco, adict, board = _board(self.mx, self.my, self.marker_len, self.marker_sep, self.dict_name)
        gray = image if image.ndim == 2 else cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        if hasattr(aruco, "ArucoDetector"):
            corners, ids, _ = aruco.ArucoDetector(adict, aruco.DetectorParameters()).detectMarkers(gray)
        else:
            corners, ids, _ = aruco.detectMarkers(gray, adict)
        if ids is None or len(ids) == 0:
            return np.empty(0, int), np.empty((0, 2), float)
        board_ids = list(np.asarray(board.getIds()).ravel().astype(int))
        out_ids, out_uv = [], []
        for mid, cs in zip(ids.flatten().astype(int), corners):
            if mid not in board_ids:
                continue
            m = board_ids.index(int(mid))
            quad = np.asarray(cs, float).reshape(4, 2)
            for c in range(4):
                out_ids.append(m * 4 + c)
                out_uv.append(quad[c])
        if not out_ids:
            return np.empty(0, int), np.empty((0, 2), float)
        return np.asarray(out_ids, int), np.asarray(out_uv, float)


def build(*, markers_x: int, markers_y: int, marker_len: float, marker_sep: float,
          dict: str = "DICT_4X4_50", min_points: int = 8) -> Target:
    cv2, aruco, adict, board = _board(markers_x, markers_y, marker_len, marker_sep, dict)
    objs = board.getObjPoints()                       # list of (4,3) per marker, board frame
    pts = np.concatenate([np.asarray(o, float).reshape(4, 3) for o in objs], axis=0)
    return Target(point_xyz=pts, detector=ArucoGridDetector(markers_x, markers_y, marker_len, marker_sep, dict),
                  min_points=int(min_points), planar=True, type="aruco_grid")
