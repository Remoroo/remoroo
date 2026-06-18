"""Single-ArUco target — one printed marker, four corners. Works for any predefined
dictionary (4X4 / 5X5 / 6X6 and the AprilTag families), so a rig that printed a single
marker is just data, not a special case the engine has to learn.

Corner order matches `cv2.aruco.detectMarkers` (clockwise from top-left), so the object
points line up with the detection without a remap:
    0 top-left   1 top-right   2 bottom-right   3 bottom-left   (+y up, board z=0)
`build()` is cv2-free (the points are a numpy square), so synth can construct this target
in the numpy venv; only `detect()` touches cv2 (the bridge, on the robot).
"""
from __future__ import annotations

from typing import Tuple

import numpy as np

from .base import Target


def marker_object_points(size_m: float) -> np.ndarray:
    s = float(size_m) / 2.0
    return np.array([[-s, s, 0.0], [s, s, 0.0], [s, -s, 0.0], [-s, -s, 0.0]], float)


class SingleArucoDetector:
    def __init__(self, dict: str, id: int):
        self.dict_name = str(dict)
        self.marker_id = int(id)

    def _detector(self):
        import cv2  # guarded
        aruco = cv2.aruco
        adict = aruco.getPredefinedDictionary(getattr(aruco, self.dict_name))
        if hasattr(aruco, "ArucoDetector"):                 # OpenCV >= 4.7
            params = aruco.DetectorParameters()
            params.cornerRefinementMethod = aruco.CORNER_REFINE_SUBPIX
            return cv2, aruco, aruco.ArucoDetector(adict, params), adict
        return cv2, aruco, None, adict

    def detect(self, image: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        cv2, aruco, detector, adict = self._detector()
        gray = image if image.ndim == 2 else cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        if detector is not None:
            corners, ids, _ = detector.detectMarkers(gray)
        else:
            corners, ids, _ = aruco.detectMarkers(gray, adict)
        if ids is None or self.marker_id not in ids.flatten():
            return np.empty(0, int), np.empty((0, 2), float)
        i = int(np.where(ids.flatten() == self.marker_id)[0][0])
        uv = np.asarray(corners[i], float).reshape(4, 2)
        return np.arange(4), uv


def build(*, dict: str = "DICT_4X4_50", id: int = 0, size_m: float = 0.05) -> Target:
    return Target(point_xyz=marker_object_points(size_m),
                  detector=SingleArucoDetector(dict, id),
                  min_points=4, planar=True, type="single_aruco")
