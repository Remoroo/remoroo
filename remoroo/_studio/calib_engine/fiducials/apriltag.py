"""AprilTag target — a single tag, detected through cv2.aruco's AprilTag dictionaries
(`DICT_APRILTAG_36h11` etc.). Mechanically the same four-corner detection as a single
ArUco marker; kept as its own registry type so an authored pipeline can say what it is and
default to an AprilTag family.
"""
from __future__ import annotations

from .base import Target
from .single_aruco import SingleArucoDetector, marker_object_points


def build(*, dict: str = "DICT_APRILTAG_36h11", id: int = 0, size_m: float = 0.05) -> Target:
    return Target(point_xyz=marker_object_points(size_m),
                  detector=SingleArucoDetector(dict, id),
                  min_points=4, planar=True, type="apriltag")
