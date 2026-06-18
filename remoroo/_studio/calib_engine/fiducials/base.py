"""The fiducial contract — the abstraction such that the engine never names a fiducial.

A `Target` is *a set of identifiable 3D points + a detector*. A single ArUco marker, a
ChArUco board, an AprilTag, a marker grid, and a checkerboard are all just data flowing
through this one shape — there is no "default board" with exceptions for the others.

Two halves, deliberately split so the numpy-only engine never imports cv2:
  * `point_xyz` / `min_points` — pure geometry the SOLVER consumes (`solve`/`metrics`/synth
    build these with numpy; no cv2).
  * `detector.detect(image)` — the cv2 detection the BRIDGE calls (`edge_real.RealBridge`
    on the robot / cv2 CI). Off-robot the FakeBridge supplies synthetic corners, so the
    detector is unused and may be `None`.

The detector returns `(point_ids, uv)` where `point_ids` index directly into `point_xyz`
(row i = the 3D coordinate of point id i), so the solver's `point_xyz[ids]` lines up with
exactly what was detected — the same contract `detect_charuco` always had, generalised.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol, Tuple, runtime_checkable

import numpy as np


@runtime_checkable
class FiducialDetector(Protocol):
    """What the bridge calls to turn a frame into corners. cv2 is imported lazily inside
    the implementation, never at module import — so importing the library is cv2-free."""

    def detect(self, image: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Return (point_ids[N], uv[N,2]); ids index into the Target's point_xyz."""
        ...


@dataclass
class Target:
    """An authored calibration target. Replaces the ChArUco-only BoardModel — no rows/cols/
    square: those are properties of one *kind* of target, not of "a target". `point_xyz`
    (M,3) are the target-frame coordinates, row i = point id i (planar targets use z=0).
    `detector` is optional: present on the real edge, None in synth (the bridge supplies
    corners). `min_points` is the smallest usable view for THIS target (4 for a single
    marker, 6+ for a board) — never a hardcoded 6."""

    point_xyz: np.ndarray                      # (M, 3) target frame
    detector: Optional[FiducialDetector] = None
    min_points: int = 4
    planar: bool = True
    type: str = ""                             # the registry key (e.g. "single_aruco"); display/provenance

    def __post_init__(self) -> None:
        self.point_xyz = np.asarray(self.point_xyz, float).reshape(-1, 3)

    @property
    def n(self) -> int:
        return int(self.point_xyz.shape[0])

    @property
    def points(self) -> np.ndarray:
        """Alias for `point_xyz` — the array the solver/metrics index by corner id. (Kept so
        call sites read `target.points[ids]` the way they read `board.points[ids]` before.)"""
        return self.point_xyz

    @property
    def point_ids(self) -> np.ndarray:
        return np.arange(self.n)

    def detect(self, image: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        if self.detector is None:
            raise RuntimeError(f"target {self.type!r} has no detector (off-robot synth target)")
        return self.detector.detect(image)

    def points_for(self, ids) -> np.ndarray:
        return self.point_xyz[np.asarray(ids, int)]
