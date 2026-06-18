"""The shipped fiducial library + the type→builder registry the authored cell calls.

`build_target({"type": ..., "params": {...}})` is the seam: the agent-authored
`remoroo_cell/calibration/targets.py` selects a shipped detector by type+params (common
case) or registers a custom `FiducialDetector` via `register()` (exotic target). The engine
core (session/solve/metrics) consumes only the returned `Target` — it never names a type.

Importing this package is cv2-free (every implementation imports cv2 lazily inside detect/
build), so the numpy-only engine and CI can import the registry without OpenCV.
"""
from __future__ import annotations

from typing import Callable, Dict

from . import apriltag, aruco_grid, charuco, checkerboard, single_aruco
from .base import FiducialDetector, Target

REGISTRY: Dict[str, Callable[..., Target]] = {
    "charuco": charuco.build,
    "single_aruco": single_aruco.build,
    "apriltag": apriltag.build,
    "aruco_grid": aruco_grid.build,
    "checkerboard": checkerboard.build,
}


def register(type_name: str, builder: Callable[..., Target]) -> None:
    """Register a custom target builder (the authored cell's exotic detector)."""
    REGISTRY[str(type_name)] = builder


def build_target(spec: dict) -> Target:
    """Build a Target from an open `{type, params}` spec. Fails loudly on an unknown type —
    never a silent fallback to a fabricated board."""
    if not isinstance(spec, dict):
        raise ValueError(f"target spec must be a mapping, got {type(spec).__name__}")
    t = spec.get("type")
    if t not in REGISTRY:
        raise ValueError(f"unknown target type {t!r}; known: {', '.join(sorted(REGISTRY))} "
                         f"(or register a custom one in remoroo_cell/calibration/targets.py)")
    params = spec.get("params") or {}
    return REGISTRY[t](**params)


__all__ = ["Target", "FiducialDetector", "REGISTRY", "register", "build_target"]
