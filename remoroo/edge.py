"""Shipped safety + transport spine for the cell Bridge.

The agent-authored `remoroo_cell/primitives.py` (and the data recorder) import their
SafetySupervisor + EpisodeWriter from HERE:

    try:
        from remoroo.edge import SafetySupervisor, EpisodeWriter   # shipped (this module)
    except Exception:
        from .safety_shim import SafetySupervisor, EpisodeWriter   # R&D fallback (now rarely hit)

This module ships WITH the `remoroo` package, so the primary import succeeds out of the box
and a cell never needs a hand-authored `safety_shim.py` (a missing/mis-imported shim was a
hard Bridge-import failure — "No module named 'safety_shim'" — that took down calibration,
the live camera feed, and every other edge route that needs the Bridge).

Deliberately conservative (slow speeds, hard bounds) and dependency-light (numpy only) — the
human supervises every motion. The customer may still drop a local `safety_shim.py` to
override; this is the default when they don't.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np


@dataclass
class SafetySupervisor:
    """Speed/bounds gate every motion passes through. Built from `cell.yaml` (`safety` +
    `workspace.bounds_m`); the operator's G0.5 envelope is the source of truth."""

    max_cartesian_speed_mps: float
    max_joint_speed_frac: float
    bounds_min: np.ndarray
    bounds_max: np.ndarray

    @classmethod
    def from_cell(cls, cell: Dict[str, Any]) -> "SafetySupervisor":
        s = cell.get("safety", {}) or {}
        w = (cell.get("workspace") or {}).get("bounds_m", {}) or {}
        # None-SAFE: a key present-but-null in cell.yaml makes dict.get(k, default) return
        # None (not the default) → float(None) would crash the Bridge connect.
        def _f(v, d):
            return float(d) if v is None else float(v)
        return cls(
            max_cartesian_speed_mps=_f(s.get("max_cartesian_speed_mps"), 0.10),
            max_joint_speed_frac=_f(s.get("max_joint_speed_frac"), 0.10),
            bounds_min=np.asarray(w.get("min") or [-0.5, -0.5, 0.0], float),
            bounds_max=np.asarray(w.get("max") or [0.5, 0.5, 0.8], float),
        )

    def clamp_speed(self, frac: Optional[float]) -> float:
        f = self.max_joint_speed_frac if frac is None else min(frac, self.max_joint_speed_frac)
        return max(0.0, f)

    def joints_within_limits(self, arm: str, q: np.ndarray) -> bool:
        return bool(np.all(np.isfinite(q)))  # cuRobo + URDF limits do the real check

    def point_in_bounds(self, xyz: np.ndarray) -> bool:
        xyz = np.asarray(xyz, float)
        return bool(np.all(xyz >= self.bounds_min) and np.all(xyz <= self.bounds_max))

    def trajectory_ok(self, arm: str, plan: Any) -> bool:
        pts = getattr(plan, "cartesian_waypoints", None)
        if pts is None:
            return True  # joint-space plan; rely on the planner's collision check
        return all(self.point_in_bounds(np.asarray(p, float)) for p in pts)


class EpisodeWriter:
    """Minimal episode writer (schema = remoroo_episode_v1). See data_capture.md."""

    def __init__(self, out_dir: str | Path):
        self.dir = Path(out_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self._frames: list = []

    def add(self, **frame) -> None:
        self._frames.append({"t": time.time(), **frame})

    def close(self) -> Path:
        meta = {"schema": "remoroo_episode_v1", "n_frames": len(self._frames)}
        (self.dir / "meta.json").write_text(json.dumps(meta, indent=2))
        return self.dir


__all__ = ["SafetySupervisor", "EpisodeWriter"]
