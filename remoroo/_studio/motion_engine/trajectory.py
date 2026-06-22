"""The SDK-agnostic `Trajectory` — the seam between the cuRobo planner and the per-arm executor.

cuRoboV2 does NOT output "a joint angle to move to"; it outputs a dynamics-aware, time-parameterised
PATH: a position/velocity/acceleration time-series sampled at a fixed `dt`. The whole point of the
planner is that this entire path is collision-free AND within every limit — so the executor must
REPLAY the full series, not jump to the endpoint. This class is that path, in plain numpy: no torch,
no cuRobo import, JSON-round-trippable so it can travel over SSE to the Studio and back.

`curobo_v2.py` builds one of these from a V2 plan (`result.get_interpolated_plan()` →
`.position/.velocity (..,T,dof)`, `.dt`, `.joint_names`). The cell's `bridge.execute_trajectory(traj)`
receives it and streams `traj.positions[i]` to the arm's controller every `traj.dt` seconds. It is
deliberately the ONLY shape the executor needs to understand — every arm SDK differs in HOW it
follows a path, never in WHAT the path is.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

import numpy as np


@dataclass
class Trajectory:
    """A time-parameterised joint path. `positions` is the contract; the rest is advisory.

    - `joint_names`   : the joints `positions` columns correspond to, in order (the executor maps
                        these onto its controller; a subset of the robot's joints for one arm).
    - `positions`     : (T, dof) radians — the path to replay, waypoint by waypoint.
    - `velocities`    : (T, dof) rad/s — for SDKs that take feed-forward velocity (servoJ, ROS2);
                        zeros if the planner didn't provide them.
    - `accelerations` : (T, dof) rad/s^2 — advisory; some controllers accept it, most ignore it.
    - `dt`            : seconds between consecutive waypoints (the replay cadence).
    """

    joint_names: List[str]
    positions: np.ndarray
    dt: float
    velocities: Optional[np.ndarray] = None
    accelerations: Optional[np.ndarray] = None
    meta: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.positions = np.asarray(self.positions, dtype=float).reshape(-1, len(self.joint_names))
        if self.velocities is not None:
            self.velocities = np.asarray(self.velocities, dtype=float).reshape(self.positions.shape)
        if self.accelerations is not None:
            self.accelerations = np.asarray(self.accelerations, dtype=float).reshape(self.positions.shape)
        self.dt = float(self.dt)

    # --- shape -------------------------------------------------------------
    def __len__(self) -> int:
        """Number of waypoints T (so `for i in range(len(traj))` is the replay loop)."""
        return int(self.positions.shape[0])

    @property
    def dof(self) -> int:
        return int(self.positions.shape[1])

    @property
    def duration(self) -> float:
        """Wall-clock seconds to replay the whole path at `dt`."""
        return max(0, len(self) - 1) * self.dt

    def waypoint(self, i: int) -> np.ndarray:
        """The i-th joint configuration (dof,) — what the executor sends to the controller."""
        return self.positions[i]

    @property
    def final(self) -> np.ndarray:
        """The last configuration — the pose the arm ends at (NOT a substitute for replay)."""
        return self.positions[-1]

    # --- validity ----------------------------------------------------------
    def is_finite(self) -> bool:
        """No NaN/Inf anywhere — a thin sanity gate before the executor touches the arm."""
        ok = np.all(np.isfinite(self.positions))
        if self.velocities is not None:
            ok = ok and np.all(np.isfinite(self.velocities))
        return bool(ok)

    def resample(self, dt: float) -> "Trajectory":
        """Linearly resample to a fixed control `dt` for SDKs that need a constant rate.

        The planner's `dt` (V2 interpolation_dt) is often ~0.02 s; some controllers want a
        different fixed period. Position is interpolated; velocity is recomputed by finite
        difference so feed-forward stays consistent with the new spacing."""
        dt = float(dt)
        if dt <= 0 or len(self) < 2 or abs(dt - self.dt) < 1e-9:
            return self
        t_old = np.arange(len(self)) * self.dt
        t_new = np.arange(0.0, t_old[-1] + 1e-9, dt)
        pos = np.stack([np.interp(t_new, t_old, self.positions[:, j]) for j in range(self.dof)], axis=1)
        vel = np.zeros_like(pos)
        if len(t_new) > 1:
            vel[1:] = (pos[1:] - pos[:-1]) / dt
        return Trajectory(list(self.joint_names), pos, dt, velocities=vel, meta=dict(self.meta))

    # --- transport (SSE / JSON) -------------------------------------------
    def to_dict(self, *, include_velocities: bool = True) -> dict:
        """JSON-safe dict (lists, not ndarrays) for SSE to the Studio / logging."""
        d = {
            "joint_names": list(self.joint_names),
            "positions": self.positions.tolist(),
            "dt": self.dt,
            "n": len(self),
            "dof": self.dof,
            "duration": self.duration,
            "meta": self.meta,
        }
        if include_velocities and self.velocities is not None:
            d["velocities"] = self.velocities.tolist()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Trajectory":
        return cls(
            joint_names=list(d["joint_names"]),
            positions=np.asarray(d["positions"], dtype=float),
            dt=float(d.get("dt", 0.02)),
            velocities=(np.asarray(d["velocities"], dtype=float) if d.get("velocities") is not None else None),
            meta=dict(d.get("meta") or {}),
        )

    def summary(self) -> str:
        return f"{len(self)} waypoints · {self.dof} joints · {self.duration:.2f}s @ {self.dt*1000:.0f}ms"

    @classmethod
    def concat(cls, segments: List["Trajectory"]) -> "Trajectory":
        """Join consecutive segments (e.g. a `move_through_poses` of plan-per-waypoint) into one
        path. Drops the duplicated seam sample where a segment starts at the previous one's end.
        All segments must share `joint_names` and `dt`."""
        segs = [s for s in segments if s is not None and len(s) > 0]
        if not segs:
            raise ValueError("concat: no non-empty segments")
        names, dt = segs[0].joint_names, segs[0].dt
        for s in segs[1:]:
            if list(s.joint_names) != list(names):
                raise ValueError("concat: joint_names differ across segments")
        pos_parts, vel_parts = [segs[0].positions], []
        have_vel = segs[0].velocities is not None
        if have_vel:
            vel_parts.append(segs[0].velocities)
        for s in segs[1:]:
            seam = 1 if np.allclose(s.positions[0], pos_parts[-1][-1], atol=1e-6) else 0
            pos_parts.append(s.positions[seam:])
            if have_vel and s.velocities is not None:
                vel_parts.append(s.velocities[seam:])
            else:
                have_vel = False
        positions = np.concatenate(pos_parts, axis=0)
        velocities = np.concatenate(vel_parts, axis=0) if have_vel else None
        return cls(list(names), positions, dt, velocities=velocities,
                   meta={"segments": [len(s) for s in segs]})
