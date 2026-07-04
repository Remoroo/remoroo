"""Turn the G0.5 safety envelope into cuRoboV2 PLANNER INPUTS — PURE (no cuRobo).

The operator's safety is fed INTO the planner so every trajectory is safe BY CONSTRUCTION, not
judged after the fact:

  - joint **acceleration / jerk** caps → the robot cspace, so the optimised B-spline respects the
    dynamics envelope. URDF joint VELOCITY limits are used UNSCALED — there is deliberately no
    planner speed scale (scaling it made a 5 cm move take 13 s; see robotcfg). The operator's
    `max_joint_speed_frac` governs the bridge's DIRECT SDK moves (move_joints / calibration).
  - **keep-out** boxes + **workspace bounds** → scene obstacles (handled in `world.py`).

cuRobo then returns a path that is collision-free AND within every limit. The only thing left at
RUNTIME is a thin **defensive audit** (`audit_trajectory`, `estop` poll) — a guard against *bugs /
SDK glitches* (a NaN, a velocity spike from a malformed plan), NOT a re-enforcement or re-judgement
of cuRobo's already-safe plan. If the audit ever fails, the right response is ABORT + route to the
agent — never "fix up" the motion.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import numpy as np


def _read_yaml(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        import yaml  # type: ignore

        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return None


def _f(v, default: float) -> float:
    try:
        return float(v) if v is not None else float(default)
    except (TypeError, ValueError):
        return float(default)


@dataclass
class Safety:
    """The normalised envelope. `bounds_m`/`keep_out` go to `world.py`; the rest to the planner."""

    max_joint_speed_frac: float = 0.10      # fraction of rated joint speed — DIRECT SDK moves only
    max_cartesian_speed_mps: float = 0.10   # safety-rated monitored cartesian speed
    max_acceleration: Optional[float] = None  # rad/s^2 cap (None → 15.0 default)
    max_jerk: Optional[float] = None          # rad/s^3 cap (None → 500.0 default)
    max_velocity: Optional[float] = None      # authored but NOT consumed by the planner today
    bounds_m: Optional[dict] = None           # {"min":[x,y,z], "max":[x,y,z]} workspace box
    keep_out: Optional[List[dict]] = None     # [{min,max,note}]

    def planner_limits(self) -> dict:
        """The `limits` dict `robotcfg.build_v2_robot_cfg` consumes — EXACTLY what the planner
        applies, nothing more: cspace `max_acceleration` / `max_jerk`. URDF joint VELOCITY limits
        are unscaled by design (a velocity scale here made a 5 cm move take 13 s / 500+ waypoints —
        see robotcfg); `max_joint_speed_frac` caps the bridge's DIRECT SDK moves instead."""
        return {
            "max_acceleration": _f(self.max_acceleration, 15.0),
            "max_jerk": _f(self.max_jerk, 500.0),
        }

    def world_kwargs(self) -> dict:
        """What `world.load_world(safety=...)` reads for keep-out/bounds obstacles."""
        return {"bounds_m": self.bounds_m, "keep_out": self.keep_out or []}


def load_safety(cell_dir: str) -> Safety:
    """Read `safety.yaml` (preferred) or fall back to `cell.yaml`'s `safety:`/`workspace:` blocks.
    Tolerant of present-but-null keys (a YAML `max_joint_speed_frac:` with no value)."""
    base = Path(cell_dir)
    raw = _read_yaml(base / "safety.yaml")
    workspace = {}
    if raw is None:
        cy = _read_yaml(base / "cell.yaml") or {}
        raw = cy.get("safety") or {}
        workspace = cy.get("workspace") or {}
    raw = raw or {}

    bounds = raw.get("bounds_m") or raw.get("workspace_bounds_m") or workspace.get("bounds_m")
    return Safety(
        max_joint_speed_frac=_f(raw.get("max_joint_speed_frac"), 0.10),
        max_cartesian_speed_mps=_f(raw.get("max_cartesian_speed_mps"), 0.10),
        max_acceleration=(float(raw["max_acceleration"]) if raw.get("max_acceleration") is not None else None),
        max_jerk=(float(raw["max_jerk"]) if raw.get("max_jerk") is not None else None),
        max_velocity=(float(raw["max_velocity"]) if raw.get("max_velocity") is not None else None),
        bounds_m=bounds,
        keep_out=list(raw.get("keep_out") or []),
    )


# --- defensive runtime audit (vs BUGS, never a re-plan) --------------------

def audit_trajectory(traj, *, bounds_m: Optional[dict] = None, max_velocity: Optional[float] = None,
                     velocity_tol: float = 1.25) -> List[str]:
    """Thin sanity gate on a planned `Trajectory` BEFORE the executor touches the arm. Returns a
    list of reasons to ABORT (empty = clean). This catches a malformed plan (NaN, a velocity spike,
    a waypoint outside the declared workspace) — a bug in the planner/transport, not a margin call
    on a good plan. It NEVER modifies the trajectory."""
    reasons: List[str] = []
    if traj is None or len(traj) == 0:
        return ["empty trajectory (planner returned no path)"]
    if not traj.is_finite():
        reasons.append("trajectory contains NaN/Inf (malformed plan)")
    if max_velocity is not None and traj.velocities is not None and len(traj) > 0:
        vmax = float(np.max(np.abs(traj.velocities)))
        if vmax > max_velocity * velocity_tol:
            reasons.append(f"joint velocity {vmax:.3f} rad/s exceeds the safety cap "
                           f"{max_velocity:.3f} rad/s (×{velocity_tol} tol) — refusing to replay")
    # dt sanity: a zero/negative dt makes the replay cadence meaningless
    if traj.dt <= 0:
        reasons.append(f"non-positive dt ({traj.dt}) — cannot time the replay")
    return reasons
