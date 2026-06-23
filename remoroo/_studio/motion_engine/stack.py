"""`MotionStack` — the single high-level motion API the cell's bridge calls.

It FUSES every prior gate's output (the calibrated URDF, the collision spheres, the scanned world,
the safety envelope) into one warmed cuRoboV2 planner and exposes TCP-keyed verbs:

    stack.move_to_pose(tcp, pose)              # one end-effector → a pose
    stack.move_to_poses({tcp: pose, ...})      # N end-effectors, coordinated, ONE trajectory
    stack.move_through_poses(tcp, [poses])     # a waypoint sequence
    stack.move_to_joints(q, arm=...)           # a single config (calibration/point-to-point)
    stack.retract(tcp)                         # collision-free path home
    stack.plan_to_point(tcp, xyz)              # convenience: position with a nominal orientation
    stack.commission()                         # build + self-test ONE plan+execute (the new gate)

`tcp` is an ARM NAME from `robot_model/arms.yaml`; the stack resolves it to an ee_link and builds
(caches) a planner per requested TCP SET — count-agnostic, never a "dual arm" branch. This module is
ORCHESTRATION ONLY: it does not import cuRobo (the planner is injected / lazily built by
`curobo_v2.py`), so the whole control flow — plan → audit → hand the FULL trajectory to the per-arm
executor → defensive supervision — is unit-tested off-GPU against a fake driver.

The executor is NOT here. cuRobo PLANS; the cell's `bridge.execute_trajectory(traj, should_abort=…)`
REPLAYS the path on the arm's SDK (every SDK differs — see `seeds/robotics/arm_adapters.md`). The
stack hands over the whole `Trajectory` and a `should_abort` poll; it never sends a single endpoint.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np

from .robot import load_robot_config, load_spheres, neighbor_ignore, sphere_health
from .robotcfg import build_v2_robot_cfg
from .safety import Safety, audit_trajectory, load_safety
from .trajectory import Trajectory
from .world import WorldInputs, load_world

# a pose: (position xyz [m], quaternion wxyz). Helpers below also accept a 7-list or a dict.
Pose = Tuple[Sequence[float], Sequence[float]]
PlannerFactory = Callable[..., object]


def _norm_pose(pose) -> Pose:
    """Accept (xyz, wxyz) | [x,y,z,qw,qx,qy,qz] | {'position','quaternion'} → (xyz, wxyz)."""
    if isinstance(pose, dict):
        return list(pose["position"])[:3], list(pose.get("quaternion") or [1, 0, 0, 0])[:4]
    if isinstance(pose, (list, tuple)) and len(pose) == 2 and np.ndim(pose[0]) == 1:
        return list(pose[0])[:3], list(pose[1])[:4]
    p = list(pose)
    if len(p) == 7:
        return p[:3], p[3:7]
    if len(p) == 3:
        return p[:3], [1.0, 0.0, 0.0, 0.0]
    raise ValueError(f"unrecognised pose shape: {pose!r}")


@dataclass
class MoveResult:
    """The outcome of a motion verb — planned + (optionally) executed."""

    ok: bool
    message: str = ""
    trajectory: Optional[Trajectory] = None
    executed: bool = False
    aborted: bool = False
    total_time: float = 0.0
    audit: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "ok": self.ok, "message": self.message, "executed": self.executed,
            "aborted": self.aborted, "total_time": self.total_time, "audit": self.audit,
            "trajectory": self.trajectory.to_dict(include_velocities=False) if self.trajectory else None,
        }


def _default_planner_factory(robot_cfg, world, *, limits, max_goalset):
    """The real factory: build a GPU CuroboV2Planner. Imported lazily so the stack stays off-GPU."""
    from .curobo_v2 import CuroboV2Planner

    return CuroboV2Planner(robot_cfg, world, limits=limits, max_goalset=max_goalset)


class MotionStack:
    def __init__(self, *, config: dict, spheres: Dict[str, List[dict]], sphere_buffer: float,
                 world: WorldInputs, safety: Safety, bridge=None,
                 urdf_path: str = "robot.urdf", planner_factory: Optional[PlannerFactory] = None) -> None:
        self.config = config
        self.spheres = spheres
        self.sphere_buffer = sphere_buffer
        self.world = world
        self.safety = safety
        self.bridge = bridge
        self.urdf_path = urdf_path
        self._factory = planner_factory or _default_planner_factory
        self._planners: Dict[frozenset, object] = {}
        self._groups = {g["name"]: g for g in config.get("groups", [])}
        self._limits = safety.planner_limits()
        # parent↔child adjacency so cuRobo ignores always-touching links (the loader doesn't auto-gen)
        self._self_ignore = neighbor_ignore(urdf_path)

    # --- construction -----------------------------------------------------
    @classmethod
    def from_cell(cls, cell_dir: str, bridge=None, *, planner_factory: Optional[PlannerFactory] = None,
                  wall_bounds: bool = True) -> "MotionStack":
        """Build from the on-disk cell: the AUTHORED config (cell.yaml groups) + collision_spheres.yml
        + world/ + safety."""
        config = load_robot_config(cell_dir)
        spheres, buffer = load_spheres(cell_dir)
        safety = load_safety(cell_dir)
        world = load_world(cell_dir, safety=safety.world_kwargs(), wall_bounds=wall_bounds)
        # ABSOLUTE path — cuRobo resolves a relative `urdf_path` against its OWN content/assets dir
        # (the "curobo/content/assets/robot.urdf is not a file" failure), never the cell.
        urdf_path = str((Path(cell_dir) / "robot_model" / "robot.urdf").resolve())
        return cls(config=config, spheres=spheres, sphere_buffer=buffer, world=world,
                   safety=safety, bridge=bridge, urdf_path=urdf_path, planner_factory=planner_factory)

    # --- group/TCP resolution --------------------------------------------
    def _group(self, tcp: str) -> dict:
        if tcp not in self._groups:
            raise KeyError(f"unknown group/TCP {tcp!r}; cell has {list(self._groups)}")
        return self._groups[tcp]

    def _tip(self, tcp: str) -> str:
        """The group's primary tip link (the frame a pose targets). A group has 1+ tips."""
        tips = self._group(tcp).get("tip_links") or []
        if not tips:
            raise ValueError(f"group {tcp!r} has no tip_links — nothing to drive")
        return tips[0]

    def _planner_for(self, active_groups: Sequence[str]):
        """Get/build (and cache) the planner whose tool_frames = these groups' tips."""
        for a in active_groups:
            self._group(a)                     # KeyError on an unknown group, before any build
        key = frozenset(active_groups)
        if key not in self._planners:
            robot_cfg = build_v2_robot_cfg(
                self.config, self.spheres, active_groups=list(active_groups),
                urdf_path=self.urdf_path, sphere_buffer=self.sphere_buffer, limits=self._limits,
                self_collision_ignore=self._self_ignore)
            self._planners[key] = self._factory(robot_cfg, self.world, limits=self._limits, max_goalset=1)
        return self._planners[key]

    # --- health -----------------------------------------------------------
    def sphere_health(self) -> dict:
        return sphere_health(self.spheres)

    # --- joint state ------------------------------------------------------
    def _current_positions(self, arms: Sequence[str]) -> Dict[str, float]:
        """Read the cell's current joint angles for `arms` (seeds the plan start). Tolerant of the
        bridge shape; empty → the planner uses its default config."""
        if self.bridge is None:
            return {}
        for getter in ("read_joint_positions", "joint_positions"):
            fn = getattr(self.bridge, getter, None)
            if callable(fn):
                try:
                    out: Dict[str, float] = {}
                    for a in arms:
                        try:
                            vals = fn(a)
                        except TypeError:
                            vals = fn()
                        names = self._group(a)["joint_names"]
                        out.update({n: float(v) for n, v in zip(names, np.asarray(vals, float).reshape(-1))})
                    return out
                except Exception:
                    return {}
        return {}

    # --- the verbs --------------------------------------------------------
    def move_to_pose(self, tcp: str, pose, *, execute: bool = True, max_attempts: int = 3) -> MoveResult:
        """Plan ONE end-effector (`tcp`) to `pose` and (default) execute it."""
        return self.move_to_poses({tcp: pose}, execute=execute, max_attempts=max_attempts)

    def move_to_poses(self, targets: Dict[str, object], *, execute: bool = True,
                      max_attempts: int = 3) -> MoveResult:
        """Drive every TCP in `targets` to its pose, SYNCHRONOUSLY, as ONE collision-free
        trajectory (cuRobo plans them jointly over the combined cspace). 1, 2, or many TCPs."""
        if not targets:
            return MoveResult(False, "no targets given")
        arms = list(targets.keys())
        planner = self._planner_for(arms)
        goals = {self._tip(a): _norm_pose(p) for a, p in targets.items()}
        res = planner.plan_pose(goals, self._current_positions(arms), max_attempts=max_attempts)
        return self._finish(res, execute)

    def move_through_poses(self, tcp: str, poses: Sequence[object], *, execute: bool = True,
                           max_attempts: int = 3) -> MoveResult:
        """Drive one TCP through a sequence of poses (one concatenated collision-free path)."""
        planner = self._planner_for([tcp])
        legs = [_norm_pose(p) for p in poses]
        res = planner.plan_through(self._tip(tcp), legs, self._current_positions([tcp]),
                                   max_attempts=max_attempts)
        return self._finish(res, execute)

    def plan_to_point(self, tcp: str, xyz: Sequence[float], *, execute: bool = True) -> MoveResult:
        """Convenience: reach a 3D point with a nominal (identity) orientation. Use `move_to_pose`
        when orientation matters."""
        return self.move_to_pose(tcp, (list(xyz)[:3], [1.0, 0.0, 0.0, 0.0]), execute=execute)

    def retract(self, tcp: Optional[str] = None, *, execute: bool = True) -> MoveResult:
        """Plan a collision-free path back to the robot's home/retract config. `tcp` selects the
        arm (default: all arms). Distinct from `move_to_joints` — this is COLLISION-FREE."""
        arms = [tcp] if tcp else list(self._groups.keys())
        planner = self._planner_for(arms)
        if not hasattr(planner, "plan_retract"):
            return MoveResult(False, "planner has no retract; use move_to_joints(home) instead")
        res = planner.plan_retract(self._current_positions(arms))
        return self._finish(res, execute)

    def move_to_joints(self, joints: Sequence[float], arm: Optional[str] = None) -> MoveResult:
        """A SINGLE-CONFIG move (no trajectory optimisation) — for calibration / point-to-point.
        Delegates straight to the bridge; NOT a cuRobo plan. Kept distinct from trajectory replay
        so the operator can jog to one pose without invoking the planner."""
        if self.bridge is None or not hasattr(self.bridge, "move_to_joints"):
            return MoveResult(False, "no bridge.move_to_joints available")
        try:
            self.bridge.move_to_joints(list(joints), arm=arm) if arm else self.bridge.move_to_joints(list(joints))
            return MoveResult(True, "moved to joint config", executed=True)
        except Exception as e:
            return MoveResult(False, f"move_to_joints failed: {e}")

    # --- commission self-test --------------------------------------------
    def commission(self, *, execute: bool = True, progress: Optional[Callable[[dict], None]] = None) -> dict:
        """Build the stack and VERIFY the end-to-end chain once: spheres healthy → planner builds →
        ONE collision-free plan → trajectory → (optionally) the per-arm executor replays it. This
        is what the new commission gate runs so G8 exercises a PROVEN stack, not an assumed one."""
        def emit(**kw):
            if progress:
                progress(kw)

        report: dict = {"ok": False, "steps": []}

        def step(name, ok, **extra):
            entry = {"step": name, "ok": bool(ok), **extra}
            report["steps"].append(entry)
            emit(**entry)
            return ok

        health = self.sphere_health()
        health_extra = {k: v for k, v in health.items() if k != "ok"}
        if not step("sphere_health", health["ok"], **health_extra) and health.get("warnings"):
            report["message"] = "collision spheres are degenerate — refusing to move (fix the model)"
            return report

        first = next(iter(self._groups), None)
        if not step("groups", first is not None, groups=list(self._groups)):
            report["message"] = "no kinematic groups in the cell — nothing to commission"
            return report

        emit(step="build_planner", ok=True, note="warming cuRobo (one-time)")
        try:
            self._planner_for([first])
            step("build_planner", True, tcp=first)
        except Exception as e:
            step("build_planner", False, error=str(e))
            report["message"] = f"planner build failed: {e}"
            return report

        emit(step="plan_move", ok=True, note="planning a safe verification move")
        mv = self.retract(first, execute=execute)
        step("plan_move", mv.ok, message=mv.message, executed=mv.executed,
             trajectory=(mv.trajectory.summary() if mv.trajectory else None), audit=mv.audit)
        report["ok"] = mv.ok
        report["message"] = "commissioned — the motion stack is proven end-to-end" if mv.ok else mv.message
        report["move"] = mv.to_dict()
        return report

    # --- plan → audit → execute → supervise ------------------------------
    def _finish(self, plan, execute: bool) -> MoveResult:
        """The shared tail: defensive audit of the planned trajectory, then hand the FULL path to
        the per-arm executor with a `should_abort` poll. The audit is vs BUGS only (NaN, a velocity
        spike, a waypoint out of bounds) — never a re-judgement of cuRobo's already-safe plan."""
        if not getattr(plan, "success", False) or getattr(plan, "trajectory", None) is None:
            return MoveResult(False, getattr(plan, "message", "planning failed"))
        traj: Trajectory = plan.trajectory
        reasons = audit_trajectory(traj, bounds_m=self.safety.bounds_m,
                                   max_velocity=self.safety.max_velocity)
        if reasons:
            return MoveResult(False, "trajectory failed the defensive audit (likely a planner bug)",
                              trajectory=traj, audit=reasons, total_time=plan.total_time)
        if not execute:
            return MoveResult(True, "planned (not executed)", trajectory=traj, total_time=plan.total_time)

        if self.bridge is None or not hasattr(self.bridge, "execute_trajectory"):
            return MoveResult(False, "no bridge.execute_trajectory — cannot replay", trajectory=traj)

        should_abort = self._abort_poll()
        try:
            ok = self.bridge.execute_trajectory(traj, should_abort=should_abort)
        except TypeError:
            ok = self.bridge.execute_trajectory(traj)  # driver that doesn't accept should_abort
        except Exception as e:
            return MoveResult(False, f"executor raised: {e}", trajectory=traj)
        aborted = bool(should_abort()) if should_abort else False
        return MoveResult(bool(ok) and not aborted,
                          "executed" if ok and not aborted else ("aborted (E-stop)" if aborted else "executor reported failure"),
                          trajectory=traj, executed=True, aborted=aborted, total_time=plan.total_time)

    def _abort_poll(self) -> Optional[Callable[[], bool]]:
        """A `should_abort()` the executor checks between waypoints — wired to the bridge's E-stop
        state if it exposes one (the defensive guard, not a re-plan)."""
        for name in ("estop_tripped", "is_estopped", "estopped"):
            fn = getattr(self.bridge, name, None)
            if callable(fn):
                return lambda f=fn: bool(f())
        return None
