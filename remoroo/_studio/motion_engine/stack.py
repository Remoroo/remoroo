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

from .robot import actuated_joints, load_robot_config, load_spheres, neighbor_ignore, sphere_health
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


def _unit(v: Tuple[float, float, float]) -> Tuple[float, float, float]:
    n = (v[0] ** 2 + v[1] ** 2 + v[2] ** 2) ** 0.5 or 1.0
    return (v[0] / n, v[1] / n, v[2] / n)


def _bbox(pts) -> Optional[dict]:
    a = np.asarray(pts, dtype=float)
    if a.size == 0:
        return None
    a = a.reshape(-1, a.shape[-1])
    return {"min": [round(float(x), 3) for x in a[:, :3].min(0)],
            "max": [round(float(x), 3) for x in a[:, :3].max(0)]}


def _sphere_box_overlaps(spheres, scene: dict) -> dict:
    """For each axis-aligned obstacle cuboid, how many of the robot's spheres PENETRATE it, and the
    min clearance (sphere-surface → box, negative = inside). Box yaw is ignored (table/walls are
    axis-aligned) — a fast, honest first read of 'is this config inside an obstacle'."""
    out: dict = {}
    sph = np.asarray(spheres, dtype=float)
    if sph.size == 0:
        return out
    for name, box in (scene.get("cuboid") or {}).items():
        pose = box.get("pose") or [0, 0, 0, 1, 0, 0, 0]
        c = np.asarray(pose[:3], dtype=float)
        half = np.asarray(box.get("dims") or [0, 0, 0], dtype=float) / 2.0
        lo, hi = c - half, c + half
        n, mind = 0, 1e9
        for s in sph:
            p, r = s[:3], float(s[3])
            d = np.maximum(np.maximum(lo - p, p - hi), 0.0)   # per-axis outside distance
            dist = float(np.linalg.norm(d)) - r
            mind = min(mind, dist)
            if dist < 0:
                n += 1
        out[name] = {"spheres_penetrating": int(n), "min_clearance_m": round(mind, 4)}
    return out


def _quat_mul(a: Sequence[float], b: Sequence[float]) -> List[float]:
    """Hamilton product of two wxyz quaternions."""
    aw, ax, ay, az = a
    bw, bx, by, bz = b
    return [
        aw * bw - ax * bx - ay * by - az * bz,
        aw * bx + ax * bw + ay * bz - az * by,
        aw * by - ax * bz + ay * bw + az * bx,
        aw * bz + ax * by - ay * bx + az * bw,
    ]


def _axis_quat(axis: Tuple[float, float, float], deg: float) -> List[float]:
    """A wxyz rotation quaternion about `axis` by `deg` degrees."""
    import math
    r = math.radians(deg) / 2.0
    c, s = math.cos(r), math.sin(r)
    return [c, s * axis[0], s * axis[1], s * axis[2]]


# When reaching a PLACED point we don't care about the exact tool orientation — cuRoboV2 (a35a708)
# has no position-only goal, so we offer the current orientation plus a few rotations of it and take
# the first that's reachable + collision-free. Generic (no tool-convention assumption): yaw flips +
# a couple of pitch/roll quarter-turns are enough to free an otherwise-feasible point.
_ORIENTATION_DELTAS: List[Tuple[Tuple[float, float, float], float]] = [
    ((0, 0, 1), 90), ((0, 0, 1), -90), ((0, 0, 1), 180),
    ((0, 1, 0), 90), ((0, 1, 0), -90), ((1, 0, 0), 90),
]


# Candidate directions on a sphere around the current TCP — the 6 axes + 8 cube diagonals — used by
# `find_free_targets` to probe where the arm can move. Up-first so the nearest safe lift is offered
# before lateral/down options.
_SPHERE_DIRS: List[Tuple[float, float, float]] = [
    (0, 0, 1), (0, 0, -1), (1, 0, 0), (-1, 0, 0), (0, 1, 0), (0, -1, 0),
    _unit((1, 1, 1)), _unit((1, 1, -1)), _unit((1, -1, 1)), _unit((1, -1, -1)),
    _unit((-1, 1, 1)), _unit((-1, 1, -1)), _unit((-1, -1, 1)), _unit((-1, -1, -1)),
]


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
        # parent↔child adjacency so cuRobo ignores always-touching links (the loader doesn't auto-gen).
        # This is the FALLBACK; `_self_collision_ignore()` upgrades it with cuRobo's default-config
        # generation (the link-pairs that overlap at rest — grippers, dual-arm bases) on first build.
        self._self_ignore = neighbor_ignore(urdf_path)
        self._ignore_full: Optional[dict] = None      # cached generated matrix
        self._ignore_warn: Optional[str] = None       # why generation fell back, if it did
        # EVERY URDF actuated joint (incl. gripper drivers not in any group) → cuRobo cspace; the
        # ones we're not driving get locked, so cuRobo never meets a joint missing from the list.
        self._actuated = actuated_joints(urdf_path)

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
    @property
    def group_names(self) -> List[str]:
        """The kinematic group / TCP names in this cell — morphology-agnostic (arms, legs, wheels,
        a humanoid's many). The default TCP for a verb is `group_names[0]`."""
        return list(self._groups)

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

    def _self_collision_ignore(self) -> dict:
        """The self-collision ignore matrix, generated ONCE via cuRobo's RobotBuilder (parent↔child
        PLUS link-pairs that overlap at the default config — the step the dict-loader path skips,
        without which by-design-touching links phantom-collide and nothing plans). Falls back to the
        parent↔child adjacency if generation fails (e.g. off-GPU), recording why in `_ignore_warn`."""
        if self._ignore_full is not None:
            return self._ignore_full
        ig = {k: list(v) for k, v in (self._self_ignore or {}).items()}
        try:
            from .curobo_v2 import generate_self_collision_ignore
            tips = [t for g in self._groups.values() for t in (g.get("tip_links") or [])]
            full = generate_self_collision_ignore(self.urdf_path, self.spheres, tool_frames=tips or None)
            if full:
                ig = full
            else:
                self._ignore_warn = "cuRobo generated an empty ignore matrix; using parent↔child only"
        except Exception as e:  # noqa: BLE001 — never block a build on generation; fall back loudly
            self._ignore_warn = (f"self-collision ignore generation failed ({type(e).__name__}: {e}); "
                                 "using parent↔child only — phantom self-collisions may block planning")
        self._ignore_full = ig
        return ig

    def _robot_cfg_for(self, active_groups: Sequence[str]) -> dict:
        """The cuRobo robot_cfg dict for these groups (active cspace + the generated self-collision
        ignore). Shared by `_planner_for` and the diagnostic's self-collision-OFF probe."""
        return build_v2_robot_cfg(
            self.config, self.spheres, active_groups=list(active_groups),
            urdf_path=self.urdf_path, sphere_buffer=self.sphere_buffer, limits=self._limits,
            self_collision_ignore=self._self_collision_ignore(), actuated_joints=self._actuated)

    def _planner_for(self, active_groups: Sequence[str]):
        """Get/build (and cache) the planner whose tool_frames = these groups' tips."""
        for a in active_groups:
            self._group(a)                     # KeyError on an unknown group, before any build
        key = frozenset(active_groups)
        if key not in self._planners:
            robot_cfg = self._robot_cfg_for(active_groups)
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

    def current_tool_pose(self, tcp: str) -> object:
        """The live (xyz, wxyz) of `tcp`'s tool frame in the base frame — FK of the current joints.
        Lets the Studio spawn a target gizmo at the real TCP and lets `plan_to_point` keep the
        current orientation. Empty/None if the planner can't FK (e.g. a non-cuRobo test factory)."""
        planner = self._planner_for([tcp])
        if not hasattr(planner, "current_tool_pose"):
            return None
        return planner.current_tool_pose(self._tip(tcp), self._current_positions([tcp]))

    def _orientation_candidates(self, tcp: str, orientation: Optional[Sequence[float]]) -> List[List[float]]:
        """Orientations to TRY at a reach point. An explicit `orientation` is used as-is; otherwise
        the current TCP orientation FIRST (smallest wrist motion), then rotations of it — so a point
        that's reachable with SOME orientation isn't rejected just because the live one doesn't fit."""
        if orientation is not None:
            return [list(orientation)[:4]]
        cur = self.current_tool_pose(tcp)
        q0 = list(cur[1])[:4] if cur else [1.0, 0.0, 0.0, 0.0]
        return [q0] + [_quat_mul(q0, _axis_quat(ax, deg)) for ax, deg in _ORIENTATION_DELTAS]

    def plan_to_point(self, tcp: str, xyz: Sequence[float], *, orientation: Optional[Sequence[float]] = None,
                      execute: bool = True, max_attempts: int = 3) -> MoveResult:
        """Plan a collision-free move so `tcp` reaches the 3D point `xyz`. Reaching a POINT cares about
        position, not the exact tool orientation, so we try the current orientation then a few rotated
        fallbacks (cuRoboV2 a35a708 has no position-only goal) and take the first reachable, collision-
        free one. Pass `orientation` (wxyz) to PIN it. The keystone the operator's target gizmo (stage
        1), the agent's chosen point (stage 2), and the commission verification all call."""
        xyz = list(xyz)[:3]
        last = MoveResult(False, "that point isn't reachable + collision-free for this TCP in any tried "
                                 "orientation — move it closer / pick another TCP")
        for q in self._orientation_candidates(tcp, orientation):
            mv = self.move_to_pose(tcp, (xyz, q), execute=False, max_attempts=max_attempts)
            if mv.ok and mv.trajectory is not None:
                if not execute:
                    return mv
                return self.move_to_pose(tcp, (xyz, q), execute=True, max_attempts=max_attempts)
            last = mv
        return last

    def safe_demo_move(self, tcp: str, *, execute: bool = True, deltas_m: Sequence[float] = (0.06, 0.10),
                       max_attempts: int = 2) -> Tuple[MoveResult, Optional[List[float]]]:
        """Auto-find ONE small collision-free move from the CURRENT pose: offset the live TCP a few cm
        along each axis (up first, then lateral, then down), keeping orientation, and return the first
        that cuRobo can plan. A legible default verification that needs no operator input and avoids
        the colliding home — and a deterministic floor under stage 2's agent target search. Returns
        (result, chosen_point) so the caller/UI can show WHERE it moved."""
        cur = self.current_tool_pose(tcp)
        if not cur:
            return MoveResult(False, "planner cannot FK the current pose — cannot pick a safe demo move"), None
        pos, quat = cur
        dirs = [(0, 0, 1), (0, 0, -1), (1, 0, 0), (-1, 0, 0), (0, 1, 0), (0, -1, 0)]
        for d in deltas_m:
            for ux, uy, uz in dirs:
                tgt = [pos[0] + ux * d, pos[1] + uy * d, pos[2] + uz * d]
                mv = self.move_to_pose(tcp, (tgt, quat), execute=False, max_attempts=max_attempts)
                if mv.ok and mv.trajectory is not None:
                    return (self.plan_to_point(tcp, tgt, orientation=quat, execute=execute) if execute else mv), tgt
        return MoveResult(False, "no small collision-free move found from the current pose — the arm "
                                 "may be boxed in by the world/obstacles"), None

    def find_free_targets(self, tcp: str, *, n: int = 8, radii: Sequence[float] = (0.10, 0.18),
                          max_attempts: int = 1) -> List[dict]:
        """REMOROO reads the space: sample candidate points around the current TCP (on a sphere, at
        each radius) and return those cuRobo can actually PLAN to — collision-free against the fused
        world, keeping the current orientation. The caller (the Studio 'suggest points' button, or
        the agent in stage 2) chooses among them; the operator can still nudge before any motion.
        Returns up to `n` dicts `{point:[x,y,z], radius, distance, trajectory}` nearest-first."""
        cur = self.current_tool_pose(tcp)
        if not cur:
            return []
        pos, quat = cur
        out: List[dict] = []
        for r in radii:
            for d in _SPHERE_DIRS:
                tgt = [float(pos[i] + d[i] * r) for i in range(3)]
                mv = self.move_to_pose(tcp, (tgt, quat), execute=False, max_attempts=max_attempts)
                if mv.ok and mv.trajectory is not None:
                    out.append({"point": tgt, "radius": float(r), "distance": float(r),
                                "trajectory": mv.trajectory.summary()})
                    if len(out) >= n:
                        return out
        return out

    def diagnose_motion(self, tcp: Optional[str] = None, xyz: Optional[Sequence[float]] = None) -> dict:
        """Honest, decisive collision diagnosis for 'no collision-free trajectory'. Plans the SAME
        small move WITH the world and WITHOUT it, and reports what the START config's spheres overlap:

          • succeeds with the world           → the earlier failure was a different target/TCP.
          • fails WITH world, ok WITHOUT world → the WORLD blocks it (start/path collides with the
            scanned cloud or an obstacle box). `start.in_cuboid` + `world.cloud_bbox` say which.
          • fails BOTH                         → NOT the world: self-collision (the self_collision_ignore
            matrix is too sparse → phantom collisions), joint limits, or IK.

        Read-only (execute=False); restores the world after the no-world probe."""
        tcp = tcp or next(iter(self._groups), None)
        rep: dict = {"tcp": tcp}
        if tcp is None:
            rep["error"] = "no kinematic groups in the cell"
            return rep
        planner = self._planner_for([tcp])     # also triggers self-collision ignore generation
        start = self._current_positions([tcp])

        ig = self._ignore_full or {}
        rep["self_collision"] = {
            "ignore_links": len(ig),
            "ignore_pairs": sum(len(v) for v in ig.values()),
            "generated_by_curobo": self._ignore_warn is None,
            "warn": self._ignore_warn,
        }

        w = self.world
        cuboids = {name: {"dims": [round(float(d), 3) for d in (b.get("dims") or [])],
                          "center": [round(float(x), 3) for x in (b.get("pose") or [0, 0, 0])[:3]]}
                   for name, b in (w.scene.get("cuboid") or {}).items()}
        rep["world"] = {"n_points": int(w.n_points), "n_points_raw": w.meta.get("n_points_raw"),
                        "voxel_size": float(w.voxel_size),
                        "cloud_bbox": _bbox(w.points) if w.n_points else None,
                        "n_cuboids": len(cuboids), "cuboids": cuboids}

        if hasattr(planner, "robot_spheres"):
            try:
                sph = planner.robot_spheres(start)
                rep["start"] = {"n_spheres": int(len(sph)), "spheres_bbox": _bbox(sph[:, :3]),
                                "in_cuboid": _sphere_box_overlaps(sph, w.scene)}
            except Exception as e:  # noqa: BLE001
                rep["start"] = {"error": f"{type(e).__name__}: {e}"}

        cur = self.current_tool_pose(tcp)
        if xyz is None:
            xyz = [cur[0][0], cur[0][1], cur[0][2] + 0.05] if cur else [0.3, 0.0, 0.4]
        quat = cur[1] if cur else [1.0, 0.0, 0.0, 0.0]
        xyz = [float(v) for v in list(xyz)[:3]]
        rep["target"] = [round(v, 3) for v in xyz]

        with_w = self.move_to_pose(tcp, (xyz, quat), execute=False)
        rep["plan_with_world"] = {"ok": with_w.ok, "message": with_w.message}

        if hasattr(planner, "clear_world") and hasattr(planner, "set_world"):
            try:
                planner.clear_world()
                no_w = self.move_to_pose(tcp, (xyz, quat), execute=False)
                rep["plan_without_world"] = {"ok": no_w.ok, "message": no_w.message}
            finally:
                planner.set_world(self.world)
        else:
            rep["plan_without_world"] = {"ok": None, "message": "planner cannot toggle the world"}

        # joint-limits check: is the START config itself outside the planner's limits?
        if hasattr(planner, "joint_limits"):
            try:
                lim = planner.joint_limits()
                viol = {j: {"q": round(float(start[j]), 4), "limit": [round(lo, 4), round(hi, 4)]}
                        for j, (lo, hi) in lim.items()
                        if j in start and not (lo - 1e-3 <= start[j] <= hi + 1e-3)}
                rep["limits"] = {"n_joints": len(lim), "violations": viol}
            except Exception as e:  # noqa: BLE001
                rep["limits"] = {"error": f"{type(e).__name__}: {e}"}

        # DECISIVE probe: plan the SAME move with NO world AND NO self-collision. Separates a
        # remaining self-collision problem (→ plans now) from joint-limits / IK reach (→ still fails).
        rep["plan_no_world_no_selfcoll"] = {"ok": None, "message": "probe not run"}
        try:
            from .curobo_v2 import CuroboV2Planner
            from .world import WorldInputs
            empty = WorldInputs(points=np.zeros((0, 3)), scene={})
            probe = CuroboV2Planner(self._robot_cfg_for([tcp]), empty, limits=self._limits,
                                    max_goalset=1, warmup=False, self_collision_check=False)
            pr = probe.plan_pose({self._tip(tcp): (xyz, quat)}, start)
            rep["plan_no_world_no_selfcoll"] = {"ok": bool(pr.success), "message": pr.message}
        except Exception as e:  # noqa: BLE001
            rep["plan_no_world_no_selfcoll"] = {"ok": None, "message": f"probe failed: {type(e).__name__}: {e}"}

        a = rep["plan_with_world"]["ok"]
        b = rep["plan_without_world"]["ok"]
        c = rep["plan_no_world_no_selfcoll"]["ok"]
        viol = rep.get("limits", {}).get("violations") or {}
        if a:
            rep["verdict"] = "OK with the world — the earlier failure was a different target/TCP, not the world."
        elif b:
            rep["verdict"] = ("WORLD BLOCKS IT: the SAME move plans once the world is removed — start/path "
                              "collides with the scanned cloud / an obstacle box (see start.in_cuboid).")
        elif viol:
            rep["verdict"] = (f"JOINT LIMITS: the START config is outside limits on {list(viol)} — fix the "
                              "limits (URDF/safety) or the start read; no trajectory can be valid.")
        elif c:
            rep["verdict"] = ("SELF-COLLISION (still): with self-collision turned OFF the SAME move plans, "
                              "so the generated ignore matrix is STILL missing pairs — the spheres overlap "
                              "more than the default-config check caught. Next: widen the ignore / fix spheres.")
        elif c is False:
            rep["verdict"] = ("NOT self-collision and NOT the world: fails even with BOTH off → IK REACH "
                              "(target unreachable for this TCP) or a degenerate start. Try a nearer point / the other TCP.")
        else:
            rep["verdict"] = ("inconclusive — the self-collision-off probe didn't run (see "
                              "plan_no_world_no_selfcoll.message); likely GPU memory or a build error.")
        return rep

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
    def commission(self, *, execute: bool = True, progress: Optional[Callable[[dict], None]] = None,
                   target: Optional[Sequence[float]] = None, tcp: Optional[str] = None) -> dict:
        """Build the stack and VERIFY the end-to-end chain once: spheres healthy → planner builds →
        ONE collision-free plan → trajectory → (optionally) the per-arm executor replays it. This
        is what the new commission gate runs so G8 exercises a PROVEN stack, not an assumed one.

        The verification move is a REACHABLE target, never the cspace home (which is commonly inside
        the table): if the operator/agent passed a 3D `target` point we plan `tcp` there (keeping its
        current orientation); otherwise `safe_demo_move` auto-finds a small collision-free nudge from
        the live pose. Both prove the same fused stack with a legible, supervised motion."""
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

        first = tcp or next(iter(self._groups), None)
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

        if target is not None:
            emit(step="plan_move", ok=True, note=f"planning to the chosen point {list(target)[:3]}")
            mv = self.plan_to_point(first, list(target)[:3], execute=execute)
            chosen = list(target)[:3]
        else:
            emit(step="plan_move", ok=True, note="auto-finding a small collision-free move from the live pose")
            mv, chosen = self.safe_demo_move(first, execute=execute)
        step("plan_move", mv.ok, tcp=first, target=chosen, message=mv.message, executed=mv.executed,
             trajectory=(mv.trajectory.summary() if mv.trajectory else None), audit=mv.audit)
        report["ok"] = mv.ok
        report["target"] = chosen
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
