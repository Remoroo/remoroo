"""THE cuRoboV2 adapter — the ONE file that imports cuRobo/torch. Everything version-specific lives
here; the rest of `motion_engine` is V2-API-independent and unit-testable off-GPU.

Written against the REAL in-repo V2 source (NOT guessed), verified in
`curobo/examples/getting_started/motion_planning.py` + `curobo/_src/motion/motion_planner.py`:

    from curobo.motion_planner import MotionPlanner, MotionPlannerCfg
    from curobo.types import GoalToolPose, JointState
    from curobo.scene import Scene, Mesh

    cfg     = MotionPlannerCfg.create(robot=<dict>, scene_model=<dict>, collision_cache=..., max_goalset=N)
    planner = MotionPlanner(cfg); planner.warmup(enable_graph=True)
    goal    = GoalToolPose(tool_frames=[...], position=[B,H,L,G,3], quaternion=[B,H,L,G,4] wxyz)
    result  = planner.plan_pose(goal, start_js, use_implicit_goal=True, max_attempts=3)
    interp  = result.get_interpolated_plan()  # .position/.velocity (..,T,dof), .dt, .joint_names

Multi-TCP is native: `GoalToolPose.position` is `[batch, horizon, num_links, num_goalset, 3]` with
`num_links == len(tool_frames)`, so driving N end-effectors is N entries on the `num_links` axis —
the SAME call for 1, 2, or a humanoid's many. The scanned cloud becomes a collision mesh via
`Mesh.from_pointcloud` (voxelised-surface extraction; robust on sparse real scans, unlike marching
cubes). Imports are LAZY (inside `__init__`/methods) so this module imports cleanly off-GPU.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from .trajectory import Trajectory
from .world import WorldInputs

# a tool-frame goal: (position xyz [m], quaternion wxyz)
Goal = Tuple[Sequence[float], Sequence[float]]


@dataclass
class PlanResult:
    """What the adapter returns; the stack never sees a torch tensor."""

    success: bool
    trajectory: Optional[Trajectory]
    message: str = ""
    total_time: float = 0.0


def _ensure_warp_torch() -> None:
    """cuRoboV2's GPU kernels shuttle tensors via NVIDIA Warp's torch interop — the TOP-LEVEL
    `wp.from_torch`/`wp.device_from_torch` (warp >= 1.0 exposes these directly; there is NO
    `warp.torch` submodule — importing one raises ModuleNotFoundError on current warp). A
    missing/mismatched warp-lang surfaces as a cryptic 'module warp has no attribute ...' DEEP inside
    planner build. Validate the interop is present here; if absent, raise an ACTIONABLE error pointing
    at the toolchain (G1) fix on the robot PC — not a config problem with the cell."""
    try:
        import torch  # noqa: F401
        import warp as wp
        if not hasattr(wp, "from_torch"):
            raise AttributeError("warp has no top-level from_torch (warp-lang too old or broken interop)")
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            "cuRoboV2 needs NVIDIA Warp's torch interop (wp.from_torch), which is unavailable "
            f"({type(e).__name__}: {e}). This is a TOOLCHAIN issue on the robot PC, not the cell: "
            "reinstall a cuRoboV2-compatible warp-lang (>= 1.0.0) built against the same torch+CUDA. "
            "Verify with:  python -c \"import torch, warp as wp; wp.init(); "
            "print(wp.from_torch(torch.zeros(1, device='cuda')))\""
        ) from e


def _tensor_min(x) -> Optional[float]:
    """The smallest FINITE value of a torch tensor (or None) — for reporting best pose error."""
    try:
        import torch
        if x is None:
            return None
        t = x.detach().float().reshape(-1)
        t = t[torch.isfinite(t)]
        return float(t.min()) if t.numel() else None
    except Exception:  # noqa: BLE001
        return None


def _plan_failure_message(result, *, home: bool = False) -> str:
    """cuRobo's ACTUAL failure reason, not a generic string. In `_plan_pose_single`/`plan_cspace`,
    `result is None` means the IK stage never found a reachable, collision-free GOAL config; a result
    whose `success` is all-False means IK found a goal but TRAJOPT couldn't connect start→goal. We
    surface which stage failed (+ the best position/rotation error when present) so the operator/agent
    sees reach-vs-path-vs-collision instead of 'no collision-free trajectory'."""
    goal = "home (cspace default)" if home else "the target pose"
    if result is None:
        return ("IK stage failed — cuRobo found NO reachable, collision-free goal configuration for "
                f"{goal} in any attempt: out of reach for this arm, or the goal config itself "
                "self-collides / hits the world.")
    pe, re_ = _tensor_min(getattr(result, "position_error", None)), _tensor_min(getattr(result, "rotation_error", None))
    bits = []
    if pe is not None:
        bits.append(f"best position error {pe * 1000:.0f} mm")
    if re_ is not None:
        bits.append(f"rotation error {re_:.3f} rad")
    extra = (" (" + ", ".join(bits) + ")") if bits else ""
    return ("trajopt stage failed — a goal config exists but cuRobo found no collision-free, "
            f"dynamically-feasible path from the start to {goal}{extra}: the start or an intermediate "
            "waypoint collides, or the path can't meet the velocity/accel limits.")


def generate_self_collision_ignore(urdf_path: str, spheres_by_link: dict,
                                   *, tool_frames: Optional[Sequence[str]] = None) -> dict:
    """Generate the COMPLETE self-collision ignore matrix via cuRobo's own RobotBuilder.

    The dict-loader path we build through only gets parent↔child adjacency; cuRobo's RobotBuilder
    additionally runs `_check_default_joint_configuration_collisions` — it FKs the default config and
    ignores every link-pair whose spheres OVERLAP at rest. Without that step, links that touch by
    design (gripper fingers, the two arms' bases, adjacent wrist links) read as PERMANENT collisions
    and nothing plans. We feed our already-fitted spheres in (no refit) and return the full matrix;
    those by-design pairs collide at ~every config, so the default-config check catches them while
    leaving real, config-dependent self-collisions intact. GPU — call on the robot PC."""
    from curobo._src.robot.builder.builder_robot import RobotBuilder

    rb = RobotBuilder(str(urdf_path), tool_frames=list(tool_frames) if tool_frames else None)
    rb._collision_spheres = {k: list(v) for k, v in spheres_by_link.items()}   # inject, don't refit
    # prune_collisions=False → skip the 1000-sample never-collide pruning (an optimisation); keep the
    # neighbour + default-config-collision matrix, which is what fixes the phantom collisions.
    return dict(rb.compute_collision_matrix(prune_collisions=False) or {})


class CuroboV2Planner:
    """A warmed cuRoboV2 MotionPlanner for one TCP set, built from the cell's artifacts."""

    def __init__(
        self,
        robot_cfg: dict,
        world: WorldInputs,
        *,
        limits: Optional[dict] = None,
        max_goalset: int = 1,
        device: str = "cuda",
        warmup: bool = True,
        self_collision_check: bool = True,
    ) -> None:
        import torch  # noqa: F401  (lazy GPU import; presence validated by the toolchain gate)
        _ensure_warp_torch()                       # force-load Warp's torch interop or fail CLEARLY
        from curobo.motion_planner import MotionPlanner, MotionPlannerCfg

        self._device = device
        limits = limits or {}
        self._vel_scale = float(np.clip(limits.get("velocity_scale", 1.0), 1e-3, 1.0))

        # Size the collision cache per type: keep-out/wall/obstacle cuboids, and mesh obstacles +
        # the single scanned-cloud mesh (+ headroom so a later update_world never overflows).
        scene = world.scene or {}
        n_cuboid = len(scene.get("cuboid", {})) + 8
        n_mesh = len(scene.get("mesh", {})) + 4
        cfg = MotionPlannerCfg.create(
            robot=robot_cfg,
            scene_model=scene or None,
            collision_cache={"mesh": int(n_mesh), "cuboid": int(n_cuboid)},
            max_goalset=int(max_goalset),
            self_collision_check=bool(self_collision_check),   # False = diagnostic probe (isolate self-collision)
        )
        self.planner = MotionPlanner(cfg)
        if warmup:
            self.planner.warmup(enable_graph=True, num_warmup_iterations=5)

        # Fuse the scanned environment (keep-out/wall cuboids + the cloud-as-mesh) into the live
        # world via update_world — symmetric with set_world/clear_world, which the motion diagnostic
        # uses to A/B the SAME move with and without the world (isolating world-collision from
        # self-collision/IK).
        self._world = world
        self.planner.update_world(self._full_scene(world))

        self._interp_dt = float(self.planner.trajopt_solver.config.interpolation_dt)

    # --- world (live, toggleable for the diagnostic) ----------------------
    def _full_scene(self, world: WorldInputs):
        """The complete collision scene: keep-out/wall/obstacle cuboids + the scanned cloud-as-mesh."""
        from curobo.scene import Scene, Mesh

        scene = Scene.create(world.scene) if world.scene else Scene()
        if world.n_points > 0:
            scene.add_obstacle(Mesh.from_pointcloud(
                np.asarray(world.points, dtype=np.float64),
                pitch=float(world.voxel_size),
                name="scanned_world",
            ))
        return scene

    def clear_world(self) -> None:
        """Drop ALL obstacles (empty world). Diagnostic only — restore with `set_world`."""
        from curobo.scene import Scene

        self.planner.update_world(Scene())

    def set_world(self, world: Optional[WorldInputs] = None) -> None:
        """Re-apply the full scene (restores after `clear_world`)."""
        self.planner.update_world(self._full_scene(world or self._world))

    def joint_limits(self) -> Dict[str, Tuple[float, float]]:
        """{joint: (min, max)} position limits over the planner's active joints — to catch a START
        config that's outside limits (a non-self-collision reason planning finds nothing)."""
        jl = self.planner.kinematics.get_joint_limits()
        pos = jl.position.detach().cpu().numpy()        # [2, dof] = (min, max)
        names = self.joint_names
        return {n: (float(pos[0, i]), float(pos[1, i])) for i, n in enumerate(names)}

    def robot_spheres(self, start_positions: Optional[Dict[str, float]] = None) -> np.ndarray:
        """The robot's ACTIVE collision spheres (x,y,z,r) at the current/given config — FK output.
        cuRobo disables spheres with a negative radius, so those are filtered out. Used to see whether
        a config sits inside an obstacle box (the start-in-collision diagnosis)."""
        js = self._start_js(start_positions)
        state = self.planner.compute_kinematics(js)
        a = state.robot_spheres.detach().cpu().numpy().reshape(-1, 4)
        return a[a[:, 3] > 0]

    # --- introspection ----------------------------------------------------
    @property
    def tool_frames(self) -> List[str]:
        return list(self.planner.tool_frames)

    @property
    def joint_names(self) -> List[str]:
        return list(self.planner.joint_names)

    def current_tool_pose(self, tool_frame: str,
                          start_positions: Optional[Dict[str, float]] = None) -> Goal:
        """FK the current (or given) config → `tool_frame`'s pose `(xyz, wxyz)` in the planner's base
        frame. Used to plan a point move that KEEPS the current orientation (far more reachable than a
        forced identity orientation), and to seed a target gizmo at the live TCP."""
        js = self._start_js(start_positions)
        state = self.planner.compute_kinematics(js)
        pose = state.tool_poses.get_link_pose(tool_frame)
        pos = pose.position.detach().cpu().numpy().reshape(-1)
        quat = pose.quaternion.detach().cpu().numpy().reshape(-1)
        return [float(v) for v in pos[:3]], [float(v) for v in quat[:4]]

    # --- planning ---------------------------------------------------------
    def _start_js(self, start_positions: Optional[Dict[str, float]]):
        """A JointState over the planner's active joints; missing joints take the robot default."""
        import torch
        from curobo.types import JointState

        names = self.planner.joint_names
        default = self.planner.default_joint_state.position.detach().cpu().numpy().reshape(-1)
        q = np.array(default[: len(names)], dtype=np.float32)
        for i, n in enumerate(names):
            if start_positions and n in start_positions:
                q[i] = float(start_positions[n])
        t = torch.tensor(q, device=self._device, dtype=torch.float32).unsqueeze(0)
        return JointState.from_position(t, joint_names=list(names))

    def _goal(self, goals: Dict[str, Goal]):
        """Build a GoalToolPose for the requested {ee_link: (xyz, wxyz)} — `num_links == len(frames)`."""
        import torch
        from curobo.types import GoalToolPose

        frames = list(goals.keys())
        n = len(frames)
        pos = torch.zeros(1, 1, n, 1, 3, device=self._device, dtype=torch.float32)
        quat = torch.zeros(1, 1, n, 1, 4, device=self._device, dtype=torch.float32)
        for i, f in enumerate(frames):
            p, qn = goals[f]
            pos[0, 0, i, 0, :] = torch.tensor(list(p)[:3], device=self._device, dtype=torch.float32)
            qn = list(qn)[:4]
            quat[0, 0, i, 0, :] = torch.tensor(qn, device=self._device, dtype=torch.float32)
        return GoalToolPose(tool_frames=frames, position=pos, quaternion=quat)

    def plan_pose(self, goals: Dict[str, Goal], start_positions: Optional[Dict[str, float]] = None,
                  *, max_attempts: int = 3) -> PlanResult:
        """Plan ONE synchronous collision-free trajectory driving every frame in `goals` to its
        pose. `goals` keys are ee_links from this planner's `tool_frames`. Safe by construction
        (collision-free + within the dynamics envelope baked into the robot cfg)."""
        if not goals:
            return PlanResult(False, None, "no goals given")
        try:
            goal = self._goal(goals)
            start = self._start_js(start_positions)
            result = self.planner.plan_pose(goal, start, use_implicit_goal=True, max_attempts=max_attempts)
        except Exception as e:  # a planner exception is a failure to surface, not a crash
            return PlanResult(False, None, f"plan_pose raised: {e}")
        if result is None or not bool(result.success.any()):
            return PlanResult(False, None, _plan_failure_message(result))
        traj = self._to_trajectory(result.get_interpolated_plan())
        return PlanResult(True, traj, "ok", float(getattr(result, "total_time", traj.duration)))

    def plan_through(self, tool_frame: str, poses: Sequence[Goal],
                     start_positions: Optional[Dict[str, float]] = None,
                     *, max_attempts: int = 3) -> PlanResult:
        """Drive ONE tool frame through a sequence of poses — plan each leg from the previous leg's
        end, concatenate into one path. Each leg is independently collision-free."""
        if not poses:
            return PlanResult(False, None, "no poses given")
        segments: List[Trajectory] = []
        cur = dict(start_positions or {})
        for k, pose in enumerate(poses):
            leg = self.plan_pose({tool_frame: pose}, cur, max_attempts=max_attempts)
            if not leg.success or leg.trajectory is None:
                return PlanResult(False, None, f"leg {k+1}/{len(poses)} failed: {leg.message}")
            segments.append(leg.trajectory)
            cur = {n: float(v) for n, v in zip(leg.trajectory.joint_names, leg.trajectory.final)}
        traj = Trajectory.concat(segments)
        return PlanResult(True, traj, "ok", traj.duration)

    def plan_retract(self, start_positions: Optional[Dict[str, float]] = None,
                     *, max_attempts: int = 5) -> PlanResult:
        """Plan a collision-free path from the current config back to the robot's home/retract
        (cspace default) — V2 `plan_cspace`. A natural, safe commissioning move (the arm is wherever
        calibration/scan left it)."""
        from curobo.types import JointState

        try:
            start = self._start_js(start_positions)
            goal = JointState.from_position(
                self.planner.default_joint_state.position.unsqueeze(0),
                joint_names=self.planner.joint_names)
            result = self.planner.plan_cspace(goal, start, max_attempts=max_attempts)
        except Exception as e:
            return PlanResult(False, None, f"plan_cspace raised: {e}")
        if result is None or not bool(result.success.any()):
            return PlanResult(False, None, _plan_failure_message(result, home=True))
        traj = self._to_trajectory(result.interpolated_trajectory)
        return PlanResult(True, traj, "ok", traj.duration)

    def update_obstacle_pose(self, name: str, pose: Sequence[float]) -> None:
        """Reposition a known obstacle (cuRobo pose `[x,y,z,qw,qx,qy,qz]`) without a rebuild."""
        from curobo.types import Pose

        p = list(pose)
        self.planner.scene_collision_checker.update_obstacle_pose(
            name, Pose.from_numpy(np.array(p[:3]), np.array(p[3:7])))

    # --- trajectory conversion -------------------------------------------
    def _to_trajectory(self, interp) -> Trajectory:
        """V2 interpolated plan → the SDK-agnostic `Trajectory` (numpy). Applies the safety speed
        scale as an exact TIME DILATION: the path is unchanged (still collision-free), replayed at
        a larger `dt` so velocity/accel scale down — the operator's slow setup speed, realised
        without re-planning."""
        pos = self._np2d(interp.position)
        names = list(getattr(interp, "joint_names", None) or self.planner.joint_names)
        dt = float(getattr(interp, "dt", None) or self._interp_dt)
        vel = None
        if getattr(interp, "velocity", None) is not None:
            vel = self._np2d(interp.velocity)

        if self._vel_scale < 0.999:
            dt = dt / self._vel_scale          # replay slower
            if vel is not None:
                vel = vel * self._vel_scale    # feed-forward consistent with the slower cadence
        return Trajectory(names, pos, dt, velocities=vel,
                          meta={"interp_dt": self._interp_dt, "vel_scale": self._vel_scale})

    @staticmethod
    def _np2d(t) -> np.ndarray:
        """A V2 tensor `(.., T, dof)` → numpy `(T, dof)` (squeeze the batch/leading dims)."""
        a = t.detach().cpu().numpy()
        if a.ndim > 2:
            a = a.reshape(a.shape[-2], a.shape[-1])
        return np.asarray(a, dtype=float)
