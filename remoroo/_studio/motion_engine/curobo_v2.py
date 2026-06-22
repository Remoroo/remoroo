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
    ) -> None:
        import torch  # noqa: F401  (lazy GPU import; presence validated by the toolchain gate)
        from curobo.motion_planner import MotionPlanner, MotionPlannerCfg
        from curobo.scene import Scene, Mesh

        self._device = device
        limits = limits or {}
        self._vel_scale = float(np.clip(limits.get("velocity_scale", 1.0), 1e-3, 1.0))

        # Size the collision cache for the keep-out/wall cuboids + the single scanned-cloud mesh.
        n_cuboid = sum(len(v) for v in (world.scene or {}).values()) + 8
        cfg = MotionPlannerCfg.create(
            robot=robot_cfg,
            scene_model=world.scene or None,
            collision_cache={"mesh": 4, "cuboid": int(n_cuboid)},
            max_goalset=int(max_goalset),
        )
        self.planner = MotionPlanner(cfg)
        if warmup:
            self.planner.warmup(enable_graph=True, num_warmup_iterations=5)

        # Fuse the scanned environment: keep-out/wall cuboids + the cloud-as-mesh, one world update.
        scene = Scene.create(world.scene) if world.scene else Scene()
        if world.n_points > 0:
            scene.add_obstacle(Mesh.from_pointcloud(
                np.asarray(world.points, dtype=np.float64),
                pitch=float(world.voxel_size),
                name="scanned_world",
            ))
            self.planner.update_world(scene)

        self._interp_dt = float(self.planner.trajopt_solver.config.interpolation_dt)

    # --- introspection ----------------------------------------------------
    @property
    def tool_frames(self) -> List[str]:
        return list(self.planner.tool_frames)

    @property
    def joint_names(self) -> List[str]:
        return list(self.planner.joint_names)

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
            return PlanResult(False, None, "planning failed — no collision-free trajectory found")
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
            return PlanResult(False, None, "retract planning failed — no collision-free path home")
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
