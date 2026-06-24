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
the SAME call for 1, 2, or a humanoid's many. The collision world is the modeled cuboids
(`_full_scene`) + the LIVE ESDF from the cameras (`update_voxel_world`); the stored scanned point
cloud is NOT loaded into cuRobo (its occupancy mesh enclosed the robot base). Imports are LAZY
(inside `__init__`/methods) so this module imports cleanly off-GPU.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from .trajectory import Trajectory
from .world import WorldInputs

# a tool-frame goal: (position xyz [m], quaternion wxyz)
Goal = Tuple[Sequence[float], Sequence[float]]


def _collision_report(d_world: np.ndarray, spheres: np.ndarray, collision_free: bool) -> dict:
    """Format a born-collision report from cuRobo's PER-SPHERE world-collision distance `d_world`
    (>0 = penetrating, as cuRobo's collision cost returns) and the FK `spheres` (n,4 = x,y,z,r), plus
    the canonical `collision_free` verdict from `RobotCollisionChecker.validate`. Pure numpy → the
    report shape is unit-tested off-GPU; the cuRobo calls that fill it are rig-validated."""
    d = np.asarray(d_world, dtype=float).reshape(-1)
    a = np.asarray(spheres, dtype=float).reshape(-1, 4)
    n = min(len(d), len(a))
    d, a = d[:n], a[:n]
    hot = [i for i in range(n) if d[i] > 0][:25]
    return {
        "collision_free": bool(collision_free),
        "n_spheres": int(n),
        "raw_cost": {"min": round(float(d.min()), 5) if n else 0.0,
                     "max": round(float(d.max()), 5) if n else 0.0,
                     "n_positive": int((d > 0).sum()), "n_negative": int((d < 0).sum())},
        "n_penetrating": int((d > 0).sum()),
        "penetrating_spheres": [{"xyz": [round(float(x), 3) for x in a[i, :3]],
                                 "r": round(float(a[i, 3]), 3), "cost": round(float(d[i]), 4)}
                                for i in hot],
    }


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


class CuroboMapper:
    """The LIVE perception subsystem, built ENTIRELY from cuRoboV2's tested-on-robots components — we
    only wire them. The ONE place that imports cuRobo perception. Camera-count / morphology-agnostic:
    `integrate(frame, joint_positions)` once per camera (any resolution), then `compute_esdf`.

      depth cleaning → `curobo.perception.FilterDepth`     (nan / flying-pixel / bilateral)
      robot masking  → `curobo.perception.RobotSegmenter`  (depth→cloud, base frame, distance to the
                        robot's LIVE collision spheres → zero the robot pixels)
      TSDF→ESDF map  → `curobo.perception.Mapper`          (+ `fuse_static` for the modeled cuboids)

    Imports are LAZY so this module still loads off-GPU."""

    def __init__(self, extent_m: Sequence[float], center: Sequence[float] = (0.0, 0.0, 0.0), *,
                 robot_cfg: Optional[dict] = None, voxel_size: float = 0.02,
                 esdf_voxel_size: float = 0.03, segment_distance: float = 0.05,
                 device: str = "cuda") -> None:
        from curobo.perception import Mapper, MapperCfg, RobotSegmenter

        self._device = device
        self._voxel_size = float(voxel_size)
        self._mapper = Mapper(MapperCfg(
            extent_meters_xyz=tuple(float(v) for v in extent_m),
            grid_center=tuple(float(v) for v in center),
            voxel_size=self._voxel_size,
            esdf_voxel_size=float(esdf_voxel_size),
            truncation_distance=self._voxel_size * 4.0,
            enable_static=True,          # allocate the static channel so fuse_static (modeled cuboids) lands
            device=device,
        ))
        # The segmenter masks the robot from each depth frame at its live config — cuRobo's own,
        # CUDA-graphed, batched. Built from the SAME robot cfg the planner uses (the inner dict under
        # "robot_cfg" carries the "kinematics" key RobotSegmenter expects).
        self._segmenter = None
        if robot_cfg is not None:
            kin = robot_cfg.get("robot_cfg", robot_cfg)
            self._segmenter = RobotSegmenter.from_robot_file(kin, distance_threshold=float(segment_distance))
        self._voxelgrid = None
        self._depth_filter = None        # built lazily per image shape

    def reset(self) -> None:
        self._mapper.reset()
        self._voxelgrid = None

    def _observation(self, depth: np.ndarray, intrinsics: np.ndarray,
                     cam_pose_in_base: Tuple[Sequence[float], Sequence[float]]):
        """Build a batched (1,H,W) cuRobo CameraObservation in the robot base frame. We map for
        COLLISION (geometry only), so RGB carries no information — but cuRobo's camera-integration
        kernel REQUIRES an `rgb_image` of shape (num_cameras,H,W,3) uint8 and validates it (it is not
        None-guarded, unlike the dataclass field). So we pass a zeros RGB of the right shape; the
        voxels are uncolored, which is irrelevant since we read only `.centers`."""
        import torch
        from curobo.types import CameraObservation, Pose

        d = np.asarray(depth, dtype=np.float32)
        h, w = d.shape
        dimg = torch.as_tensor(np.nan_to_num(d, nan=0.0, posinf=0.0, neginf=0.0),
                               device=self._device).view(1, h, w)
        rgb = torch.zeros((1, h, w, 3), dtype=torch.uint8, device=self._device)   # kernel requires it
        intr = torch.as_tensor(np.asarray(intrinsics, dtype=np.float32), device=self._device).view(1, 3, 3)
        pos = torch.as_tensor(np.asarray(cam_pose_in_base[0], dtype=np.float32), device=self._device).view(1, 3)
        quat = torch.as_tensor(np.asarray(cam_pose_in_base[1], dtype=np.float32), device=self._device).view(1, 4)
        return CameraObservation(depth_image=dimg, rgb_image=rgb, intrinsics=intr,
                                 pose=Pose(position=pos, quaternion=quat))

    def integrate(self, depth: np.ndarray, intrinsics: np.ndarray,
                  cam_pose_in_base: Tuple[Sequence[float], Sequence[float]],
                  joint_positions: Optional[Dict[str, float]] = None) -> int:
        """Fuse ONE camera into the TSDF: clean depth (FilterDepth) → mask the robot at `joint_positions`
        (RobotSegmenter) → integrate. Returns the number of masked (robot) pixels. `cam_pose_in_base`
        is the camera pose in the robot base frame (the calibration output)."""
        from curobo.perception import FilterDepth
        from curobo.types import JointState

        obs = self._observation(depth, intrinsics, cam_pose_in_base)
        h, w = obs.depth_image.shape[-2], obs.depth_image.shape[-1]
        if self._depth_filter is None:
            self._depth_filter = FilterDepth(image_shape=(h, w))
        filtered, _ = self._depth_filter(obs.depth_image.unsqueeze(0))
        obs.depth_image = filtered[0]

        n_masked = 0
        if self._segmenter is not None and joint_positions is not None:
            names = list(self._segmenter.kinematics.joint_names)
            q = self._segmenter_q([float(joint_positions.get(n, 0.0)) for n in names])
            js = JointState.from_position(q, joint_names=names)
            mask, filtered_image = self._segmenter.get_robot_mask(obs, js)
            obs.depth_image = filtered_image       # robot pixels zeroed by cuRobo
            n_masked = int(mask.sum().item())
        self._mapper.integrate(camera_observation=obs)
        return n_masked

    def _segmenter_q(self, vals):
        import torch
        return torch.as_tensor(np.asarray(vals, dtype=np.float32), device=self._device).view(1, -1)

    def fuse_static(self, scene) -> None:
        """Stamp the modeled cuboids (table/wall/cage) into the SAME TSDF (cuRobo combines the static
        channel with the live depth via min()), so the planner sees ONE canonical world. `scene` is a
        public cuRobo Scene (SceneCfg); we tensorize it to SceneData for the mapper. No-op when empty."""
        import torch
        from curobo.types import DeviceCfg
        from curobo.scene import SceneData

        if scene is None or not getattr(scene, "cuboid", None):
            return
        device_cfg = DeviceCfg(device=torch.device(self._device), dtype=torch.float32)
        self._mapper.update_static_obstacles(SceneData.from_scene_cfg(scene, device_cfg))

    def compute_esdf(self):
        """Compute the ESDF and return cuRobo's `VoxelGrid` (signed distance field for collision)."""
        self._voxelgrid = self._mapper.compute_esdf()
        return self._voxelgrid

    def occupied_voxels(self) -> np.ndarray:
        """Occupied voxel centres `[N, 3]` (x, y, z) in the base frame — for the Studio render. Uses
        cuRobo's tested `extract_occupied_voxels` (surface + inside), not a raw grid threshold."""
        occ = self._mapper.extract_occupied_voxels(surface_only=False)
        if len(occ) == 0:
            return np.zeros((0, 3), dtype=float)
        return occ.centers.detach().cpu().numpy().reshape(-1, 3)

    @property
    def voxelgrid(self):
        return self._voxelgrid


def make_mapper(extent_m, center=(0.0, 0.0, 0.0), *, robot_cfg=None, **kw) -> "CuroboMapper":
    """The real mapper factory (injected into `MotionStack`); a test passes a fake instead."""
    return CuroboMapper(extent_m, center, robot_cfg=robot_cfg, **kw)


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

        # Size the collision cache per type: modeled keep-out/wall/obstacle cuboids + any modeled
        # mesh obstacles + a VOXEL slot for the live ESDF (+ headroom so update_world never overflows).
        # The scanned cloud is NOT loaded (see _full_scene) — the live ESDF is the real world.
        scene = world.scene or {}
        n_cuboid = len(scene.get("cuboid", {})) + 8
        n_mesh = len(scene.get("mesh", {})) + 4
        # The live-ESDF voxel slot MUST be a DICT {layers, dims, voxel_size}, NOT an int: cuRobo's
        # SceneData.create_cache does `voxel_cache.get("layers", ...)`, so an int crashes planner build
        # with "'int' object has no attribute 'get'". `layers: 1` is the real requirement — the live
        # grid is referenced directly at update_world (create_from_voxel_grids, max_n==1), so these
        # dims are a placeholder; we size them from the workspace (matching the mapper) for safety.
        from .live_world import workspace_extent
        _ext, _ = workspace_extent(getattr(world, "bounds_m", None))
        voxel_cache = {"layers": 1, "dims": [float(e) for e in _ext], "voxel_size": 0.03}
        cfg = MotionPlannerCfg.create(
            robot=robot_cfg,
            scene_model=scene or None,
            collision_cache={"mesh": int(n_mesh), "cuboid": int(n_cuboid), "voxel": voxel_cache},
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
        self._robot_cfg = robot_cfg            # kept so link_pose can FK any URDF link (camera frames)
        self._fk = None
        self.planner.update_world(self._full_scene(world))

        self._interp_dt = float(self.planner.trajopt_solver.config.interpolation_dt)

    # --- forward kinematics for ANY link (e.g. a camera's calibrated optical frame) --------
    def link_pose(self, link: str, start_positions: Optional[Dict[str, float]] = None):
        """FK ANY URDF link → its pose `(xyz, wxyz)` in the base frame at the given (or zero) config.
        Used to resolve a camera's calibrated optical-frame pose for the live map (eye-in-hand: at the
        LIVE joints; static: the same fixed result). Isolated from planning: a FK-only kinematics that
        tracks ALL links (tool_frames=None), so adding camera frames never perturbs the goal logic."""
        import torch
        from curobo.types import DeviceCfg, JointState

        if self._fk is None:
            import copy
            from curobo._src.types.robot import RobotCfg
            from curobo._src.robot.kinematics.kinematics import Kinematics

            kin_dict = copy.deepcopy(self._robot_cfg.get("robot_cfg", self._robot_cfg))
            kin_dict.get("kinematics", {}).pop("tool_frames", None)     # None → track every link
            rc = RobotCfg.create(kin_dict, device_cfg=DeviceCfg(device=torch.device(self._device),
                                                                dtype=torch.float32))
            self._fk = Kinematics(rc.kinematics)
        names = list(self._fk.joint_names)
        q = torch.as_tensor([float((start_positions or {}).get(n, 0.0)) for n in names],
                            dtype=torch.float32, device=self._device).view(1, -1)
        pose = self._fk.get_link_poses(q, [link])
        pos = pose.position.detach().cpu().numpy().reshape(-1)
        quat = pose.quaternion.detach().cpu().numpy().reshape(-1)
        return [float(v) for v in pos[:3]], [float(v) for v in quat[:4]]

    # --- world (live, toggleable for the diagnostic) ----------------------
    def _full_scene(self, world: WorldInputs):
        """The modeled collision scene = the MODELED cuboids only (keep-out / wall / table / cage).

        The stored scanned point CLOUD is deliberately NOT loaded into cuRobo: `Mesh.from_pointcloud`
        builds a watertight occupancy mesh that ENCLOSES the robot base (mounted in the scan) → cuRobo
        flags the base as born-in-collision even when it's clear. The real collision world at
        commission/demo is the LIVE ESDF from the cameras (`update_voxel_world`), into which the
        modeled cuboids are fused. The cloud survives only as a Studio visual reference, never as
        geometry cuRobo plans against."""
        from curobo.scene import Scene

        return Scene.create(world.scene) if world.scene else Scene()

    def clear_world(self) -> None:
        """Drop ALL obstacles (empty world). Diagnostic only — restore with `set_world`."""
        from curobo.scene import Scene

        self.planner.update_world(Scene())

    def set_world(self, world: Optional[WorldInputs] = None) -> None:
        """Re-apply the full scene (restores after `clear_world`)."""
        self.planner.update_world(self._full_scene(world or self._world))

    def update_voxel_world(self, voxelgrid) -> None:
        """LIVE world: the planner's collision = the ESDF `voxelgrid`. This is the ONE canonical world
        — the modeled cuboids (table/wall/cage) are already fused INTO the map (via the mapper's
        `update_static_obstacles`, combined with live depth by min()), so the scene is just the voxel
        grid (matching cuRobo's `Scene(voxel=[vg])` live pipeline). No cloud mesh."""
        from curobo.scene import Scene

        self.planner.update_world(Scene(voxel=[voxelgrid]))

    @property
    def modeled_scene(self):
        """The modeled cuboids (table/wall/cage) as a cuRobo Scene, for fusing into the live map."""
        from curobo.scene import Scene

        return Scene.create(self._world.scene) if (self._world and self._world.scene) else None

    def ik_check(self, tool_frame: str, start_positions: Optional[Dict[str, float]] = None,
                 offsets: Optional[Dict[str, Sequence[float]]] = None) -> dict:
        """cuRobo's CANONICAL IK round-trip (from getting_started/inverse_kinematics.py), bypassing
        our `_goal`/`plan_pose`: FK the current config → its tool pose, then `ik_solver.solve_pose`
        via `GoalToolPose.from_poses` to that SAME pose (offset 0) + small offsets, keeping the FK
        quaternion. Offset-0 MUST succeed (the current config solves it) — if it doesn't, the
        robot_cfg/kinematics is broken, full stop. If offset-0 succeeds here but our `plan_pose`
        fails, the bug is in OUR goal construction, not the config. Pure IK (no trajopt, no world)."""
        import torch  # noqa: F401
        from curobo.types import GoalToolPose, Pose

        js = self._start_js(start_positions)
        kin = self.planner.compute_kinematics(js)
        pose = kin.tool_poses.get_link_pose(tool_frame)
        base_pos = pose.position.detach().cpu().numpy().reshape(-1)[:3]
        offs = offsets or {"0cm": [0, 0, 0], "+z5cm": [0, 0, 0.05], "-z5cm": [0, 0, -0.05],
                           "+x5cm": [0.05, 0, 0], "-x5cm": [-0.05, 0, 0],
                           "+y5cm": [0, 0.05, 0], "-y5cm": [0, -0.05, 0]}
        checks: dict = {}
        ik = self.planner.ik_solver
        for label, d in offs.items():
            new_pos = pose.position.clone()
            new_pos[..., 0] += float(d[0]); new_pos[..., 1] += float(d[1]); new_pos[..., 2] += float(d[2])
            gp = Pose(position=new_pos, quaternion=pose.quaternion.clone())
            res = ik.solve_pose(GoalToolPose.from_poses({tool_frame: gp}, num_goalset=1))
            pe = getattr(res, "position_error", None)
            checks[label] = {"success": bool(res.success.any()),
                             "pos_err_mm": (round(float(pe.min()) * 1000, 2) if pe is not None else None)}
        return {"tool_frame": tool_frame, "planner_tool_frames": list(self.planner.tool_frames),
                "active_joints": list(self.planner.joint_names), "fk_position": [float(x) for x in base_pos],
                "checks": checks}

    def _collision_checker(self):
        """A PUBLIC, tested `RobotCollisionChecker` over THIS planner's world — built once, lazily.
        cuRobo's supported born-collision API (validate / per-sphere distance / sample); we no longer
        reach into `scene_collision_checker.get_sphere_distance` with a hand-built CollisionBuffer."""
        if getattr(self, "_coll_checker", None) is None:
            import torch
            from curobo.collision_checking import RobotCollisionChecker, RobotCollisionCheckerCfg
            from curobo.types import DeviceCfg

            scene = self._world.scene or None
            n_cub = len((scene or {}).get("cuboid", {})) + 8
            n_mesh = len((scene or {}).get("mesh", {})) + 4
            cfg = RobotCollisionCheckerCfg.load_from_config(
                robot_config=self._robot_cfg, scene_model=scene,
                device_cfg=DeviceCfg(device=torch.device(self._device), dtype=torch.float32),
                n_cuboids=int(n_cub), n_meshes=int(n_mesh),
                collision_activation_distance=0.0)          # 0 = exact penetration verdict
            self._coll_checker = RobotCollisionChecker(cfg)
        return self._coll_checker

    def _q_bhd(self, start_positions: Optional[Dict[str, float]]):
        """Active-joint config as a `[1, 1, dof]` tensor (the [batch, horizon, dof] the collision
        checker expects), in the planner's joint order."""
        import torch
        names = list(self.planner.joint_names)
        vals = [float((start_positions or {}).get(n, 0.0)) for n in names]
        return torch.as_tensor(vals, dtype=torch.float32, device=self._device).view(1, 1, -1)

    def world_sphere_collision(self, start_positions: Optional[Dict[str, float]] = None) -> dict:
        """GROUND TRUTH via cuRobo's PUBLIC `RobotCollisionChecker` at the EXACT given config (NO IK):
        `validate` is the canonical born-collision verdict (world+self+bounds); `get_collision_distance`
        gives the per-sphere world penetration so we can still see WHICH spheres + WHERE. Reports the
        raw per-sphere cost + the world-frame positions of the penetrating spheres + `collision_free`."""
        checker = self._collision_checker()
        q = self._q_bhd(start_positions)
        valid = bool(checker.validate(q).view(-1)[0].item())
        state = checker.get_kinematics(q)
        d = checker.get_collision_distance(state).detach().cpu().numpy()
        sph = state.robot_spheres.detach().cpu().numpy().reshape(-1, 4)
        return _collision_report(d, sph, collision_free=valid)

    def free_config(self, n: int = 1) -> np.ndarray:
        """cuRobo's tested rejection sampler → up to `n` collision-free, in-bounds configs `[k, dof]`
        (world+self+bounds), in the planner's joint order. A grounded source of a known-safe config
        (e.g. a retract seed) — never a hand-guessed 'home' that may sit in the table."""
        q = self._collision_checker().sample(int(n), mask_valid=True)
        return q.detach().cpu().numpy().reshape(-1, len(self.planner.joint_names))

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
    def reset_cuda_graph(self) -> None:
        """Reset the CUDA graphs of the IK / trajopt / graph solvers. A warm-reused planner (we build
        ONCE and reuse across commission + every demo leg) can leave a STALE cuda graph where the same
        goal that planned before now fails — resolved only by a reset (cuRobo issue #503). Cheap;
        the next plan recompiles the graph."""
        for sol in (getattr(self.planner, "ik_solver", None),
                    getattr(self.planner, "trajopt_solver", None),
                    getattr(self.planner, "graph_planner", None)):
            if sol is not None and hasattr(sol, "reset_cuda_graph"):
                try:
                    sol.reset_cuda_graph()
                except Exception:  # noqa: BLE001 — best-effort hygiene, never fatal
                    pass

    def _start_js(self, start_positions: Optional[Dict[str, float]]):
        """A JointState over the planner's active joints; missing joints take the robot default. The
        start is CLAMPED to the joint limits: a marginally out-of-range seed (sensor noise, or the arm
        resting exactly AT a limit) otherwise fails with INVALID_START_STATE_JOINT_LIMITS (issue #524);
        clamping a hair inside lets planning proceed from the nearest valid config."""
        import torch
        from curobo.types import JointState

        names = self.planner.joint_names
        default = self.planner.default_joint_state.position.detach().cpu().numpy().reshape(-1)
        q = np.array(default[: len(names)], dtype=np.float32)
        lims = self.joint_limits()
        for i, n in enumerate(names):
            if start_positions and n in start_positions:
                q[i] = float(start_positions[n])
            if n in lims:
                lo, hi = lims[n]
                q[i] = min(max(float(q[i]), lo + 1e-4), hi - 1e-4)
        t = torch.tensor(q, device=self._device, dtype=torch.float32).unsqueeze(0)
        return JointState.from_position(t, joint_names=list(names))

    def _goal(self, goals: Dict[str, Goal]):
        """Build a GoalToolPose for {ee_link: (xyz, wxyz)} via cuRobo's CANONICAL `from_poses` — the
        same path `ik_check`/the IK example use and that the validator PROVED works. (We used to hand-
        assemble the [B,H,L,G,*] tensors; `from_poses` does the link ordering + goalset shaping so a
        subtle mismatch can't slip in.) Multi-TCP falls straight out: one Pose per ee_link."""
        from curobo.types import GoalToolPose, Pose
        import torch

        pose_dict = {}
        for f, (p, qn) in goals.items():
            pos = torch.tensor([list(p)[:3]], device=self._device, dtype=torch.float32)
            quat = torch.tensor([list(qn)[:4]], device=self._device, dtype=torch.float32)
            pose_dict[f] = Pose(position=pos, quaternion=quat)
        return GoalToolPose.from_poses(pose_dict, num_goalset=1)

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
