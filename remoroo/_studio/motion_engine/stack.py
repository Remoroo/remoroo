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

from .robot import (actuated_joints, load_robot_config, load_self_collision_ignore, load_spheres,
                    mimic_coupled_ignore, neighbor_ignore, sphere_health)
from .robotcfg import build_v2_robot_cfg
from .safety import Safety, audit_trajectory, load_safety
from .trajectory import Trajectory
from .world import WorldInputs, load_world, mask_robot_points
from .live_world import observed_extent, esdf_resolution, union_extent, obstacle_bboxes

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


def _default_mapper_factory(extent_m, center, robot_cfg, esdf_voxel_size=0.03, voxel_size=0.02):
    """The real mapper factory: build the GPU cuRobo perception subsystem (Mapper + RobotSegmenter +
    FilterDepth) from the SAME robot cfg the planner uses, at the (data-derived) resolution. Lazy
    import keeps the stack off-GPU."""
    from .curobo_v2 import make_mapper

    return make_mapper(extent_m, center, robot_cfg=robot_cfg,
                       esdf_voxel_size=esdf_voxel_size, voxel_size=voxel_size)


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
                 urdf_path: str = "robot.urdf", planner_factory: Optional[PlannerFactory] = None,
                 mapper_factory=None, self_collision_ignore: Optional[Dict[str, List[str]]] = None) -> None:
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
        # The spheres gate's AUTHORED self-collision ignore matrix (cuRobo's complete
        # compute_collision_matrix, written into collision_spheres.yml). When present it is the source
        # of truth — the planner reads it instead of re-deriving (artifact == reality, Decision A).
        self._authored_ignore = {k: list(v) for k, v in (self_collision_ignore or {}).items()}
        self._ignore_full: Optional[dict] = None      # cached generated matrix
        self._ignore_warn: Optional[str] = None       # why generation fell back, if it did
        self._ignore_provenance: Optional[str] = None  # "authored" (from the YAML) | "regenerated"
        self._world_finalized = False                 # has the cloud been self-masked yet?
        self._world_warn: Optional[str] = None        # why the self-mask didn't run, if it didn't
        # LIVE volumetric world (cuRobo Mapper → ESDF). Injectable factory so the wiring tests off-GPU.
        self._mapper_factory = mapper_factory
        self._mapper = None
        self._grid_meta: dict = {}                     # last ESDF grid extent/center (for legibility)
        self._live_voxels: np.ndarray = np.zeros((0, 4), dtype=float)
        # EVERY URDF actuated joint (incl. gripper drivers not in any group) → cuRobo cspace; the
        # ones we're not driving get locked, so cuRobo never meets a joint missing from the list.
        self._actuated = actuated_joints(urdf_path)
        self._cell_dir: Optional[str] = None          # set by from_cell → enables reload_world hot-swap

    # --- construction -----------------------------------------------------
    @classmethod
    def from_cell(cls, cell_dir: str, bridge=None, *, planner_factory: Optional[PlannerFactory] = None) -> "MotionStack":
        """Build from the on-disk cell: the AUTHORED config (cell.yaml groups) + collision_spheres.yml
        + world/ + safety."""
        config = load_robot_config(cell_dir)
        spheres, buffer = load_spheres(cell_dir)
        authored_ignore = load_self_collision_ignore(cell_dir)
        safety = load_safety(cell_dir)
        world = load_world(cell_dir)
        # ABSOLUTE path — cuRobo resolves a relative `urdf_path` against its OWN content/assets dir
        # (the "curobo/content/assets/robot.urdf is not a file" failure), never the cell.
        urdf_path = str((Path(cell_dir) / "robot_model" / "robot.urdf").resolve())
        st = cls(config=config, spheres=spheres, sphere_buffer=buffer, world=world,
                 safety=safety, bridge=bridge, urdf_path=urdf_path, planner_factory=planner_factory,
                 self_collision_ignore=authored_ignore)
        st._cell_dir = str(cell_dir)
        return st

    # --- world hot-swap (obstacle edits: update_world, NOT rebuild/re-warm) -----------------
    def set_world(self, world: WorldInputs) -> dict:
        """HOT-SWAP the modeled collision world into EVERY warm (cached) planner via cuRobo's
        `update_world` — no rebuild, no re-warmup (the costly part). The planners were built with cache
        headroom; a change within it just reloads. The live ESDF path re-fuses these cuboids on its
        next `update_world_live`. Returns {ok, n_planners}."""
        self.world = world
        n = 0
        for p in self._planners.values():
            if hasattr(p, "set_world"):
                p.set_world(world)
                n += 1
        return {"ok": True, "n_planners": n}

    def reload_world(self) -> dict:
        """Re-read the collision world from the cell on disk and HOT-SWAP it into the warm planners
        (cheap obstacle edits — no rebuild). Falls back to {ok:False} when there's no cell dir or the
        swap overflows the cache (the caller then rebuilds)."""
        if not self._cell_dir:
            return {"ok": False, "message": "stack has no cell dir to reload from"}
        world = load_world(self._cell_dir)
        return self.set_world(world)

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
        parent↔child adjacency if generation fails (e.g. off-GPU), recording why in `_ignore_warn`.
        ALWAYS unioned with the MIMIC-COUPLED mechanism pairs (a gripper's sibling knuckles/fingers):
        cuRobo's default-config matrix misses them (siblings, clear at the open default but overlapping
        at other gripper positions), so they phantom-collide. Deterministic + generic — in the
        GENERATOR (not a one-off gate dump), so every build for every rig gets it."""
        if self._ignore_full is not None:
            return self._ignore_full
        ig = {k: list(v) for k, v in (self._self_ignore or {}).items()}
        if self._authored_ignore:
            # DECISION A: the spheres gate already computed cuRobo's COMPLETE matrix and wrote it to
            # collision_spheres.yml. Trust it (artifact == reality) — no re-generation, no GPU at plan
            # time. Still union the mechanism-coupled pairs as a belt-and-braces invariant.
            ig = {k: list(v) for k, v in self._authored_ignore.items()}
            self._ignore_provenance = "authored"
        else:
            self._ignore_provenance = "regenerated"
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
        for link, coupled in mimic_coupled_ignore(self.urdf_path).items():
            ig[link] = sorted(set(ig.get(link, [])) | set(coupled))
        self._ignore_full = ig
        return ig

    def _robot_cfg_for(self, active_groups: Sequence[str]) -> dict:
        """The cuRobo robot_cfg dict for these groups. The cspace DEFAULT (retract / null-space
        target) is the CURRENT robot config, not zeros — so when we drive one limb's TCP over the
        full cspace (lock_joints: None), the un-driven limbs are HELD at their real pose instead of
        being pulled toward zero (which can swing them into the world). Shared by `_planner_for` and
        the diagnostic probes so they all see the same held configuration."""
        return build_v2_robot_cfg(
            self.config, self.spheres, active_groups=list(active_groups),
            urdf_path=self.urdf_path, sphere_buffer=self.sphere_buffer, limits=self._limits,
            self_collision_ignore=self._self_collision_ignore(), actuated_joints=self._actuated,
            default_joint_position=self._seed_positions())

    def _self_mask_world(self, planner) -> bool:
        """DEAD by design: the scanned cloud is no longer loaded into cuRobo (see
        `CuroboV2Planner._full_scene` — modeled cuboids only; the live ESDF is the real world), so
        there is nothing to self-mask and no reason to rebuild the planner. The robot is masked from
        the LIVE depth by cuRobo's `RobotSegmenter` instead (`update_world_live`). Kept as a no-op so
        the `_planner_for` build path stays single-shot (never the old mask→rebuild double build)."""
        self._world_finalized = True
        return False

    # --- LIVE volumetric world (cuRobo Mapper → ESDF) --------------------
    def _scene_extent(self, frames, tcp: str, *, margin: float = 0.25, stride: int = 10):
        """The extent of the SCENE the cameras see (depth with the ROBOT's own points removed), for
        sizing the ESDF grid so the real object is INSIDE it. We back-project each frame and drop the
        points within a sphere of the robot (the same rule the segmenter uses), then take the robust
        bbox of what's LEFT. Returns (extent, center) or None when the cameras see only the robot."""
        from . import debug_probe as dp
        planner = self._planner_for([tcp])
        spheres = (planner.robot_spheres(self._seed_positions())
                   if hasattr(planner, "robot_spheres") else np.zeros((0, 4)))
        kept = []
        for f in frames:
            cloud = dp.backproject_to_base(f.depth, f.intrinsics, f.cam_pose_in_base, stride=stride)
            k, _, _ = dp.split_cloud(cloud, spheres)
            if len(k):
                kept.append(k)
        if not kept:
            return None
        P = np.concatenate(kept, axis=0)
        lo = np.percentile(P, 1.0, axis=0)
        hi = np.percentile(P, 99.0, axis=0)
        ext = tuple(float(max(0.3, (hi[i] - lo[i]) + 2 * margin)) for i in range(3))
        ctr = tuple(float((hi[i] + lo[i]) / 2) for i in range(3))
        return ext, ctr

    def update_world_live(self, frames, tcp: Optional[str] = None) -> dict:
        """Refresh the LIVE collision world from N camera depth frames (ANY rig — the agent's
        commission pipeline calls this each cycle). cuRobo does the heavy lifting: each frame is
        cleaned (`FilterDepth`) and robot-MASKED at the live config (`RobotSegmenter`, so the robot is
        never in its own world — the bug the stored cloud kept hitting), fused into the `Mapper`; the
        modeled cuboids (table/wall/cage) are stamped into the SAME map; the ESDF is computed and
        applied to the planner. The stored cloud is unused — the collision world IS this one map."""
        tcp = tcp or next(iter(self._groups), None)
        if tcp is None:
            return {"ok": False, "message": "no kinematic groups"}
        planner = self._planner_for([tcp])
        if not hasattr(planner, "update_voxel_world"):
            return {"ok": False, "message": "planner has no live/voxel world support"}
        if self._mapper is None:
            robot_cfg = self._robot_cfg_for([tcp])
            # Size the grid to the SCENE — the depth with the ROBOT REMOVED — NOT the raw depth. A
            # wrist (eye-in-hand) camera's CLOSE robot pixels dominate the raw depth (~98%), so a raw
            # extent centres the grid on the robot and the real OBJECT falls OUTSIDE the grid: it gets
            # integrated but CLIPPED → 0 occupied voxels (the empty-ESDF bug). Union with the modeled
            # obstacles so they're always covered too.
            scene = self._scene_extent(frames, tcp)
            ext, ctr = union_extent(scene or observed_extent(frames), obstacle_bboxes(self.world.scene))
            esdf_vs, voxel_sz = esdf_resolution(ext)
            self._mapper = (self._mapper_factory or _default_mapper_factory)(
                ext, ctr, robot_cfg, esdf_vs, voxel_sz)
            self._grid_meta = {"extent": [round(v, 3) for v in ext], "center": [round(v, 3) for v in ctr],
                               "esdf_voxel_size": round(float(esdf_vs), 4), "from_scene": scene is not None}
        self._mapper.reset()
        q = self._seed_positions()                 # the live config the robot is masked at
        total_px, n_masked = 0, 0
        for f in frames:
            n_masked += int(self._mapper.integrate(f.depth, f.intrinsics, f.cam_pose_in_base, q) or 0)
            total_px += int(np.asarray(f.depth).size)
        if hasattr(self._mapper, "fuse_static"):   # one canonical world: live depth + modeled cuboids
            self._mapper.fuse_static(getattr(planner, "modeled_scene", None))
        voxelgrid = self._mapper.compute_esdf()
        planner.update_voxel_world(voxelgrid)
        self._live_voxels = np.asarray(self._mapper.occupied_voxels(), dtype=float).reshape(-1, 3)

        # GROUND TRUTH side-by-side: OUR masking (the one that keeps the object GREEN in the debug view)
        # vs cuRobo's RobotSegmenter (`n_masked` above). If `our_mask_fraction` ≪ `mask_fraction`, the
        # segmenter is over-masking (the bug); if they AGREE, the object isn't surviving masking at all
        # and it's the grid/occupancy. Cheap (subsampled). No more guessing — measure.
        ours: dict = {}
        try:
            from . import debug_probe as dp
            sph = (planner.robot_spheres(q) if hasattr(planner, "robot_spheres") else np.zeros((0, 4)))
            ok = om = 0
            kept_pts = []
            for f in frames:
                c = dp.backproject_to_base(f.depth, f.intrinsics, f.cam_pose_in_base, stride=12)
                k, m, _ = dp.split_cloud(c, sph)
                ok += int(len(k)); om += int(len(m))
                if len(k):
                    kept_pts.append(k)
            kb = dp.bbox(np.concatenate(kept_pts, 0)) if kept_pts else None
            ours = {"our_kept_pts": ok, "our_masked_pts": om,
                    "our_mask_fraction": round(om / max(1, ok + om), 3), "our_kept_bbox": kb}
        except Exception as e:  # noqa: BLE001
            ours = {"our_mask_error": f"{type(e).__name__}: {e}"}

        # MASKING SANITY — the robot is masked from the depth at the joints we hand the segmenter. If
        # the segmenter's joint NAMES don't match the seed, every missing joint silently defaults to
        # 0.0 → the robot is masked at the ZERO pose, in the wrong place, so its OWN body survives as
        # world voxels: the arm gets voxelised, the start is born in collision, and NOTHING plans. We
        # surface the mask fraction + the seed↔segmenter name match so this is visible at the gate
        # (a 96%-masked image or 0/N matched joints is the smoking gun), not a silent mystery.
        seg_names = list(getattr(self._mapper, "segmenter_joint_names", []) or [])
        n_matched = sum(1 for n in seg_names if n in q)
        frac = round(n_masked / total_px, 3) if total_px else 0.0
        warnings: List[str] = []
        if seg_names and n_matched < len(seg_names):
            warnings.append(f"seed↔segmenter joint mismatch: only {n_matched}/{len(seg_names)} of the "
                            "robot-mask joints are in the live config — the robot is masked at the WRONG "
                            "pose (missing joints default to 0), so its own body is voxelised. Fix the "
                            "bridge joint names / the seed (get_observation joint_positions).")
        if frac > 0.85:
            warnings.append(f"{round(frac * 100)}% of depth masked as robot — implausibly high: the mask "
                            "is misplaced (wrong config, or the camera extrinsics/calibration are off), "
                            "voxelising the arm itself rather than the scene.")
        # SPLIT live-depth vs modeled-obstacle voxels so "is the camera contributing anything?" is
        # answerable: count occupied voxels NOT inside any modeled obstacle AABB (+1 voxel margin) =
        # the LIVE depth's contribution. n_live_voxels≈0 with frames present ⇒ the depth is masked away
        # / invalid (see camera_inputs valid_frac), not a render bug.
        n_live = self._live_voxels.shape[0]
        boxes = obstacle_bboxes(self.world.scene)
        if boxes and n_live:
            m = self.esdf_voxel_size() + 1e-6      # mapper voxel size (esdf_vs is unset on a reused mapper)
            inside = np.zeros(n_live, dtype=bool)
            for lo, hi in boxes:
                lo = np.asarray(lo) - m; hi = np.asarray(hi) + m
                inside |= np.all((self._live_voxels >= lo) & (self._live_voxels <= hi), axis=1)
            n_live = int((~inside).sum())
        out = {"ok": True, "n_frames": len(frames), "n_robot_pixels_masked": n_masked,
               "total_pixels": total_px, "mask_fraction": frac, "n_voxels": int(len(self._live_voxels)),
               "n_live_voxels": int(n_live), "n_obstacle_voxels": int(len(self._live_voxels) - n_live),
               "segmenter_joints": len(seg_names), "seed_joints_matched": n_matched,
               "grid": dict(self._grid_meta), **ours}
        if warnings:
            out["warnings"] = warnings
        return out

    def esdf_voxels(self) -> np.ndarray:
        """The current live world's occupied voxel centres `[N, 3]` (x, y, z) in the base frame — the
        Studio renders these as the live collision world."""
        return self._live_voxels

    def esdf_voxel_size(self) -> float:
        """The live ESDF voxel edge length (m), so the Studio sizes the debug cubes at the planner's
        TRUE resolution. Falls back to a nominal 0.03 m before any live build."""
        m = self._mapper
        return float(getattr(m, "esdf_voxel_size", 0.03) or 0.03) if m is not None else 0.03

    # --- debug world: EXACTLY what the planner avoids, as inspectable geometry ----------------
    def debug_world(self) -> dict:
        """The EXACT collision world the planner clears, as plain geometry for render/inspection:
        the live ESDF occupied voxels (which ALREADY include the fused static obstacles) + the modeled
        cuboids. The ONE source of truth for 'what got into the planner' — used to SEE why the start is
        born in collision (the occupied volume sitting on the arm) instead of guessing on the rig."""
        vox = np.asarray(self.esdf_voxels(), dtype=float).reshape(-1, 3)
        cuboids = [{"name": name, "dims": list(b.get("dims") or [0.1, 0.1, 0.1]),
                    "pose": list(b.get("pose") or [0, 0, 0, 1, 0, 0, 0])}
                   for name, b in (self.world.scene.get("cuboid") or {}).items()]
        return {"voxels": vox, "voxel_size": self.esdf_voxel_size(), "cuboids": cuboids,
                "n_voxels": int(len(vox)), "n_cuboids": len(cuboids)}

    def debug_dump(self, frames, *, tcp: Optional[str] = None, save_dir: Optional[str] = None,
                   stride: int = 6, max_points: int = 6000, seg_dist: float = 0.05,
                   offend_dist: float = 0.03) -> dict:
        """COMPLETE, transparent live-world debug — SAVE + SHOW everything, before vs after masking:
        camera poses, the RAW back-projected clouds (is the cloud on the real scene? → is the camera
        pose right?), what the robot mask KEEPS vs REMOVES (does masking eat the scene?), the robot
        collision spheres at the seed, the world voxels + modeled obstacles, and the born-collision
        verdict. Saves a `.npz` (full clouds) + `.json` (summary) to `save_dir`; returns subsampled
        clouds for the Studio to render. The split uses the SAME sphere rule the segmenter applies,
        done transparently here so it's inspectable (no opaque GPU mask)."""
        from . import debug_probe as dp
        tcp = tcp or next(iter(self._groups), None)
        rep: dict = {"tcp": tcp, "seg_dist": float(seg_dist)}
        if tcp is None:
            rep["error"] = "no kinematic groups"
            return rep
        planner = self._planner_for([tcp])
        seed = self._seed_positions()
        rep["seed"] = {"n_joints": len(seed),
                       "joints": {k: round(float(v), 4) for k, v in seed.items()}}
        spheres = np.zeros((0, 4))
        if hasattr(planner, "robot_spheres"):
            try:
                spheres = np.asarray(planner.robot_spheres(seed), float).reshape(-1, 4)
            except Exception as e:  # noqa: BLE001
                rep["spheres_error"] = f"{type(e).__name__}: {e}"
        rep["n_spheres"] = int(len(spheres))
        rep["spheres_bbox"] = dp.bbox(spheres[:, :3]) if spheres.size else None
        rep["spheres"] = spheres.round(4).tolist()        # [x,y,z,r] — small; the Studio renders them
        save: dict = {"spheres": spheres} if spheres.size else {}
        cams = []
        for i, f in enumerate(frames):
            cloud = dp.backproject_to_base(f.depth, f.intrinsics, f.cam_pose_in_base, stride=stride)
            kept, masked, _ = dp.split_cloud(cloud, spheres, seg_dist=seg_dist)
            # OFFENDING = kept world points that are STILL within `offend_dist` of a robot sphere
            # surface (incl. inside, negative) — the ones that survive masking yet sit on/in the robot,
            # i.e. the born-collision culprits. If many, the robot SPHERES don't match where the robot
            # really is (mask/extrinsic/base-to-base) OR masking is under-clearing. Shown LARGE +
            # see-through so even points buried inside the robot are visible.
            d_kept = dp.nearest_sphere_signed_distance(kept, spheres)
            offending = kept[d_kept < float(offend_dist)] if (kept.size and spheres.size) else np.zeros((0, 3))
            p, q = f.cam_pose_in_base
            a = np.asarray(f.depth, float)
            valid = np.isfinite(a) & (a > 1e-3)
            cams.append({
                "name": f.name, "pose_xyz": [round(float(v), 4) for v in list(p)[:3]],
                "pose_wxyz": [round(float(v), 4) for v in list(q)[:4]],
                "depth_hw": list(a.shape), "valid_frac": round(float(valid.mean()), 3) if a.size else 0.0,
                "n_points": int(len(cloud)), "n_kept": int(len(kept)), "n_masked": int(len(masked)),
                "n_offending": int(len(offending)),
                "cloud_bbox": dp.bbox(cloud), "kept_bbox": dp.bbox(kept), "offending_bbox": dp.bbox(offending),
                "raw": dp.subsample(cloud, max_points).round(4).tolist(),
                "kept": dp.subsample(kept, max_points).round(4).tolist(),
                "masked": dp.subsample(masked, max_points).round(4).tolist(),
                "offending": dp.subsample(offending, max_points).round(4).tolist()})
            save[f"cam{i}_{f.name}_raw"] = cloud
            save[f"cam{i}_{f.name}_kept"] = kept
            save[f"cam{i}_{f.name}_masked"] = masked
            save[f"cam{i}_{f.name}_offending"] = offending
        rep["cameras"] = cams
        vox = np.asarray(self.esdf_voxels(), float).reshape(-1, 3)
        rep["world"] = {"n_voxels": int(len(vox)), "voxel_size": self.esdf_voxel_size(),
                        "voxels_bbox": dp.bbox(vox),
                        "obstacles": [{"name": n, "dims": b.get("dims"), "pose": b.get("pose")}
                                      for n, b in (self.world.scene.get("cuboid") or {}).items()]}
        if vox.size:
            save["voxels"] = vox
        if hasattr(planner, "world_sphere_collision"):
            try:
                rep["start_collision"] = planner.world_sphere_collision(seed)
            except Exception as e:  # noqa: BLE001
                rep["start_collision_error"] = f"{type(e).__name__}: {e}"
        if save_dir:
            import json
            from pathlib import Path as _P
            d = _P(save_dir)
            d.mkdir(parents=True, exist_ok=True)
            np.savez_compressed(str(d / "debug_clouds.npz"),
                                **{k: np.asarray(v, float) for k, v in save.items()})
            summary = {k: v for k, v in rep.items() if k != "cameras"}
            summary["cameras"] = [{kk: vv for kk, vv in c.items() if kk not in ("raw", "kept", "masked")}
                                  for c in cams]
            (d / "debug_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
            rep["saved"] = {"npz": str(d / "debug_clouds.npz"), "json": str(d / "debug_summary.json")}
        return rep

    def save_debug_world(self, path: str) -> dict:
        """Write `debug_world()` to a binary STL at `path` — a cube per occupied ESDF voxel + an
        oriented box per modeled cuboid — for offline inspection (MeshLab/Blender). Same geometry the
        Studio renders live; this is the saveable artifact the operator asked for."""
        from pathlib import Path as _P
        from .debug_world import world_stl_bytes
        d = self.debug_world()
        data = world_stl_bytes(d["voxels"], d["voxel_size"], d["cuboids"])
        _P(path).parent.mkdir(parents=True, exist_ok=True)
        _P(path).write_bytes(data)
        return {"ok": True, "path": str(path), "n_voxels": d["n_voxels"], "n_cuboids": d["n_cuboids"],
                "voxel_size": d["voxel_size"], "n_triangles": (d["n_voxels"] + d["n_cuboids"]) * 12,
                "bytes": len(data)}

    def reset_cuda_graph(self) -> None:
        """Reset every warm planner's CUDA graphs — hygiene before a repeatable run that reuses the
        build (a stale graph can make a previously-plannable goal fail; cuRobo issue #503)."""
        for p in self._planners.values():
            if hasattr(p, "reset_cuda_graph"):
                p.reset_cuda_graph()

    def link_pose(self, link: str, joints: Optional[Dict[str, float]] = None,
                  tcp: Optional[str] = None):
        """FK ANY URDF link → `(xyz, wxyz)` in the base frame at the given (or live) config. The
        general helper the agent's commission pipeline uses to resolve each camera's calibrated
        optical-frame pose for `update_world_live` (eye-in-hand: at the live joints; static: fixed).
        Returns None if the planner can't FK (a non-cuRobo test factory without link_pose)."""
        tcp = tcp or next(iter(self._groups), None)
        if tcp is None:
            return None
        planner = self._planner_for([tcp])
        if not hasattr(planner, "link_pose"):
            return None
        return planner.link_pose(link, joints if joints is not None else self._seed_positions())

    def _planner_for(self, active_groups: Sequence[str]):
        """Get/build (and cache) the planner whose tool_frames = these groups' tips."""
        for a in active_groups:
            self._group(a)                     # KeyError on an unknown group, before any build
        key = frozenset(active_groups)
        if key not in self._planners:
            planner = self._factory(self._robot_cfg_for(active_groups), self.world,
                                    limits=self._limits, max_goalset=1)
            if self._self_mask_world(planner):     # masked the cloud → rebuild from the masked world
                planner = self._factory(self._robot_cfg_for(active_groups), self.world,
                                        limits=self._limits, max_goalset=1)
            self._planners[key] = planner
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

    def _seed_positions(self) -> Dict[str, float]:
        """The CURRENT joints of the WHOLE robot (every group), to seed the plan start. SAFETY-
        CRITICAL now that we drive ALL joints (no lock_joints): if the un-goaled limbs were left at
        the default 0.0, the plan would command them to fly home. Prefer the bridge's full
        `get_observation().joint_positions` (covers grippers too), falling back to per-group reads."""
        if self.bridge is None:
            return {}
        try:
            obs = self.bridge.get_observation()
            jp = getattr(obs, "joint_positions", None)
            if jp:
                return {str(n): float(v) for n, v in dict(jp).items()}
        except Exception:  # noqa: BLE001
            pass
        return self._current_positions(list(self._groups.keys()))

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
        res = planner.plan_pose(goals, self._seed_positions(), max_attempts=max_attempts)
        return self._finish(res, execute)

    def move_through_poses(self, tcp: str, poses: Sequence[object], *, execute: bool = True,
                           max_attempts: int = 3) -> MoveResult:
        """Drive one TCP through a sequence of poses (one concatenated collision-free path)."""
        planner = self._planner_for([tcp])
        legs = [_norm_pose(p) for p in poses]
        res = planner.plan_through(self._tip(tcp), legs, self._seed_positions(),
                                   max_attempts=max_attempts)
        return self._finish(res, execute)

    def current_tool_pose(self, tcp: str) -> object:
        """The live (xyz, wxyz) of `tcp`'s tool frame in the base frame — FK of the current joints.
        Lets the Studio spawn a target gizmo at the real TCP and lets `plan_to_point` keep the
        current orientation. Empty/None if the planner can't FK (e.g. a non-cuRobo test factory)."""
        planner = self._planner_for([tcp])
        if not hasattr(planner, "current_tool_pose"):
            return None
        return planner.current_tool_pose(self._tip(tcp), self._seed_positions())

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

    def _validate_structure(self, kin: dict) -> dict:
        """Every base_link / tool_frame / cspace joint / collision link in our cfg MUST exist in the
        URDF — a typo or stale name is a silent config bug. Pure parse, no GPU."""
        out: dict = {"errors": []}
        try:
            from calib_engine import urdf_io
            facts = urdf_io.urdf_facts(self.urdf_path)
            links = {l["name"] for l in facts.get("links") or []}      # urdf_facts links are DICTS
            joints = {j["name"] for j in facts.get("joints", [])}
            out["n_urdf_links"], out["n_urdf_joints"] = len(links), len(joints)
            if kin.get("base_link") not in links:
                out["errors"].append(f"base_link {kin.get('base_link')!r} not in URDF links")
            for t in kin.get("tool_frames") or []:
                if t not in links:
                    out["errors"].append(f"tool_frame {t!r} not in URDF links")
            for j in kin["cspace"]["joint_names"]:
                if j not in joints:
                    out["errors"].append(f"cspace joint {j!r} not in URDF joints")
            for cl in (kin.get("collision_link_names") or []):
                if cl not in links:
                    out["errors"].append(f"collision_link {cl!r} not in URDF links")
        except Exception as e:  # noqa: BLE001
            out["errors"].append(f"urdf_facts failed: {type(e).__name__}: {e}")
        return out

    def validate_config(self, tcp: Optional[str] = None) -> dict:
        """SOLID check that our cuRobo robot_cfg is correct — structure + cuRobo's CANONICAL IK
        round-trip (not our plan path). The decisive field is `ik.checks['0cm'].success`: cuRobo MUST
        re-solve the EXACT current tool pose; if it can't, the cfg/kinematics is broken (base_link /
        tool_frame / cspace / quaternion), not reach or collision."""
        tcp = tcp or next(iter(self._groups), None)
        rep: dict = {"tcp": tcp}
        if tcp is None:
            rep["error"] = "no kinematic groups in the cell"
            return rep
        kin = self._robot_cfg_for([tcp])["robot_cfg"]["kinematics"]
        rep["cfg"] = {"base_link": kin.get("base_link"), "tool_frames": kin.get("tool_frames"),
                      "tip_used": self._tip(tcp), "n_cspace_joints": len(kin["cspace"]["joint_names"]),
                      "cspace_joints": kin["cspace"]["joint_names"], "n_locked": len(kin.get("lock_joints") or {}),
                      "locked": list((kin.get("lock_joints") or {}).keys()),
                      "n_collision_links": len(kin.get("collision_link_names") or [])}
        rep["structure"] = self._validate_structure(kin)
        start = self._seed_positions()
        tip = self._tip(tcp)

        # Canonical IK on a NO-COLLISION probe (locked cfg) — isolates KINEMATICS from collision.
        rep["ik_no_collision"] = self._ik_probe(self._robot_cfg_for([tcp]), tip, start)
        ik0 = ((rep["ik_no_collision"].get("checks", {}) or {}).get("0cm", {}) or {}).get("success")

        # If the locked cfg's kinematics can't even re-solve the current pose, test with lock_joints
        # REMOVED (all joints active) — the decisive test for whether OUR lock_joints (V2 dict-loader)
        # is the bug, just like self_collision_ignore was.
        if ik0 is False:
            nolock = self._robot_cfg_for([tcp])
            nolock["robot_cfg"]["kinematics"]["lock_joints"] = None
            rep["ik_no_collision_no_lock"] = self._ik_probe(nolock, tip, start)

        nolock0 = ((rep.get("ik_no_collision_no_lock", {}).get("checks", {}) or {}).get("0cm", {}) or {}).get("success")
        struct_bad = bool(rep["structure"].get("errors"))
        if struct_bad:
            rep["verdict"] = f"CONFIG BROKEN (structure): {rep['structure']['errors']}"
        elif ik0 is True:
            rep["verdict"] = ("KINEMATICS OK: cuRobo's canonical IK re-solves the current pose with collision "
                              "OFF. So the cfg is kinematically fine — the failure is COLLISION (world/self), or "
                              "OUR plan path (_goal builds GoalToolPose by hand vs from_poses). Not the kinematics.")
        elif ik0 is False and nolock0 is True:
            rep["verdict"] = ("LOCK_JOINTS IS THE BUG: with lock_joints REMOVED, cuRobo IK solves the current "
                              "pose; with our lock_joints it CANNOT. The V2 dict-loader mishandles our lock spec "
                              "(same class as the self_collision_ignore gap). Fix build_v2_robot_cfg's lock — "
                              "generically, for any morphology.")
        elif ik0 is False and nolock0 is False:
            rep["verdict"] = ("DEEPER KINEMATIC BUG: IK fails even with NO locks and NO collision → base_link / "
                              "tool_frame / cspace / quaternion. See cfg + ik_no_collision_no_lock.")
        else:
            rep["verdict"] = "IK probe did not run — see ik_no_collision.error."
        return rep

    def _ik_probe(self, robot_cfg: dict, tip: str, start: Dict[str, float]) -> dict:
        """Build a throwaway NO-COLLISION planner from `robot_cfg` and run cuRobo's canonical IK
        round-trip on it — pure kinematics, no world, no self-collision. GPU; guarded."""
        try:
            from .curobo_v2 import CuroboV2Planner
            from .world import WorldInputs
            p = CuroboV2Planner(robot_cfg, WorldInputs(scene={}),
                                limits=self._limits, max_goalset=1, warmup=False, self_collision_check=False)
            return p.ik_check(tip, start)
        except Exception as e:  # noqa: BLE001
            return {"error": f"{type(e).__name__}: {e}"}

    def world_alignment(self, tcp: Optional[str] = None) -> dict:
        """Is the scanned CLOUD in the SAME frame as the ROBOT, and is the seed actually applied?
        Reports the robot-spheres bbox (FK at the seed) vs the cloud bbox, the min clearance between
        them, and whether the bridge's joint NAMES match the planner's (a mismatch ⇒ the seed is
        ignored ⇒ the robot is modelled at the DEFAULT config, misaligned with the scan ⇒ cuRobo
        collides while a point-vs-sphere mask finds nothing)."""
        tcp = tcp or next(iter(self._groups), None)
        rep: dict = {"tcp": tcp}
        if tcp is None:
            rep["error"] = "no groups"
            return rep
        planner = self._planner_for([tcp])
        seed = self._seed_positions()
        pj = list(getattr(planner, "joint_names", []) or [])
        matched = [n for n in pj if n in seed]
        rep["seed"] = {"n_read": len(seed), "names_sample": list(seed.keys())[:14],
                       "values_sample": {k: round(float(v), 3) for k, v in list(seed.items())[:8]}}
        rep["planner_joint_names"] = pj
        # APPLIED = every joint the bridge reports matched a planner joint (the planner may have a few
        # MORE, e.g. gripper drive_joints the bridge doesn't report — those correctly default).
        applied = len(seed) > 0 and len(matched) == len(seed)
        rep["seed_matches_planner"] = {"matched": matched, "n_matched": len(matched),
                                       "n_planner": len(pj), "n_seed": len(seed),
                                       "unmatched_planner_joints": [n for n in pj if n not in seed],
                                       "applied": applied}
        if hasattr(planner, "robot_spheres"):
            try:
                sph = planner.robot_spheres(seed)
                rep["robot_spheres_bbox"] = _bbox(sph[:, :3])
                rep["n_robot_spheres"] = int(len(sph))
                pts = np.asarray(self.world.points, dtype=float).reshape(-1, 3)
                rep["cloud_bbox"] = _bbox(pts) if len(pts) else None
                rep["n_cloud_points"] = int(len(pts))
                if len(pts) and len(sph):
                    mind, inside = 1e9, 0
                    for s in sph:
                        near = np.sqrt(((pts - s[:3]) ** 2).sum(axis=1)) - float(s[3])
                        mind = min(mind, float(near.min()))
                        inside += int((near < 0.02).sum())
                    rep["cloud_to_robot_min_clearance_m"] = round(mind, 4)
                    rep["cloud_points_within_2cm_of_robot"] = inside
                    # DEBUG: what the self-mask WOULD remove right now, vs what actually persisted —
                    # exposes a finalize-flow / seed-timing bug (mask ran at a different config).
                    voxel = float(getattr(self.world, "voxel_size", 0.02))
                    mm = max(0.04, 2.0 * voxel + float(self.sphere_buffer) + 0.02)
                    _, n_would = mask_robot_points(self.world.points, sph, margin=mm)
                    rep["mask_check"] = {
                        "voxel_size_m": round(voxel, 4), "sphere_buffer_m": round(float(self.sphere_buffer), 4),
                        "margin_m": round(mm, 4), "n_would_remove_now": int(n_would),
                        "n_robot_masked_persisted": self.world.meta.get("n_robot_masked"),
                        "world_finalized": self._world_finalized,
                        "n_planners_built": len(self._planners)}
            except Exception as e:  # noqa: BLE001
                rep["robot_spheres_error"] = f"{type(e).__name__}: {e}"
        clr = rep.get("cloud_to_robot_min_clearance_m")
        voxel = float(getattr(self.world, "voxel_size", 0.02))
        if not rep["seed_matches_planner"]["applied"]:
            rep["verdict"] = ("SEED NOT APPLIED — the bridge reports joints the planner doesn't know (or vice "
                              "versa), so the robot is modelled at the wrong config. Fix _seed_positions / the "
                              "bridge joint names.")
        elif clr is None:
            rep["verdict"] = "no cloud or no spheres — nothing to align."
        elif clr < 0:
            rep["verdict"] = ("ROBOT INSIDE THE CLOUD (negative clearance) — residual robot points the scan "
                              "didn't mask. The world-build self-mask removes these.")
        elif clr <= voxel + 0.04:
            rep["verdict"] = (f"RESIDUAL-ROBOT SHELL: the cloud sits {round(clr*100,1)}cm off the robot — within "
                              f"cuRobo's VOXEL-INFLATED footprint (each point → a {round(voxel*100)}cm cube, so the "
                              "occupancy mesh reaches the robot). NOT a frame error. The self-mask margin now "
                              "covers voxel+buffer+activation, so these get removed.")
        else:
            rep["verdict"] = (f"CLOUD {round(clr*100,1)}cm OFF THE ROBOT yet cuRobo collides — too far for voxel "
                              "inflation → suspect a FRAME/CALIBRATION offset (the scan isn't truly in the robot "
                              "base frame). Investigate the hand-eye X used by world-scan.")
        return rep

    def world_collision_at_seed(self, tcp: Optional[str] = None, *, cloud_only: bool = True) -> dict:
        """GROUND TRUTH + CROSS-CHECK: ask cuRobo's collision checker DIRECTLY (no IK) whether the
        robot penetrates the world at the seed config, then for each sphere cuRobo flags, compute the
        ANALYTIC nearest-cloud clearance. If cuRobo says penetrating but the analytic clearance is
        large → what we hand cuRobo (spheres/cloud/frame) differs from what we debug. If they agree →
        the geometry really collides and we trust it."""
        tcp = tcp or next(iter(self._groups), None)
        rep: dict = {"tcp": tcp, "cloud_only": cloud_only}
        if tcp is None:
            rep["error"] = "no groups"
            return rep
        seed = self._seed_positions()
        rep["seed_n_joints"] = len(seed)
        try:
            from dataclasses import replace
            from .curobo_v2 import CuroboV2Planner
            world = replace(self.world, scene={}) if cloud_only else self.world
            rep["world"] = {"n_points": int(world.n_points),
                            "n_cuboids": sum(len(v) for v in (world.scene or {}).values()),
                            "voxel_size": float(world.voxel_size)}
            probe = CuroboV2Planner(self._robot_cfg_for([tcp]), world, limits=self._limits,
                                    max_goalset=1, warmup=False, self_collision_check=False)
            info = probe.world_sphere_collision(seed)
            rep.update(info)
            if info.get("penetrating_spheres") and world.n_points:
                pts = np.asarray(world.points, dtype=float).reshape(-1, 3)
                cross = []
                for s in info["penetrating_spheres"][:8]:
                    c = np.asarray(s["xyz"], dtype=float)
                    nearest = float(np.sqrt(((pts - c) ** 2).sum(axis=1)).min()) - float(s["r"])
                    cross.append({"sphere_xyz": s["xyz"], "sphere_r": s["r"], "curobo_cost": s["cost"],
                                  "analytic_nearest_cloud_clearance_m": round(nearest, 4)})
                rep["cross_check"] = cross
                worst = min(c["analytic_nearest_cloud_clearance_m"] for c in cross)
                rep["verdict"] = (
                    "AGREE — cuRobo's penetrating spheres ARE near the cloud (analytic clearance ≤ a few cm). "
                    "The geometry genuinely collides; trust the mask/clearance path."
                    if worst < 0.03 else
                    f"MISMATCH — cuRobo flags spheres as penetrating but the nearest cloud point is {round(worst*100,1)}cm "
                    "away analytically. What we give cuRobo ≠ what we debug: different spheres/cloud/FRAME. "
                    "Likely the cloud mesh pose or the sphere frame inside cuRobo.")
            else:
                rep["verdict"] = ("cuRobo reports NO world penetration at the seed config → the start is clear; the "
                                  "ik_check failure is the IK SOLUTION moving joints into the world, not the start.")
        except Exception as e:  # noqa: BLE001
            rep["error"] = f"{type(e).__name__}: {e}"
        return rep

    def _world_without(self, names: Sequence[str]):
        """A copy of the live world with the named cuboids removed (for ablation/verification)."""
        from dataclasses import replace
        drop = set(names)
        cub = {k: v for k, v in (self.world.scene.get("cuboid") or {}).items() if k not in drop}
        return replace(self.world, scene={**self.world.scene, "cuboid": cub})

    def verify_start_collision(self, tcp: Optional[str] = None) -> dict:
        """RIGOROUS verification (cuRobo's own IK collision verdict, self-collision OFF to isolate the
        WORLD): is the robot's current/start config inside a world obstacle, and WHICH one? Controlled
        ablation — remove the whole world, then each penetrated box one at a time, and re-check IK at
        the exact current pose. A box whose REMOVAL makes the start collision-free is PROVEN to clip
        the robot. This is the evidence before we design any prevention."""
        tcp = tcp or next(iter(self._groups), None)
        rep: dict = {"tcp": tcp}
        if tcp is None:
            rep["error"] = "no kinematic groups in the cell"
            return rep
        tip = self._tip(tcp)
        try:
            self._planner_for([tcp])        # finalize the canonical world (self-mask) so we verify the REAL world
        except Exception:  # noqa: BLE001
            pass
        start = self._seed_positions()
        import gc

        def check(world, want_pen=False):
            """Build a FRESH no-self probe with `world` (clear_world is unreliable — cuRobo bakes
            scene_model at build, so we rebuild), return (world_collision_free, penetrated_boxes|None).
            The verdict is cuRobo's PUBLIC RobotCollisionChecker per-sphere WORLD distance (via
            world_sphere_collision) — world-only (n_penetrating), so this stays a clean WORLD-vs-self
            isolation (we don't use the full `validate`, which also folds in self-collision)."""
            try:
                from .curobo_v2 import CuroboV2Planner
                p = CuroboV2Planner(self._robot_cfg_for([tcp]), world, limits=self._limits,
                                    max_goalset=1, warmup=False, self_collision_check=False)
                ok = (int(p.world_sphere_collision(start).get("n_penetrating", 0)) == 0)
                pen = None
                if want_pen and hasattr(p, "robot_spheres"):
                    sph = p.robot_spheres(start)
                    pen = {n: v for n, v in _sphere_box_overlaps(sph, world.scene).items()
                           if v.get("spheres_penetrating", 0) > 0}
                del p
                gc.collect()
                try:
                    import torch
                    torch.cuda.empty_cache()
                except Exception:  # noqa: BLE001
                    pass
                return ok, pen
            except Exception as e:  # noqa: BLE001
                return f"err: {type(e).__name__}: {e}", None

        full_ok, pen = check(self.world, want_pen=True)
        rep["start_ok_full_world"] = full_ok
        rep["penetrated_boxes"] = pen or {}

        from dataclasses import replace
        from .world import WorldInputs
        rep["start_ok_no_world"], _ = check(WorldInputs(scene={}))
        # ISOLATE cloud vs cuboids (collision is monotonic, so each ablation is decisive):
        rep["start_ok_no_cloud"], _ = check(replace(self.world, points=np.zeros((0, 3))))   # cuboids only
        rep["start_ok_no_cuboids"], _ = check(replace(self.world, scene={}))                 # cloud only

        nc = rep["start_ok_no_cloud"]        # True ⇒ cuboids ALONE are fine ⇒ the CLOUD blocks
        ncu = rep["start_ok_no_cuboids"]     # True ⇒ cloud ALONE is fine ⇒ the CUBOIDS block
        if full_ok is True:
            rep["verdict"] = ("NOT THE WORLD: with self-collision off the start is already collision-free against "
                              "the full world. The blocker is self-collision — cross-check validate_config.")
        elif rep["start_ok_no_world"] is not True:
            rep["verdict"] = ("start in collision even with an EMPTY world (self off) → kinematics/cfg or the seed; "
                              "validate_config says KINEMATICS OK, so suspect _seed_positions. Inconclusive here.")
        elif nc is True and ncu is not True:
            rep["verdict"] = ("VERIFIED — the SCANNED CLOUD clips the robot at rest: removing the cloud clears the "
                              "start; the cuboids alone don't. The world-scan didn't fully MASK the robot, so the "
                              "robot's own body is in the cloud → it collides with itself-as-environment. Fix the "
                              "scan masking / crop the cloud away from the robot's volume.")
        elif ncu is True and nc is not True:
            rep["verdict"] = ("VERIFIED — the CUBOIDS clip the robot at rest: removing them clears the start; the "
                              "cloud alone doesn't. A cage wall / obstacle box intersects the robot (e.g. ws_zlo "
                              "floor at z=0, or obs_1_wall). Fix the safety bounds / obstacle placement.")
        else:
            rep["verdict"] = ("BOTH the cloud AND the cuboids clip the robot independently (each removal alone "
                              "doesn't clear it). Fix both: the scan masking AND the offending cage/obstacle box.")
        return rep

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
        start = self._seed_positions()

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

        # PURE-IK reach probe (no world, no self-collision) — uses cuRobo's canonical `ik_check`
        # (ik_solver.solve_pose), NOT plan_pose: trajopt on an un-warmed probe can fail spuriously and
        # mislabel a fine config as broken. `0cm` is the EXACT current pose; if even pure IK can't
        # solve it, the cfg is broken. If pure IK reaches but the PLANS (above) fail, it's COLLISION.
        rep["plan_no_world_no_selfcoll"] = {"ok": None, "message": "probe not run"}
        rep["ik_reach_no_world_no_selfcoll"] = {}
        try:
            from .curobo_v2 import CuroboV2Planner
            from .world import WorldInputs
            tip = self._tip(tcp)
            empty = WorldInputs(scene={})
            probe = CuroboV2Planner(self._robot_cfg_for([tcp]), empty, limits=self._limits,
                                    max_goalset=1, warmup=False, self_collision_check=False)
            res = probe.ik_check(tip, start)   # PURE IK — same path validate_config proved works
            reach = {k: bool(v.get("success")) for k, v in (res.get("checks") or {}).items()}
            rep["ik_reach_no_world_no_selfcoll"] = reach
            rep["plan_no_world_no_selfcoll"] = {"ok": reach.get("0cm"),
                                                "message": f"pure-IK reach by offset: {reach}"}
        except Exception as e:  # noqa: BLE001
            rep["plan_no_world_no_selfcoll"] = {"ok": None, "message": f"probe failed: {type(e).__name__}: {e}"}

        a = rep["plan_with_world"]["ok"]
        b = rep["plan_without_world"]["ok"]
        reach = rep.get("ik_reach_no_world_no_selfcoll") or {}
        c0 = reach.get("0cm")
        viol = rep.get("limits", {}).get("violations") or {}
        if a:
            rep["verdict"] = "OK with the world — the earlier failure was a different target/TCP, not the world."
        elif c0 is None:
            rep["verdict"] = ("inconclusive — the pure-IK probe didn't run (see "
                              "plan_no_world_no_selfcoll.message); use validate_config for the cfg/IK check.")
        elif c0 is False:
            rep["verdict"] = ("ROBOT CFG / IK BROKEN: cuRobo's PURE IK can't re-solve the EXACT current pose "
                              "(no world, no self-collision) — a robot_cfg/IK mismatch (base_link / tool_frame "
                              "/ cspace / quaternion). See ik_reach_no_world_no_selfcoll + validate_config.")
        elif viol:
            rep["verdict"] = (f"JOINT LIMITS: the START config is outside limits on {list(viol)} — fix the "
                              "limits (URDF/safety) or the start read; no trajectory can be valid.")
        elif b:
            rep["verdict"] = ("WORLD BLOCKS IT: pure IK reaches the pose and removing the world makes the move "
                              "plan — the START/path collides with the scanned cloud / an obstacle box. See "
                              "start.in_cuboid (which box the robot is inside).")
        else:
            # Pure IK reaches (kinematics fine), yet planning fails even with the world removed → the
            # START config is in COLLISION: self-collision on the path AND (with the world) the cage/
            # obstacles the robot is already inside. NOT a kinematics/reach bug.
            inside = {k: v for k, v in (rep.get("start", {}).get("in_cuboid", {}) or {}).items()
                      if v.get("spheres_penetrating", 0) > 0}
            rep["verdict"] = ("COLLISION, NOT KINEMATICS: pure IK reaches the pose, but the START config is in "
                              f"collision — the robot is INSIDE these obstacles at rest: {list(inside)}. Fix the "
                              "world model (a cage wall/obstacle clipping the robot) and/or the self-collision "
                              "margin; the kinematics + cfg are fine (validate_config = KINEMATICS OK).")
        return rep

    def retract(self, tcp: Optional[str] = None, *, execute: bool = True) -> MoveResult:
        """Plan a collision-free path back to the robot's home/retract config. `tcp` selects the
        arm (default: all arms). Distinct from `move_to_joints` — this is COLLISION-FREE."""
        arms = [tcp] if tcp else list(self._groups.keys())
        planner = self._planner_for(arms)
        if not hasattr(planner, "plan_retract"):
            return MoveResult(False, "planner has no retract; use move_to_joints(home) instead")
        res = planner.plan_retract(self._seed_positions())
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

    def _world_blocks_exit(self, tcp: str, *, delta: float = 0.05) -> dict:
        """DECISIVE born-in-collision test against the planner's ACTUAL world (the live ESDF when the
        cameras have run, else the modeled obstacles) — using ONLY the rig-proven plan path, so it can
        never drift from what the planner really sees (unlike a parallel collision checker, which holds
        the modeled scene only and would pass even with the robot buried in live voxels).

        Try a tiny nudge from the current pose along each axis WITH the world; if none plan, ablate the
        world (`clear_world`) and retry. If the SAME nudge plans with the world cleared but not with it,
        the START is born inside the live world (the robot can't even leave it) → THE reason every move
        fails. If it fails even cleared, it's self-collision / kinematics / joint-limits, not the world.
        `set_world()` restores the live ESDF (it's tracked), so the world is intact afterwards."""
        planner = self._planner_for([tcp])
        cur = self.current_tool_pose(tcp)
        if not cur:
            return {"born_in_collision": None, "verdict": "planner cannot FK the current pose"}
        pos, quat = cur
        dirs = [(0, 0, 1), (0, 0, -1), (1, 0, 0), (-1, 0, 0), (0, 1, 0), (0, -1, 0)]

        def any_plans() -> bool:
            for ux, uy, uz in dirs:
                tgt = [pos[0] + ux * delta, pos[1] + uy * delta, pos[2] + uz * delta]
                if self.move_to_pose(tcp, (tgt, quat), execute=False, max_attempts=1).ok:
                    return True
            return False

        if any_plans():
            return {"born_in_collision": False, "verdict": "the start can move within the live world"}
        if not (hasattr(planner, "clear_world") and hasattr(planner, "set_world")):
            return {"born_in_collision": None,
                    "verdict": "no tiny move plans, and the planner can't ablate the world to localise it"}
        try:
            planner.clear_world()
            cleared_ok = any_plans()
        finally:
            planner.set_world()                      # restore the LIVE ESDF (tracked), not the modeled scene
        if cleared_ok:
            return {"born_in_collision": True,
                    "verdict": ("BORN IN COLLISION against the LIVE world: the same nudge plans with the "
                                "world cleared but not with it — the start config is inside/surrounded by "
                                "the live ESDF, so the planner can't leave it. This is why every move "
                                "fails. Suspect the live-world build (the robot mask at the wrong pose, or "
                                "the camera extrinsics), voxelising the arm — see build_world.warnings.")}
        return {"born_in_collision": False,
                "verdict": ("NOT the world: no tiny move plans even with the world cleared → self-collision "
                            "(ignore matrix too sparse), joint-limits, or kinematics at the start — not the "
                            "ESDF. Cross-check validate_config / diagnose_motion.")}

    def self_collision_pairs(self, tcp: Optional[str] = None, *, top: int = 40,
                             q: Optional[Dict[str, float]] = None) -> dict:
        """100% PROOF of WHICH link pairs self-collide at a config (the seed by default, or the given
        `q`) — cuRobo's self-collision IS sphere-pair distance over NON-ignored link pairs, so we
        replicate it transparently: FK each link, place ITS collision spheres (radius + the same buffer
        cuRobo adds) in the base frame, and report every NON-ignored pair whose spheres overlap, the
        penetration, and whether the links are parent↔child ADJACENT (→ a by-design touch the ignore
        matrix should cover = PHANTOM) vs apart (→ a REAL overlap, e.g. the two arms / a bad
        base-to-base). Pure FK + numpy → matches what cuRobo flags, and names it. Also returns the
        involved spheres `[x,y,z,r]` for the Studio."""
        from .live_world import _quat_wxyz_to_R
        tcp = tcp or next(iter(self._groups), None)
        seed = dict(q) if q else self._seed_positions()
        ignore = self._self_collision_ignore()
        nb = self._self_ignore or {}                       # parent↔child adjacency
        # TRUE sphere radii (NO safety buffer): cuRobo's self-collision uses the un-inflated radii (we
        # cancel the world `collision_sphere_buffer` for self via `self_collision_buffer`, see
        # robotcfg.py), so the prober must match — else it reports buffer-induced phantoms the planner
        # no longer sees.
        # FK every sphere link in ONE build (not one-per-link) — the slow part otherwise.
        planner = self._planner_for([tcp]) if tcp else None
        poses = planner.link_poses(list(self.spheres.keys()), seed) if (planner and hasattr(planner, "link_poses")) else {}
        by_link: Dict[str, list] = {}
        for link, sl in self.spheres.items():
            pose = poses.get(link) or self.link_pose(link, seed, tcp=tcp)   # batched, else per-link fallback
            if pose is None:
                continue
            R = _quat_wxyz_to_R(pose[1])
            t = np.asarray(pose[0], dtype=float)
            arr = []
            for s in (sl or []):
                r = float(s.get("radius", 0.0))
                if r <= 0:
                    continue
                arr.append((R @ np.asarray(s["center"], dtype=float) + t, r))
            if arr:
                by_link[link] = arr
        links = list(by_link)
        pairs = []
        for i in range(len(links)):
            for j in range(i + 1, len(links)):
                la, lb = links[i], links[j]
                if lb in (ignore.get(la) or []) or la in (ignore.get(lb) or []):
                    continue                                # cuRobo ignores this pair
                worst = 1e9
                for ca, ra in by_link[la]:
                    for cb, rb in by_link[lb]:
                        worst = min(worst, float(np.linalg.norm(ca - cb)) - ra - rb)
                if worst < 0:
                    adjacent = (lb in (nb.get(la) or [])) or (la in (nb.get(lb) or []))
                    pairs.append({"links": [la, lb], "penetration_mm": round(-worst * 1000, 1),
                                  "adjacent": bool(adjacent)})
        pairs.sort(key=lambda p: -p["penetration_mm"])
        hot = {l for p in pairs for l in p["links"]}
        spheres = [[round(float(c[0]), 4), round(float(c[1]), 4), round(float(c[2]), 4), round(float(r), 4)]
                   for link in hot for (c, r) in by_link[link]]
        return {"n_pairs": len(pairs), "pairs": pairs[:top], "spheres": spheres,
                "all_adjacent": bool(pairs) and all(p["adjacent"] for p in pairs)}

    def _link_to_group(self) -> Dict[str, str]:
        """`{link: group_name}` — which actuated GROUP moves each link (a joint's child link belongs to
        the group that drives it; tips too). Lets the audit tell an INTER-arm overlap (links of two
        different groups — expected, the planner never commands it) from an INTRA-arm one (same group →
        a real per-arm collision / over-fat spheres). Static/base links stay unmapped."""
        from calib_engine import urdf_io
        try:
            joints = urdf_io.urdf_facts(self.urdf_path).get("joints", [])
        except Exception:  # noqa: BLE001 — a parse hiccup just means coarser (group-blind) classification
            joints = []
        child_of = {j.get("name"): j.get("child") for j in joints}
        l2g: Dict[str, str] = {}
        for gname, g in self._groups.items():
            for jn in g.get("joint_names", []):
                c = child_of.get(jn)
                if c:
                    l2g[c] = gname
            for t in (g.get("tip_links") or []):
                l2g[t] = gname
        return l2g

    def audit_self_collision(self, *, n: int = 400, phantom_warn: float = 0.15,
                             phantom_fail: float = 0.50, miss_fail: float = 0.01) -> dict:
        """CHECK 2 (spheres gate) — does the SPHERE model AGREE with the MESH model? Instead of asking
        the un-answerable "how often does it self-collide?" (a valid robot self-collides at plenty of
        real configs, so there's no good baseline), we compare cuRobo's sphere self-collision verdict to
        the EXACT mesh verdict at the same config (the `foam` sphere-approx methodology):

          • PHANTOM  spheres collide, meshes DON'T → planner blocked for nothing (over-fat spheres /
                     missing ignore pair / two chains too close). `phantom_rate` baseline is ~0.
          • MISS     meshes collide, spheres DON'T → planner blind to a real collision (UNSAFE).

        Delegates to the PURE, unit-tested `sphere_audit` (CPU; FCL ground truth, scipy hull fallback) —
        no cuRobo, no GPU. Uses the SAME true radii + AUTHORED ignore matrix the planner uses, so the
        verdict == planner reality. Adds a cross-arm tag to each phantom pair (different groups ⇒ the
        arms are modelled too close / a bad base-to-base; same group ⇒ over-fat spheres)."""
        from . import sphere_audit
        if not self._cell_dir:
            return {"verdict": "warn", "error": "no cell dir on this stack — CHECK 2 needs the link "
                    "meshes to ground-truth the spheres (build via from_cell)"}
        rep = sphere_audit.audit(self.urdf_path, self.spheres, self._self_collision_ignore(),
                                 pkg_root=self._cell_dir, seed=self._seed_positions(),
                                 n=int(n), phantom_warn=phantom_warn, phantom_fail=phantom_fail,
                                 miss_fail=miss_fail)
        rep["ignore_provenance"] = self._ignore_provenance
        l2g = self._link_to_group()
        for p in rep.get("phantom_pairs", []):
            la, lb = p.get("links", [None, None])[:2]
            ga, gb = l2g.get(la), l2g.get(lb)
            p["cross_arm"] = bool(ga and gb and ga != gb)
        return rep

    def diagnose_plan(self, tcp: Optional[str] = None, *, delta: float = 0.05,
                      test_plan_no_self: bool = True) -> dict:
        """DECISIVE ablation for 'no collision-free move' once the WORLD is ruled out — the exact tests
        to run instead of guessing: (1) is the START config SELF-colliding (empty world, so a non-free
        verdict = self/bounds, not world)? (2) is the target REACHABLE by pure IK (no world, no self)?
        (3) are the seed joints within LIMITS? (4) does removing SELF-COLLISION (+ empty world) let the
        same nudge plan? Names the cause. Read-only; builds throwaway probes (no warm planner touched)."""
        from .curobo_v2 import CuroboV2Planner
        from .world import WorldInputs
        tcp = tcp or next(iter(self._groups), None)
        rep: dict = {"tcp": tcp, "delta": delta}
        if tcp is None:
            rep["error"] = "no kinematic groups"
            return rep
        tip = self._tip(tcp)
        seed = self._seed_positions()
        cur = self.current_tool_pose(tcp)
        pos, quat = (cur if cur else ([0.3, 0.0, 0.4], [1.0, 0.0, 0.0, 0.0]))
        tgt = [pos[0], pos[1], pos[2] + delta]                  # a small lift — the easiest safe move
        rep["target"] = [round(float(v), 3) for v in tgt]
        empty = WorldInputs(scene={})
        try:
            probe = CuroboV2Planner(self._robot_cfg_for([tcp]), empty, limits=self._limits,
                                    max_goalset=1, warmup=False, self_collision_check=True)
            sc = probe.world_sphere_collision(seed)            # empty world ⇒ collision_free = self+bounds
            rep["start_self_collision_free"] = sc.get("collision_free")
            rep["n_world_penetrating"] = sc.get("n_penetrating")
            ik = probe.ik_check(tip, seed)
            rep["ik_reach"] = {k: bool(v.get("success")) for k, v in (ik.get("checks") or {}).items()}
            lim = probe.joint_limits()
            rep["joint_limit_violations"] = {
                j: {"q": round(float(seed[j]), 4), "limit": [round(lo, 4), round(hi, 4)]}
                for j, (lo, hi) in lim.items()
                if j in seed and not (lo - 1e-3 <= seed[j] <= hi + 1e-3)}
        except Exception as e:  # noqa: BLE001
            rep["probe_error"] = f"{type(e).__name__}: {e}"
        ig = self._self_collision_ignore()
        rep["self_collision_ignore"] = {"n_links": len(ig), "n_pairs": sum(len(v) for v in ig.values()),
                                        "generated_by_curobo": self._ignore_warn is None, "warn": self._ignore_warn}
        # If the start is self-colliding, NAME the exact link pairs (100% — replicates cuRobo's rule).
        if rep.get("start_self_collision_free") is False:
            try:
                rep["self_collision_pairs"] = self.self_collision_pairs(tcp)
            except Exception as e:  # noqa: BLE001
                rep["self_collision_pairs_error"] = f"{type(e).__name__}: {e}"
        if test_plan_no_self:                                   # the confirmatory plan (needs warmup)
            try:
                pn = CuroboV2Planner(self._robot_cfg_for([tcp]), empty, limits=self._limits,
                                     max_goalset=1, warmup=True, self_collision_check=False)
                r = pn.plan_pose({tip: (tgt, quat)}, seed, max_attempts=3)
                rep["plan_no_self_no_world"] = {"ok": bool(r.success), "message": r.message}
            except Exception as e:  # noqa: BLE001
                rep["plan_no_self_no_world"] = {"ok": None, "message": f"{type(e).__name__}: {e}"}

        ssf = rep.get("start_self_collision_free")
        ik0 = (rep.get("ik_reach") or {}).get("0cm")
        viol = rep.get("joint_limit_violations") or {}
        pns = (rep.get("plan_no_self_no_world") or {}).get("ok")
        if ik0 is False:
            rep["verdict"] = ("KINEMATICS/CFG BROKEN: pure IK can't re-solve the current pose with NO world and "
                              "NO self-collision — a robot_cfg/IK fault (base_link/tool_frame/cspace/quaternion), "
                              "not collision. See validate_config.")
        elif viol:
            rep["verdict"] = (f"JOINT LIMITS: the seed is outside limits on {list(viol)} — no trajectory can be "
                              "valid. Fix the limits (URDF/safety) or the seed read.")
        elif ssf is False:
            scp = rep.get("self_collision_pairs") or {}
            pairs = scp.get("pairs") or []
            top = "; ".join(f"{p['links'][0]}↔{p['links'][1]} ({p['penetration_mm']}mm"
                            f"{', adjacent' if p['adjacent'] else ''})" for p in pairs[:4])
            if pairs and scp.get("all_adjacent"):
                cause = (f"{scp.get('n_pairs')} pair(s), ALL parent↔child ADJACENT → PHANTOM self-collision: the "
                         "self-collision IGNORE matrix is missing by-design-touching links. FIX the ignore-matrix "
                         "generation (RobotBuilder default-config check) for these links — not the robot.")
            elif pairs:
                cause = (f"{scp.get('n_pairs')} pair(s); some are NON-adjacent → a REAL overlap (two limbs / a wrong "
                         "base-to-base placement) AND/OR missing ignore pairs. Inspect the non-adjacent pairs.")
            else:
                cause = ("no overlapping NON-ignored sphere pair found by the analytic check, yet cuRobo's validate "
                         "says self-collision — a sphere-buffer/activation mismatch or the ignore matrix cuRobo baked "
                         "≠ ours. Inspect self_collision_ignore.")
            rep["verdict"] = (f"100% SELF-COLLISION at the START (empty world; joints within limits): {cause}"
                              + (f" Pairs: {top}." if top else "")
                              + (" CONFIRMED — the nudge plans with self-collision OFF." if pns else ""))
        elif pns is True:
            rep["verdict"] = ("SELF-COLLISION on the PATH: the nudge plans with self-collision OFF (and the start "
                              "isn't world- or self-colliding) → a sphere/ignore-matrix is too aggressive along the "
                              "motion. Loosen the offending pair or the sphere.")
        elif pns is False:
            rep["verdict"] = ("NOT collision: even with self-collision OFF and an EMPTY world the nudge won't plan → "
                              "kinematics/IK/dynamics, not collision. Cross-check ik_reach + limits + validate_config.")
        else:
            rep["verdict"] = "inconclusive — read the fields (ik_reach, start_self_collision_free, limits)."
        return rep

    # --- commission self-test --------------------------------------------
    def commission(self, *, execute: bool = True, progress: Optional[Callable[[dict], None]] = None,
                   target: Optional[Sequence[float]] = None, tcp: Optional[str] = None,
                   frames: Optional[Callable[[], Sequence]] = None) -> dict:
        """Build the stack and VERIFY the end-to-end chain once: spheres healthy → planner builds →
        the LIVE collision world is built from the cameras → ONE collision-free plan → trajectory →
        (optionally) the per-arm executor replays it. This is what the commission gate runs so G8
        exercises a PROVEN stack, not an assumed one.

        `frames` is a callable returning the per-camera `DepthFrame`s (the agent's rig-specific grab —
        see commission.md); when given, the verify plans against the LIVE ESDF (robot auto-masked,
        modeled cuboids fused) rather than just the modeled cuboids. The verification move is a
        REACHABLE target, never the cspace home (commonly inside the table): an explicit 3D `target`
        is planned (keeping the current orientation); otherwise `safe_demo_move` auto-finds a small
        collision-free nudge from the live pose. Both prove the same fused stack, supervised."""
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

        # Build the LIVE volumetric world from the cameras (robot masked, cuboids fused) — the move
        # plans against THIS map. Skipped (modeled cuboids only) when no `frames` provider is wired,
        # or when it yields no frames (a cell with no live depth — graceful, not an error).
        live = list(frames() or []) if frames is not None else None
        if live:
            emit(step="build_world", ok=True, note="building the live ESDF from the cameras")
            try:
                wr = self.update_world_live(live, tcp=first)
                report["world"] = wr
                if not step("build_world", wr.get("ok", False), **{k: v for k, v in wr.items() if k != "ok"}):
                    report["message"] = wr.get("message", "live world build failed")
                    return report
            except Exception as e:  # noqa: BLE001
                step("build_world", False, error=str(e))
                report["message"] = f"live world build failed: {e}"
                return report

        # BORN-IN-COLLISION pre-check (the canonical world cuRobo plans against): is the START config
        # already clipping a STATIC obstacle (table/wall/cage/floor)? If so, EVERY plan fails "no
        # collision-free trajectory" — a NAMED step here distinguishes "the world clips the robot at
        # rest, fix the obstacle/safety placement" from "the target is unreachable". World-only verdict
        # (n_penetrating from RobotCollisionChecker's per-sphere distance), so self-collision/bounds —
        # a different fault — don't masquerade as a world problem.
        try:
            planner = self._planner_for([first])
            # (a) MODELED-obstacle born check (cuRobo's RobotCollisionChecker) — catches the robot born
            # inside a table/wall/box at rest. This checker sees the modeled scene ONLY, not the live
            # ESDF, so a clear verdict here is necessary but NOT sufficient when cameras have run.
            if hasattr(planner, "world_sphere_collision"):
                wc = planner.world_sphere_collision(self._seed_positions())
                report["start_collision"] = wc
                n_pen = int(wc.get("n_penetrating", 0))
                free = wc.get("collision_free")     # validate() folds in SELF-collision + bounds, not just world
                ok = (n_pen == 0 and free is not False)
                if n_pen > 0:
                    msg = (f"{n_pen} collision sphere(s) penetrate a MODELED obstacle at the start — the robot "
                           "is BORN in collision. Fix the obstacle/wall placement (or the seed) — not the target.")
                elif free is False:
                    msg = ("the robot is BORN in collision with NO world obstacle penetrating → it's SELF-"
                           "COLLISION (the robot's own spheres overlap at this config) or a joint-limit/bounds "
                           "violation, so planning can't begin. Most likely the self-collision IGNORE matrix is "
                           "too sparse for this robot (by-design-touching links phantom-collide) or two limbs "
                           "overlap at the seed. Run the plan diagnosis to confirm.")
                else:
                    msg = None
                if not step("start_collision", ok, n_penetrating=n_pen, collision_free=free,
                            penetrating=wc.get("penetrating_spheres"),
                            self_or_bounds=(n_pen == 0 and free is False), message=msg):
                    report["message"] = msg
                    return report
            # (b) LIVE-world born check — the one that actually catches "start ✓ / every move ✗". The
            # modeled checker above is blind to the live ESDF, so when cameras have run we ABLATE the
            # planner's real world via the plan path: if the robot can't make any tiny move WITH the
            # live world but can with it cleared, it's born inside the live voxels (the real failure).
            if getattr(planner, "has_live_world", False):
                be = self._world_blocks_exit(first)
                report["start_collision_live"] = be
                if not step("start_collision", be.get("born_in_collision") is not True,
                            born_in_collision=be.get("born_in_collision"), message=be.get("verdict"),
                            world="live ESDF"):
                    report["message"] = be.get("verdict")
                    return report
        except Exception as e:  # noqa: BLE001 — never let the pre-check itself abort commission
            step("start_collision", True, warn=f"start-collision check skipped: {type(e).__name__}: {e}")

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

    def demo_run(self, *, n_poses: int = 3, execute: bool = True,
                 frames: Optional[Callable[[], Sequence]] = None, tcp: Optional[str] = None,
                 progress: Optional[Callable[[dict], None]] = None) -> dict:
        """The REPEATABLE autonomous-motion DEMO (gate 12) — distinct from commission (which proves
        ONE move): REUSE the already-commissioned WARM stack (no rebuild/re-warm), and move through
        several reachable, collision-free poses, REFRESHING the live ESDF before each leg so the robot
        re-plans against the CURRENT scene (the reactive bar). Retracts home at the end. Streams each
        leg; supervised (the executor polls the E-stop every waypoint). `frames` = the live-depth
        provider (None / [] → plans against the modeled world, still a valid repeatable demo)."""
        def emit(**kw):
            if progress:
                progress(kw)

        tcp = tcp or next(iter(self._groups), None)
        rep: dict = {"ok": False, "legs": []}
        if tcp is None:
            rep["message"] = "no kinematic groups in the cell"
            return rep
        try:
            self._planner_for([tcp])                 # REUSE the warm planner (built at commission)
        except Exception as e:  # noqa: BLE001
            rep["message"] = f"planner unavailable: {e}"
            return rep
        self.reset_cuda_graph()                      # clear stale graph state from commission (#503)

        done = 0
        for i in range(int(n_poses)):
            # refresh the LIVE world so each leg plans against the current scene (move an obstacle →
            # the robot re-plans). Hot-swap (update_world_live), never a rebuild.
            if frames is not None:
                fr = list(frames() or [])
                if fr:
                    wr = self.update_world_live(fr, tcp=tcp)
                    emit(step="world", leg=i, ok=wr.get("ok", False), n_voxels=wr.get("n_voxels"))
            targets = self.find_free_targets(tcp, n=1)        # a reachable, collision-free point
            if not targets:
                emit(step="leg", leg=i, ok=False, message="no reachable collision-free pose found "
                     "from here — the arm may be boxed in by the live world")
                continue
            pt = targets[0]["point"]
            mv = self.plan_to_point(tcp, pt, execute=execute)
            done += 1 if mv.ok and not mv.aborted else 0
            emit(step="leg", leg=i, ok=mv.ok, target=pt, executed=mv.executed,
                 trajectory=(mv.trajectory.summary() if mv.trajectory else None), message=mv.message)
            rep["legs"].append({"leg": i, "ok": mv.ok, "target": pt})
            if mv.aborted:
                rep["message"] = "aborted (E-stop)"
                return rep

        rt = self.retract(tcp, execute=execute)               # collision-free path home
        emit(step="retract", ok=rt.ok, executed=rt.executed, message=rt.message)
        rep["ok"] = done > 0 and rt.ok
        rep["n_legs"] = done
        rep["tcp"] = tcp
        rep["message"] = (f"demo complete — {done} autonomous collision-free pose(s) + retract, "
                          "re-planned against the live world" if rep["ok"]
                          else "demo did not complete a full autonomous run")
        return rep

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
