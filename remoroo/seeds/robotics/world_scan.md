# World scan — `remoroo_cell/world/` (Phase 4 → G4)

Build a **geometric world** good enough to plan small **collision-free
autonomous moves**, and a **queryable scene**. The gate is *functional*: not a
sweep count, not a resolution number — *can we plan safe autonomous motion
against this world?* Take **as many sweeps as it takes**.

Do this after calibration (you may move, slowly, supervised). The operator
watches the live reconstruction in the Studio.

## The Studio integration contract — `world/scan.py`

The Studio's World gate runs YOUR scan live (Start / Re-scan) and renders the
cloud as it fills. Author `remoroo_cell/world/scan.py` with this **streaming entry
point** (the edge imports + calls it):

```python
def run(bridge, cell, on_event=None):
    """Scan the environment and build the world. Stream as you go (no-op if None):
      {"type":"status","message":str,"coverage":float}      # coverage in 0..1
      {"type":"points","xyz":[[x,y,z],...]}                  # NEW points (base frame)
    Write the cuRobo collision world + scene, AND a simple
    `world/cloud.json` = {"points":[[x,y,z],...], "coverage":0.x} for the Studio to
    render on resume. RETURN {"coverage":float,"points":int}. Env only — subtract
    the arm. Raise on failure (no fake success)."""
```

**Stream so the operator can SEE it working — this gate is otherwise blind.** The Studio
panel renders ONLY what you emit. A scan that builds an ESDF/mesh internally but never streams
points or status leaves the operator at a silent `0 points / 0% coverage` with no idea if it
ran. So:

- **Emit a descriptive `status` often** (every sweep hop / fused frame): *which* step
  (`"sweep 3/12 — fusing wrist-cam depth"`), the running point count, and `coverage`. The panel
  shows these as a live narration log.
- **Stream `points` incrementally** as you fuse (down-sample to a few thousand for the live
  view) — don't wait until the end. The 3D cloud must fill in *during* the scan, not after.
- **Never finish silently with 0 points.** If a frame yields no usable depth, or masking
  removed everything, or the arm didn't move, say so in a `status` (`"frame 4: 0 depth points —
  camera returned no depth"`) and **raise** with that reason rather than returning a hollow
  success. A 0-point world means the planner has no obstacles — that must be loud, not silent.
- First thing in `run()`, emit a `status` like `"connecting to <camera> …"` so the panel shows
  life immediately (the GPU mapper / first sweep can take seconds to warm up).

> **The world is the ENVIRONMENT ONLY — subtract the arm(s).** Never fuse the
> robot's own body into the world. cuRobo carries the arm(s) as collision
> spheres (`robot_model.md`) and checks them for BOTH self-collision (arm vs
> itself) and world-collision (arm vs this world). If you leave the arm in the
> world, cuRobo collides the *live* arm against its own *frozen ghost* and finds
> no valid plan. So mask the robot out of every depth frame before fusing (code
> below — cuRobo ships the exact tool), or scan with the arm parked clear of the
> camera.

## Coverage methods — pick what fits this cell (combine freely)

1. **Static-depth fusion** — if there's a static/eye-to-hand depth camera,
   fuse its frames into a TSDF over the workspace box (`cell.yaml: workspace`).
2. **Eye-in-hand active sweep** (powerful, preferred for wrist cams) — drive
   the arm through a set of **safe** viewpoints (within the G3 envelope) and
   fuse wrist-cam depth using the hand-eye extrinsic. Plan each hop
   collision-free against the partial world so far (bootstrapping).
3. **Operator hand-guided scan** (fallback / fast coverage) — if autonomous
   safe sweeps aren't available yet, `ask_human` to enable hand-guiding (only
   if `cell.yaml: safety.hand_guiding.supported`), and have the operator walk
   the wrist cam around the cell while you fuse. The human is the safety
   guarantee here. If hand-guiding is unsupported, fall back to (1)/(2) or
   stop with what's blocking.

## Subtract the robot from each depth frame (cuRobo `RobotSegmenter`)

Our stack already ships the exact tool. cuRobo's `RobotSegmenter` reuses the
SAME kinematics + collision spheres you built in `robot_model.md`: it projects
each depth image to a point cloud in the base frame, computes the sphere
positions for the current joint config, and drops every point within
`distance_threshold` of a sphere — returning a robot mask and a "world depth"
with the arm erased. It runs inside a CUDA graph (~3000 Hz on a 4090 at
480×480), so masking every fused frame is essentially free. (The
`isaac_ros_cumotion_robot_segmenter` ROS 2 node wraps this exact math if your
capture pipeline is ROS-based.)

```python
import torch
from curobo.types.camera import CameraObservation
from curobo.types.math import Pose
from curobo.types.state import JointState
from curobo.wrap.model.robot_segmenter import RobotSegmenter

# Build ONCE from the cuRobo robot YAML produced in robot_model.md.
segmenter = RobotSegmenter.from_robot_file(
    "remoroo_cell/robot_model/collision_spheres.yml",
    distance_threshold=0.05,   # extra buffer (m) around the spheres — TUNE (see note)
    use_cuda_graph=True,
)

def mask_robot_body(depth_m, intr, cam_T_base, joint_pos, joint_names):
    """Zero out the robot's own pixels so ONLY the environment gets fused.
    depth_m: HxW metres; intr: 3x3; cam_T_base: 4x4 camera pose in base frame;
    joint_pos: the joint config SYNCED to this frame (use the G5 time offset)."""
    cam_obs = CameraObservation(
        # NOTE: RobotSegmenter expects depth in MILLIMETRES (depth_to_meter=1e-3).
        # Our pipeline is in metres, so send mm here — a documented scale gotcha
        # (NVlabs/curobo#395: meters-vs-mm makes the cloud mis-scale and mask wrong).
        depth_image=torch.as_tensor(depth_m * 1000.0, dtype=torch.float32, device="cuda")[None],
        intrinsics=torch.as_tensor(intr, dtype=torch.float32, device="cuda")[None],
        pose=Pose.from_matrix(torch.as_tensor(cam_T_base, dtype=torch.float32, device="cuda")),
    )
    js = JointState.from_position(
        torch.as_tensor(joint_pos, dtype=torch.float32, device="cuda")[None],
        joint_names=list(joint_names),
    )
    robot_mask, world_depth = segmenter.get_robot_mask_from_active_js(cam_obs, js)
    return world_depth[0].detach().cpu().numpy() / 1000.0          # back to metres
```

Tuning + caveats (R&D — verify visually):
- `distance_threshold` too large ALSO erases real objects sitting near the arm
  (a known failure mode — voxels around the gripper vanish, so the robot stops
  reacting to nearby obstacles). Start ~0.02–0.05 m and have the operator
  confirm objects close to the tool still survive the mask.
- `joint_pos` MUST be the joint state synchronized to that exact frame (use the
  recorder's timestamped `joint_states` + the time-sync offset from G5), or the
  spheres sit in the wrong place and mask the wrong pixels.
- Units/extrinsics: depth (mm here) and `cam_T_base` must be right, or the
  projected cloud is mis-scaled — the #1 cause of "segmentation looks wrong".

Not on a cuRobo/GPU capture path? Equivalent options, same idea (render the
robot from the URDF + live joints, subtract those points):
- ROS 2: `isaac_ros_cumotion_robot_segmenter` (same cuRobo math), or MoveIt's
  mesh filter / `robot_self_filter` / `robot_body_filter` (URDF collision shapes
  vs the cloud).
- Dependency-light: render the URDF at the camera pose with the current joints
  (`pyrender` / `yourdfpy`) to get a robot-only depth, then drop pixels where
  `|depth − robot_depth| < eps`. Simplest of all: **park the arm clear of the
  camera frustum while scanning** and skip masking entirely.

## Build the collision world (cuRoboV2)

```python
# Fuse the (robot-masked) depth -> TSDF/ESDF, then hand cuRobo a collision
# world it can plan against. Shapes follow your installed cuRobo version.

def fuse_world(frames, workspace_bounds, voxel_m=0.01, joint_names=None):
    """frames: list of (depth_m, intrinsics, cam_pose_in_base, joint_pos).
    Returns a TSDF/ESDF clipped to the workspace box — ENVIRONMENT ONLY: each
    frame is robot-masked (above) before integration; the arm is never fused."""
    tsdf = TSDFVolume(workspace_bounds, voxel_m)          # nvblox / open3d / cuRobo
    for depth_m, intr, cam_T_base, joint_pos in frames:
        env_depth = mask_robot_body(depth_m, intr, cam_T_base, joint_pos, joint_names)
        tsdf.integrate(env_depth, intr, cam_T_base)
    return tsdf

# Export to cuRobo's collision world (mesh / voxel / blox). The planner pairs
# this env-only world with the robot's sphere model (robot_model.md), so the
# arm is handled by cuRobo — not by the world. Smoke-plan to validate:
world = tsdf_to_curobo_world(tsdf)                         # adapt to cuRobo API
plan = planner.plan(arm="right", target_pose=safe_pose, world=world)
assert plan is not None, "world not plannable yet — add coverage"
```

## Queryable scene — `remoroo_cell/world/scene.json`

A compact, LLM-readable summary the agent and recorder can query (the geometry
serialization detail lives in `ADC/remoroo_scene_representation_to_llm.md`):

```json
{
  "world_version": "2026-06-14T12:00:00Z",
  "frame": "base",
  "workspace_bounds_m": {"min": [-0.4,-0.6,0.0], "max": [0.6,0.6,0.8]},
  "keep_out": [{"min": [-0.1,-0.7,0.0], "max": [0.1,-0.5,0.4], "note": "operator"}],
  "collision_world": "remoroo_cell/world/collision.nvblox",
  "voxel_m": 0.01,
  "free_space_samples": [[0.2,0.0,0.4], [0.1,0.2,0.5]],
  "coverage": {"method": ["eye_in_hand_sweep"], "sweeps": 3, "occupied_voxels": 18234},
  "objects": []
}
```

## G4 exit criteria (functional)

- [ ] cuRoboV2 produces collision-free plans to several safe poses against the
      world (the bootstrapping smoke above succeeds repeatably).
- [ ] No obvious holes in the swept region of the workspace box (operator
      eyeballs the reconstruction).
- [ ] **No robot body in the world** — the arm was masked out of every fused
      frame (operator confirms no arm/gripper voxels in the reconstruction).
- [ ] `scene.json` written and queryable; collision world saved under
      `remoroo_cell/world/`.
- [ ] `scan_report.md`: method(s) used, coverage, screenshots/clips, and the
      "plannable" verdict.

This world is **versioned + editable** and will be rebuilt as the cell changes
— do not treat it as frozen. It is the geometry the Phase-8 safe-motion demo
plans against.
