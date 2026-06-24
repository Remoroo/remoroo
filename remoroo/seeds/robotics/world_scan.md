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

**The streamed `points` are RAW back-projected depth — NEVER a reconstructed surface.** The
single most common failure: building a TSDF/ESDF and streaming **mesh vertices** (marching cubes)
as the cloud. On a real, sparse, noisy scan the TSDF has occupied blocks but **no clean
zero-crossing surface**, so marching cubes returns **0 vertices → 0 points**, even though fusion
"worked" — and a *synthetic* selftest hides it because synthetic depth makes a clean surface. So
the viz cloud and the cuRobo collision world are SEPARATE outputs: stream raw points for the
operator; build the ESDF/voxel world separately for the planner. Back-project masked depth
directly — this always yields points when depth is valid:

```python
# camera pose in base: eye-in-hand = fk(joints) @ X ; static = the calibrated camera pose
ys, xs = np.nonzero(valid_mask)            # valid pixels, robot already masked out
z = depth[ys, xs].astype(np.float32)
x = (xs - cx) * z / fx
y = (ys - cy) * z / fy
pts_cam  = np.stack([x, y, z], -1)         # (N,3) camera optical frame
pts_base = pts_cam @ T_base_cam[:3, :3].T + T_base_cam[:3, 3]
if len(pts_base) == 0:
    on_event({"type": "status", "message": f"frame {i}: 0 valid depth px — skipping"})
else:
    on_event({"type": "points", "xyz": pts_base[::stride].tolist()})   # the Studio cloud
# (separately: feed pts_base / depth into your cuRobo ESDF/voxel world for planning)
```

If your `points` count is 0 while fusion reports occupied blocks, you are exporting a surface —
switch to the raw back-projection above. Do NOT gate the streamed cloud on mesh extraction.

**Multi-pass scans ACCUMULATE — one pass rarely covers the cell.** The Studio runs `run()` once
per "Scan again" and the operator keeps adding passes until the coverage looks complete. So a
pass must ADD to the world, not replace it:

- **Load the existing world first.** If `world/cloud.json` (and your cuRobo voxel/ESDF world)
  already exists, load it and FUSE this pass's frames into it; save the UNION. A pass with no
  existing world starts clean. This way the persisted world + the cuRobo collision world build
  up across passes, matching what the operator sees accumulating in the 3D view.
- **Emit only THIS pass's NEW points** via `{"type":"points","xyz":...}` (the Studio accumulates
  them onto the cloud from earlier passes — don't re-emit the whole cloud each pass or it
  double-counts).
- Report cumulative `coverage` and a `status` like `"pass 2 — +3.1k points (12.4k total)"`.

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

## Emit the masked environment point cloud — `world/cloud.json`

`scan.py`'s ONLY job is to produce the robot-masked environment points, in the
robot base frame. It does **not** build the cuRobo collision world — the shipped
`motion_engine` owns that (it voxelises the cloud into a collision mesh, pairs it
with the robot's sphere model + the safety keep-out/bounds, and feeds it to the
cuRoboV2 planner at the COMMISSION gate). Owning the world in ONE place means every
cell's `scan.py` is the same small thing, and the planner always pairs the same
world with the same robot model.

```python
# Back-project each robot-masked frame to base-frame points and accumulate.
def scan_to_cloud(frames, joint_names=None):
    """frames: list of (depth_m, intrinsics, cam_pose_in_base, joint_pos).
    ENVIRONMENT ONLY — each frame is robot-masked (above) before back-projection;
    the arm is never included. Returns an (N,3) array of points in the base frame."""
    pts = []
    for depth_m, intr, cam_T_base, joint_pos in frames:
        env_depth = mask_robot_body(depth_m, intr, cam_T_base, joint_pos, joint_names)
        pts.append(backproject_to_base(env_depth, intr, cam_T_base))   # (Mi, 3)
    return np.concatenate(pts, axis=0) if pts else np.zeros((0, 3))

# Stream points to the Studio (multi-scan accumulates across passes) AND persist:
cloud = scan_to_cloud(frames, joint_names)
json.dump({"points": cloud.tolist()}, open("remoroo_cell/world/cloud.json", "w"))
```

Stream raw **back-projected points** to the Studio (`{"points": [[x,y,z],...]}`),
NOT mesh vertices — a marching-cubes mesh of a sparse real scan is empty, which is
why the gate showed "0 points". The collision world is built later from this cloud
by `motion_engine`; you can sanity-check coverage in the Studio's point-cloud view.

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
— do not treat it as frozen. At commission/demo the collision world the planner
plans against is the **LIVE ESDF built from the cameras** (cuRobo Mapper, robot
masked out); this scan supplies the **modeled static obstacles** (table/wall/cage)
that get fused into that live map. So keep the modeled cuboids accurate here, but
the demo's real-time geometry comes from the live cameras, not this stored scan.
