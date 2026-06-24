# Commission — the LIVE volumetric demo (build the world from the cameras, then move in it)

By this gate every ingredient for safe autonomous motion exists and is calibrated:
`robot.urdf` (+ each camera's optical frame written in by calibration), the
`collision_spheres.yml`, the `cell.yaml` groups, and the `safety.yaml` envelope.
**Commission fuses them into one live, autonomous-motion stack and proves the full
chain — see the world → plan a collision-free move → execute under supervision —
ONCE**, so the G8 demo exercises a stack that is *proven*, not assumed.

The collision world here is the **cuRoboV2 live ESDF**, built from the cameras
THIS cycle — NOT a stored scan. Each depth frame is cleaned, the robot is masked
out of it at its live config, and it is fused into a TSDF→ESDF map; the modeled
cuboids (table/wall/cage) are stamped into the SAME map. That one map is the
collision world for the move AND for the night run. (The old `world/` scan stays
only as the source of the modeled cuboids.)

You do **not** write cuRobo, masking, or mapping. The SHIPPED `motion_engine`
(it travels with the edge, like `calib_engine`) owns all of it — it uses cuRobo's
own `RobotSegmenter` / `FilterDepth` / `Mapper`, tested on live robots. **Your job
is the non-rigid glue: pull a depth frame + intrinsics from each of THIS cell's
cameras through your bridge, hand them to the stack, and run the verify.**

## 1. The live-world contract (ANY rig — N cameras, N arms)

```python
from motion_engine import MotionStack, DepthFrame
import numpy as np

stack = MotionStack.from_cell("remoroo_cell", bridge=bridge)   # reads every setup artifact

def live_frames():
    """One DepthFrame per camera, in the robot BASE frame. The ONLY rig-specific code:
    grabbing depth+intrinsics from YOUR cameras. The pose is general FK — let the stack do it."""
    frames = []
    for cam in bridge.cameras():                    # however your bridge enumerates cameras
        obs = cam.grab()                            # camera_capture.md: {'depth_m','intrinsics',...}
        K = np.array([[obs["intrinsics"]["fx"], 0, obs["intrinsics"]["cx"]],
                      [0, obs["intrinsics"]["fy"], obs["intrinsics"]["cy"]],
                      [0, 0, 1]], dtype=float)
        # the camera's CALIBRATED optical frame (calibration wrote it into the URDF as a link).
        # eye_in_hand → FK at the LIVE joints; static/eye_to_hand → the same fixed result.
        pose = stack.link_pose(cam.optical_frame)   # (xyz, wxyz) in base — general, no extrinsic math
        frames.append(DepthFrame(depth=obs["depth_m"], intrinsics=K,
                                 cam_pose_in_base=pose, name=cam.name))
    return frames

report = stack.update_world_live(live_frames())     # build/refresh the live ESDF collision world
assert report["ok"], report
# report: {ok, n_frames, n_robot_pixels_masked, n_voxels}  → stream it to the Studio panel
```

`stack.update_world_live(frames)` does ALL of: clean each depth (`FilterDepth`) →
mask the robot at its live config (`RobotSegmenter`, so the robot is never in its
own world) → fuse into the `Mapper` → stamp the modeled cuboids into the same map →
compute the ESDF → load it as the planner's collision world. `stack.esdf_voxels()`
returns the occupied voxel centres `[N,3]` for the Studio's live view.

**Do not** build point clouds, mask the robot, convert depth, or touch cuRobo —
the stack does. Your only rig-specific surface is `cam.grab()` (see
`camera_capture.md`) and how your bridge enumerates cameras + their `optical_frame`
link (from `cell.yaml: cameras[].link`, calibrated child `*_optical_frame`).

## 2. Plan + move in the live world (TCP-keyed, count-agnostic)

`tcp` is a group NAME from `cell.yaml: groups`. The SAME calls drive 1 arm, 2 arms,
a quadruped's legs, or a humanoid's many end-effectors — the stack builds (and
caches) a planner per group set; nothing special-cases morphology.

```python
stack.move_to_pose("right", (xyz, quat_wxyz))         # one TCP → a pose
stack.move_to_poses({"right": poseR, "left": poseL})  # N TCPs, coordinated, ONE trajectory
stack.move_through_poses("right", [p1, p2, p3])       # a waypoint sequence
stack.plan_to_point("right", [x, y, z])               # convenience: point, nominal orientation
stack.retract("right")                                # collision-free path home
```

Every motion verb returns a `MoveResult` (`.ok`, `.trajectory`, `.executed`,
`.aborted`, `.audit`); these plan against the live ESDF you just built and replay
the FULL trajectory through the bridge's executor, checking the E-stop each
waypoint. Pass `execute=False` to plan + visualise without moving.

## 3. Run the verify (what the gate checks)

```python
report = stack.commission(frames=live_frames, progress=lambda s: print(s))
assert report["ok"], report["message"]
```

`commission()` runs, in order: sphere health (refuses to move on the mm-scale
degeneracy) → groups present → planner builds/warms → **builds the live world from
your `frames`** → plans ONE collision-free move (a safe **retract home** from
wherever calibration left the arm) → defensive audit → the executor replays it
under the operator gate, slow, hand on the E-stop. Each step is reported for the
Studio panel; on failure it routes back to you (fix + re-checkpoint), never a
per-error UI.

## The bridge contract (provide these — see `bridge_primitives.md`)

- `bridge.read_joint_positions(group) -> np.ndarray` — seeds each plan's start.
- `bridge.execute_trajectory(traj, should_abort=None) -> bool` — REPLAYS the full
  `Trajectory` on the arm SDK (`arm_adapters.md`); routes by `traj.joint_names`.
- `bridge.estop_tripped() -> bool` — the E-stop poll the stack checks mid-replay.
- camera access — enumerate the cell's cameras + `grab()` depth+intrinsics
  (`camera_capture.md`) and each camera's calibrated `optical_frame` link.

## Commission exit criteria (functional)

- [ ] `MotionStack.from_cell` loads with no missing-artifact error (URDF, spheres,
      `cell.yaml: groups`+`cameras`, world, safety all present and consistent).
- [ ] `sphere_health().ok` — collision spheres are in metres, not the mm degeneracy.
- [ ] `update_world_live(frames)` returns `ok` with `n_voxels > 0` and
      `n_robot_pixels_masked > 0` — the cameras built a real volumetric world and
      the robot was masked out of it. The Studio shows the live ESDF.
- [ ] One `plan_to_point`/`retract` returns a collision-free `Trajectory` against
      the LIVE ESDF (not a box), within the safety envelope.
- [ ] `execute_trajectory` replays the FULL path on the real arm under the operator
      gate; the E-stop aborts it mid-motion (verified live, slow speed).
- [ ] `commission_report.md`: the stack config, the live-world stats, the verify
      trajectory, pass/fail.

The G8 demo then just exercises this stack (refresh the live world → `move_to_pose`
/ `move_through_poses` / `retract`) — no re-derivation of a cuRobo integration.
