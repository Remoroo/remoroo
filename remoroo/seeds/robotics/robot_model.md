# Robot-model assembly — `remoroo_cell/robot_model/` (Phase 5 → G5)

After calibration (`calibration.md`), turn the calibrated hardware into the
single, cuRoboV2-ready **robot model** the planner needs: ONE URDF + a
**collision-sphere** approximation in cuRobo YAML. That sphere model is what
lets cuRobo plan joint trajectories with self- and world-collision checking.

Do this AFTER hand-eye is solved (and, for dual-arm, after base-to-base below).
Outputs are **versioned + editable** — rebuild them whenever the mechanical
setup or a transform changes. Keep it light: **avoid Isaac Sim** (too heavy).

## Step 0 — (dual-arm only) base-to-base from a shared ArUco

Skip for a single arm. Skip if the customer already ships a whole-robot URDF
(go straight to Step 1's skip path). Precondition: BOTH arms have a wrist
(eye-in-hand) camera and BOTH finished eye-in-hand calibration
(`calibration.md`). Put ONE ArUco where both wrist cams can see it; with each
arm, view the marker and record the arm's pose.

The base-to-base transform then falls out of simple matrix algebra — no extra
solver:

```text
For arm k at its capture pose:
  T_base_cam(k)   = T_base_eef(k) · X(k)            # arm FK · hand-eye (eye-in-hand)
  T_base_aruco(k) = T_base_cam(k) · T_cam_aruco(k)  # · ArUco pose from solvePnP

Same physical marker => both base->aruco transforms describe one point:
  T_base0_aruco = T_base0_base1 · T_base1_aruco
=> T_base0_base1 = T_base0_aruco · inv(T_base1_aruco)
```

```python
import numpy as np

def base_to_base(T_base_aruco_0, T_base_aruco_1):
    """4x4 homogeneous transforms; returns arm1's base expressed in arm0's base."""
    return T_base_aruco_0 @ np.linalg.inv(T_base_aruco_1)

# Capture several marker/arm poses and average for robustness:
#   - translation: mean of the t vectors
#   - rotation: quaternion mean (or SVD-orthonormalize the mean rotation matrix)
```

Write the result to `calibration/base_to_base.yaml` and use it as the fixed
joint between the two arms' base links in Step 1.

## Step 1 — one URDF (combine the arms, or use the provided whole-robot URDF)

- **Customer already has a whole-robot URDF → use it as-is. SKIP combining.**
- Otherwise compose the per-arm URDFs (fetched from the library, never
  reconstructed — see `cell.yaml`) into one, joining `arm1`'s base to `arm0`'s
  base with the fixed `base_to_base` transform from Step 0. xacro keeps it
  readable:

```xml
<!-- remoroo_cell/robot_model/combined_dual_arm.urdf.xacro -->
<robot name="combined_dual_arm" xmlns:xacro="http://www.ros.org/wiki/xacro">
  <xacro:include filename="arm0.urdf.xacro"/>            <!-- e.g. right -->
  <xacro:include filename="arm1.urdf.xacro"/>            <!-- e.g. left  -->
  <link name="world"/>
  <joint name="world_to_arm0" type="fixed">
    <parent link="world"/><child link="arm0_base_link"/>
    <origin xyz="0 0 0" rpy="0 0 0"/>
  </joint>
  <joint name="arm0_to_arm1" type="fixed">               <!-- = base_to_base.yaml -->
    <parent link="arm0_base_link"/><child link="arm1_base_link"/>
    <origin xyz="${b2b_x} ${b2b_y} ${b2b_z}" rpy="${b2b_r} ${b2b_p} ${b2b_yaw}"/>
  </joint>
</robot>
```

Resolve the xacro to a flat `robot.urdf` and sanity-check it: it loads, and FK
at a known joint config lands where the real TCP is (within calibration
residual).

## Step 2 — collision spheres → cuRobo YAML (NO Isaac Sim)

cuRobo plans on a **sphere approximation** of the robot, not raw meshes.
Generate spheres from the (resolved) URDF's collision meshes and write cuRobo's
robot YAML. Stay lightweight — do not pull in Isaac Sim.

```python
# Adapt to your installed cuRobo version's API. The flow is version-stable:
#   1) load each collision mesh from the resolved URDF
#   2) fit spheres per link (voxel / medial-axis fill; cap count per link)
#   3) emit cuRobo robot YAML: kinematics + per-link `collision_spheres`,
#      joint limits, self-collision ignore pairs, ee_link.
import trimesh, numpy as np

def spheres_from_mesh(mesh_path, radius=0.02, max_spheres=40):
    m = trimesh.load(mesh_path)
    # Voxelize, then place a sphere at each filled cell centre — simple,
    # deterministic, and Isaac-Sim-free. Cluster/decimate down to max_spheres.
    vox = m.voxelized(pitch=radius).fill()
    centers = np.asarray(vox.points)
    return [{"center": c.tolist(), "radius": float(radius)} for c in centers]

# Assemble into your cuRobo RobotConfig schema, e.g.:
#   robot_cfg.kinematics.collision_spheres = {link: [{center, radius}, ...]}
# Then load it back into cuRobo and run the G1-style smoke plan to validate.
```

Tunables (R&D): per-link radius, sphere count, self-collision ignore pairs.
Bigger/fewer spheres are safer but more conservative — tune until the real-world
smoke plan succeeds without phantom collisions.

## Step 3 — visualize the spheres on the desk (lightweight)

Pose the spheres on the robot meshes at a few joint configs and show the
operator (`view_image`). Use a light viewer — trimesh scene, Open3D, or
pyrender — **not** Isaac Sim. The operator confirms the spheres envelop the real
geometry (gripper fingers covered, nothing clipping through links).

```python
import trimesh
scene = trimesh.Scene()
scene.add_geometry(robot_mesh_at(config))              # FK-posed link meshes
for s in spheres_at(config):
    sph = trimesh.creation.icosphere(radius=s["radius"]).apply_translation(s["center"])
    scene.add_geometry(sph)
png = scene.save_image(resolution=(1280, 720))         # show the operator via view_image
```

## The world ignores the arms

When you build the geometric world (`world_scan.md`), it is the **environment
only** — do NOT bake the arm(s) into it. cuRobo already carries the robot as
these collision spheres and plans joint trajectories with self- and
world-collision checking. Baking the arms into the world double-counts the robot
and blocks valid plans.

This same sphere model does the subtraction: cuRobo's `RobotSegmenter`
(`curobo.wrap.model.robot_segmenter`) reuses these spheres to mask the arm out
of each depth frame before fusion. See `world_scan.md` for the masking code.

## `remoroo_cell/robot_model/` outputs

```text
robot_model/
  combined_dual_arm.urdf.xacro   # only if combined (skip if whole-robot URDF given)
  robot.urdf                     # the single resolved URDF cuRobo loads
  collision_spheres.yml          # cuRobo robot config (spheres + limits + ignore pairs)
  spheres_preview.png            # desk visualization shown to the operator
  report.md                      # sphere counts, base_to_base used, FK check
```

## Acceptance (part of G5)

- One resolved URDF; FK matches the live arm within the calibration residual.
- cuRobo loads the robot YAML and the G1-style smoke plan still succeeds.
- Spheres visualized and operator-confirmed to envelop the real geometry.
- (dual-arm) `base_to_base.yaml` produced from the shared ArUco and embedded.
