# Robot model — `remoroo_cell/robot_model/` (Phase 5 → G5)

**The robot URDF is the OPERATOR's, not yours.** You cannot see the rig — the
camera holder, the mount offsets, the dual-arm layout — so you cannot model it.
The operator assembles the entire URDF in the **Studio editor** (arm + holder +
camera mounts + frames, and for dual-arm the second arm's placement). Your job
is to **consume** their exported URDF and turn it into the cuRobo collision
model the planner needs.

So at Phase 5:

1. **Hand off the model gate and WAIT.** Issue `gate_checkpoint(gate=model)`. The
   operator builds/exports `remoroo_cell/robot_model/robot.urdf` in the editor.
   Do **not** fetch a stock URDF, stitch xacros, or hand-edit geometry.
2. On resume, **read `robot_model/robot.urdf`** — this is your ground-truth model.
   Sanity-check only: it loads, and FK at a known joint config lands where the
   live TCP is (within the calibration residual). If it's wrong, hand it back to
   the operator with a `gate_checkpoint` note; do not "fix" the geometry yourself.
3. **Sphere-fit it (this part is yours)** and **(dual-arm)** fold in the
   base-to-base transform that *calibration* computed (see `calibration.md`) — the
   operator placed the arms approximately in the editor; calibration gives the
   precise `arm0→arm1` transform that cuRobo uses alongside the URDF.

## Collision spheres → cuRobo YAML (YOURS — NO Isaac Sim)

cuRobo plans on a **sphere approximation** of the robot, not raw meshes. Generate
spheres from the operator's resolved URDF collision meshes and write cuRobo's
robot YAML. Stay lightweight — do not pull in Isaac Sim.

```python
# Adapt to your installed cuRobo version's API. The flow is version-stable:
#   1) load each collision mesh from the operator's resolved URDF
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

## Preview the spheres for the operator (lightweight)

Pose the spheres on the operator's URDF meshes at a few joint configs and show
them via `gate_checkpoint(gate=spheres)` with a `view_image` preview. Use a light
viewer — trimesh scene, Open3D, or pyrender — **not** Isaac Sim. The operator
confirms the spheres envelop the real geometry (gripper fingers covered, nothing
clipping through links), or flags strays.

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
world-collision checking. Baking the arms in double-counts the robot and blocks
valid plans. cuRobo's `RobotSegmenter` (`curobo.wrap.model.robot_segmenter`)
reuses these spheres to mask the arm out of each depth frame before fusion.

## `remoroo_cell/robot_model/` outputs

```text
robot_model/
  robot.urdf                     # the OPERATOR's exported URDF (you read it; you do not author it)
  collision_spheres.yml          # cuRobo robot config (spheres + limits + ignore pairs)  ← YOURS
  spheres_preview.png            # desk visualization shown to the operator                ← YOURS
  report.md                      # sphere counts, base_to_base used, FK check              ← YOURS
```

## Acceptance (part of G5)

- The operator's `robot.urdf` is present; FK matches the live arm within the
  calibration residual (if not, hand back to the operator — do not edit geometry).
- cuRobo loads the robot YAML and the G1-style smoke plan still succeeds.
- Spheres previewed and operator-confirmed to envelop the real geometry.
