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
3. **AUTHOR THE KINEMATIC CONFIG** (the core of this gate — see below): read the
   URDF and DECLARE the robot's actuated `groups` in `cell.yaml`. YOU are the
   generalizer — there is no auto-discovery to lean on. Ask the operator whenever
   the URDF is ambiguous.
4. **Sphere-fit it (this part is yours)** and **(dual-arm)** fold in the
   base-to-base transform that *calibration* computed (see `calibration.md`) — the
   operator placed the arms approximately in the editor; calibration gives the
   precise `arm0→arm1` transform that cuRobo uses alongside the URDF.

## Author the kinematic config — `cell.yaml: groups` (the heart of G5)

A robot is ANY morphology: one arm, two arms, an arm + a gantry, a humanoid (arms
+ legs + head), a quadruped, a wheeled base. There is **no deterministic
discovery** of "arms" — **you read the URDF and declare its actuated GROUPS.** A
group is one named, actuated kinematic chain. This declaration is the single
source of truth every later stage reads (motion planning, calibration, the
bridge, the Studio); nothing re-derives or "guesses" it.

**Use the mechanical helpers — never hand-invent names.** The shipped
`calib_engine.urdf_io` gives you exact, parse-only facts so you interpret real
structure, not hallucinated strings:

```python
from calib_engine import urdf_io
facts = urdf_io.urdf_facts("remoroo_cell/robot_model/robot.urdf")
#   facts["links"]   -> [{name, mesh}]            (mesh filename hints cameras/tools)
#   facts["joints"]  -> [{name, type, parent, child, axis, limit}]
#   facts["roots"]   -> [link]  (the shared base frame; usually one)
#   facts["movable_joints"] -> [name]  (every actuated joint — ALL must be placed)
# For each end-effector/tool tip you identify, get its EXACT joint chain:
_, joint_names, _ = urdf_io.chain_from_urdf("…/robot.urdf", tip_link)   # base→tip order
```

Then write `cell.yaml`:

```yaml
groups:                                   # one per actuated chain — ANY kind
  - name: arm_left                        # YOUR stable id; the bridge keys drivers by it
    kind: arm                             # arm | leg | wheel | head | gripper | torso | free  (a ROLE TAG)
    base_link: world                      # the shared planning root (facts["roots"][0])
    tip_links: [left_tcp]                 # end-effector/tool link(s) — a LIST (a hand, a foot, a mount)
    joint_names: [l_j1, l_j2, l_j3, l_j4, l_j5, l_j6]   # from chain_from_urdf — exact, in order
    cameras: [left_wrist_cam]             # URDF camera link(s) rigidly on this chain (if any)
    tags: { side: left }                  # OPTIONAL advisory labels — never structural
  # … a leg, a wheel, a head: the SAME shape, different kind/tips/joints
cameras:                                  # add the URDF `link` to each camera (the join key)
  - name: left_wrist_cam
    link: left_wrist_cam                  # the camera body link IN THE URDF
    mount: eye_in_hand
    owner: arm_left
ignore_joints: []                         # any actuated joint deliberately NOT in a group (rare)
```

**Rules (these are what make it robust):**
- **Trace joints, never guess them** — `joint_names` come from `chain_from_urdf(tip)`, in order.
  The bridge reports joint states in this exact order; a wrong order silently mis-drives the robot.
- **Read the morphology, never assume it** — count groups from the URDF, not from "it's probably
  dual-arm". A leg/wheel/head is just a group with a different `kind` and tip.
- **ASK when the URDF is ambiguous** (`ask_human`/`gate_checkpoint`), never guess: which physical
  side a chain is, whether a link is a real tool tip or just a flange, a chain's role. Physical
  identity (left vs right in the room) is OPERATOR ground truth — confirm it via the Studio model
  gate's "verify by motion" (wiggle one limb → it shows which modeled group moved → operator labels).
- **Every actuated joint must land somewhere** — in a group or in `ignore_joints`. The shipped
  `urdf_io.validate_robot_config(config, urdf)` enforces this and that every name/tip exists and no
  joint is in two groups; run it before you checkpoint, and FIX what it reports (a mistake here
  fails in the gate, not on the robot).
- **Back-compat:** a legacy `arms:` list still works — each entry is read as a `kind: arm` group with
  joints traced from its camera flange. Prefer authoring `groups:` for anything that isn't plain arms.

Then `gate_checkpoint(gate=model)` with the authored groups for the operator to confirm.

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

def spheres_from_mesh(mesh_path, scale=(1.0, 1.0, 1.0), radius=0.02, max_spheres=40):
    m = trimesh.load(mesh_path)
    # APPLY THE URDF <mesh scale> FIRST — load-bearing. Vendor arm meshes (xArm, UR, …) are often
    # in MILLIMETRES with scale="0.001 0.001 0.001"; if you skip this the geometry is ~1000× too
    # small and every sphere comes out MICROSCOPIC (radii < 1 mm) → the arm has NO effective
    # collision model and cuRobo plans straight through it. (Symptom seen on hardware: arm-link
    # spheres at radius ~5e-5 m while camera spheres were a sane ~0.013 m.)
    m.apply_scale(scale)
    # Voxelize, then place a sphere at each filled cell centre — simple,
    # deterministic, and Isaac-Sim-free. Cluster/decimate down to max_spheres.
    vox = m.voxelized(pitch=radius).fill()
    centers = np.asarray(vox.points)
    return [{"center": c.tolist(), "radius": float(radius)} for c in centers]

# Assemble into your cuRobo RobotConfig schema, e.g.:
#   robot_cfg.kinematics.collision_spheres = {link: [{center, radius}, ...]}
# Then load it back into cuRobo and run the G1-style smoke plan to validate.
```

**Mesh units are load-bearing — read + apply the URDF `<mesh scale>`.** Each
`<collision><geometry><mesh scale=…>` must be applied before fitting, and confirm the mesh isn't
otherwise in millimetres. A units slip makes the spheres MICROSCOPIC and the arm un-modelled (cuRobo
will plan through it) — or, if doubled, fills the whole world. **SANITY-CHECK before you emit
`collision_spheres.yml`:** each arm link's spheres should have radii on the order of CENTIMETRES (a
wrist link ~1–5 cm) and their union should envelop the link mesh; if the median radius is below a few
millimetres the scale is wrong — fix it, don't ship it (the Studio now flags a degenerate set with a
red "planning UNSAFE" banner and falls back to a mesh approximation, but the cuRobo model is still
broken until you regenerate it correctly).

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
cell.yaml                        # gains `groups:` — the AUTHORED kinematic config (source of truth) ← YOURS
robot_model/
  robot.urdf                     # the OPERATOR's exported URDF (you read it; you do not author it)
  collision_spheres.yml          # cuRobo robot config (spheres + limits + ignore pairs)  ← YOURS
  spheres_preview.png            # desk visualization shown to the operator                ← YOURS
  report.md                      # sphere counts, base_to_base used, FK check              ← YOURS
```
There is NO `arms.yaml` — the kinematic config is the authored `cell.yaml: groups`, computed by no
one but you and confirmed by the operator, so it never drifts.

## Acceptance (part of G5)

- The operator's `robot.urdf` is present; FK matches the live arm within the
  calibration residual (if not, hand back to the operator — do not edit geometry).
- `cell.yaml: groups` authored from the URDF and `urdf_io.validate_robot_config` returns NO errors
  (every joint placed, every name/tip real); operator confirmed the groups (+ physical labels).
- cuRobo loads the robot YAML and the G1-style smoke plan still succeeds.
- Spheres previewed and operator-confirmed to envelop the real geometry.
