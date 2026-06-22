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

## Collision spheres → cuRobo YAML (YOURS — fast, NO Isaac Sim)

cuRobo plans on a **sphere approximation** of the robot, not raw meshes. Fit spheres PER LINK with
cuRobo's purpose-built `fit_spheres_to_mesh`. **Two hard-won rules (do NOT skip):**

1. **Use `fit_type=SphereFitType.VOXEL`, NOT the default `MORPHIT`.** MorphIt runs ~200 optimisation
   iterations PER LINK — on a 30-link robot on a Jetson Orin that is **~80 minutes and often times
   out**. VOXEL is a single Warp-SDF voxel-fill: deterministic, seconds per link. Also pass
   `compute_metrics=False` (the metrics pass is expensive). **Do NOT use `RobotBuilder`** to generate
   spheres — it additionally computes a self-collision matrix by sampling thousands of configs (very
   slow on edge HW) AND it **ignores the per-mesh `<mesh scale>`** (see rule 2).
2. **Apply the URDF `<mesh scale>` to each mesh BEFORE fitting** — load-bearing. Vendor meshes are
   often in MILLIMETRES (`scale="0.001 0.001 0.001"`). `estimate_sphere_count` and the fit read the
   mesh bbox in metres, so an un-scaled mm mesh comes out **1000× too big** (real symptom: a ZED
   camera link fit to a **5.2 m** sphere). Mixed units bite per-link: xArm arm meshes were already in
   metres (sane), the ZED/holder meshes were in mm (5 m spheres). Apply each mesh's own scale.

```python
import os, trimesh, numpy as np, yaml
import xml.etree.ElementTree as ET
from curobo.sphere_fit import fit_spheres_to_mesh, estimate_sphere_count, SphereFitType

PKG_ROOT = "remoroo_cell"          # package://remoroo_cell/... → this dir
CAP = 20                            # max spheres/link (planning speed); arms ~6-16, big base more

def _origin_T(el):                 # <collision><origin xyz rpy> → 4x4 (link-local)
    rpy = [float(v) for v in ((el.get("rpy") if el is not None else None) or "0 0 0").split()]
    xyz = [float(v) for v in ((el.get("xyz") if el is not None else None) or "0 0 0").split()]
    T = trimesh.transformations.euler_matrix(*rpy)     # URDF fixed-axis rpy (tf default 'sxyz')
    T[:3, 3] = xyz
    return T

def _resolve(filename):            # package://remoroo_cell/meshes/x.stl → remoroo_cell/meshes/x.stl
    if filename.startswith("package://"):
        filename = filename[len("package://"):].split("/", 1)[1]   # drop the "<pkg>/" prefix
    return os.path.join(PKG_ROOT, filename)

def fit_link(link_el):
    out = []
    for col in link_el.findall("collision"):
        geo = col.find("geometry/mesh")
        m = None
        if geo is not None:
            m = trimesh.load(_resolve(geo.get("filename")), force="mesh")
            m.apply_scale([float(s) for s in (geo.get("scale") or "1 1 1").split()])   # RULE 2 — mm→m
        # (primitives: box/cylinder/sphere → trimesh.creation.* ; fit them too)
        if m is None or m.is_empty: continue
        m.apply_transform(_origin_T(col.find("origin")))     # collision origin → link frame
        n = max(3, min(CAP, estimate_sphere_count(m)))       # bigger link → more spheres, capped
        r = fit_spheres_to_mesh(m, num_spheres=n, fit_type=SphereFitType.VOXEL, compute_metrics=False)  # RULE 1
        out += [{"center": c.tolist(), "radius": float(rad)}
                for c, rad in zip(r.centers.cpu().numpy(), r.radii.cpu().numpy()) if rad > 1e-3]
    return out

root = ET.parse("remoroo_cell/robot_model/robot.urdf").getroot()
spheres = {l.get("name"): fit_link(l) for l in root.findall("link")}
spheres = {k: v for k, v in spheres.items() if v}            # drop links with no collision geom
```

**Self-collision ignore — derive it cheaply from URDF adjacency, do NOT sample.** Ignore each
parent↔child link pair (and pairs that are always touching). That's an XML walk, not a 10k-config
sampling job:
```python
ignore = {}
for j in root.findall("joint"):
    p, c = j.find("parent").get("link"), j.find("child").get("link")
    ignore.setdefault(p, []).append(c); ignore.setdefault(c, []).append(p)
```
Then assemble + write `robot_model/collision_spheres.yml` — **exactly this structure, with PLAIN
python floats** (the Studio's collision view reads it from here):
```python
robot_cfg = {"robot_cfg": {"kinematics": {
    "urdf_path": "robot.urdf",
    "collision_spheres": spheres,                  # {link: [{center:[x,y,z], radius:r}]}
    "collision_link_names": list(spheres.keys()),
    "self_collision_ignore": ignore,
    "collision_sphere_buffer": 0.005,
}}}
import yaml
yaml.safe_dump(robot_cfg, open("remoroo_cell/robot_model/collision_spheres.yml", "w"), sort_keys=False)
```
**Use `safe_dump`, and make every center/radius a PLAIN `float`/`list` (`.tolist()`, `float(r)`) —
NOT numpy.** `safe_dump` REFUSES numpy types (so it's your guarantee the file is clean); a file dumped
with `yaml.dump` of `np.float32` is full of `!!python/object` tags that the Studio (and other
`safe_load` consumers) can't render — the spheres save to disk but show as nothing in the collision
view. Load it back and run the G1 smoke plan to validate.

**SANITY-CHECK before you emit `collision_spheres.yml`** (this is where the failures show up): every
link's sphere radii should be CENTIMETRES (a wrist ~1–5 cm), and the union should envelop the mesh. If
any link's median radius is **> ~0.3 m** the scale is wrong (mm not converted) — fix it, don't ship.
If a link's median is **< a few mm** the geometry is ~1000× too small. The Studio flags a degenerate
set with a red "planning UNSAFE" banner, but the cuRobo model is broken until you regenerate it
correctly. Tunables (R&D): `CAP`, `sphere_density`, ignore pairs — bigger/fewer is safer but more
conservative; tune until the real-world smoke plan succeeds without phantom collisions.

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
