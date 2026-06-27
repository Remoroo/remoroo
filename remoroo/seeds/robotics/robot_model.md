# Robot model — `remoroo_cell/robot_model/` (Phase 5 → G5)

**The robot URDF is the OPERATOR's, not yours.** You cannot see the rig — the
camera holder, the mount offsets, the dual-arm layout — so you cannot model it.
The operator assembles the entire URDF in the **Studio editor** (arm + holder +
camera mounts + frames, and for dual-arm the second arm's placement). Your job
is to **consume** their exported URDF and turn it into the cuRobo collision
model the planner needs.

> **ONE URDF — `remoroo_cell/robot_model/robot.urdf` is the SINGLE SOURCE OF TRUTH.**
> The Studio loads it, the operator edits it, calibration writes the optical
> frames back INTO it, and the planner (`motion_engine` / cuRobo) reads it. **Do
> NOT create a parallel/"resolved" URDF (`robot_resolved.urdf`, `robot_model/resolved/…`)
> and treat it as the model** — that silently diverges from what the Studio shows
> and what calibration updates. You do NOT need one: cuRobo parses `robot.urdf`
> with `load_meshes=False` (kinematics only — the `package://…` mesh refs are never
> loaded), and for sphere-fitting you resolve each mesh path in-memory (see the
> sphere section). If you ever write a scratch URDF for a one-off tool, keep it in
> `/tmp`, never under `robot_model/`, and never point cell.yaml / the spheres /
> the planner at it.

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

## Collision spheres → cuRobo YAML (YOURS — quality over speed, NO Isaac Sim)

cuRobo plans on a **sphere approximation** of the robot, not raw meshes — the model is only as safe as
this approximation. So generate it for QUALITY, then PROVE it with two checks before the gate. Fit
spheres PER LINK with cuRobo's `fit_spheres_to_mesh`. **Hard-won rules:**

1. **Use `fit_type=SphereFitType.MORPHIT` with `compute_metrics=True`.** MorphIt is a voxel-seeded Adam
   optimisation of coverage/protrusion — strictly tighter than VOXEL (which is only MorphIt's *init*,
   no optimisation). It is SLOW on a Jetson Orin (~200 iters × ~30 links ≈ tens of minutes) — that is
   ACCEPTED: correctness beats speed for the collision model, and we make it OBSERVABLE by writing
   per-link progress (below). `compute_metrics=True` returns the coverage numbers CHECK 1 gates on.
2. **Apply the URDF `<mesh scale>` to each mesh BEFORE fitting** — load-bearing, and the historical bug.
   Vendor meshes are often in MILLIMETRES (`scale="0.001 0.001 0.001"`); an un-scaled mm mesh fits to
   METRE-wide spheres (a ZED link → 5.2 m), or doubled, microscopic ones. We apply scale explicitly in
   trimesh (below) so it is under YOUR control — independent of any cuRobo mesh-loader behaviour — and
   CHECK 1's `volume_ratio` catches it if it ever slips.
3. **The self-collision ignore matrix is cuRobo's COMPLETE one**, not a cheap adjacency walk (below).

### Fit each link (MorphIt + metrics + progress)

```python
import os, json, trimesh, numpy as np, yaml
import xml.etree.ElementTree as ET
from curobo.sphere_fit import fit_spheres_to_mesh, estimate_sphere_count, SphereFitType

PKG_ROOT = "remoroo_cell"          # package://remoroo_cell/... → this dir
OUT      = "remoroo_cell/robot_model"
CAP      = 24                       # max spheres/link (planning speed); arms ~6-16, big base more

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

def link_mesh(link_el):            # concat a link's collision meshes, mm→m scaled + origin'd, link frame
    parts = []
    for col in link_el.findall("collision"):
        geo = col.find("geometry/mesh")
        if geo is None:
            continue                                   # (primitives box/cyl/sphere → trimesh.creation.*)
        m = trimesh.load(_resolve(geo.get("filename")), force="mesh")
        m.apply_scale([float(s) for s in (geo.get("scale") or "1 1 1").split()])   # RULE 2 — mm→m
        m.apply_transform(_origin_T(col.find("origin")))                            # collision origin
        if not m.is_empty:
            parts.append(m)
    return trimesh.util.concatenate(parts) if parts else None

def fit_link(m, coverage_weight=1000.0):
    n = max(3, min(CAP, estimate_sphere_count(m)))
    r = fit_spheres_to_mesh(m, num_spheres=n, fit_type=SphereFitType.MORPHIT, compute_metrics=True,
                            coverage_weight=coverage_weight, protrusion_weight=10.0)   # RULE 1
    spheres = [{"center": c.tolist(), "radius": float(rad)}
               for c, rad in zip(r.centers.cpu().numpy(), r.radii.cpu().numpy()) if rad > 1e-3]
    mt = r.metrics
    metrics = {"coverage": mt.coverage, "max_uncovered_gap": mt.max_uncovered_gap,
               "surface_gap_p95": mt.surface_gap_p95, "protrusion": mt.protrusion,
               "protrusion_dist_p95": mt.protrusion_dist_p95, "volume_ratio": mt.volume_ratio,
               "num_spheres": mt.num_spheres}
    return spheres, metrics

root = ET.parse(f"{PKG_ROOT}/robot_model/robot.urdf").getroot()
mesh_links = [l for l in root.findall("link") if link_mesh(l) is not None]
spheres, metrics, n = {}, {}, len(mesh_links)
for i, link_el in enumerate(mesh_links):
    name = link_el.get("name")
    json.dump({"link_i": i + 1, "n": n, "link": name},          # ← Studio reads /project/spheres/progress
              open(f"{OUT}/spheres_progress.json", "w"))
    spheres[name], metrics[name] = fit_link(link_mesh(link_el))
spheres = {k: v for k, v in spheres.items() if v}               # drop links with no collision geom
```

### CHECK 1 — coverage / scale (per link, geometry in isolation)

`compute_metrics=True` already measured each link. The safety-critical metric is **`max_uncovered_gap`**
(how much real robot surface sits OUTSIDE its spheres → cuRobo is blind to it there), and
**`volume_ratio`** is the scale-bug detector. Gate on them; tighten a warn link by re-fitting with a
higher coverage weight.

```python
MAX_UNCOVERED_GAP_FAIL = 0.020      # m — >2cm of surface outside spheres = UNSAFE (robot invisible there)
MAX_UNCOVERED_GAP_WARN = 0.005
VOLUME_RATIO_LO, VOLUME_RATIO_HI = 0.3, 6.0   # sphere-vol/mesh-vol outside this ⇒ a scale/units bug
COVERAGE_WARN, PROTRUSION_WARN = 0.90, 0.10

def link_fails(mt):
    return (mt["volume_ratio"] < VOLUME_RATIO_LO or mt["volume_ratio"] > VOLUME_RATIO_HI
            or mt["max_uncovered_gap"] > MAX_UNCOVERED_GAP_FAIL)

for name in list(spheres):          # tighten WARN links ONCE (push spheres to fill) before judging
    mt = metrics[name]
    warn = (mt["coverage"] < COVERAGE_WARN or mt["protrusion"] > PROTRUSION_WARN
            or mt["max_uncovered_gap"] > MAX_UNCOVERED_GAP_WARN)
    if warn and not link_fails(mt):
        el = next(l for l in mesh_links if l.get("name") == name)
        spheres[name], metrics[name] = fit_link(link_mesh(el), coverage_weight=3000.0)

fails = [f"{name}: volume_ratio={metrics[name]['volume_ratio']:.2f}, "
         f"max_uncovered_gap={metrics[name]['max_uncovered_gap'] * 1000:.0f}mm"
         for name in spheres if link_fails(metrics[name])]
if fails:
    raise SystemExit("SPHERES CHECK 1 (coverage/scale) FAILED — fix, do NOT ship:\n  " + "\n  ".join(fails))
```

### Full self-collision ignore matrix (cuRobo's complete one)

Derive the ignore matrix from cuRobo's `RobotBuilder` — neighbour pairs **plus** pairs that overlap at
the default config (gripper fingers, dual-arm bases) **plus** sample-pruned never-collide pairs. This
is the matrix the planner needs; the old cheap parent↔child walk left by-design touches reading as
PERMANENT self-collisions, and nothing planned. Feed in the spheres you just fitted (no refit):

```python
from curobo._src.robot.builder.builder_robot import RobotBuilder

cell = yaml.safe_load(open(f"{PKG_ROOT}/cell.yaml")) or {}
tips = [t for g in (cell.get("groups") or []) for t in (g.get("tip_links") or [])]
rb = RobotBuilder(f"{PKG_ROOT}/robot_model/robot.urdf", tool_frames=tips or None)
rb._collision_spheres = {k: list(v) for k, v in spheres.items()}     # inject our spheres — no refit
ignore = dict(rb.compute_collision_matrix(prune_collisions=True) or {})
```

### Write `collision_spheres.yml` (exact schema, PLAIN python floats)

```python
robot_cfg = {"robot_cfg": {"kinematics": {
    "urdf_path": "robot.urdf",
    "collision_spheres": spheres,                  # {link: [{center:[x,y,z], radius:r}]}
    "collision_link_names": list(spheres.keys()),
    "self_collision_ignore": ignore,               # the COMPLETE matrix — the planner READS this
    "collision_sphere_buffer": 0.005,
}}}
yaml.safe_dump(robot_cfg, open(f"{OUT}/collision_spheres.yml", "w"), sort_keys=False)
```
**Use `safe_dump` with PLAIN `float`/`list` (`.tolist()`, `float(r)`) — NOT numpy.** `safe_dump`
REFUSES numpy types; a `yaml.dump` of `np.float32` writes `!!python/object` tags the Studio can't
render (spheres save to disk but show as nothing in the collision view).

### CHECK 2 — self-collision: does the SPHERE model agree with the MESH model?

"How often does the robot self-collide?" has no good answer — a valid arm self-collides at plenty of
real configs, so a raw collision frequency has no baseline to gate on. The answerable question is
whether the SPHERES agree with the real GEOMETRY. Load the cell you just wrote and run the SHIPPED
`motion_engine` audit (`sphere_audit`): over many configs sampled within the URDF joint limits, it
compares cuRobo's sphere self-collision verdict (same true radii + the ignore matrix you just wrote) to
the EXACT mesh verdict (FCL, scipy hull fallback) — the `foam` sphere-approximation methodology:

  • **PHANTOM** — spheres collide, meshes DON'T → the planner is blocked for nothing (over-fat spheres /
    a missing ignore pair / two chains too close). `phantom_rate` baseline is ~0, so nonzero is a signal.
  • **MISS** — meshes collide, spheres DON'T → the planner is blind to a real collision (UNSAFE).

It gates on `home_free` + `phantom_rate` (warn >2%, fail >10%) + `miss_rate` (any miss ⇒ fail). Each
phantom pair is named (with a `cross_arm` tag: different groups ⇒ arms modelled too close / bad
base-to-base; same group ⇒ over-fat spheres) so you know what to fix:

```python
from motion_engine import MotionStack

check2 = MotionStack.from_cell(PKG_ROOT).audit_self_collision(n=400)
if check2.get("verdict") == "fail":
    raise SystemExit("SPHERES CHECK 2 (mesh-vs-sphere) FAILED:\n" + json.dumps(check2, indent=2))
```

### Report + preview, then checkpoint

Write the structured report the Studio renders (per-link coverage + the Check-2 verdict + phantom
pairs), a desk preview (trimesh / Open3D / pyrender — **not** Isaac Sim), then hand off:

```python
report = {"n_links": len(spheres), "n_spheres": sum(len(v) for v in spheres.values()),
          "metrics": metrics, "check2": check2}
json.dump(report, open(f"{OUT}/spheres_report.json", "w"), indent=2)     # ← /project/spheres/report

scene = trimesh.Scene()                                # FK-pose the spheres for a preview
for name in spheres:
    for s in spheres[name]:
        scene.add_geometry(trimesh.creation.icosphere(radius=s["radius"]).apply_translation(s["center"]))
scene.save_image(resolution=(1280, 720))               # → spheres_preview.png, shown via view_image
```

Then `gate_checkpoint(gate=spheres)` with the verdict + preview. The operator approves on the SURFACED
evidence (per-link coverage, named phantom pairs) — not by eyeballing. **Tunables (R&D):** `CAP`,
`coverage_weight`/`protrusion_weight`, the CHECK-1 thresholds, and `n` for the audit.

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
  collision_spheres.yml          # cuRobo robot config (spheres + COMPLETE ignore matrix + buffer)  ← YOURS
  spheres_report.json            # per-link coverage metrics + the CHECK-2 self-collision verdict ← YOURS
  spheres_preview.png            # desk visualization shown to the operator                       ← YOURS
  report.md                      # sphere counts, base_to_base used, FK check                     ← YOURS
```
There is NO `arms.yaml` — the kinematic config is the authored `cell.yaml: groups`, computed by no
one but you and confirmed by the operator, so it never drifts.

## Acceptance (part of G5)

- The operator's `robot.urdf` is present; FK matches the live arm within the
  calibration residual (if not, hand back to the operator — do not edit geometry).
- `cell.yaml: groups` authored from the URDF and `urdf_io.validate_robot_config` returns NO errors
  (every joint placed, every name/tip real); operator confirmed the groups (+ physical labels).
- cuRobo loads the robot YAML and the G1-style smoke plan still succeeds.
- **CHECK 1 (coverage/scale) passes** — no link FAILs `volume_ratio`/`max_uncovered_gap`; warn links
  re-fit. The spheres envelop the real geometry (the per-link metrics prove it, not just the eye).
- **CHECK 2 (mesh-vs-sphere) verdict is not `fail`** — `home_free` true, `miss_rate` 0 (no unsafe
  blind spots), `phantom_rate` below threshold (`MotionStack.audit_self_collision`). The matrix written
  IS what the planner reads.
- Spheres previewed and operator-confirmed on the surfaced report (coverage + named phantom pairs).
