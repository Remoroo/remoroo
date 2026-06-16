# Robotics setup seed catalog — routing guide

You (the Remoroo agent) are running `@robot_setup`. Your job is to author the
**cell-specific integration inside this repo** (under `remoroo_cell/`) and
prove the robot can **move autonomously and safely** in its own world — with
**no task execution**. This catalog gives you reference patterns to adapt.
You read the closest seed and **adapt it to THIS cell's real hardware**; you
never copy a stub verbatim and never hardcode one camera or one arm.

> The full design lives in `ADC/remoroo_setup_spec.md`. This README is the
> operational summary you act on.

## Ground rules (read once, hold for the whole run)

- **The bar is autonomous SAFE MOTION, not task execution.** Done = the cell
  plans (cuRoboV2) and executes small collision-free moves against the world
  it built, repeatably, with every interlock verified live, and the operator
  signs off. No manipulation objective.
- **R&D stance — freeze nothing.** Every artifact you write is *versioned and
  editable*. The customer owns and will evolve it. Do not add "locked",
  "do not edit", or immutability language to anything you author.
- **The human is the safety net.** Every motion is operator-supervised with an
  E-stop. You must get an explicit operator go-ahead before *any* motion
  (the FIRST motion is calibration's first pose move — Phase 5) and before the
  autonomous demo (Phase 8). These never auto-pass on a timeout.
- **Ask first — this is collaborative.** It is the operator's responsibility to
  provide the info needed to get through setup; your job is to make it easy and
  make them glad they did it. OPEN by asking the operator the questions that
  shape the cell (arm/DOF, each camera + role/mount + stereo?, gripper/payload,
  network/creds, standing zone) via `ask_human` BEFORE scanning the filesystem.
  Probe (`hardware_preflight`, `bash`, `run_steps`) and read provided files
  (`read_file`, `view_image`) to FILL GAPS and VERIFY — not as the first move.
  UNDER-asking is the failure here; batch the opening questions, give defaults.
- **No fakes.** A gate passes on real evidence, or you fix it, or you stop
  with an actionable message. Never present a stub as working hardware.

## Sensor rule (important — corrects older guidance)

There is **no overhead/static-camera requirement**. A **wrist (eye-in-hand)
depth camera is first-class and often the most powerful option**, because it
can be moved — autonomously or hand-guided — to model the world from many
viewpoints. The acceptance test is **functional**: *can we build a reliable,
safe-enough world for small but autonomous movements?* A **monocular-only**
cell (no reliable metric depth/scale) is **rejected** — you cannot ground a
trustworthy collision world from it. Acceptable sensing: a wrist depth cam, a
static depth cam, stereo, or any combination that yields reliable metric
geometry. Record this feasibility judgement in `cell.yaml` at G0 and prove it
for real at G4/G8.

## The phases and gates (a CHECKLIST by gate — NOT a strict order)

> ORDER: do **calibration (G5) first** among the modeling steps — before the
> world scan (G4) and the safe-motion demo (G8), because the world, planning,
> and capture all depend on accurate extrinsics/intrinsics.
> Recommended: G0 → G1 → G2 → G5 → G4 → G6 → G7 → G8 → G9.

| Phase | What | Gate |
|---|---|---|
| 0 | Inventory host + arm/camera/gripper → `cell.yaml` | **G0** inventory complete; sensing feasible (reject monocular-only) |
| 1 | Toolchain incl. cuRoboV2 | **G1** cuRoboV2 imports + smoke plan on this GPU; SDKs import; deps pinned |
| 2 | Author the Bridge `primitives.py` + capture recorder (no motion) | **G2** imports; no-motion smoke; operator confirms frame; E-stop reachable |
| 4 | World scan → TSDF/ESDF + collision world + scene (**env only — exclude arms**) | **G4** world reliable for small autonomous moves; scene queryable |
| 5 | **Calibration — do FIRST, before the world scan.** Cell's first motion: operator-gated go-ahead + live E-stop check on the first move. Hand-eye (stereo: both lenses) + sysid + **robot-model assembly** (base→base, combined URDF, cuRobo spheres) | **G5** residuals under threshold (reject high-reprojection samples; validate stereo `T_L_R`); robot model built + visualized |
| 6 | Data-capture validation | **G6** one sample episode valid (sync/schema/complete) |
| 7 | Task & eval **spec** (read-and-approve, not code) | **G7** `task_spec.md` operator-approved |
| 8 | Autonomous safe-motion demo (**THE bar**) | **G8** repeatable safe autonomous motion; interlocks verified; no task |
| 9 | Snapshot + readiness card + handoff | **G9** all green, committed, operator signs off |

## Which seed to read for each phase

- `cell_yaml.md` — Phase 0. The `cell.yaml` schema (the canonical RobotModel
  source). Pre-fillable by the operator.
- `bridge_primitives.md` — Phase 2. The Bridge contract (`primitives.py`): the
  cell's robot action surface + how it imports the shipped safety/transport
  spine.
- `arm_adapters.md` — Phase 2. Arm-API binding patterns (xArm / UR / Franka /
  ROS) you adapt into the Bridge.
- `camera_capture.md` — Phase 2. Synchronized RGB-D capture + intrinsics/
  extrinsics (RealSense / ZED / GenICam).
- `data_capture.md` — Phases 2 & 6. The data-capture **recorder** + episode
  schema. Authored for THIS cell's modalities; not hardcoded to any camera.
- `calibration.md` — Phase 5. Hand-eye, intrinsics, time-sync, payload sysid,
  and the shared-ArUco dual-arm base-to-base.
- `robot_model.md` — Phase 5 (after calibration). Assemble the cuRobo-ready
  model: combine per-arm URDFs (skip if a whole-robot URDF is provided),
  generate collision spheres from meshes → cuRobo YAML, visualize on the desk
  (NO Isaac Sim).
- `world_scan.md` — Phase 4. Scan strategy: static-depth fusion, eye-in-hand
  active sweep, and operator hand-guided scan; build TSDF/ESDF + scene as the
  **environment only** (ignore the arms — cuRobo handles the robot).
- `task_spec_template.md` — Phase 7. The human-readable task + eval **spec**
  the operator approves (no code).

## What you author vs. what is shipped

- **You author (in `remoroo_cell/`, editable):** `cell.yaml`, `primitives.py`
  (Bridge), the capture recorder, `calibration/`, `robot_model/`, `world/`,
  `task_spec.md`, `requirements.lock`, `setup_report.md`.
- **You import, never author:** the deterministic safety supervisor, the
  brain↔worker transport, and the episode-writer base. These ship in the
  installed Remoroo package. (Import them from the shipped runtime spine; see
  `bridge_primitives.md` for the import surface and a fallback shim if the
  spine isn't importable yet during R&D.)

## Target layout after setup

```text
<repo>/
  remoroo_cell/
    cell.yaml
    primitives.py            # Bridge (authored; versioned + editable)
    capture/
      recorder.py            # data-capture recorder (authored)
      schema.json
      sample_episode/
    calibration/
      hand_eye.yaml
      base_to_base.yaml      # dual-arm only (shared-ArUco)
      report.md
    robot_model/             # cuRobo-ready model (Phase 5; skip combine if URDF given)
      robot.urdf             # single resolved URDF cuRobo loads
      collision_spheres.yml  # cuRobo robot config (spheres + limits)
      spheres_preview.png    # desk visualization (no Isaac Sim)
    world/
      collision.*            # TSDF/ESDF / cuRobo collision world (env only — no arms)
      scene.json
      scan_report.md
    task_spec.md             # task + eval SPEC (operator-approved; NOT code)
    requirements.lock
    setup_report.md          # the readiness card: G0–G8 status + evidence
```

When all gates are green and the operator signs off: `git add remoroo_cell/`,
commit once, and call `done(verdict="success")`. If blocked, call `done` with
a partial verdict and make sure `setup_report.md` lists exactly which gates
are red and why.
