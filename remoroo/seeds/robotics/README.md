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

- **Split of responsibility (the whole point of the Studio).** You own the
  COMPUTATIONAL work you can probe/execute (toolchain, `primitives.py`, cuRobo
  spheres + planning, the calibration ROUTINE, world scan, capture, the demo).
  The OPERATOR owns the PHYSICAL/JUDGEMENT work in the Studio: **building the
  robot URDF in the editor**, **defining the safety envelope**, confirming the
  inventory, accepting calibration, defining the task. You must NOT build/edit the
  URDF (you can't see the rig), invent safety boundaries, or scavenge old
  calibration off disk. Seed inputs + consume outputs; never substitute your
  judgement for the operator's on an operator-owned gate.
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

> ORDER: the OPERATOR builds the robot model FIRST (in the Studio editor) — you
> cannot model a rig you cannot see. Then you sphere-fit it and calibrate (the
> first motion), which need the model + spheres + the operator's safety envelope.
> Recommended: G0 detect → **G0.5 safety envelope (operator)** → G1 toolchain →
> G2 bridge → **G5 model (operator, editor)** → G5 spheres (you) → G5 calibrate
> (first motion) → G4 world → G6 → G7 → G8 → G9.

| Phase | What | Owner | Gate |
|---|---|---|---|
| 0 | Probe host + arm/camera/gripper → draft `cell.yaml` | A→O | **G0** inventory confirmed by operator; sensing feasible (reject monocular-only) |
| 0.5 | **Safety envelope** — keep-out, standing zone, bounds, max speed | **O** | **G0.5** operator-defined in the Studio → `safety.yaml`; no motion yet |
| 1 | Toolchain incl. cuRoboV2 | A | **G1** cuRoboV2 imports + smoke plan on this GPU; SDKs import; deps pinned |
| 2 | Author the Bridge `primitives.py` + capture recorder (no motion) | A→O | **G2** imports; no-motion smoke; operator confirms frame; E-stop reachable |
| 5·model | **Operator builds the robot URDF in the editor** | **O** | exports `robot_model/robot.urdf` (you do NOT author it) |
| 5·spheres | cuRobo collision spheres FROM the operator's URDF | A→O | spheres built; operator approves the preview |
| 5·calib | **Calibration ROUTINE** — cell's first motion, operator-gated; FRESH (no disk reuse) | A⚠O | **G5** residuals under threshold; stereo `T_L_R` valid; reports re-runnable |
| 4 | World scan → TSDF/ESDF + collision world (**env only — exclude arms**) | A⚠O | **G4** world reliable for small autonomous moves; operator confirms coverage |
| 6 | Data-capture validation | A | **G6** one sample episode valid (sync/schema/complete) |
| 7 | Task & eval **spec** (the operator's intent) | **O** | **G7** `task_spec.md` operator-defined + approved |
| 8 | Autonomous safe-motion demo (**THE bar**) | A⚠O | **G8** repeatable safe autonomous motion; interlocks verified; no task |
| 9 | Snapshot + readiness card + handoff | A→O | **G9** all green, committed, operator signs off |

> Owner key: **A** = you (operator watches/approves) · **A→O** = you propose,
> operator confirms · **A⚠O** = you execute, operator supervises + PERMITS
> (motion) · **O** = the operator does it in the Studio; you seed inputs + consume.

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
- `robot_model.md` — Phase 5. The OPERATOR builds the URDF in the Studio editor;
  you CONSUME `robot_model/robot.urdf` and sphere-fit it (cuRobo collision
  spheres → YAML + preview for approval). You do not author the URDF.
- `world_scan.md` — Phase 4. Scan strategy: static-depth fusion, eye-in-hand
  active sweep, and operator hand-guided scan; build TSDF/ESDF + scene as the
  **environment only** (ignore the arms — cuRobo handles the robot).
- `task_spec_template.md` — Phase 7. The human-readable task + eval **spec**
  the operator approves (no code).

## What you author vs. what is shipped

- **You author (computational, in `remoroo_cell/`):** `primitives.py` (Bridge),
  the capture recorder, `calibration/`, `robot_model/collision_spheres.yml` (from
  the operator's URDF), `world/`, `requirements.lock`, `setup_report.md`; and you
  DRAFT `cell.yaml` for the operator to confirm.
- **The OPERATOR authors in the Studio (you consume, never author):**
  `robot_model/robot.urdf` (the editor), `safety.yaml` (the safety envelope), and
  `task_spec.md` (the task intent).
- **You import, never author:** the deterministic safety supervisor, the
  brain↔worker transport, and the episode-writer base. These ship in the
  installed Remoroo package. (Import them from the shipped runtime spine; see
  `bridge_primitives.md` for the import surface and a fallback shim if the
  spine isn't importable yet during R&D.)

## Target layout after setup

```text
<repo>/
  remoroo_cell/
    cell.yaml                # you draft; the operator confirms (G0)
    safety.yaml              # the OPERATOR's safety envelope (G0.5); you consume it
    primitives.py            # Bridge (authored; versioned + editable)
    capture/
      recorder.py            # data-capture recorder (authored)
      schema.json
      sample_episode/
    calibration/
      hand_eye.yaml
      base_to_base.yaml      # dual-arm only (shared-ArUco)
      report.md
    robot_model/             # the model (Phase 5)
      robot.urdf             # the OPERATOR's URDF, built in the Studio editor (you read it)
      collision_spheres.yml  # cuRobo robot config (spheres + limits) — YOU fit this from robot.urdf
      spheres_preview.png    # desk visualization (no Isaac Sim)
    world/
      collision.*            # TSDF/ESDF / cuRobo collision world (env only — no arms)
      scene.json
      scan_report.md
    task_spec.md             # task + eval SPEC (operator-approved; NOT code)
    requirements.lock
    setup_state.json         # AUTHORITATIVE gate state machine — read it on --continue; keep it in lockstep
    setup_report.md          # the readiness card: G0–G8 status + evidence
```

When all gates are green and the operator signs off: `git add remoroo_cell/`,
commit once, and call `done(verdict="success")`. If blocked, call `done` with
a partial verdict and make sure `setup_report.md` lists exactly which gates
are red and why.
