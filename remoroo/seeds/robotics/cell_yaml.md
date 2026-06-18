# `cell.yaml` — the canonical cell description (Phase 0 → G0)

`remoroo_cell/cell.yaml` is the single source of truth for this cell's
hardware: the **RobotModel** that the Bridge, calibration, world scan, and
cuRoboV2 all read. It is **operator-pre-fillable** — if the operator dropped a
partial `cell.yaml`, read it first and only fill the gaps.

Author it incrementally: write what you discover in Phase 0, then let later
phases fill their sections (calibration residuals, workspace bounds, capture
schema path, etc.). It is **versioned and editable** — never mark it locked.

## G0 exit criteria

- Every hardware field below is filled or explicitly marked `unknown` with a
  `notes:` reason (never silently omit).
- `sensing.feasible: true` with a justification, **or** STOP: a monocular-only
  cell (no reliable metric depth) is rejected (see `README.md` sensor rule).
- Any unsupported/unknown component is flagged, not assumed.

## Annotated template (adapt every value to THIS cell)

```yaml
schema_version: 1
meta:
  cell_id: "site-a-cell-01"          # stable id for this physical cell
  operator_contact: "unknown"        # who to ask; fill via ask_human if needed
  created_by: "remoroo @robot_setup"

host:                                 # discovered via hardware_preflight/bash
  os: "Ubuntu 22.04"
  gpu: "NVIDIA RTX 4090"
  cuda: "12.4"
  driver: "550.x"
  ram_gb: 64
  disk_free_gb: 800
  python: "3.10"

arms:                                 # one entry per arm (dual-arm => two)
  - name: "right"
    make: "uFactory"
    model: "xArm6"
    dof: 6
    urdf: "library://xarm6"           # fetched from library; never reconstructed
    connection:
      kind: "tcp"                     # tcp | ros | usb | vendor_sdk
      host: "192.168.1.213"           # ask_human if not discoverable
      port: 502
    payload_kg: 2.0
    notes: ""

gripper:
  make: "Robotiq"
  model: "2F-85"
  kind: "parallel"                    # parallel | vacuum | custom
  connection: { kind: "tcp", host: "192.168.1.213" }
  cad: "unknown"                      # path to gripper CAD if available

robot_model:                          # the cuRobo-ready model (assembled in Phase 5)
  provided_full_urdf: null            # path if the customer ships a WHOLE-robot URDF
                                      #   (then base-to-base + URDF combine are SKIPPED)
  combined_urdf: null                 # else built here: remoroo_cell/robot_model/robot.urdf
  collision_spheres: null             # cuRobo YAML: remoroo_cell/robot_model/collision_spheres.yml
  base_to_base: null                  # dual-arm: calibration/base_to_base.yaml (shared-ArUco)

cameras:                              # one entry per camera
  - name: "wrist"
    make: "Intel"
    model: "RealSense D435i"
    mount: "eye_in_hand"             # eye_in_hand (wrist) | static | eye_to_hand
    attached_to: "right"             # arm name if eye_in_hand
    rgb: true
    depth: true                       # MUST be true for at least enough coverage
    depth_kind: "stereo_ir"           # stereo_ir | tof | structured_light | none
    resolution: [1280, 720]
    intrinsics: "factory"             # factory | calibrate | path/to/intrinsics
    serial: "unknown"

sensing:                              # the G0 feasibility judgement
  feasible: true
  reason: >
    Wrist D435i provides reliable metric depth and can be moved to cover the
    workspace; sufficient to ground a safe collision world.
  rejected_if: "monocular-only / no reliable metric depth"

network:
  controller_reachable: true
  notes: ""

workspace:                            # filled/confirmed in Phase 3
  bounds_m:                           # axis-aligned safe box in robot base frame
    min: [-0.4, -0.6, 0.0]
    max: [0.6, 0.6, 0.8]
  keep_out:                           # list of boxes the arm must never enter
    - { min: [-0.1, -0.7, 0.0], max: [0.1, -0.5, 0.4], note: "operator stands here" }
  home_pose_joints_rad: null          # set once a known-safe home is established

safety:                               # ALL tunable knobs (R&D) — not frozen
  max_cartesian_speed_mps: 0.10       # safety-rated monitored speed for setup
  max_joint_speed_frac: 0.10          # fraction of rated joint speed
  estop:
    kind: "hardware"                  # hardware | software_only(reject if only this)
    verified: false                   # set true only after live E-stop test (G3)
  hand_guiding:
    supported: "unknown"              # for the manual scan fallback (Phase 4)

capture:                              # the data-capture target (Phase 2/6)
  modalities: ["rgb", "depth", "joint_states", "gripper", "tcp_pose"]
  rate_hz: 30
  format: "remoroo_episode_v1"        # the shipped EpisodeWriter schema
  schema_path: "remoroo_cell/capture/schema.json"
  est_gb_per_hour: 40

calibration:                          # Phase 5/G5 — input to the SHIPPED calib engine
  target:                             # the printed target — an OPEN spec (NOT ChArUco-only).
                                      # `type` selects a shipped detector (single_aruco |
                                      # charuco | apriltag | aruco_grid | checkerboard) or a
                                      # custom one in calibration/targets.py. The values are a
                                      # PLACEHOLDER EXAMPLE — confirm the real type/size with
                                      # the operator (ask_human) or the Studio target form.
    type: "single_aruco"              # EXAMPLE — what the operator actually printed
    params: { dict: "DICT_4X4_50", id: 7, size_m: 0.075 }
  # The STEPS are authored separately in calibration/pipeline.yaml (which targets each step
  # binds, in dependency order). cell.yaml.target is the single default the Studio form edits.
  accept_heldout_px: 1.5              # accept gate: held-out reprojection (tunable)
  accept_tip_mm: 3.0                  # accept gate: physical tip-landing (tunable)
  # Intrinsics are read LIVE from the camera SDK (obs.intrinsics) — NOT set here.
  # K/wh below are an OPTIONAL override for a camera with no factory K (normally omit):
  # K: [fx, 0, cx, 0, fy, cy, 0, 0, 1]
  # wh: [1280, 720]
  # dist: [k1, k2, p1, p2, k3]        # OPTIONAL — only if the cell feeds a RAW frame
  # T_left_right: [...]               # OPTIONAL — SDK stereo baseline (enables T_L_R self-check)

gates:                                # the readiness card mirrors setup_report.md
  G0: pending
  G1: pending
  G2: pending
  G3: pending
  G4: pending
  G5: pending
  G6: pending
  G7: pending
  G8: pending
```

## Notes

- **Dual-arm:** add a second `arms:` entry. If each arm has its own URDF, the
  combined model is built from a shared-ArUco **base-to-base** transform (both
  wrist cams view one marker, after each arm's eye-in-hand) — see
  `robot_model.md`. Skip combining if the customer ships a whole-robot URDF
  (set `robot_model.provided_full_urdf`).
- **URDF is fetched from the library, never reconstructed.** The cuRobo-ready
  planning model (one resolved URDF + collision spheres) is assembled locally
  in Phase 5 (`robot_model.md`); record where it lands under `robot_model:`.
- **`software_only` E-stop** is a red flag — prefer a hardware E-stop; if only
  software is available, raise it with the operator (`ask_human`) before any
  motion and record the decision.
- Keep values honest: `unknown` is acceptable in Phase 0, but every `unknown`
  on the safety/sensing path must be resolved before its gate.
