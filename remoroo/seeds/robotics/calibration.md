# Calibration & system identification — Phase 5 → G5 (the SHIPPED engine)

Calibration makes the world geometry and the data trustworthy. It is the cell's
FIRST motion: run it under operator supervision (slow moves, hand on the E-stop),
AFTER the operator's URDF + spheres exist and the operator has set the safety
envelope (G0.5), and BEFORE the world scan.

> **You do NOT author calibration math, and there is NO `collect_poses.py`.** The
> calibration engine is **SHIPPED** with Remoroo (the `calib_engine` module that
> travels with the Studio edge). It is version-pinned and CI-tested; you import it
> via the Studio, never reauthor it — exactly like the safety supervisor and the
> transport spine. Your only cell-specific job is the **Bridge** (`primitives.py`)
> the engine calls, plus the one input the URDF can't give: the **printed board**.

> **FRESH every setup — do NOT scavenge.** Do not search the disk for old
> `calibration/` files to reuse or "save time"; a stale extrinsic is a safety
> hazard. The engine runs the routine for real each time. Get the operator's
> explicit go-ahead — `gate_checkpoint(gate=calibrate, default_proceed=false)` —
> before the first move; the operator clears the area and permits motion.

## What the shipped engine does (so you know what NOT to build)

`calib_engine` provides, version-robust and tested against a synthetic-data
harness with known ground truth:

- **Detection** — ArUco/ChArUco with an OpenCV-version fallback (no API guessing).
- **Plan derivation from the URDF** — one item per camera: a camera on a moving
  flange → **eye-in-hand**; a camera anchored to the world → **eye-to-hand** (its
  own observation model — static camera, board on the gripper); two arms each with a
  wrist camera → a **base-to-base** item (the transform between the arms' bases from
  a board both cams see, recovered once both eye-in-hand solves are accepted). Each
  item starts from the camera's **nominal** transform in the URDF and is *refined*,
  not guessed.
- **Observability-guided pose generation** — next-best pose by information gain
  (which DOF of `X` is still weakly observed), collision-free + in-envelope.
  ~20–40 *informative* poses, not thousands.
- **The solve = a reprojection-error BUNDLE** — closed-form `calibrateHandEye`
  (Tsai/Park) is only the initializer; the deliverable jointly fits
  `X + board world-pose + a small per-joint FK correction + board scale`, robust
  (Huber). This removes systematic FK/scale bias instead of averaging around it
  (the path off the ~1° floor that defeats pose-count-only approaches).
- **Honest metrics** — **held-out** reprojection (train/validate split), a
  **task-space tip-landing** prediction, per-DOF observability/covariance,
  multi-solver consensus spread, and the stereo `T_L_R` self-check. Accept is
  judged on *held-out + tip-landing*, **never** training residual.
- **Corner curation + sub-pixel re-snap + eye-nudge** — the operator can exclude
  bad images/corners and nudge the camera frame by eye; the optimizer re-snaps and
  every edit must improve the held-out error.
- **Optical-frame I/O** — reads each camera's nominal transform, writes the
  calibrated result back to an explicit **`*_optical_frame`** child link (left
  rectified for stereo), never the camera body center, with tracked provenance.

The Studio drives the supervised loop and renders the rig, the live-cam detection
overlay, the pose cloud, the observability meter, and the before→after camera
slide. **The operator clicks Start/Accept/Reject; the engine does the math.**

## YOUR job — the Bridge contract the engine calls

The engine talks to THIS cell only through `primitives.py`. Make it satisfy this
contract (verify it in the G2 bridge smoke before you checkpoint calibrate):

1. **`get_observation()`** returns an object exposing:
   - `joint_positions` — a `{joint_name: value}` map covering **every revolute
     joint in the URDF**. The engine reads them in **URDF joint order**; joint
     states are what let the bundle fit the FK correction. (Names must match the
     URDF `<joint name=…>`.)
   - an **RGB image** under one of `rgb` / `color` / `image` / `rgb_image` /
     `left` (the left rectified lens for a stereo cam).
   - **`intrinsics`** — `{fx, fy, cx, cy, width, height}` read from the **camera
     SDK** (factory intrinsics are trustworthy; do NOT hand-type a `K`):
     ```python
     # ZED (Stereolabs):
     ci = cam.get_camera_information().camera_configuration.calibration_parameters.left_cam
     intrinsics = {"fx": ci.fx, "fy": ci.fy, "cx": ci.cx, "cy": ci.cy,
                   "width": w, "height": h}
     # RealSense: intr = profile.as_video_stream_profile().intrinsics
     #            {"fx": intr.fx, "fy": intr.fy, "cx": intr.ppx, "cy": intr.ppy, ...}
     ```
     The edge reads these live at calibration time — **you only confirm they are
     populated.** (See `camera_capture.md`; this is the same observation you
     already build for capture.)
2. **A joint-move method** named one of `move_to_joints` / `move_joints` /
   `goto_joints` / `set_joint_positions` / `move_j`, taking a **joint-position
   vector in URDF order**, planning collision-free + slow within the operator's
   G0.5 envelope (cuRobo + the spheres). The engine probes these names; expose one.
3. **The E-stop hook** (already from G2). The first commanded move is the
   operator-gated **pre-flight motion check**: the engine nudges one joint, reads
   joints back, and confirms the arm moved by ~the commanded delta and the E-stop
   halts it — this catches a wrong/stub move API *before* a wasted calibration.

**Optional Bridge methods the engine USES IF PRESENT** (probed by name; absent → that
feature simply isn't offered, never an error):
- **`motion_probe() -> {"joint": int, "delta": float}`** — the RIG-SPECIFIC safe test
  motion for the pre-flight check, **authored by YOU** for this cell. Only you know what's
  safe to twitch on THIS rig (which joint, how far) — a 6-DOF arm, a 7-DOF arm, a humanoid
  limb, a gantry's prismatic axis, or a joint chosen to stay clear of the operator. If you
  don't author it, the engine falls back to a COMPUTED choice: the joint that moves the
  flange origin the least (smallest, safest excursion) — never a hardcoded "joint 0" or
  "the last joint". Author `motion_probe` whenever the computed default isn't the safest
  test motion for your cell.
- a **cartesian TCP move** — `move_tcp_to_world` / `move_tcp` / `move_to_point` /
  `move_cartesian` / `move_l` taking a base-frame point — enables the **physical
  tip-landing test** (drive the TCP to the predicted fiducial; the operator watches it
  land). Without it the tip metric is the software fiducial-consistency only.
- a **right-lens capture** — `get_observation().right` / `right_image` / `rgb_right` —
  enables the **stereo `T_L_R` self-check** (a ZED's second lens is a free sanity check).
- a **feasibility check** — `is_joint_pose_feasible(joints)->bool` (cuRobo) — lets
  suggested poses be **collision/in-envelope filtered before** the operator accepts.

**NOTHING about the engine is rig-specific.** It derives EVERYTHING from the model: the
plan + each camera's kinematic chain (revolute AND prismatic joints) from the URDF; pose
sampling within the URDF joint LIMITS, with each joint's range set by how much it tilts the
camera's view (computed, not by joint index); the safe test joint computed from FK; N-arm
base-to-base (one item per extra arm). No assumption of arm count, DOF, joint type, or "the
last 3 joints are a wrist". A 1-arm cell, 3 arms, or a humanoid all flow through the same
code — the only per-cell inputs are the Bridge (above) and the printed board.

**Rectified-only contract (load-bearing):** the solver is a pinhole model on the
camera's **rectified** left image + **rectified** intrinsics (what a ZED/RealSense
publishes). If the cell can only provide a RAW frame, also supply distortion coeffs in
`cell.yaml calibration.dist` ([k1,k2,p1,p2,k3]) so the edge undistorts the corners first.

That's the whole cell-specific surface. No solver, no pose generation, no
`cv2.calibrateHandEye` in your code.

## The one text input — the printed board (cell.yaml `calibration`)

The board is the only thing the URDF can't give. The operator enters it in the
**Studio board-params form** (read off the printed board); it lands in `cell.yaml`:

```yaml
calibration:
  board:
    dict: "DICT_5X5_1000"   # the ArUco dictionary printed on the board
    squares_x: 7            # ChArUco columns
    squares_y: 5            # ChArUco rows
    square_len: 0.030       # chessboard square side, METRES
    marker_len: 0.022       # ArUco marker side, METRES
  # intrinsics: read LIVE from the camera SDK (obs.intrinsics). The two below are an
  # OPTIONAL override only for a camera with no factory K — normally omit them:
  # K:  [fx, 0, cx,  0, fy, cy,  0, 0, 1]   # or {fx, fy, cx, cy}
  # wh: [1280, 720]
  # dist: [k1, k2, p1, p2, k3]   # OPTIONAL — only if the cell feeds a RAW (unrectified) frame
  # T_left_right: [...4x4...]    # OPTIONAL — SDK stereo baseline, enables the T_L_R self-check
  accept_heldout_px: 1.5    # accept gate: held-out reprojection (tunable)
  accept_tip_mm: 3.0        # accept gate: physical tip-landing (tunable)
```

Propose a sensible default board but **do NOT hardcode it** — confirm with the
operator what they actually printed. Wrong params fail loudly (the live detection
overlay shows the board not-seen before any motion), not silently.

## How the gate runs (you orchestrate + supervise; you do not drive the math)

1. Ensure the Bridge exposes joints + image + intrinsics + a joint-move method,
   and `calibration.board` is set. Confirm the G2 smoke passed.
2. `gate_checkpoint(gate=calibrate, default_proceed=false)`. The operator clears
   the area and permits motion.
3. The Studio runs the SHIPPED engine live over the edge: derive the plan from the
   URDF → pre-flight motion check → detect the board → observability-guided
   suggest/Accept/move/capture loop → solve the bundle → validate on **fresh
   held-out + tip-landing** → curate/nudge if needed → the operator **Accepts**.
4. You **supervise**: watch the held-out / observability / consensus the protocol
   reports. On a **PROBLEM** report (motion check fails → joint-move API wrong; no
   intrinsics → camera not reporting `obs.intrinsics`; board not seen → wrong
   `calibration.board`), **fix the Bridge or cell.yaml yourself** and **re-issue
   the same checkpoint**. Never tell the operator to edit code.
5. For **dual-arm**, calibrate each wrist camera eye-in-hand, then the plan's
   **base-to-base** item: place ONE board both cams can see and capture a few shared
   views — the engine recovers the transform between the arms' bases (matrix algebra,
   no extra solve) and writes `calibration/base_to_base.json`, which cuRobo uses
   alongside the operator's URDF layout (see `robot_model.md`).
6. **Time-sync / payload sysid** (camera↔arm latency, tool mass/COM) are recorded
   for the recorder + controller where the cell needs them — small, separate from
   the hand-eye solve.

## Outputs (written by the engine, consumed downstream)

```text
remoroo_cell/
  calibration/<camera>.json        # the CalibResult per camera (X, fk_offsets, board
                                   #   scale, metrics: held-out px, tip mm, observability,
                                   #   consensus, T_L_R; samples used)
  calibration/base_to_base.json    # dual-arm only: transform between the two arm bases
  robot_model/robot.urdf           # updated: calibrated *_optical_frame per camera,
                                   #   with provenance (sdk | measured | assumed)
```

The calibrated `*_optical_frame` transforms make the rig in 3D match reality and
feed the cuRobo model (`robot_model.md`), the world scan (`world_scan.md`), and
every recorded episode (`data_capture.md`). The accept evidence is the **held-out
reprojection + tip-landing**, surfaced in the Studio — not a training residual and
not a paragraph you write.
