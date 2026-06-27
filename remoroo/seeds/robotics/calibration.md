# Calibration & system identification — Phase 5 → G5 (the SHIPPED engine)

Calibration makes the world geometry and the data trustworthy. It is the cell's
FIRST motion: run it under operator supervision (slow moves, hand on the E-stop),
AFTER the operator's URDF + spheres exist and the operator has set the safety
envelope (G0.5), and BEFORE the world scan.

> **You do NOT author calibration math, and there is NO `collect_poses.py`.** The
> calibration engine is **SHIPPED** with Remoroo (the `calib_engine` module that
> travels with the Studio edge). It is version-pinned and CI-tested; you import it
> via the Studio, never reauthor it — exactly like the safety supervisor and the
> transport spine. Your cell-specific job is three authored things the engine
> **consumes**: the **Bridge** (`primitives.py`), the **calibration pipeline**
> (`calibration/pipeline.yaml` — the ordered steps you derive from the rig), and the
> **target(s)** it references (shipped detectors selected by an open spec, or a custom
> detector in `calibration/targets.py`). The engine never guesses the steps or names a
> fiducial — it executes what you authored.

> **FRESH every setup — do NOT scavenge.** Do not search the disk for old
> `calibration/` files to reuse or "save time"; a stale extrinsic is a safety
> hazard. The engine runs the routine for real each time. Get the operator's
> explicit go-ahead — `gate_checkpoint(gate=calibrate, default_proceed=false)` —
> before the first move; the operator clears the area and permits motion.

## What the shipped engine does (so you know what NOT to build)

`calib_engine` provides, version-robust and tested against a synthetic-data
harness with known ground truth:

- **A fiducial-target LIBRARY** — `single_aruco`, `charuco`, `apriltag`, `aruco_grid`,
  `checkerboard` (OpenCV-version-robust, no API guessing). The engine names no fiducial:
  it consumes a `Target` (named 3D points + a detector + `min_points`) built from your
  open `{type, params}` spec, or a custom detector you register.
- **A step-executor registry** keyed by KIND (`eye_in_hand` for a wrist camera,
  `eye_to_hand` for a fixed/world camera, `base_to_base` to tie arm bases together). The engine runs
  whatever steps your `pipeline.yaml` declares, in dependency order — it does NOT derive the
  plan from URDF link-name strings. Each step starts from the bound camera's **nominal** URDF
  transform and is *refined*, not guessed.
- **Cartesian LOOK-AT pose generation** — the camera is ORBITED on a hemisphere over the
  marker, every pose LOOKING AT it (so the marker is in frame BY CONSTRUCTION), the desired
  camera pose converted to joints by the engine's damped-least-squares IK; ranked by
  next-best-view information gain (which DOF of `X` is still weakly observed), collision-free +
  in-envelope. ~20–40 *informative* poses that always face the marker — not blind joint jitter.
- **The solve = a reprojection-error BUNDLE** — closed-form `calibrateHandEye`
  (Tsai/Park) is only the initializer; the deliverable jointly fits
  `X + board world-pose + a small per-joint FK correction + board scale`, robust
  (Huber). This removes systematic FK/scale bias instead of averaging around it
  (the path off the ~1° floor that defeats pose-count-only approaches).
- **Honest accept gates** — **held-out** reprojection (train/validate split); the
  **OBSERVABILITY gate** (per-DOF 1σ of `X` from the parameter covariance — every DOF must be
  pinned, so a low-rotation-diversity collection that leaves translation unobservable, the
  classic *2x-translation* failure, is REFUSED, not silently accepted); a **VISUAL verify**
  (the predicted marker overlay must TRACK the real marker across fresh poses); plus the
  task-space tip-landing, consensus spread, and the stereo `T_L_R` self-check. Accept is
  **never** the training residual.
- **Corner curation + sub-pixel re-snap + eye-nudge** — the operator can exclude
  bad images/corners and nudge the camera frame by eye; the optimizer re-snaps and
  every edit must improve the held-out error.
- **Optical-frame I/O** — reads each camera's nominal transform, writes the
  calibrated result back to an explicit **`*_optical_frame`** child link (left
  rectified for stereo), never the camera body center, with tracked provenance.

The Studio drives the supervised loop as a VISUAL process: the 3D arm MIRRORS the real robot
(the live joint mirror auto-enables during calibrate), the engine SEEDS the hand-eye from the
URDF nominal + the marker and draws the camera + frustum on the marker for the operator to
confirm + WIGGLE, collection renders the look-at pose cloud + the observability meter, and the
live cam shows the GREEN detected vs AMBER predicted corners (their drift is the trust signal)
plus the before→after camera slide. **The operator confirms a picture and clicks Accept; the
engine does the math.**

## YOUR job — the Bridge contract the engine calls

The engine talks to THIS cell only through `primitives.py`. Make it satisfy this
contract (verify it in the G2 bridge smoke before you checkpoint calibrate):

1. **`get_observation(camera=<id>)`** returns an object exposing, FOR THE NAMED CAMERA:
   - `joint_positions` — a `{joint_name: value}` map covering **every revolute
     joint in the URDF**. The engine reads the bound arm's joints in **URDF joint
     order**; joint states are what let the bundle fit the FK correction. (Names
     must match the URDF `<joint name=…>`.)
   - an **RGB image** under one of `rgb` / `color` / `image` / `rgb_image` / `left`.
     On a **multi-camera** cell this MUST be the named camera's frame — the engine
     calibrates each camera against its own image (key your `_cameras` dict by the
     camera's **URDF link name**, which is what the pipeline binds). Single-camera
     cells may ignore the `camera=` argument.
   - **`intrinsics`** — `{fx, fy, cx, cy, width, height}` for THAT camera, read from
     the **camera SDK** (factory intrinsics are trustworthy; do NOT hand-type a `K`):
     ```python
     # ZED (Stereolabs):
     ci = cam.get_camera_information().camera_configuration.calibration_parameters.left_cam
     intrinsics = {"fx": ci.fx, "fy": ci.fy, "cx": ci.cx, "cy": ci.cy,
                   "width": w, "height": h}
     # RealSense: intr = profile.as_video_stream_profile().intrinsics
     #            {"fx": intr.fx, "fy": intr.fy, "cx": intr.ppx, "cy": intr.ppy, ...}
     ```
     The edge reads these live, **per camera** — you only confirm they are populated.
     (May also be keyed `intrinsics = {camera_id: {...}}`.)
2. **A per-arm joint move** — `move_to_joints(joints, arm=<id>)` (or the multi-arm
   `move_joints(arm, joints)`), taking a **joint-position vector in URDF order** and
   driving **the named arm**, planning collision-free + slow within the operator's
   G0.5 envelope (cuRobo + the spheres). On a **multi-arm** cell the `arm=` argument
   is load-bearing SAFETY — the engine drives exactly the arm the step bound, never a
   shared default. A single-arm cell may ignore `arm=` (also accepts
   `goto_joints`/`set_joint_positions`/`move_j` taking just the vector).
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

**The engine makes NO rig assumption — it executes what you authored.** It walks each
bound camera's kinematic chain (revolute AND prismatic joints, any count) from the URDF;
samples poses within the URDF joint LIMITS with each joint's range set by how much it tilts
the camera's view (computed, not by joint index); computes the safe test joint from FK; and
runs the steps your pipeline declared in dependency order. No assumption of arm count, DOF,
joint type, fiducial, or "the last 3 joints are a wrist". A 1-arm cell, 3 arms, or a
humanoid all flow through the same code — the per-cell inputs are the Bridge, the
**pipeline**, and the **target(s)**.

**Rectified-only contract (load-bearing):** the solver is a pinhole model on the
camera's **rectified** left image + **rectified** intrinsics (what a ZED/RealSense
publishes). If the cell can only provide a RAW frame, also supply distortion coeffs in
`cell.yaml calibration.dist` ([k1,k2,p1,p2,k3]) so the edge undistorts the corners first.

## YOUR job — author the pipeline + the target(s)

You derive the calibration STEPS from the rig you modeled (gates 1–5) and author them
declaratively in `remoroo_cell/calibration/pipeline.yaml`. The engine validates it against
the registered kinds + the cell's cameras/arms and **executes it** — it never guesses the
steps. A 2-arm / 2-cam rig is three steps; a 1-arm / 1-cam rig is one; a humanoid is however
many that rig needs.

```yaml
# remoroo_cell/calibration/pipeline.yaml — authored from the rig
schema: 1
targets:                       # named targets the steps bind; OPEN spec, no fixed fiducial
  big_tag:  { type: single_aruco, params: { dict: DICT_4X4_50, id: 7, size_m: 0.075 } }
  # board:  { type: charuco, params: { dict: DICT_5X5_1000, squares_x: 7, squares_y: 5,
  #                                    square_len: 0.03, marker_len: 0.022 } }
steps:                         # ordered; `depends_on` gates execution + the Studio rail
  - { id: a1c1, kind: eye_in_hand, arm: arm_1, camera: cam_1, target: big_tag }
  - { id: a2c2, kind: eye_in_hand, arm: arm_2, camera: cam_2, target: big_tag }
  - { id: b2b,  kind: base_to_base, arms: [arm_1, arm_2], cameras: [cam_1, cam_2],
      target: big_tag, depends_on: [a1c1, a2c2] }
```

`camera`/`cameras` are the **URDF camera-link names** (what the Bridge keys on and the
engine walks the chain to). `arm`/`arms` are the cell.yaml arm names. `kind`:

- **`eye_in_hand`** — a camera on a **moving flange** (a wrist cam). The arm moves the camera
  over a fixed board; the engine orbits the camera look-at and solves `camera→flange`.
- **`eye_to_hand`** — a **FIXED / world camera** (overhead, on a post, a bench cam) calibrated
  to the **robot base**. The camera can't move, so the **arm presents the board to it**: set
  **`board_source: arm`** and bind the **presenting arm** (`arm:`); the operator mounts the board
  on that arm's **end-effector** (gripper / hand / tool mount) and **hand-moves the arm** to show
  the board to the camera from many
  angles & distances (no look-at orbit — the camera is fixed). Solves **`base→camera`**, the
  transform every downstream consumer needs.
- **`base_to_base`** — relates **two** arms' bases via a marker both their wrist cams see (gated
  on those two `eye_in_hand` steps). A rig with **N** arms needs **N−1** of these to span every
  base (2 arms → 1, 3 arms → 2, a humanoid's limbs → one per extra limb) — never assume two.

> **A fixed/world camera ALWAYS uses `eye_to_hand` with `board_source: arm` + a presenting arm.**
> Do **NOT** use `kind: static`, and do **NOT** author `eye_to_hand` without `board_source: arm`:
> those only LOCALIZE the camera against a hand-placed board (camera↔board), they never relate it
> to the robot base, and Remoroo has **no use** for that. If a fixed camera has no arm that can
> present a board into its view, raise it as a `gate_checkpoint` PROBLEM — don't fall back to
> board-localization.

```yaml
# a single arm + a FIXED overhead camera: arm_1 presents the board to cam_oh → base→camera
steps:
  - { id: a1c1, kind: eye_in_hand, arm: arm_1, camera: cam_1,  target: big_tag }
  - { id: oh,   kind: eye_to_hand, arm: arm_1, camera: cam_oh, target: big_tag,
      board_source: arm }
```

**Targets — shipped library, no fiducial named in your code.** Most rigs need nothing more
than the `{type, params}` spec above (`single_aruco`, `charuco`, `apriltag`, `aruco_grid`,
`checkerboard`). The operator confirms it in the **Studio target form** (read off the printed
target) → it lands in `cell.yaml calibration.target` and the live overlay confirms detection
before any motion. For an EXOTIC printed target, author a custom detector:

```python
# remoroo_cell/calibration/targets.py — only for a target the shipped library can't do
import numpy as np
from calib_engine.fiducials import register, Target

class MyDetector:
    def detect(self, image): ...        # -> (point_ids[N], uv[N,2]) aligned to point_xyz
def _build(**params):
    return Target(point_xyz=np.array([...]), detector=MyDetector(),
                  min_points=4, type="my_target")
register("my_target", _build)           # now usable as a pipeline target type
```

```yaml
# cell.yaml calibration — accept gates + optional overrides
calibration:
  target: { type: single_aruco, params: { dict: DICT_4X4_50, id: 7, size_m: 0.075 } }
  accept_heldout_px: 1.5    # accept gate: held-out reprojection (tunable)
  accept_tip_mm: 3.0        # accept gate: physical tip-landing (tunable)
  accept_rot_sigma_deg: 0.5 # accept gate: observability — worst per-DOF rotation 1σ of X (deg)
  accept_trans_sigma_mm: 2.0 # accept gate: observability — worst per-DOF translation 1σ of X (mm)
  # K / wh: OPTIONAL override only for a camera with no factory K (normally omit — read live)
  # dist: [k1, k2, p1, p2, k3]   # OPTIONAL — only if the cell feeds a RAW (unrectified) frame
  # T_left_right: [...4x4...]    # OPTIONAL — SDK stereo baseline, enables the T_L_R self-check
```

Propose a sensible target but **do NOT hardcode it** — confirm with the
operator what they actually printed. Wrong params fail loudly (the live detection
overlay shows the board not-seen before any motion), not silently.

## How the gate runs (you orchestrate + supervise; you do not drive the math)

1. Author `calibration/pipeline.yaml` + the target(s); ensure the Bridge exposes
   per-camera image + intrinsics + a per-arm joint move. Confirm the G2 smoke passed.
2. `gate_checkpoint(gate=calibrate, default_proceed=false)`. The operator clears
   the area and permits motion.
3. The Studio runs the SHIPPED engine live over the edge: **execute your authored
   pipeline** step by step in dependency order → per step: pre-flight motion check →
   detect the target → **SEED** the hand-eye + the operator confirms it visually (frustum on
   the marker) and **WIGGLES** to confirm tracking → look-at suggest/Accept/move/capture loop →
   solve the bundle → validate on **fresh held-out** + the **observability gate** + a **visual
   verify** (the overlay tracks the marker) → the operator **Accepts**.
4. You **supervise**: watch the held-out / observability / consensus the protocol
   reports. On a **PROBLEM** report (motion check fails → per-arm joint-move wrong; no
   intrinsics → camera not reporting `obs.intrinsics`; target not seen → wrong
   `calibration.target` or pipeline binding), **fix the Bridge / pipeline / target
   yourself** and **re-issue the same checkpoint**. Never tell the operator to edit code.
5. For **multiple arms**, the eye-in-hand steps run first, then **each** `base_to_base`
   step (each gated on its two eye-in-hand steps — a rig with N arms has N−1 of them): place
   ONE target both of that step's cams see and capture a few shared views — the engine recovers
   the transform between those two bases (matrix algebra, no extra solve) and accumulates them in
   `calibration/base_to_base.json`, which cuRobo uses alongside the operator's URDF layout
   (see `robot_model.md`).
6. **Time-sync / payload sysid** (camera↔arm latency, tool mass/COM) are recorded
   for the recorder + controller where the cell needs them — small, separate from
   the hand-eye solve.

## Outputs (written by the engine, consumed downstream)

```text
remoroo_cell/
  calibration/<camera>.json        # the CalibResult per camera (X, fk_offsets, board
                                   #   scale, metrics: held-out px, tip mm, observability,
                                   #   consensus, T_L_R; samples used)
  calibration/base_to_base.json    # multi-arm: transform(s) between arm bases (N arms → N−1)
  robot_model/robot.urdf           # updated: calibrated *_optical_frame per camera,
                                   #   with provenance (sdk | measured | assumed)
```

The calibrated `*_optical_frame` transforms make the rig in 3D match reality and
feed the cuRobo model (`robot_model.md`), the world scan (`world_scan.md`), and
every recorded episode (`data_capture.md`). The accept evidence is the **held-out
reprojection + the observability gate + the visual verify (the overlay tracks the marker)**,
surfaced in the Studio — not a training residual and not a paragraph you write.
