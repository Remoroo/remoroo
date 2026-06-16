# Calibration & system identification — `remoroo_cell/calibration/` (Phase 5 → G5)

Calibration makes the world geometry and the data trustworthy. It is the
cell's FIRST motion: run it under operator supervision (slow moves, hand on the
E-stop), AFTER the operator's URDF + spheres exist and the operator has set the
safety envelope (G0.5), and BEFORE the world scan. **Collect poses AUTONOMOUSLY
(see the next section) — never hand-jog them.** Outputs are **versioned +
editable** and must be **re-runnable**.

> **FRESH every setup — do NOT scavenge.** Do not search the disk for old
> `calibration/` files to reuse or "save time"; a stale extrinsic is a safety
> hazard. Run the routine for real each time. (Resuming a prior setup is driven
> by the gate state, not by you reading old artifacts.) And get the operator's
> explicit go-ahead — `gate_checkpoint(gate=calibrate, default_proceed=false)` —
> before the first move; the operator clears the area and permits motion.

## What to calibrate (only what this cell needs)

1. **Camera intrinsics** — skip if factory intrinsics are trustworthy
   (RealSense/ZED usually are). Otherwise calibrate from a target.
2. **Hand-eye extrinsics** — the core one:
   - *eye-in-hand* (wrist cam): solve camera↔gripper `X` from `AX = XB`.
   - *eye-to-hand* (static cam): solve camera↔base.
   - both, if the cell has both.
3. **Dual-arm base-to-base** — if two arms, the transform between their bases.
   Once BOTH arms' eye-in-hand is solved, recover it from a **shared ArUco**
   both wrist cams observe — simple matrix transforms, no extra solver. You
   COMPUTE this transform; the operator laid the two arms out approximately in
   the editor, and cuRobo uses your precise `base_to_base` alongside the
   operator's URDF (see `robot_model.md`).
4. **Time-sync / latency** — camera↔arm offset; record it for the recorder.
5. **Payload / tool sysid** — mass + COM of the mounted tool/gripper so the
   controller and limits are honest.

Hand-eye + base-to-base are *inputs* to the cuRobo model. You do NOT build the
URDF (the operator does, in the editor); you sphere-fit their URDF and fold in
these transforms — see `robot_model.md`.

## Automated pose collection — REQUIRED (never hand-jog ~100 poses)

Hand-jogging poses defeats the purpose and does not scale. Two facts drive the
design:

- **You do NOT need ~100 poses.** Hand-eye accuracy *plateaus* around 12-20
  *well-chosen* poses (MoveIt's calibrator plateaus ~12-15); 100 mediocre poses
  are worse than 15 diverse ones. Rotation-axis DIVERSITY beats raw count.
- **Remoroo can move itself.** You already have cuRobo (collision-free
  planning), a coarse world, the robot URDF, and a depth/stereo wrist cam — so
  generate poses, plan to them, capture, and solve hands-off.

Author `calibration/collect_poses.py` running this autonomous loop. The
operator's ONLY job is to place the board once and supervise with the E-stop.

**This needs only SUPERVISED motion — not the scanned world or full autonomy.**
cuRobo plans self-collision- and joint-limit-safe moves from the robot URDF
alone; for external obstacles rely on a conservative workspace box (table plane
+ bounds from `cell.yaml`) plus the operator-cleared area, slow speed, and the
E-stop — NOT a TSDF scan (the scan comes AFTER calibration and depends on its
extrinsics). To AIM the camera you only need a ROUGH initial hand-eye guess:
take it from the mount CAD (the camera is bolted to the flange at a roughly
known offset) or from 2-3 hand-guided board-visible poses; it self-refines once
the first solve lands. You do NOT need a prior accurate calibration to generate
poses, and you do NOT need the world scan to move safely here.

1. **Place the target once.** Operator drops a ChArUco / AprilGrid board flat in
   the workspace (its location/orientation need NOT be measured — multi-pose
   hand-eye cancels it). Take one look to detect it and get an initial
   `T_cam_target`. No board available -> see the markerless option below.
2. **Generate observability-optimized candidates (target-centric).** Sample
   camera viewpoints on a hemisphere/cone around the board: vary AZIMUTH +
   ELEVATION about the board normal (~±30-45 deg), vary STANDOFF (e.g. 0.3-0.6 m
   so the board fills a good fraction of the frame), vary IN-PLANE ROLL, and
   above all **maximize rotation-AXIS diversity between poses** — clustered
   orientations make `AX=XB` near-singular (Tsai-Lenz). Require the WHOLE board
   to project inside the FOV at a non-degenerate angle (project board corners
   through the camera model; reject out-of-frame / too oblique).
3. **Filter reachable + collision-free with cuRobo.** Convert each candidate
   camera pose to the required EE pose via the current hand-eye guess; run cuRobo
   IK + SELF-collision + joint-limit check against the robot model plus a
   conservative workspace box (table plane + bounds from `cell.yaml`) — NOT a
   TSDF scan (it does not exist yet). Keep only the reachable, in-limits,
   collision-free ones.
4. **Execute + capture, hands-off.** Per kept pose: cuRobo plans a collision-free
   trajectory, move at safety-rated slow speed, settle, capture synchronized L+R
   frames, detect the board (solvePnP). Record (`T_base_ee` from FK,
   `T_cam_target` from detection). The FIRST move is the operator-gated E-stop
   check (Phase 5 intro).
5. **Auto-reject bad samples** in the loop: too few detected corners, high
   per-corner reprojection error, board too oblique, or motion not settled — drop
   them before they reach the solve.
6. **Active stopping (don't over-collect).** After ~6-8 poses, solve hand-eye and
   estimate parameter covariance/condition. Then pick the NEXT pose by
   information gain (next-best-view: the candidate that most reduces parameter
   uncertainty; arXiv 2303.06766) or keep drawing from the diverse set, and STOP
   when residual (`trans_std_mm`, `rot_std_deg`) and uncertainty PLATEAU —
   typically 12-20 poses, a few minutes, fully autonomous. Report how many you
   actually needed, not a fixed 100.
7. **Validate** (stereo `T_L_R` agreement + held-out reprojection, below) BEFORE
   writing `hand_eye.yaml`.

### Markerless option (no board) — when the URDF + a GPU are available
If a board is impractical, calibrate against the ROBOT ITSELF: segment the arm
in the wrist images with a pretrained model (e.g. SAM), render the known
URDF/mesh via differentiable rendering, and optimize camera<->EE so the rendered
arm matches the masks across a handful of auto-moved poses (EasyHeC++,
arXiv 2410.09293 — fully automatic, markerless, no per-arm training). Needs the
robot mesh (you have it from setup) + a GPU. Default to the marker pipeline;
use this as the no-board path. Open-source automation refs: `easy_handeye2`
(auto-move + capture via MoveIt) and MoveIt Calibration.

## Hand-eye (eye-in-hand) pattern — OpenCV

```python
import cv2, numpy as np

# Collect N (>=12) poses: move to AUTO-GENERATED, cuRobo-filtered poses (~12-20, NOT 100),
# at each capture (a) the gripper pose in base frame from the arm, and
# (b) the calibration target pose in the camera frame.
R_g2b, t_g2b = [], []   # gripper -> base  (from arm FK)
R_t2c, t_t2c = [], []   # target  -> camera (from cv2.solvePnP on the board)

for pose in auto_generated_poses:        # target-centric + cuRobo-filtered (see above)
    bridge.move_joints(arm, pose, speed_frac=cell["safety"]["max_joint_speed_frac"])
    obs = bridge.get_observation()
    # (a) arm FK -> gripper pose in base
    Rg, tg = gripper_pose_in_base(obs)      # from read_eef_pose / FK
    # (b) detect board -> target pose in camera
    ok, rvec, tvec = detect_board_pose(obs.rgb, obs.intrinsics)  # solvePnP
    if not ok:
        continue
    R_g2b.append(Rg); t_g2b.append(tg)
    R_t2c.append(cv2.Rodrigues(rvec)[0]); t_t2c.append(tvec)

R_cam2gripper, t_cam2gripper = cv2.calibrateHandEye(
    R_g2b, t_g2b, R_t2c, t_t2c, method=cv2.CALIB_HAND_EYE_TSAI,
)
```

For eye-to-hand, calibrate camera↔base with the same data arranged for that
configuration (`cv2.calibrateHandEye` with base↔gripper inverted, or
`calibrateRobotWorldHandEye`).

## Acceptance (G5) — thresholds are TUNABLE (R&D), start conservative

- Hand-eye **reprojection / residual** below threshold (e.g. start ~ a few mm /
  ~1°); record the actual number — do not just assert "passed".
- Reprojection error consistent across held-out poses (no overfit).
- Time-sync offset measured and stable.
- Payload sysid mass within a sane band of the known tool mass.

These numbers are starting points; tune per cell and record the chosen values.
What matters is that the residual is **measured, reported, and good enough for
safe motion**, and the procedure can be re-run.

## `remoroo_cell/calibration/` outputs

```text
calibration/
  intrinsics.yaml        # if calibrated (else note "factory")
  hand_eye.yaml          # X transforms (eye-in-hand / eye-to-hand)
  base_to_base.yaml      # dual-arm only
  time_sync.yaml         # camera<->arm latency (s)
  payload.yaml           # tool mass + COM
  report.md              # residuals, pose count, plots/screenshots, verdict
  recompute.py           # re-runs the solve from saved correspondences
```

`report.md` is the G5 evidence: pose count, residual numbers, what passed/
failed, and the thresholds used. The hand-eye + base transforms feed the
robot model (`robot_model.md`) and world scan (`world_scan.md`) and get
embedded in every episode (`data_capture.md`).


## Stereo (eye-in-hand) hand-eye — calibrate BOTH lenses and validate

A stereo wrist camera (e.g. ZED) has two lenses (left + right). Do not calibrate
only one lens:

- Solve hand-eye for BOTH lenses against the SAME target across the SAME pose
  set, producing `T_cam_gripper` for the left lens and for the right lens.
- VALIDATE against the SDK-provided stereo baseline. Most stereo SDKs expose the
  factory left->right extrinsic (Stereolabs gives `T_L_R`). The two
  independently-solved hand-eye results must agree with it:
  `inv(T_L_gripper) @ T_R_gripper` should equal the SDK `T_L_R` within
  tolerance. If they disagree beyond threshold the calibration is bad — reject
  it and recollect; do NOT write it to `hand_eye.yaml`.
- Use MANY, DIVERSE poses (wide rotation + translation coverage), not the
  minimum — more, varied poses condition the solve far better.
- REJECT samples with high per-pose reprojection error before solving (robust
  outlier rejection). Record pose count, `trans_std_mm`, `rot_std_deg`, the
  SDK-baseline agreement, and reprojection RMSE in `report.md`; recollect if any
  is above target.
