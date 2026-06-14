# Calibration & system identification — `remoroo_cell/calibration/` (Phase 5 → G5)

Calibration makes the world geometry and the data trustworthy. Do it under
operator supervision (it involves slow moves), after G3/G4. Outputs are
**versioned + editable** and must be **re-runnable**.

## What to calibrate (only what this cell needs)

1. **Camera intrinsics** — skip if factory intrinsics are trustworthy
   (RealSense/ZED usually are). Otherwise calibrate from a target.
2. **Hand-eye extrinsics** — the core one:
   - *eye-in-hand* (wrist cam): solve camera↔gripper `X` from `AX = XB`.
   - *eye-to-hand* (static cam): solve camera↔base.
   - both, if the cell has both.
3. **Dual-arm base-to-base** — if two arms, the transform between their bases.
   Once BOTH arms' eye-in-hand is solved, recover it from a **shared ArUco**
   both wrist cams observe — simple matrix transforms, no extra solver. Full
   method + the combined-URDF build live in `robot_model.md` (it feeds the
   model, so it's covered there).
4. **Time-sync / latency** — camera↔arm offset; record it for the recorder.
5. **Payload / tool sysid** — mass + COM of the mounted tool/gripper so the
   controller and limits are honest.

Hand-eye + base-to-base are the *inputs* to the cuRobo-ready robot model
(combined URDF + collision spheres). Build that next in `robot_model.md`.

## Hand-eye (eye-in-hand) pattern — OpenCV

```python
import cv2, numpy as np

# Collect N (>=12) poses: move to varied, SAFE joint configs (G3 envelope),
# at each capture (a) the gripper pose in base frame from the arm, and
# (b) the calibration target pose in the camera frame.
R_g2b, t_g2b = [], []   # gripper -> base  (from arm FK)
R_t2c, t_t2c = [], []   # target  -> camera (from cv2.solvePnP on the board)

for pose in safe_calibration_poses:        # author these within the G3 bounds
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
