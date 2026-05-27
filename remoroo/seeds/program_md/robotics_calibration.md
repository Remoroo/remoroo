# Eye-in-Hand Calibration — Autoresearch

Autonomous loop to minimise eye-in-hand hand-eye calibration error in a MuJoCo
sim. The simulator owns ground truth `X_gt`; the agent never reads it except
post-hoc for cross-checking.

## Single entry point

```
python run.py
```

Pipeline: load scene → collect **calib** poses (agent) + **val** poses
(locked) → solve hand-eye on calib → measure consistency on val → render
pick-task video for best solver.

## Headline metric — `trans_std_mm` (on LOCKED val poses)

Fixed-target self-consistency on the validation pose set:

    T_base_target_pred_i = T_base_gripper_i · X_est · T_cam_target_i

If `X_est` is right, all `i` agree on one world point. Std of their
positions (mm) is the headline. Lower = better.

**Two independent pose sets:**

- **Calibration (agent).** `n_poses`, `distance`, `sampler`, `seed` passed
  to `run.main()`. Used only to solve `X_est`.
- **Validation (LOCKED — `VAL_*` at top of `run.py`).** `diverse` sampler,
  `distance=1.0 m`, `n_poses=30`, `seed=20260416`. Deliberately harder
  than typical calib sets so the agent can't game by picking easy-for-me
  poses. Editing `VAL_*` invalidates cross-commit comparisons.

Cross-checks (never inform the solve): `trans_err_vs_gt_mm`, `rot_err_vs_gt_deg` against `X_gt`.

## Repo layout

| File | Locked? | Purpose |
|---|---|---|
| `run.py` | `VAL_*` + sensor/metrics rules | entry point; solvers, consistency, CSV |
| `collector.py` | top half (sensor pipeline, frame math, `X_gt`) | MuJoCo scene + render→detect→PnP + `collect_scenario` |
| `samplers.py` | no | pose-sampling strategies |
| `render_reach_video.py` | no | pick-task video for best solver |
| `metrics.py` | yes | `pose_error_deg_mm` |
| `scene.xml` + `assets/` | yes | physical world (camera, marker, `X_gt`) |

Outputs: `artifacts/` (CSVs, per-pose PNGs for calib+val), `video/<commit>_<solver>.mp4`, `results.tsv` (append-only headline history).

## What the agent is allowed to touch

Editable: `run.py` (solvers, ensembling, refinement, outlier rejection),
`samplers.py` (new strategies), `collect_scenario` in `collector.py` (adaptive
sampling, retries, filtering), and the calib knobs passed to `main()`.

Off-limits: sensor pipeline half of `collector.py`, `metrics.py`, `scene.xml`,
`assets/`, and the `VAL_*` constants in `run.py`. Touching any of these
changes the benchmark and invalidates comparisons.

## Known failure modes to attack

1. **Planar-PnP ambiguity.** `SOLVEPNP_IPPE_SQUARE` returns one of two
   solutions; choice can flip across views, inflating `rot_std_deg` even
   when `X_est` is near GT. Switch to `SOLVEPNP_IPPE` + disambiguate.
2. **Poor pose diversity** (near-collinear/planar): classical `AX=XB`
   ill-conditioning.
3. **Detector noise at distance/steep angle**: physical floor on every
   metric. Fight with more poses + refinement, never by retuning noise.

## Loop (every experiment, no exceptions)

1. **Commit first.** `git add -A && git commit -m "<hypothesis>"`. Every
   experiment is identified by its commit. No commit, no experiment.
2. **Run.** `python run.py`.
3. **Log.** Append ONE line to `results.tsv` — always, regardless of outcome
   (improvement, regression, crash, inconclusive): `commit  best_solver
   trans_std_mm  rot_std_deg  trans_err_vs_gt_mm  rot_err_vs_gt_deg  status
   description`. `status ∈ {keep, regress, neutral, crash}`. Missing rows are
   bugs; nothing gets discarded silently. Do not ignore this instruction!
