# World scan — `remoroo_cell/world/` (Phase 4 → G4)

Build a **geometric world** good enough to plan small **collision-free
autonomous moves**, and a **queryable scene**. The gate is *functional*: not a
sweep count, not a resolution number — *can we plan safe autonomous motion
against this world?* Take **as many sweeps as it takes**.

Do this after G3 (you may move, slowly, supervised). The operator watches the
live reconstruction; show frames/clips with `view_image` / `view_video`.

## Coverage methods — pick what fits this cell (combine freely)

1. **Static-depth fusion** — if there's a static/eye-to-hand depth camera,
   fuse its frames into a TSDF over the workspace box (`cell.yaml: workspace`).
2. **Eye-in-hand active sweep** (powerful, preferred for wrist cams) — drive
   the arm through a set of **safe** viewpoints (within the G3 envelope) and
   fuse wrist-cam depth using the hand-eye extrinsic. Plan each hop
   collision-free against the partial world so far (bootstrapping).
3. **Operator hand-guided scan** (fallback / fast coverage) — if autonomous
   safe sweeps aren't available yet, `ask_human` to enable hand-guiding (only
   if `cell.yaml: safety.hand_guiding.supported`), and have the operator walk
   the wrist cam around the cell while you fuse. The human is the safety
   guarantee here. If hand-guiding is unsupported, fall back to (1)/(2) or
   stop with what's blocking.

## Build the collision world (cuRoboV2)

```python
# Fuse depth -> TSDF/ESDF, then hand cuRobo a collision world it can plan
# against. Shapes follow your installed cuRobo version; validate at G1.
import numpy as np

def fuse_world(frames, workspace_bounds, voxel_m=0.01):
    """frames: list of (depth_m, intrinsics, cam_pose_in_base). Returns a
    TSDF/ESDF volume clipped to the workspace box."""
    tsdf = TSDFVolume(workspace_bounds, voxel_m)          # nvblox / open3d / cuRobo
    for depth_m, intr, cam_T_base in frames:
        tsdf.integrate(depth_m, intr, cam_T_base)
    return tsdf

# Export to cuRobo's collision world (mesh / voxel / blox), then smoke-plan:
world = tsdf_to_curobo_world(tsdf)                         # adapt to cuRobo API
plan = planner.plan(arm="right", target_pose=safe_pose, world=world)
assert plan is not None, "world not plannable yet — add coverage"
```

## Queryable scene — `remoroo_cell/world/scene.json`

A compact, LLM-readable summary the agent and recorder can query (the geometry
serialization detail lives in `ADC/remoroo_scene_representation_to_llm.md`):

```json
{
  "world_version": "2026-06-14T12:00:00Z",
  "frame": "base",
  "workspace_bounds_m": {"min": [-0.4,-0.6,0.0], "max": [0.6,0.6,0.8]},
  "keep_out": [{"min": [-0.1,-0.7,0.0], "max": [0.1,-0.5,0.4], "note": "operator"}],
  "collision_world": "remoroo_cell/world/collision.nvblox",
  "voxel_m": 0.01,
  "free_space_samples": [[0.2,0.0,0.4], [0.1,0.2,0.5]],
  "coverage": {"method": ["eye_in_hand_sweep"], "sweeps": 3, "occupied_voxels": 18234},
  "objects": []
}
```

## G4 exit criteria (functional)

- [ ] cuRoboV2 produces collision-free plans to several safe poses against the
      world (the bootstrapping smoke above succeeds repeatably).
- [ ] No obvious holes in the swept region of the workspace box (operator
      eyeballs the reconstruction).
- [ ] `scene.json` written and queryable; collision world saved under
      `remoroo_cell/world/`.
- [ ] `scan_report.md`: method(s) used, coverage, screenshots/clips, and the
      "plannable" verdict.

This world is **versioned + editable** and will be rebuilt as the cell changes
— do not treat it as frozen. It is the geometry the Phase-8 safe-motion demo
plans against.
