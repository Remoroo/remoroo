# Commission — wire + verify the autonomous-motion stack (the gate before the demo)

By this gate every ingredient for safe autonomous motion exists but nothing has
fused them: the calibrated `robot.urdf`, the `collision_spheres.yml`, the scanned
`world/`, and the `safety.yaml` envelope. **Commission** builds them into ONE
warmed cuRoboV2 planner and proves the full chain — plan → trajectory → the
per-arm executor → real motion — ONCE, so the G8 demo exercises a stack that is
*proven*, not assumed.

You do **not** write cuRobo here. The planner is the SHIPPED `motion_engine`
(it travels with the edge, like `calib_engine`). Your job is to WIRE the bridge
to it and run the verify.

## 1. Wire the bridge (two methods, both in `bridge_primitives.md`)

`motion_engine.MotionStack` plans; the bridge supplies state + the executor:

```python
from motion_engine import MotionStack

# built once from ALL the setup artifacts (the stack reads them off disk):
stack = MotionStack.from_cell("remoroo_cell", bridge=bridge)
```

The bridge must provide:

- `bridge.read_joint_positions(arm) -> np.ndarray` — seeds each plan's start.
- `bridge.execute_trajectory(traj, should_abort=None) -> bool` — REPLAYS the full
  `Trajectory` on the arm SDK (see `arm_adapters.md`); routes by `traj.joint_names`.
- `bridge.estop_tripped() -> bool` — the E-stop poll the stack checks mid-replay.

That's it. The stack owns the cuRobo planner, the scanned-world collision model,
and feeding `safety.yaml` (accel/jerk/speed → planner inputs; keep-out + bounds
→ obstacles) so every trajectory is collision-free AND within the envelope **by
construction**.

## 2. The call surface (TCP-keyed, count-agnostic)

`tcp` is a group NAME from the authored `cell.yaml: groups`. The SAME calls drive 1
arm, 2 arms, a quadruped's legs, or a humanoid's many end-effectors — the stack
builds (and caches) a planner per group set; nothing special-cases morphology.

```python
stack.move_to_pose("right", (xyz, quat_wxyz))      # one TCP → a pose
stack.move_to_poses({"right": poseR, "left": poseL})  # N TCPs, coordinated, ONE trajectory
stack.move_through_poses("right", [p1, p2, p3])    # a waypoint sequence
stack.plan_to_point("right", [x, y, z])            # convenience: point, nominal orientation
stack.retract("right")                             # collision-free path home
stack.move_to_joints(q, arm="right")               # SINGLE config (calibration/jog) — not a plan
```

Every motion verb returns a `MoveResult` (`.ok`, `.trajectory`, `.executed`,
`.aborted`, `.audit`). Pass `execute=False` to plan + visualise without moving.

## 3. Run the verify (what the gate checks)

```python
report = stack.commission(progress=lambda s: print(s))   # streams each step
assert report["ok"], report["message"]
```

`commission()` runs, in order: sphere health (refuses to move on the mm-scale
degeneracy) → arm map present → planner builds/warms → plans ONE collision-free
move (a safe **retract home** from wherever calibration/scan left the arm) →
defensive audit → the executor replays it. Each step is reported for the Studio
panel; on failure it routes back to you (fix + re-checkpoint), never a per-error UI.

## Commission exit criteria (functional)

- [ ] `MotionStack.from_cell` loads with no missing-artifact error (URDF, spheres,
      `cell.yaml: groups`, world, safety all present and consistent).
- [ ] `sphere_health().ok` — collision spheres are in metres, not the mm degeneracy.
- [ ] One `plan_to_pose`/`retract` returns a collision-free `Trajectory` against the
      SCANNED world (not a box), within the safety envelope.
- [ ] `execute_trajectory` replays the FULL path on the real arm under the operator
      gate; the E-stop aborts it mid-motion (verified live, slow speed).
- [ ] `commission_report.md`: the stack config, the verify trajectory, pass/fail.

The G8 demo then just exercises this stack (`move_to_pose` / `move_through_poses`
/ `retract`) — no re-derivation of a cuRobo integration.
