"""motion_engine — the shipped, rig-agnostic autonomous-motion stack.

Travels with the Studio edge bundle (like `calib_engine`). It FUSES every prior setup gate's output
— the calibrated URDF, the collision spheres, the scanned world, the safety envelope — into ONE
warmed cuRoboV2 planner and exposes high-level, TCP-keyed motion verbs (`move_to_pose`,
`move_to_poses`, `move_through_poses`, `retract`, …). The cell's bridge wires it up and supplies the
per-arm executor (`execute_trajectory`); cuRobo PLANS, the bridge REPLAYS.

Public surface:
    MotionStack          — orchestration: from_cell + the motion verbs + commission()/demo_run()
    DepthFrame           — one camera's depth+intrinsics+pose for the LIVE ESDF (update_world_live)
    Trajectory           — the SDK-agnostic time-parameterised joint path (the planner↔executor seam)
    MoveResult           — the outcome of a verb (planned + executed + audit)
    CuroboV2Planner      — the ONE adapter that imports cuRobo (lazy GPU import)

Everything except `curobo_v2.py` is cuRobo/torch-free and unit-tested off-GPU.
"""
from __future__ import annotations

from .live_world import DepthFrame
from .stack import MotionStack, MoveResult
from .trajectory import Trajectory

__all__ = ["MotionStack", "DepthFrame", "MoveResult", "Trajectory"]
