"""The LIVE volumetric world — cuRoboV2's intended collision pipeline (live depth → TSDF → ESDF),
NOT a stored scan. This module is the GENERAL, off-GPU glue: the agent-facing input contract
(`DepthFrame`) and the safety-bounds→`MapperCfg` sizing (`workspace_extent`). The heavy lifting is
cuRobo's, tested on live robots, and lives in `curobo_v2.py`:

  - robot masking  → `curobo.perception.RobotSegmenter` (project depth→cloud, robot-frame, distance to
                     the live collision spheres). We do NOT hand-roll this.
  - depth cleaning → `curobo.perception.FilterDepth` (nan / flying-pixel / bilateral).
  - TSDF→ESDF map  → `curobo.perception.Mapper` (+ `update_static_obstacles` to fuse the modeled
                     cuboids — table/wall — into the SAME map, so there is ONE canonical world).

The contract the agent's `commission.py` fills, every cycle, for ANY rig (N cameras, N arms):

    from motion_engine import MotionStack, DepthFrame
    stack = MotionStack.from_cell("remoroo_cell", bridge=bridge)
    frames = [DepthFrame(depth=<HxW m>, intrinsics=<3x3>, cam_pose_in_base=(xyz, wxyz))
              for cam in bridge.cameras()]     # the agent pulls these from ITS bridge + calib
    stack.update_world_live(frames)            # build/refresh the ESDF (robot auto-masked), then
    stack.commission(...) / stack.move_to_pose(...)   # plan against the LIVE world

`cam_pose_in_base` is the camera pose in the robot BASE frame — exactly the calibration output
(`fk(live joints) @ X` for eye-in-hand, or the static hand-eye pose) — so the map, the mask, and the
robot it plans for are all in the same frame. The robot is masked from each depth frame at its LIVE
config, so it is never in its own collision world (the bug the stored-cloud path kept hitting).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Tuple

import numpy as np


@dataclass
class DepthFrame:
    """One camera's depth observation, in the cell's own units/frame — the agent's bridge fills it.

    depth:            (H, W) float metres; 0 / NaN / <=0 = invalid (no return).
    intrinsics:       (3, 3) pinhole [[fx,0,cx],[0,fy,cy],[0,0,1]].
    cam_pose_in_base: (position[3] xyz, quaternion[4] wxyz) — the camera pose in the robot base frame
                      (the calibration result). For eye-in-hand this is `fk(joints) @ X`.
    name:             a label (e.g. the camera link) — advisory.
    """

    depth: np.ndarray
    intrinsics: np.ndarray
    cam_pose_in_base: Tuple[Sequence[float], Sequence[float]]
    name: str = "cam"


def workspace_extent(bounds_m: dict | None, *, default: float = 2.0,
                     pad: float = 0.3) -> Tuple[Tuple[float, float, float], Tuple[float, float, float]]:
    """(extent_xyz, center_xyz) for the mapper's voxel volume, from the safety workspace bounds
    (+pad). Falls back to a `default`-metre cube at the origin. Morphology-agnostic — this is the one
    bit of OUR config (`safety.bounds_m`) that cuRobo's `MapperCfg` needs and can't infer."""
    lo = (bounds_m or {}).get("min")
    hi = (bounds_m or {}).get("max")
    try:
        lo = [float(v) for v in lo][:3]
        hi = [float(v) for v in hi][:3]
        extent = tuple(max(0.2, (hi[i] - lo[i]) + 2 * pad) for i in range(3))
        center = tuple((hi[i] + lo[i]) / 2 for i in range(3))
        return extent, center
    except Exception:  # noqa: BLE001
        return (default, default, default), (0.0, 0.0, 0.0)
