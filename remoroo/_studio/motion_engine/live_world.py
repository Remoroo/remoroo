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
from typing import Dict, Optional, Sequence, Tuple

import numpy as np


@dataclass
class DepthFrame:
    """One camera's depth observation, in the cell's own units/frame — the agent's bridge fills it.

    depth:            (H, W) float metres; 0 / NaN / <=0 = invalid (no return).
    intrinsics:       (3, 3) pinhole [[fx,0,cx],[0,fy,cy],[0,0,1]].
    cam_pose_in_base: (position[3] xyz, quaternion[4] wxyz) — the camera pose in the robot base frame
                      (the calibration result). For eye-in-hand this is `fk(joints) @ X`.
    name:             a label (e.g. the camera link) — advisory.
    q:                the joint config (combined-URDF name→radians) the robot was at WHEN THIS FRAME
                      WAS CAPTURED — the config the robot MASK uses for THIS frame. CRITICAL while the
                      arm moves: the depth + `cam_pose_in_base` are placed at the capture instant, so the
                      mask MUST use the SAME capture-time joints; otherwise the robot (moved since) is
                      masked in the WRONG place and its own body fuses into the world. None → fall back
                      to the live `get_observation()` config — correct ONLY when the arm is still
                      (commission's one-shot build); for the realtime loop ALWAYS set it (= the synced
                      capture-time config you also FK'd for `cam_pose_in_base`).
    """

    depth: np.ndarray
    intrinsics: np.ndarray
    cam_pose_in_base: Tuple[Sequence[float], Sequence[float]]
    name: str = "cam"
    q: Optional[Dict[str, float]] = None


# The live ESDF's voxel budget along its LONGEST axis. A resolution/compute budget (like an image
# resolution), NOT a spatial assumption: the extent comes from the data and the voxel SIZE adapts so
# the grid is ~this many voxels at any scale (a 1 m desk or a 30 m hall → same voxel count, coarser
# voxels for the hall). Keeps memory/compute bounded for ANY robot, fixed-base or mobile.
ESDF_AXIS_VOXELS = 128


def _quat_wxyz_to_R(q: Sequence[float]) -> np.ndarray:
    """wxyz unit quaternion → 3×3 rotation matrix."""
    w, x, y, z = [float(v) for v in q]
    n = (w * w + x * x + y * y + z * z) ** 0.5 or 1.0
    w, x, y, z = w / n, x / n, y / n, z / n
    return np.array([
        [1 - 2 * (y * y + z * z), 2 * (x * y - w * z), 2 * (x * z + w * y)],
        [2 * (x * y + w * z), 1 - 2 * (x * x + z * z), 2 * (y * z - w * x)],
        [2 * (x * z - w * y), 2 * (y * z + w * x), 1 - 2 * (x * x + y * y)],
    ], dtype=float)


def observed_extent(frames, *, margin: float = 0.3, stride: int = 12, lo_pct: float = 1.0,
                    hi_pct: float = 99.0):
    """(extent_xyz, center_xyz) covering the VALID depth ACTUALLY observed across all frames, in the
    robot base frame — the real sensed scene, NOT a workspace box or robot reach. Back-projects a
    subsample of each frame's valid depth pixels to base-frame points and takes their robust bbox
    (percentile, to reject flyers) + `margin`. Scale/morphology-agnostic: a desk arm's workspace or a
    mobile robot's LOCAL camera view (it re-centres as the robot moves — you only ever map what you
    see, never the whole world). Returns None if there's no valid depth. Pure numpy → unit-tested."""
    pts = []
    for f in frames:
        d = np.asarray(f.depth, dtype=np.float32)
        if d.ndim != 2:
            continue
        H, W = d.shape
        K = np.asarray(f.intrinsics, dtype=np.float64).reshape(3, 3)
        fx, fy, cx, cy = K[0, 0], K[1, 1], K[0, 2], K[1, 2]
        vv, uu = np.mgrid[0:H:stride, 0:W:stride]
        dd = d[vv, uu].astype(np.float64)
        m = np.isfinite(dd) & (dd > 1e-3)
        if not m.any():
            continue
        z = dd[m]
        x = (uu[m] - cx) * z / fx
        y = (vv[m] - cy) * z / fy
        Xc = np.stack([x, y, z], axis=1)                      # camera frame
        p, q = f.cam_pose_in_base
        R = _quat_wxyz_to_R(q)
        t = np.asarray(p, dtype=np.float64).reshape(-1)[:3]
        pts.append(Xc @ R.T + t)                              # → base frame
    if not pts:
        return None
    P = np.concatenate(pts, axis=0)
    lo = np.percentile(P, lo_pct, axis=0)
    hi = np.percentile(P, hi_pct, axis=0)
    extent = tuple(float(max(0.3, (hi[i] - lo[i]) + 2 * margin)) for i in range(3))
    center = tuple(float((hi[i] + lo[i]) / 2) for i in range(3))
    return extent, center


def esdf_resolution(extent: Sequence[float], *, axis_voxels: int = ESDF_AXIS_VOXELS,
                    min_voxel: float = 0.01) -> Tuple[float, float]:
    """(esdf_voxel_size, tsdf_voxel_size) for an observed `extent`: pick the ESDF voxel size so the
    grid is ~`axis_voxels` along its longest axis (so the grid count is bounded at any scale), and a
    finer TSDF (block-sparse → memory tracks surface area, not volume). NOT a spatial assumption."""
    longest = max(float(e) for e in extent) if len(extent) else 1.0
    esdf_vs = max(longest / float(axis_voxels), float(min_voxel))
    return esdf_vs, max(esdf_vs / 2.0, 0.005)


def obstacle_bboxes(scene) -> list:
    """Axis-aligned `(lo, hi)` bounds of each MODELED cuboid (`center ± dims/2`). Rotation is ignored
    (a slightly loose box) — used only to GROW the ESDF grid so the modeled obstacles are never dropped
    from the fused map. Pure data."""
    out = []
    for b in ((scene or {}).get("cuboid") or {}).values():
        pose = list(b.get("pose") or [0, 0, 0])
        c = [float(v) for v in (pose[:3] + [0, 0, 0])[:3]]
        d = [float(v) for v in (list(b.get("dims") or [0.1, 0.1, 0.1]) + [0.1, 0.1, 0.1])[:3]]
        out.append(([c[i] - d[i] / 2 for i in range(3)], [c[i] + d[i] / 2 for i in range(3)]))
    return out


def union_extent(observed, bboxes, *, fallback=((1.0, 1.0, 1.0), (0.0, 0.0, 0.0)),
                 margin: float = 0.1):
    """Grow the observed-depth `(extent, center)` to ALSO contain `bboxes` (a list of `(lo, hi)`), so
    the modeled obstacles (input 2) are ALWAYS inside the fused ESDF grid — an obstacle the camera
    can't see (the whole reason it's modeled) must not be dropped because it fell outside the depth
    bbox. Returns `(extent, center)`; `fallback` when there's neither depth nor an obstacle."""
    boxes = list(bboxes or [])
    if observed is not None:
        ext, ctr = observed
        boxes.append(([ctr[i] - ext[i] / 2 for i in range(3)], [ctr[i] + ext[i] / 2 for i in range(3)]))
    if not boxes:
        return fallback
    lo = [min(b[0][i] for b in boxes) for i in range(3)]
    hi = [max(b[1][i] for b in boxes) for i in range(3)]
    extent = tuple(max(0.3, (hi[i] - lo[i]) + 2 * margin) for i in range(3))
    center = tuple((hi[i] + lo[i]) / 2 for i in range(3))
    return extent, center


def workspace_extent(bounds_m: dict | None, *, default: float = 1.0,
                     pad: float = 0.3) -> Tuple[Tuple[float, float, float], Tuple[float, float, float]]:
    """LEGACY fallback only (used when there's no live depth at all). The live world's extent comes
    from `observed_extent` — the actual sensed geometry — never from an arbitrary workspace box."""
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
