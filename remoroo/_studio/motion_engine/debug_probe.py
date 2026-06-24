"""TRANSPARENT, off-GPU debug geometry for the live world — so we can SEE (and SAVE) exactly what the
planner sees, before vs after masking, instead of trusting opaque counts:

  - `backproject_to_base(depth, K, cam_pose)` — the RAW point cloud the camera produced, in the robot
    base frame (the same pinhole + pose the live mapper uses). If this cloud doesn't sit on the real
    scene (table/objects in front of the arm), the camera POSE is wrong — visible at a glance.
  - `split_cloud(points, spheres, seg_dist)` — which points the robot mask removes (within `seg_dist`
    of a collision sphere) vs keeps. The SAME rule cuRobo's RobotSegmenter applies, but ours so we can
    render the masked-out (red) vs kept (green) clouds and prove whether masking is eating the scene.

Pure numpy → unit-tested; the GPU pieces (robot spheres at the seed) come from the planner."""
from __future__ import annotations

from typing import Optional, Sequence, Tuple

import numpy as np

from .live_world import _quat_wxyz_to_R


def backproject_to_base(depth, intrinsics, cam_pose_in_base, *, stride: int = 6) -> np.ndarray:
    """RAW valid-depth pixels → `[N,3]` points in the robot base frame (subsampled by `stride`).
    `cam_pose_in_base` = (xyz, wxyz). Invalid (0/NaN/≤0) pixels dropped."""
    d = np.asarray(depth, dtype=np.float64)
    if d.ndim != 2:
        return np.zeros((0, 3), dtype=float)
    H, W = d.shape
    K = np.asarray(intrinsics, dtype=np.float64).reshape(3, 3)
    fx, fy, cx, cy = K[0, 0], K[1, 1], K[0, 2], K[1, 2]
    vv, uu = np.mgrid[0:H:stride, 0:W:stride]
    dd = d[vv, uu]
    m = np.isfinite(dd) & (dd > 1e-3)
    if not m.any():
        return np.zeros((0, 3), dtype=float)
    z = dd[m]
    x = (uu[m] - cx) * z / fx
    y = (vv[m] - cy) * z / fy
    Xc = np.stack([x, y, z], axis=1)                      # camera frame
    p, q = cam_pose_in_base
    R = _quat_wxyz_to_R(q)
    t = np.asarray(p, dtype=np.float64).reshape(-1)[:3]
    return Xc @ R.T + t                                   # base frame


def nearest_sphere_signed_distance(points, spheres) -> np.ndarray:
    """Per point, the min over spheres of `||p - center|| - radius` (<0 = inside a sphere). `spheres`
    is `[M,4]` (x,y,z,r). Empty spheres → +inf (nothing masked)."""
    p = np.asarray(points, dtype=np.float64).reshape(-1, 3)
    s = np.asarray(spheres, dtype=np.float64).reshape(-1, 4)
    if p.shape[0] == 0:
        return np.zeros((0,), dtype=float)
    if s.shape[0] == 0:
        return np.full((p.shape[0],), np.inf)
    out = np.full((p.shape[0],), np.inf)
    for c in s:                                            # loop spheres (few) → vectorise over points
        dist = np.sqrt(((p - c[:3]) ** 2).sum(axis=1)) - float(c[3])
        np.minimum(out, dist, out=out)
    return out


def split_cloud(points, spheres, *, seg_dist: float = 0.05) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(kept, masked, signed_dist): points farther than `seg_dist` from every sphere are KEPT (world);
    the rest are MASKED (robot) — the same rule the segmenter uses, made visible."""
    p = np.asarray(points, dtype=np.float64).reshape(-1, 3)
    d = nearest_sphere_signed_distance(p, spheres)
    masked = d < float(seg_dist)
    return p[~masked], p[masked], d


def subsample(arr, n: int = 6000, *, seed: int = 0) -> np.ndarray:
    """At most `n` rows (uniform random) — to ship a cloud to the Studio without flooding it."""
    a = np.asarray(arr, dtype=float).reshape(-1, arr.shape[-1] if np.ndim(arr) > 1 else 1)
    if a.shape[0] <= n:
        return a
    idx = np.random.default_rng(seed).choice(a.shape[0], size=n, replace=False)
    return a[idx]


def bbox(points) -> Optional[dict]:
    a = np.asarray(points, dtype=float).reshape(-1, 3) if np.size(points) else np.zeros((0, 3))
    if a.shape[0] == 0:
        return None
    return {"min": [round(float(x), 4) for x in a.min(0)], "max": [round(float(x), 4) for x in a.max(0)]}
