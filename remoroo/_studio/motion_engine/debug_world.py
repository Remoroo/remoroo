"""Export the planner's collision world to a binary STL — 'the world that gets into the planner',
for the Studio debug layer + offline inspection (MeshLab/Blender). PURE numpy, no cuRobo:

The world the planner avoids after a live build = the cuRoboV2 ESDF occupied voxels (which ALREADY
include the fused static obstacles) + the operator's modeled cuboids. We turn each occupied voxel into
a cube (edge = the ESDF voxel size) and each modeled cuboid into an oriented box, and write one binary
STL. This is the ground truth for debugging 'why is the start born in collision' — you SEE exactly what
volume the planner treats as occupied (and whether it's sitting on the arm).
"""
from __future__ import annotations

import struct
from typing import Dict, List, Optional, Sequence

import numpy as np

# unit-cube corner offsets (×half-extent) and the 12 triangles (corner-index triples, CCW outward).
_OFFS = np.array([[-1, -1, -1], [1, -1, -1], [1, 1, -1], [-1, 1, -1],
                  [-1, -1, 1], [1, -1, 1], [1, 1, 1], [-1, 1, 1]], dtype=np.float64)
_TRIS = np.array([(0, 3, 2), (0, 2, 1),        # z-   (bottom)
                  (4, 5, 6), (4, 6, 7),        # z+   (top)
                  (0, 1, 5), (0, 5, 4),        # y-
                  (2, 3, 7), (2, 7, 6),        # y+
                  (1, 2, 6), (1, 6, 5),        # x+
                  (0, 4, 7), (0, 7, 3)], dtype=np.int64)   # x-

_STL_DTYPE = np.dtype([("n", "<3f4"), ("v0", "<3f4"), ("v1", "<3f4"),
                       ("v2", "<3f4"), ("attr", "<u2")])


def _quat_wxyz_to_R(q: Sequence[float]) -> np.ndarray:
    w, x, y, z = (list(q) + [1, 0, 0, 0])[:4]
    n = (w * w + x * x + y * y + z * z) ** 0.5 or 1.0
    w, x, y, z = w / n, x / n, y / n, z / n
    return np.array([
        [1 - 2 * (y * y + z * z), 2 * (x * y - z * w), 2 * (x * z + y * w)],
        [2 * (x * y + z * w), 1 - 2 * (x * x + z * z), 2 * (y * z - x * w)],
        [2 * (x * z - y * w), 2 * (y * z + x * w), 1 - 2 * (x * x + y * y)],
    ], dtype=np.float64)


def _box_corners(center: np.ndarray, half: np.ndarray, R: Optional[np.ndarray]) -> np.ndarray:
    """The 8 world corners of one box (optionally rotated)."""
    local = _OFFS * half[None, :]                      # (8,3)
    if R is not None:
        local = local @ R.T
    return local + center[None, :]


def _triangles_for_boxes(boxes) -> np.ndarray:
    """`boxes` = iterable of (center3, half3, R|None) → an (T,3,3) array of triangle vertices."""
    tris: List[np.ndarray] = []
    for center, half, R in boxes:
        c = np.asarray(center, dtype=np.float64).reshape(3)
        h = np.asarray(half, dtype=np.float64).reshape(3)
        corners = _box_corners(c, h, R)                # (8,3)
        tris.append(corners[_TRIS])                    # (12,3,3)
    if not tris:
        return np.zeros((0, 3, 3), dtype=np.float64)
    return np.concatenate(tris, axis=0)


def _world_boxes(voxel_centers, voxel_size: float, cuboids: Optional[List[dict]]):
    """Yield (center, half, R) for every occupied voxel (axis-aligned cube) + every modeled cuboid
    (oriented box from its pose quaternion)."""
    v = np.asarray(voxel_centers, dtype=np.float64).reshape(-1, 3)
    half_v = np.array([voxel_size / 2.0] * 3, dtype=np.float64)
    for c in v:
        yield c, half_v, None
    for b in cuboids or []:
        dims = np.asarray(b.get("dims") or [0.1, 0.1, 0.1], dtype=np.float64).reshape(3)
        pose = list(b.get("pose") or [0, 0, 0, 1, 0, 0, 0])
        center = np.asarray(pose[:3], dtype=np.float64)
        R = _quat_wxyz_to_R(pose[3:7]) if len(pose) >= 7 else None
        yield center, dims / 2.0, R


def world_stl_bytes(voxel_centers, voxel_size: float,
                    cuboids: Optional[List[dict]] = None,
                    *, header: bytes = b"remoroo planner debug world") -> bytes:
    """Binary STL of the planner's collision world: a cube per occupied ESDF voxel + an oriented box
    per modeled cuboid. 12 triangles/box; face normals computed per triangle."""
    tri = _triangles_for_boxes(_world_boxes(voxel_centers, float(voxel_size), cuboids))   # (T,3,3)
    T = int(tri.shape[0])
    rec = np.zeros(T, dtype=_STL_DTYPE)
    if T:
        v0, v1, v2 = tri[:, 0, :], tri[:, 1, :], tri[:, 2, :]
        nrm = np.cross(v1 - v0, v2 - v0)
        ln = np.linalg.norm(nrm, axis=1, keepdims=True)
        nrm = np.divide(nrm, ln, out=np.zeros_like(nrm), where=ln > 0)
        rec["n"], rec["v0"], rec["v1"], rec["v2"] = nrm, v0, v1, v2
    head = header[:80].ljust(80, b" ")
    return head + struct.pack("<I", T) + rec.tobytes()
