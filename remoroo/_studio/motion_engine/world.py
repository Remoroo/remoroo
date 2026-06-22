"""Assemble the cuRobo collision world from the cell's scan + safety envelope — PURE (no cuRobo).

The world the planner avoids has two layers, both produced here as plain data the adapter turns
into V2 geometry:

1. **The scanned environment** — `world/cloud.json`, the robot-masked point cloud the world-scan
   gate accumulated (the arm is masked OUT; only the environment is fused). `curobo_v2.py` feeds
   these points to `Mesh.from_pointcloud` (V2's voxelised-surface extraction) → one collision mesh.
   Owning this in ONE place is a key unification: `scan.py` shrinks to "produce the masked cloud",
   and the cuRobo-world construction no longer lives ad-hoc in every cell's `scan.py`.

2. **The safety envelope** — `keep_out` boxes the operator drew, plus the `workspace_bounds_m` box
   turned INTO obstacles (six thin walls around the allowed region) so the planner physically
   cannot leave the workspace. These are scene cuboids. This is how the G0.5 safety envelope
   becomes a PLANNER INPUT, not an after-the-fact check.

`load_world(cell_dir)` returns a `WorldInputs` carrying the cloud points + a scene dict
(`{cuboid: {...}}`); the adapter is the only thing that imports cuRobo.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

# wall thickness (m) for the workspace-bounds cage
_WALL = 0.02


@dataclass
class WorldInputs:
    """Plain-data world the adapter consumes. `points` → a cloud mesh; `scene` → keep-out/bounds."""

    points: np.ndarray                      # (N, 3) masked environment points, base frame
    scene: Dict[str, Dict[str, dict]]       # {"cuboid": {name: {dims, pose}}, ...} keep-out + walls
    voxel_size: float = 0.02                # pitch for cloud → mesh voxelisation
    bounds_m: Optional[dict] = None         # {"min":[x,y,z], "max":[x,y,z]} for reference / clamping
    meta: dict = field(default_factory=dict)

    @property
    def n_points(self) -> int:
        return int(self.points.shape[0])

    @property
    def n_obstacles(self) -> int:
        return sum(len(v) for v in self.scene.values())


def _read_json(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _points_from(obj) -> np.ndarray:
    """Pull (N,3) points out of whatever the scanner wrote: a bare list, or {points|xyz|cloud|points_xyz}."""
    if obj is None:
        return np.zeros((0, 3), dtype=float)
    arr = obj
    if isinstance(obj, dict):
        for k in ("points", "xyz", "cloud", "points_xyz"):
            if obj.get(k) is not None:
                arr = obj[k]
                break
        else:
            arr = []
    a = np.asarray(arr, dtype=float)
    if a.ndim == 1 and a.size % 3 == 0:        # flat [x,y,z,x,y,z,...]
        a = a.reshape(-1, 3)
    if a.ndim != 2 or a.shape[1] < 3:
        return np.zeros((0, 3), dtype=float)
    return a[:, :3]


def _box_walls(bounds: dict, name: str = "ws") -> Dict[str, dict]:
    """Six thin cuboids hugging the faces of the workspace box → the planner can't leave it.
    pose is cuRobo's `[x,y,z,qw,qx,qy,qz]` (centre + identity orientation)."""
    lo = [float(v) for v in bounds["min"]]
    hi = [float(v) for v in bounds["max"]]
    c = [(lo[i] + hi[i]) / 2 for i in range(3)]
    size = [hi[i] - lo[i] for i in range(3)]
    walls: Dict[str, dict] = {}
    faces = [
        ("xlo", [lo[0], c[1], c[2]], [_WALL, size[1], size[2]]),
        ("xhi", [hi[0], c[1], c[2]], [_WALL, size[1], size[2]]),
        ("ylo", [c[0], lo[1], c[2]], [size[0], _WALL, size[2]]),
        ("yhi", [c[0], hi[1], c[2]], [size[0], _WALL, size[2]]),
        ("zlo", [c[0], c[1], lo[2]], [size[0], size[1], _WALL]),
        ("zhi", [c[0], c[1], hi[2]], [size[0], size[1], _WALL]),
    ]
    for label, ctr, dims in faces:
        walls[f"{name}_{label}"] = {"dims": dims, "pose": [*ctr, 1.0, 0.0, 0.0, 0.0]}
    return walls


def keepout_cuboids(keep_out: List[dict]) -> Dict[str, dict]:
    """`keep_out: [{min,max,note}]` → named cuboid obstacles (axis-aligned boxes)."""
    out: Dict[str, dict] = {}
    for i, k in enumerate(keep_out or []):
        lo = [float(v) for v in k["min"]]
        hi = [float(v) for v in k["max"]]
        c = [(lo[j] + hi[j]) / 2 for j in range(3)]
        dims = [max(1e-3, hi[j] - lo[j]) for j in range(3)]
        name = str(k.get("note") or f"keepout_{i}").replace(" ", "_")[:40]
        out[f"ko_{i}_{name}"] = {"dims": dims, "pose": [*c, 1.0, 0.0, 0.0, 0.0]}
    return out


def build_scene(*, bounds_m: Optional[dict], keep_out: Optional[List[dict]],
                wall_bounds: bool = True) -> Dict[str, Dict[str, dict]]:
    """The keep-out + (optional) workspace-cage cuboids as a cuRobo scene dict."""
    cuboid: Dict[str, dict] = {}
    cuboid.update(keepout_cuboids(keep_out or []))
    if wall_bounds and bounds_m and bounds_m.get("min") and bounds_m.get("max"):
        cuboid.update(_box_walls(bounds_m))
    return {"cuboid": cuboid}


def load_world(cell_dir: str, *, safety: Optional[dict] = None, voxel_size: float = 0.02,
               wall_bounds: bool = True) -> WorldInputs:
    """Read `world/cloud.json` + `world/scene.json` (and/or `safety`) into `WorldInputs`.

    Bounds/keep-out resolution order: explicit `safety` arg → `world/scene.json` → none. The
    cloud is the masked environment; an EMPTY cloud is allowed (planner just has the cage + keep-out
    and the operator was warned at G4), never a crash."""
    base = Path(cell_dir)
    points = _points_from(_read_json(base / "world" / "cloud.json"))
    scene_json = _read_json(base / "world" / "scene.json") or {}

    bounds = None
    keep_out: List[dict] = []
    if safety:
        bounds = safety.get("bounds_m") or safety.get("workspace_bounds_m")
        keep_out = list(safety.get("keep_out") or [])
    if bounds is None:
        bounds = scene_json.get("workspace_bounds_m")
    if not keep_out:
        keep_out = list(scene_json.get("keep_out") or [])

    scene = build_scene(bounds_m=bounds, keep_out=keep_out, wall_bounds=wall_bounds)
    return WorldInputs(
        points=points,
        scene=scene,
        voxel_size=float(scene_json.get("voxel_m") or voxel_size),
        bounds_m=bounds,
        meta={"n_points": int(points.shape[0]), "n_keepout": len(keep_out),
              "walls": wall_bounds and bool(bounds)},
    )
