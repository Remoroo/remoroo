"""Assemble the cuRobo collision world from the cell's scan + safety envelope — PURE (no cuRobo).

The world the planner avoids has THREE layers, all produced here as plain data the adapter turns
into V2 geometry:

1. **The scanned environment** — `world/cloud.json`, the robot-masked point cloud the world-scan
   gate accumulated (the arm is masked OUT; only the environment is fused). `curobo_v2.py` feeds
   these points to `Mesh.from_pointcloud` (V2's voxelised-surface extraction) → one collision mesh.
   Owning this in ONE place is a key unification: `scan.py` shrinks to "produce the masked cloud",
   and the cuRobo-world construction no longer lives ad-hoc in every cell's `scan.py`.

2. **The operator-modeled static obstacles** — `cell.yaml: obstacles`, the table / wall / box /
   post the operator placed in the Studio (the ObstaclesSection writes them via /edge/obstacles/set
   as `{name, type, dims|radius/height, pose:[x,y,z,qw,qx,qy,qz]}`). These become scene
   cuboids/meshes so the planner avoids them — the SAME obstacles the classic path consumed via
   `build_world_cfg`, now also feeding the unified stack. A cylinder ("post") is approximated by a
   tight cuboid bounding box (conservative = strictly safe), matching the classic behaviour.

3. **The safety envelope** — `keep_out` boxes the operator drew, plus the `workspace_bounds_m` box
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


def _read_yaml(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        import yaml  # type: ignore

        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
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


def _xyz(v) -> Optional[List[float]]:
    """A `[x,y,z]` of floats, or None if `v` isn't a usable 3-vector (so a malformed/free-text
    safety entry is skipped, never crashing the motion stack with a KeyError/ValueError)."""
    if not isinstance(v, (list, tuple)) or len(v) < 3:
        return None
    try:
        return [float(v[0]), float(v[1]), float(v[2])]
    except (TypeError, ValueError):
        return None


def _box_walls(bounds: dict, name: str = "ws") -> Dict[str, dict]:
    """Six thin cuboids hugging the faces of the workspace box → the planner can't leave it.
    pose is cuRobo's `[x,y,z,qw,qx,qy,qz]` (centre + identity orientation)."""
    lo, hi = _xyz((bounds or {}).get("min")), _xyz((bounds or {}).get("max"))
    if lo is None or hi is None:
        return {}
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
    """`keep_out: [{min,max,note}]` → named cuboid obstacles (axis-aligned boxes). TOLERANT: an
    entry that isn't a `{min,max}` box (e.g. a free-text keep-out the operator typed at G0.5, or a
    partially-authored dict) is SKIPPED, not crashed — a bad safety entry must not take down the
    whole commission."""
    out: Dict[str, dict] = {}
    for i, k in enumerate(keep_out or []):
        if not isinstance(k, dict):
            continue
        lo, hi = _xyz(k.get("min")), _xyz(k.get("max"))
        if lo is None or hi is None:
            continue
        c = [(lo[j] + hi[j]) / 2 for j in range(3)]
        dims = [max(1e-3, hi[j] - lo[j]) for j in range(3)]
        name = str(k.get("note") or f"keepout_{i}").replace(" ", "_")[:40]
        out[f"ko_{i}_{name}"] = {"dims": dims, "pose": [*c, 1.0, 0.0, 0.0, 0.0]}
    return out


def obstacle_geometry(obstacles: Optional[List[dict]]) -> Dict[str, Dict[str, dict]]:
    """The operator's `cell.yaml: obstacles` → a cuRobo scene dict (cuboids + meshes). Schema per
    obstacle: `{name, type: cuboid|cylinder|mesh, dims|radius/height|file_path, pose}`. A cylinder
    is approximated by a tight cuboid bounding box (dims [2r, 2r, h]) — conservative (the avoided
    volume is slightly larger), matching `calib_engine.curobo_cfg.build_world_cfg` so the unified
    stack and the classic path agree on where the table is."""
    cuboid: Dict[str, dict] = {}
    mesh: Dict[str, dict] = {}
    for i, o in enumerate(obstacles or []):
        name = f"obs_{i}_{str(o.get('name') or 'obs').replace(' ', '_')[:32]}"
        pose = list(o.get("pose") or [0, 0, 0, 1, 0, 0, 0])
        kind = str(o.get("type") or "cuboid")
        if kind == "mesh" and o.get("file_path"):
            mesh[name] = {"file_path": o["file_path"], "pose": pose}
        elif kind == "cylinder":
            r, h = float(o.get("radius", 0.05)), float(o.get("height", 0.1))
            cuboid[name] = {"dims": [2 * r, 2 * r, h], "pose": pose}
        else:
            cuboid[name] = {"dims": list(o.get("dims") or [0.1, 0.1, 0.1]), "pose": pose}
    out: Dict[str, Dict[str, dict]] = {}
    if cuboid:
        out["cuboid"] = cuboid
    if mesh:
        out["mesh"] = mesh
    return out


def build_scene(*, bounds_m: Optional[dict], keep_out: Optional[List[dict]],
                obstacles: Optional[List[dict]] = None,
                wall_bounds: bool = True) -> Dict[str, Dict[str, dict]]:
    """The keep-out + (optional) workspace-cage cuboids + the operator's static obstacles, as one
    cuRobo scene dict."""
    cuboid: Dict[str, dict] = {}
    cuboid.update(keepout_cuboids(keep_out or []))
    if wall_bounds and bounds_m and bounds_m.get("min") and bounds_m.get("max"):
        cuboid.update(_box_walls(bounds_m))
    scene: Dict[str, Dict[str, dict]] = {"cuboid": cuboid}
    for kind, items in obstacle_geometry(obstacles).items():
        scene.setdefault(kind, {}).update(items)
    return {k: v for k, v in scene.items() if v}


def load_world(cell_dir: str, *, safety: Optional[dict] = None, voxel_size: float = 0.02,
               wall_bounds: bool = True) -> WorldInputs:
    """Read `world/cloud.json` + `world/scene.json` (and/or `safety`) into `WorldInputs`.

    Bounds/keep-out resolution order: explicit `safety` arg → `world/scene.json` → none. The
    cloud is the masked environment; an EMPTY cloud is allowed (planner just has the cage + keep-out
    and the operator was warned at G4), never a crash."""
    base = Path(cell_dir)
    points = _points_from(_read_json(base / "world" / "cloud.json"))
    scene_json = _read_json(base / "world" / "scene.json") or {}
    # the operator's static obstacles (table/wall/box/post) live in cell.yaml (Studio writes them
    # via /edge/obstacles/set) — the planner must avoid them, exactly like the classic path did.
    obstacles = list((_read_yaml(base / "cell.yaml") or {}).get("obstacles") or [])

    bounds = None
    keep_out: List[dict] = []
    if safety:
        bounds = safety.get("bounds_m") or safety.get("workspace_bounds_m")
        keep_out = list(safety.get("keep_out") or [])
    if bounds is None:
        bounds = scene_json.get("workspace_bounds_m")
    if not keep_out:
        keep_out = list(scene_json.get("keep_out") or [])

    scene = build_scene(bounds_m=bounds, keep_out=keep_out, obstacles=obstacles, wall_bounds=wall_bounds)
    return WorldInputs(
        points=points,
        scene=scene,
        voxel_size=float(scene_json.get("voxel_m") or voxel_size),
        bounds_m=bounds,
        meta={"n_points": int(points.shape[0]), "n_keepout": len(keep_out),
              "n_obstacles": len(obstacles), "walls": wall_bounds and bool(bounds)},
    )
