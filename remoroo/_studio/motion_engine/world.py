"""The cuRobo collision SCENE = the operator's MODELED static obstacles — PURE data (no cuRobo).

The real-time world the planner avoids at commission/demo is the LIVE depth ESDF (built from the
cameras in `curobo_v2.CuroboMapper`). The ONLY persistent geometry is the operator's modeled static
obstacles (`cell.yaml: obstacles` — table / wall / box / post the Studio's ObstaclesSection wrote
via /edge/obstacles/set), which the planner avoids because the cameras can be occluded. A cylinder
("post") is approximated by a tight cuboid bounding box (conservative = strictly safe).

There is NO stored scan point cloud, NO arbitrary workspace-box cage, and NO keep-out zones — those
were UI-bounds artefacts, not real geometry, and are gone. `load_world(cell_dir)` returns a
`WorldInputs` carrying just the obstacle scene dict; the adapter is the only thing that imports cuRobo.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np


@dataclass
class WorldInputs:
    """The modeled-obstacle collision scene the adapter turns into cuRobo geometry. The real world is
    the LIVE depth ESDF — there is no stored cloud, so `scene` is the whole world here."""

    scene: Dict[str, Dict[str, dict]]              # {"cuboid": {name:{dims,pose}}, "mesh": {...}}
    meta: dict = field(default_factory=dict)
    # Vestigial: only the legacy cloud-debug diagnostics still read these; always empty/None now.
    points: np.ndarray = field(default_factory=lambda: np.zeros((0, 3), dtype=float))
    bounds_m: Optional[dict] = None
    voxel_size: float = 0.02

    @property
    def n_points(self) -> int:
        return int(self.points.shape[0])

    @property
    def n_obstacles(self) -> int:
        return sum(len(v) for v in self.scene.values())


def _read_yaml(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        import yaml  # type: ignore

        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return None


def mask_robot_points(points: np.ndarray, spheres, *, margin: float = 0.01):
    """Drop cloud points INSIDE the robot's own collision spheres (+margin) — the robot must never be
    in its OWN collision cloud. World-scan masks the robot per depth-frame, but that leaks; this is
    the hard guarantee at world-build. `spheres` is `[[x,y,z,r], …]` in the BASE frame at the robot's
    current config (from the adapter's GPU FK). Returns (kept_points, n_removed)."""
    a = np.asarray(points, dtype=float)
    if a.ndim != 2 or a.shape[0] == 0 or a.shape[1] < 3:
        return (a.reshape(-1, 3) if a.size else np.zeros((0, 3), dtype=float)), 0
    sph = np.asarray(spheres, dtype=float).reshape(-1, 4)
    sph = sph[sph[:, 3] > 0]                                   # active spheres only
    if sph.size == 0:
        return a[:, :3], 0
    p = a[:, :3]
    keep = np.ones(p.shape[0], dtype=bool)
    for s in sph:
        rr = (float(s[3]) + margin) ** 2
        keep &= ((p - s[:3]) ** 2).sum(axis=1) > rr           # outside this sphere (+margin)
    return p[keep], int((~keep).sum())


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


def build_scene(obstacles: Optional[List[dict]] = None) -> Dict[str, Dict[str, dict]]:
    """The collision scene = the operator's MODELED static obstacles (table/wall/post) as a cuRobo
    scene dict. REAL geometry only — NO arbitrary workspace-box cage and NO abstract keep-out zones:
    the live depth ESDF + these modeled obstacles ARE the world the planner avoids."""
    scene: Dict[str, Dict[str, dict]] = {}
    for kind, items in obstacle_geometry(obstacles).items():
        scene[kind] = dict(items)
    return {k: v for k, v in scene.items() if v}


def load_world(cell_dir: str) -> WorldInputs:
    """The collision scene = the operator's MODELED static obstacles (`cell.yaml: obstacles`). Nothing
    else: no stored scan cloud, no `scene.json`, no workspace-box bounds, no keep-out zones — the real
    world at commission/demo is the LIVE depth ESDF. Pure data, no cuRobo."""
    obstacles = list((_read_yaml(Path(cell_dir) / "cell.yaml") or {}).get("obstacles") or [])
    return WorldInputs(scene=build_scene(obstacles), meta={"n_obstacles": len(obstacles)})


def sphere_box_overlaps(spheres, scene: dict) -> dict:
    """For each obstacle cuboid, how many of the robot's spheres PENETRATE it, and the min
    clearance (sphere-surface → box, negative = inside). Sphere centers are transformed into
    the box's LOCAL frame first — a rotated wall must never read as an axis-aligned slab
    through the workspace (the 'START INSIDE obs_5_wall' misdiagnosis, 2026-07-18; resurfaced
    2026-07-20 via curobo_v2's private AABB copy — this is now the ONE obstacle-naming test,
    shared by the stack's world introspection AND the planner's failure diagnostics)."""
    out: dict = {}
    sph = np.asarray(spheres, dtype=float)
    if sph.size == 0:
        return out
    for name, box in (scene.get("cuboid") or {}).items():
        pose = list(box.get("pose") or [0, 0, 0, 1, 0, 0, 0])
        c = np.asarray(pose[:3], dtype=float)
        q = np.asarray(pose[3:7] if len(pose) >= 7 else [1, 0, 0, 0], dtype=float)
        nq = float(np.linalg.norm(q))
        w, x, y, z = (q / nq) if nq > 1e-9 else (1.0, 0.0, 0.0, 0.0)
        rot = np.array([[1 - 2 * (y * y + z * z), 2 * (x * y - w * z), 2 * (x * z + w * y)],
                        [2 * (x * y + w * z), 1 - 2 * (x * x + z * z), 2 * (y * z - w * x)],
                        [2 * (x * z - w * y), 2 * (y * z + w * x), 1 - 2 * (x * x + y * y)]])
        half = np.asarray(box.get("dims") or [0, 0, 0], dtype=float) / 2.0
        n, mind = 0, 1e9
        for s in sph:
            p = rot.T @ (np.asarray(s[:3], dtype=float) - c)  # sphere center in the BOX frame
            d = np.maximum(np.abs(p) - half, 0.0)             # per-axis outside distance
            dist = float(np.linalg.norm(d)) - float(s[3])
            mind = min(mind, dist)
            if dist < 0:
                n += 1
        out[name] = {"spheres_penetrating": int(n), "min_clearance_m": round(mind, 4)}
    return out
