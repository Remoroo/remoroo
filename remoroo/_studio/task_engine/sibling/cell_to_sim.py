"""cell_to_sim (COMP-20) — deterministic converter: the SET-UP cell's artifacts become the
sibling's exact robot half. Emits (a) a scene spec dict every backend consumes (the
behavioral SimWorld today, the Isaac worker on the GPU box next) and (b) a USDA text file
(ASCII USD; no pxr dependency) that Isaac ingests directly. Zero authoring: the robot half is
converted, never composed.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


def load_cell(cell_dir: str) -> Dict[str, Any]:
    """Collect what Setup guaranteed: urdf path, spheres, cell.yaml (obstacles, cameras,
    instrumentation), safety envelope. Missing pieces are STATED, not defaulted silently."""
    root = Path(cell_dir)
    out: Dict[str, Any] = {"cell_dir": str(root), "missing": []}
    urdf = root / "robot_model" / "robot.urdf"
    spheres = root / "robot_model" / "collision_spheres.yml"
    out["urdf"] = str(urdf) if urdf.exists() else out["missing"].append("robot.urdf") or None
    out["spheres"] = (str(spheres) if spheres.exists()
                      else out["missing"].append("collision_spheres.yml") or None)
    cell_yaml = root / "cell.yaml"
    if cell_yaml.exists():
        try:
            import yaml
            out["cell"] = yaml.safe_load(cell_yaml.read_text()) or {}
        except Exception as e:                        # noqa: BLE001
            out["missing"].append(f"cell.yaml unreadable: {e}")
            out["cell"] = {}
    else:
        out["missing"].append("cell.yaml")
        out["cell"] = {}
    return out


def scene_spec(cell: Dict[str, Any], *, objects: Optional[List[Dict[str, Any]]] = None,
               goal_region: Optional[Dict[str, List[float]]] = None) -> Dict[str, Any]:
    """The backend-neutral sibling scene: exact robot refs + obstacles from cell.yaml +
    RETRIEVED objects placed at perceived poses (the authored sibling_scene.py supplies
    `objects`; this function never invents them)."""
    c = cell.get("cell") or {}
    return {
        "version": 1,
        "robot": {"urdf": cell.get("urdf"), "spheres": cell.get("spheres"),
                  "home": (c.get("home") or {})},
        "obstacles": c.get("obstacles") or [],
        "cameras": c.get("cameras") or [],
        "objects": objects or [],
        "goal_region": goal_region or {},
        "provenance": {"cell_dir": cell.get("cell_dir"), "missing": cell.get("missing", [])},
    }


def to_usda(spec: Dict[str, Any]) -> str:
    """A minimal, valid USDA stage: obstacles as cubes, objects as referenced or primitive
    prims, the robot as a payload reference to the (converted) URDF asset. Isaac's URDF
    importer runs on the worker; here we emit the stage that binds everything."""
    lines = ["#usda 1.0", '(\n    upAxis = "Z"\n    metersPerUnit = 1\n)', "",
             'def Xform "World"', "{"]
    urdf = (spec.get("robot") or {}).get("urdf") or ""
    lines += [f'    def Xform "robot" (payload = @{urdf}@)', "    {", "    }"]
    for i, ob in enumerate(spec.get("obstacles") or []):
        dims = ob.get("dims", [0.1, 0.1, 0.1])
        pos = ob.get("position", [0, 0, 0])
        lines += [
            f'    def Cube "obstacle_{i}"', "    {",
            f"        double size = 1",
            f"        float3 xformOp:scale = ({dims[0]}, {dims[1]}, {dims[2]})",
            f"        double3 xformOp:translate = ({pos[0]}, {pos[1]}, {pos[2]})",
            '        uniform token[] xformOpOrder = ["xformOp:translate", "xformOp:scale"]',
            "    }"]
    for i, obj in enumerate(spec.get("objects") or []):
        pos = obj.get("position", [0, 0, 0])
        asset = obj.get("asset")
        name = obj.get("id", f"object_{i}")
        if asset:
            lines += [f'    def Xform "{name}" (references = @{asset}@)', "    {"]
        else:
            dims = obj.get("dims", [0.03, 0.03, 0.03])
            lines += [f'    def Cube "{name}"', "    {",
                      "        double size = 1",
                      f"        float3 xformOp:scale = ({dims[0]}, {dims[1]}, {dims[2]})"]
        lines += [
            f"        double3 xformOp:translate = ({pos[0]}, {pos[1]}, {pos[2]})",
            '        uniform token[] xformOpOrder = ["xformOp:translate"'
            + (', "xformOp:scale"]' if not asset else "]"),
            "    }"]
    lines += ["}", ""]
    return "\n".join(lines)


def convert(cell_dir: str, out_dir: str, *,
            objects: Optional[List[Dict[str, Any]]] = None,
            goal_region: Optional[Dict[str, List[float]]] = None) -> Dict[str, Any]:
    cell = load_cell(cell_dir)
    spec = scene_spec(cell, objects=objects, goal_region=goal_region)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "scene_spec.json").write_text(json.dumps(spec, indent=1))
    (out / "scene.usda").write_text(to_usda(spec))
    return {"spec": str(out / "scene_spec.json"), "usda": str(out / "scene.usda"),
            "missing": cell.get("missing", [])}
