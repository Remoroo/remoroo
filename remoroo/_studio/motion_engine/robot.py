"""Load + V2-normalise the cell's robot artifacts (`arms.yaml`, `collision_spheres.yml`).

Two jobs, both PURE (no cuRobo/torch — unit-tested off-GPU):

1. **Read** the canonical arm map and the collision-sphere mapping the calibration/model gates
   produced (`collision_spheres.yml` is a full classic `robot_cfg`; the spheres live at
   `robot_cfg.kinematics.collision_spheres`). `robotcfg.py` rebuilds the V2 cspace from the arm
   map, so here we only dig out the `{link: [{center, radius}]}` mapping + the buffer.

2. **Normalise + sanity-check** for the two failure modes the agent already hit on V2:
   - classic-only keys (`load_collision_spheres`, `num_envs`) that V2's loader rejects — stripped.
   - the mm-scale sphere degeneracy (radii/centres authored in millimetres, so the robot is
     wrapped in metre-wide balls or pinprick ones) — a LOUD `sphere_health()` the commission gate
     surfaces BEFORE any motion. A bad sphere map makes "collision-free" meaningless.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

# classic-only kinematics keys that V2's robot loader does not accept (must be stripped)
_CLASSIC_ONLY_KEYS = ("load_collision_spheres", "num_envs", "ee_link", "retract_config")


def _load_yaml(path: Path) -> dict:
    import yaml  # type: ignore

    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _kinematics(cfg: dict) -> dict:
    """Dig to the kinematics dict regardless of nesting (full robot_cfg vs bare kinematics)."""
    if "robot_cfg" in cfg:
        cfg = cfg["robot_cfg"]
    return cfg.get("kinematics", cfg) if isinstance(cfg, dict) else {}


def extract_spheres(cfg: dict) -> Dict[str, List[dict]]:
    """The `{link: [{center, radius}]}` mapping, wherever it lives in the file."""
    kin = _kinematics(cfg)
    spheres = kin.get("collision_spheres") or cfg.get("collision_spheres") or {}
    out: Dict[str, List[dict]] = {}
    for link, lst in spheres.items():
        out[link] = [{"center": list(s["center"]), "radius": float(s["radius"])} for s in (lst or [])]
    return out


def load_robot_config(cell_dir: str) -> dict:
    """The AUTHORED kinematic config — `cell.yaml: groups` (or `arms[]` back-compat), validated and
    projected against the URDF via the shipped `calib_engine.urdf_io`. The SINGLE source of truth;
    there is no derived `arms.yaml`. Returns `{groups:[{name, kind, base_link, tip_links[],
    joint_names[], cameras[], tags}], cameras, ignore_joints}`."""
    from calib_engine import urdf_io

    base = Path(cell_dir)
    cellp, urdf = base / "cell.yaml", base / "robot_model" / "robot.urdf"
    if not cellp.exists():
        raise FileNotFoundError(f"missing {cellp} — no cell description")
    if not urdf.exists():
        raise FileNotFoundError(f"missing {urdf} — model the cell first")
    cfg = urdf_io.robot_config(_load_yaml(cellp), str(urdf))
    if not cfg.get("groups"):
        raise ValueError("no kinematic groups authored — run the model gate (cell.yaml: groups)")
    return cfg


def load_spheres(cell_dir: str) -> Tuple[Dict[str, List[dict]], float]:
    """Read `robot_model/collision_spheres.yml` → (spheres_by_link, sphere_buffer)."""
    p = Path(cell_dir) / "robot_model" / "collision_spheres.yml"
    if not p.exists():
        raise FileNotFoundError(f"missing {p} — run the model gate (no collision spheres)")
    cfg = _load_yaml(p)
    spheres = extract_spheres(cfg)
    buffer = float(_kinematics(cfg).get("collision_sphere_buffer", 0.005))
    if not spheres:
        raise ValueError(f"{p} has no collision_spheres — the robot has no collision geometry")
    return spheres, buffer


def normalise_kinematics(kin: dict) -> dict:
    """Strip classic-only keys V2 rejects; return a shallow copy safe to merge into a V2 cfg."""
    return {k: v for k, v in dict(kin).items() if k not in _CLASSIC_ONLY_KEYS}


def sphere_health(spheres: Dict[str, List[dict]]) -> dict:
    """LOUD sanity check for the mm-scale degeneracy. Healthy cuRobo spheres are in METRES:
    radii ~[5 mm, 0.5 m] and centres within a couple of metres of each link origin. Radii in the
    micrometre range (authored in mm → /1000) or metre-wide balls mean planning is meaningless —
    the commission gate must refuse to move until this is fixed."""
    radii: List[float] = []
    max_center = 0.0
    n_spheres = 0
    for lst in spheres.values():
        for s in lst or []:
            r = float(s["radius"])
            radii.append(r)
            n_spheres += 1
            max_center = max(max_center, max(abs(float(c)) for c in s["center"]))
    warnings: List[str] = []
    if n_spheres == 0:
        return {"ok": False, "n_links": len(spheres), "n_spheres": 0,
                "warnings": ["no collision spheres at all — the robot has no collision geometry"]}

    radii.sort()
    rmin, rmax = radii[0], radii[-1]
    rmed = radii[len(radii) // 2]
    if rmed < 1e-3:
        warnings.append(
            f"median sphere radius is {rmed*1000:.2f} mm — looks like the radii were authored in "
            f"MILLIMETRES, not metres (the known degeneracy). Planning would treat the robot as "
            f"a cloud of pinpricks.")
    if rmax > 0.6:
        warnings.append(f"largest sphere radius is {rmax:.2f} m — implausibly large; check the units.")
    if max_center > 5.0:
        warnings.append(f"a sphere centre is {max_center:.1f} m from its link origin — likely mm/m unit mix-up.")
    return {
        "ok": not warnings,
        "n_links": len(spheres),
        "n_spheres": n_spheres,
        "radius_m": {"min": rmin, "median": rmed, "max": rmax},
        "max_center_m": max_center,
        "warnings": warnings,
    }
