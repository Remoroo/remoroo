"""Bake the SAVED calibration artifacts into `robot.urdf` — the single complete artifact the motion
stack loads. Calibration's `accept` writes per-camera results (`calibration/<cam>.json`) and the
inter-arm transform (`calibration/base_to_base.json`); this RE-APPLIES all of them to the URDF in one
idempotent pass:

  - eye-in-hand / eye-to-hand: each `<cam>_optical_frame` joint origin = `inv(flange→body) @ T_optical`
    (exactly what `accept` writes — recomputed from the saved `T_optical`, so a model-gate rewrite that
    dropped the optical frames is fully recoverable WITHOUT re-running calibration capture).
  - base_to_base: the joint that places arm B's base is set so `baseA → baseB == T_baseA_baseB` — the
    bimanual planner's arms are then in their CALIBRATED relative pose (previously the JSON was written
    but applied NOWHERE, so the planner used the URDF's nominal arm-B placement).

Pure URDF + numpy (no cuRobo, no GPU) → unit-tested off-rig on a synthetic two-arm URDF.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import numpy as np
import xml.etree.ElementTree as ET

from . import urdf_io


def _inv(T: np.ndarray) -> np.ndarray:
    R, t = T[:3, :3], T[:3, 3]
    out = np.eye(4)
    out[:3, :3] = R.T
    out[:3, 3] = -R.T @ t
    return out


def _root_of(pmap: dict, link: str) -> str:
    """The topmost ancestor of `link` (the world/root link) via the child→parent map."""
    cur, seen = link, set()
    while cur in pmap and cur not in seen:
        seen.add(cur)
        cur = pmap[cur][1]
    return cur


def _base_link_of(urdf_path: str, camera_link: str) -> str:
    """The base (planning root) of the arm carrying `camera_link`: its flange's chain base."""
    flange = urdf_io.find_flange_link(urdf_path, camera_link)
    _, _, base_link = urdf_io.chain_from_urdf(urdf_path, flange)
    return base_link


def apply_base_to_base(urdf_path: str, cam_a: str, cam_b: str, T_ab: np.ndarray) -> dict:
    """Set the joint that places arm B's base so that `baseA → baseB == T_ab` (the calibrated inter-arm
    transform). General: composes through the shared world root, so it's correct whether the two bases
    are direct children of `world` or sit under intermediate fixed mounts. Writes `robot.urdf` in place."""
    T_ab = np.asarray(T_ab, float).reshape(4, 4)
    base_a = _base_link_of(urdf_path, cam_a)
    base_b = _base_link_of(urdf_path, cam_b)
    root = ET.parse(urdf_path).getroot()
    pmap = urdf_io._parent_joint_map(root)
    if base_b not in pmap:
        raise ValueError(f"arm B base {base_b!r} has no placement joint to calibrate (it IS the world root)")
    _, parent_b, _ = pmap[base_b]
    world = _root_of(pmap, base_a)
    T_world_baseA = urdf_io.link_chain_transform(urdf_path, world, base_a) if base_a != world else np.eye(4)
    T_world_parentB = urdf_io.link_chain_transform(urdf_path, world, parent_b) if parent_b != world else np.eye(4)
    new_origin = _inv(T_world_parentB) @ (T_world_baseA @ T_ab)      # origin of the parent_b→baseB joint
    tree = ET.parse(urdf_path)
    for j in tree.getroot().findall("joint"):
        c = j.find("child")
        if c is not None and c.get("link") == base_b:
            org = j.find("origin")
            if org is None:
                org = ET.SubElement(j, "origin")
            urdf_io._T_to_origin(org, new_origin)
            j.set("remoroo_base_to_base", "measured")
            tree.write(urdf_path, encoding="utf-8", xml_declaration=False)
            return {"base_a": base_a, "base_b": base_b, "parent_b": parent_b, "joint": j.get("name")}
    raise ValueError(f"no joint places arm B base {base_b!r}")


def bake_calibration(urdf_path: str, calib_dir: str) -> dict:
    """Re-apply EVERY saved calibration artifact in `calib_dir` to `robot.urdf`. Idempotent. Returns a
    report {optical:[...], base_to_base:{...}|None, errors:[...]} — the operator's 'apply saved
    calibration' action and the recovery path after a model-gate rewrite dropped the frames."""
    urdf_path, calib_dir = str(urdf_path), Path(calib_dir)
    report: dict = {"optical": [], "base_to_base": None, "errors": []}
    if not Path(urdf_path).exists():
        report["errors"].append(f"no robot.urdf at {urdf_path}")
        return report
    if not calib_dir.exists():
        report["errors"].append(f"no calibration dir at {calib_dir}")
        return report

    # (1) per-camera optical frames — recompute body→optical = inv(flange→body) @ T_optical
    for jf in sorted(calib_dir.glob("*.json")):
        if jf.name == "base_to_base.json":
            continue
        try:
            d = json.loads(jf.read_text(encoding="utf-8"))
            if d.get("kind") not in ("eye_in_hand", "eye_to_hand", "static"):
                continue
            cam = d.get("camera")
            T_optical = np.asarray(d["T_optical"], float).reshape(4, 4)
            flange = urdf_io.find_flange_link(urdf_path, cam)
            flange_body = urdf_io.link_chain_transform(urdf_path, flange, cam)
            body_optical = _inv(flange_body) @ T_optical
            urdf_io.write_calibrated_optical(urdf_path, cam, body_optical,
                                             provenance=d.get("provenance", "measured"))
            report["optical"].append({"camera": cam, "kind": d.get("kind")})
        except Exception as e:  # noqa: BLE001 — one bad file must not abort the rest
            report["errors"].append(f"{jf.name}: {type(e).__name__}: {e}")

    # (2) base_to_base — place arm B's base at its CALIBRATED pose relative to arm A
    b2b = calib_dir / "base_to_base.json"
    if b2b.exists():
        try:
            d = json.loads(b2b.read_text(encoding="utf-8"))
            T_ab = np.asarray(d["T_base_to_base"], float).reshape(4, 4)
            info = apply_base_to_base(urdf_path, d["arm_a"], d["arm_b"], T_ab)
            report["base_to_base"] = info
        except Exception as e:  # noqa: BLE001
            report["errors"].append(f"base_to_base.json: {type(e).__name__}: {e}")
    return report
