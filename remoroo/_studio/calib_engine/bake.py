"""Bake the SAVED calibration artifacts into `robot.urdf` — the single complete artifact the motion
stack loads. Calibration's `accept` writes per-camera results (`calibration/<cam>.json`) and the
inter-arm transform (`calibration/base_to_base.json`); this RE-APPLIES all of them to the URDF in one
idempotent pass, IN THIS ORDER (an optical frame can be anchored to an arm base, so the bases must
be final first):

  1. base_to_base: the joint that places arm B's base is set so `baseA → baseB == T_baseA_baseB` —
     the bimanual planner's arms are then in their CALIBRATED relative pose (previously the JSON was
     written but applied NOWHERE, so the planner used the URDF's nominal arm-B placement).
  2. eye-in-hand / eye-to-hand / static: each `<cam>_optical_frame` joint origin =
     `inv(flange→body) @ (flange→reference) @ X` (exactly what `accept` writes — recomputed from the
     saved `T_optical`, so a model-gate rewrite that dropped the optical frames is fully recoverable
     WITHOUT re-running calibration capture). `X` is the sense-normalised camera pose
     (`urdf_io.optical_pose_in_reference` — `static` solves the board in the camera, the inverse of
     the other two), and the `flange→reference` factor is identity except for an arm-presented
     eye-to-hand camera, where it is the PRESENTING arm's world placement, named by `<cam>.json`'s
     `reference_link` (see `urdf_io.optical_reference_link`).

Pure URDF + numpy (no cuRobo, no GPU) → unit-tested off-rig on a synthetic two-arm URDF.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import numpy as np
import xml.etree.ElementTree as ET

from . import urdf_io
from .geometry import average_transforms


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


def _hand_eye_entry(calib_dir: Path, camera: str) -> dict:
    """`calibration/hand_eye.yaml`'s per-camera entry, or {} when the file simply has nothing for
    this camera. A MISSING PyYAML or an unparseable file is NOT "nothing to recover" — it raises,
    so the caller reports the real cause instead of telling the operator their calibration is lost
    and to redo the capture."""
    he = Path(calib_dir) / "hand_eye.yaml"
    if not he.exists():
        return {}
    import yaml  # type: ignore  # ImportError propagates: "no module named yaml" is the true cause
    data = yaml.safe_load(he.read_text(encoding="utf-8")) or {}   # YAMLError propagates likewise
    cams = data.get("cameras")
    return cams.get(camera, {}) if isinstance(cams, dict) else {}


def _legacy_reference_link(urdf_path: str, calib_dir: Path, camera: str, kind: str) -> str:
    """`reference_link` for a `<cam>.json` saved before the frame was recorded in it.

    eye_in_hand / static anchor to the camera's own mount link, which the URDF still tells us.
    An eye_to_hand X anchors to the PRESENTING arm's base — a fact only `hand_eye.yaml` kept (its
    `reference`, or the `flange` of the arm that held the board). With neither, the frame is
    genuinely unknowable, so raise: the bake reports it rather than silently re-writing a pose that
    is one arm-mount out."""
    if kind != "eye_to_hand":
        return urdf_io.find_flange_link(urdf_path, camera)
    entry = _hand_eye_entry(calib_dir, camera)
    ref = entry.get("reference")
    if ref:
        return str(ref)
    flange = entry.get("flange")
    if not flange:
        raise ValueError(
            f"{camera}: this eye_to_hand calibration predates `reference_link` and "
            "calibration/hand_eye.yaml does not say which arm presented the board, so the frame its "
            "T_optical is in is unknowable — re-accept this camera's calibration to record it")
    _, _, base_link = urdf_io.chain_from_urdf(urdf_path, str(flange))
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



def _root_links(urdf_path: str) -> set:
    """Links that are never a joint's child — the URDF's world root(s)."""
    root = ET.parse(urdf_path).getroot()
    return {l.get("name") for l in root.findall("link")} - {
        j.find("child").get("link") for j in root.findall("joint") if j.find("child") is not None}


def flange_body_world(urdf_path: str, flange: str) -> np.ndarray:
    """world -> the camera's mount link. Identity when the mount IS the world root, which is the
    only case the body inversion applies to — kept general so an intermediate fixed mount works."""
    roots = _root_links(urdf_path)
    world = sorted(roots)[0] if roots else flange
    return np.eye(4) if flange == world else urdf_io.link_chain_transform(urdf_path, world, flange)


def _visual_mesh(root, link_name: str):
    """The mesh filename of a link's first visual, or None — the only per-camera-MODEL identity the
    URDF carries."""
    for link in root.findall("link"):
        if link.get("name") != link_name:
            continue
        for vis in link.findall("visual"):
            mesh = vis.find("geometry/mesh")
            if mesh is not None and mesh.get("filename"):
                return mesh.get("filename")
    return None


def _lens_offset(urdf_path: str, camera_link: str):
    """The camera MODEL's lens offset (body origin -> optical centre), and where it came from.

    A world-mounted camera's body pose is a hand placement, so calibration cannot separate "where the
    box is" from "where the lens sits inside it" — only their PRODUCT is measured. Borrow the split
    from same-model cameras on THIS rig whose body pose is a machined mount (arm-mounted), which is
    the only place the split is actually known. Never borrow from another world-mounted camera: its
    body pose is a hand placement too, so its "lens offset" is just somebody else's placement error.

    There is deliberately no hardcoded constant and no part-library lookup: the shipped library has
    no entry for this camera and the two it does have are `license: "primitive-stand-in"` with
    invented numbers. Falls back to identity — body origin AT the lens — which is honest: it keeps
    the measured world pose exact and simply declines to guess where the box is around it."""
    root = ET.parse(urdf_path).getroot()
    mine = _visual_mesh(root, camera_link)
    if mine is None:
        return np.eye(4), "assumed"
    roots = {l.get("name") for l in root.findall("link")} - {
        j.find("child").get("link") for j in root.findall("joint") if j.find("child") is not None}
    peers = []
    for link in root.findall("link"):
        name = link.get("name")
        if name == camera_link or _visual_mesh(root, name) != mine:
            continue
        if urdf_io.find_flange_link(urdf_path, name) in roots:
            continue                                        # another hand-placed camera: no information
        T = urdf_io.read_nominal_optical(urdf_path, name)
        if not np.allclose(T, np.eye(4)):
            peers.append(T)
    if peers:
        return average_transforms(peers), "assumed"
    return np.eye(4), "assumed"


def _body_joint_of(root, camera_link: str):
    """The joint that PLACES a camera (child == the camera link), or None."""
    for j in root.findall("joint"):
        c = j.find("child")
        if c is not None and c.get("link") == camera_link:
            return j
    return None


def _write_body_from_calibration(urdf_path: str, camera_link: str, T_world_optical: np.ndarray,
                                 lens: np.ndarray) -> bool:
    """Place a WORLD-MOUNTED camera's BODY from its calibration, keeping the lens offset physical.

    The operator hand-places the camera and then calibrates; calibration outranks the placement by
    ~350x (1.4 mm sigma vs a drag in a 3D editor). Pinning the hand placement forever and dumping the
    whole correction into `<cam>_to_optical` produced a 496 mm "lens offset" on a 93 mm camera, left
    the body mesh half a metre from the real camera, and — the part that actually bites — left the
    camera's COLLISION geometry there too, where nothing reads the optical frame to correct it.

    Writes `body = inv(world->parent) @ (world->optical) @ inv(lens)`. The composed world->optical is
    unchanged by construction, so this is a pure re-parameterisation: lossless, and idempotent
    because it never reads the old body pose. Arm-mounted cameras are NOT touched — their body pose
    is a machined bracket and moving it would detach the camera from its mount."""
    tree = ET.parse(urdf_path)
    root = tree.getroot()
    joint = _body_joint_of(root, camera_link)
    if joint is None:
        return False
    parent = joint.find("parent").get("link")
    roots = {l.get("name") for l in root.findall("link")} - {
        j.find("child").get("link") for j in root.findall("joint") if j.find("child") is not None}
    world = sorted(roots)[0] if roots else parent
    T_world_parent = (urdf_io.link_chain_transform(urdf_path, world, parent)
                      if parent != world else np.eye(4))
    origin = joint.find("origin")
    if origin is None:
        origin = ET.SubElement(joint, "origin")
    urdf_io._T_to_origin(origin, _inv(T_world_parent) @ T_world_optical @ _inv(lens))
    joint.set("remoroo_body_from_calibration", "derived")
    tree.write(urdf_path, encoding="utf-8", xml_declaration=False)
    return True


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

    # (1) base_to_base FIRST — it MOVES arm B's base, and an eye_to_hand optical frame is ANCHORED to
    # a base (step 2), so the arm placement must be final before any optical frame is composed
    # against it. Reversed, a single pass writes those frames against the NOMINAL base and the bake
    # stops converging in one run. `apply_base_to_base` reads only flange/base chains and never an
    # optical frame, so running it first changes nothing else.
    b2b = calib_dir / "base_to_base.json"
    if b2b.exists():
        try:
            d = json.loads(b2b.read_text(encoding="utf-8"))
            T_ab = np.asarray(d["T_base_to_base"], float).reshape(4, 4)
            report["base_to_base"] = apply_base_to_base(urdf_path, d["arm_a"], d["arm_b"], T_ab)
        except Exception as e:  # noqa: BLE001
            report["errors"].append(f"base_to_base.json: {type(e).__name__}: {e}")

    # (2) per-camera optical frames — body→optical = inv(flange→body) @ (flange→reference) @ X,
    # composed against the CALIBRATED arm placement written above.
    #
    # ARM-MOUNTED CAMERAS GO FIRST. A world-mounted camera's body is derived from calibration, and
    # the lens offset it needs is borrowed from a same-model camera on a machined mount — which has
    # to be written before it can be read. Baking in directory order instead made the result depend
    # on filenames (`overhead_cam.json` sorts before `wrist_cam.json`), so the first pass found no
    # peer and the second found one: the camera moved between two consecutive bakes.
    artifacts = []
    for jf in sorted(calib_dir.glob("*.json")):
        if jf.name == "base_to_base.json":
            continue
        try:
            d = json.loads(jf.read_text(encoding="utf-8"))
            if d.get("kind") not in ("eye_in_hand", "eye_to_hand", "static"):
                continue
            flange = urdf_io.find_flange_link(urdf_path, d.get("camera"))
            artifacts.append((jf, d, flange))
        except Exception as e:  # noqa: BLE001 — one bad file must not abort the rest
            report["errors"].append(f"{jf.name}: {type(e).__name__}: {e}")

    roots = _root_links(urdf_path)
    for jf, d, flange in sorted(artifacts, key=lambda a: a[2] in roots):
        try:
            cam, kind = d.get("camera"), d.get("kind")
            T_optical = np.asarray(d["T_optical"], float).reshape(4, 4)
            flange_body = urdf_io.link_chain_transform(urdf_path, flange, cam)
            # Carry X from the frame it was SOLVED in into the camera's mount frame. Identity for
            # eye_in_hand/static; the presenting arm's world placement for eye_to_hand, which this
            # composition used to drop (see urdf_io.optical_reference_link).
            reference = d.get("reference_link") or _legacy_reference_link(urdf_path, calib_dir, cam, kind)
            T_ref = urdf_io.optical_reference_transform(urdf_path, cam, reference)
            # `static` solves the BOARD in the camera — the inverse sense of the other two kinds.
            X = urdf_io.optical_pose_in_reference(T_optical, kind)
            entry = {"camera": cam, "kind": kind, "reference_link": reference}
            if flange in roots:
                # WORLD-MOUNTED: the body pose is a hand placement, not a measurement, so calibration
                # gets to move it. Keep the lens offset physical and derive the body from the
                # measured world pose (see _write_body_from_calibration).
                lens, lens_prov = _lens_offset(urdf_path, cam)
                world_optical = flange_body_world(urdf_path, flange) @ T_ref @ X
                if _write_body_from_calibration(urdf_path, cam, world_optical, lens):
                    urdf_io.write_calibrated_optical(urdf_path, cam, lens, provenance=lens_prov)
                    entry.update(body="derived", lens_provenance=lens_prov)
                    report["optical"].append(entry)
                    continue
            # ARM-MOUNTED: the body pose is a machined bracket. Only the lens offset is calibrated.
            urdf_io.write_calibrated_optical(urdf_path, cam, _inv(flange_body) @ T_ref @ X,
                                             provenance=d.get("provenance", "measured"))
            entry["body"] = "authored"
            report["optical"].append(entry)
        except Exception as e:  # noqa: BLE001 — one bad file must not abort the rest
            report["errors"].append(f"{jf.name}: {type(e).__name__}: {e}")
    return report
