"""URDF optical-frame I/O (F9). Read a camera's NOMINAL optical transform from the
rig to seed the solve, and write the CALIBRATED result back to the explicit
`*_optical_frame` link — never the camera body centre (so the stereo lens offset is
modelled, not lost). Pure stdlib xml — no deps, runs in the dev `.venv` and CI.

A camera in the URDF is a body link `<cam>` with a child link `<cam>_optical_frame`
joined by a fixed joint whose <origin> holds the optical offset. If the optical link
is absent, `ensure_optical_frame` adds it (identity offset) so we always write to it.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np

from .geometry import Chain, R_to_rpy, make_T, rpy_to_R

# Names/mesh hints that mark a link as a camera body (so the agent can derive the plan
# from the rig without guessing). Extend as needed.
CAMERA_HINTS = ("zed", "realsense", "d4", "oak", "femto", "kinect", "_cam", "camera")
# ...minus the mounting hardware that often carries "camera" in its name (a bracket is
# not a camera). These are CANDIDATES the operator confirms in 3D (Pillar B), not truth.
NON_CAMERA_HINTS = ("holder", "mount", "bracket", "stand", "adapter", "plate", "flange")


def _origin_to_T(origin: Optional[ET.Element]) -> np.ndarray:
    if origin is None:
        return np.eye(4)
    xyz = [float(v) for v in (origin.get("xyz", "0 0 0")).split()]
    rpy = [float(v) for v in (origin.get("rpy", "0 0 0")).split()]
    return make_T(rpy_to_R(rpy), xyz)


def _T_to_origin(origin: ET.Element, T: np.ndarray) -> None:
    rpy = R_to_rpy(T[:3, :3])
    origin.set("xyz", " ".join(f"{v:.9g}" for v in T[:3, 3]))
    origin.set("rpy", " ".join(f"{v:.9g}" for v in rpy))


def _find_optical_joint(root: ET.Element, camera_link: str) -> Optional[ET.Element]:
    """The fixed joint whose child is `<camera_link>_optical_frame` (parent = camera)."""
    optical = f"{camera_link}_optical_frame"
    for joint in root.findall("joint"):
        child = joint.find("child")
        if child is not None and child.get("link") == optical:
            return joint
    return None


def find_camera_links(urdf_path: str) -> List[str]:
    """Detect camera body links so the agent can derive the calibration plan from the
    rig (Pillar B) — matches on the link name or its visual mesh filename. The optical
    frame links themselves are excluded. For the dual-arm xArm+ZED rig this returns
    e.g. ['ZEDX_Mini', 'ZEDX_Mini_1']."""
    root = ET.parse(urdf_path).getroot()
    hits: List[str] = []
    for link in root.findall("link"):
        name = (link.get("name") or "")
        if name.endswith("_optical_frame"):
            continue
        hay = name.lower()
        mesh = link.find("./visual/geometry/mesh")
        if mesh is not None:
            hay += " " + (mesh.get("filename") or "").lower()
        if any(h in hay for h in CAMERA_HINTS) and not any(h in hay for h in NON_CAMERA_HINTS):
            hits.append(name)
    return hits


def _parent_joint_map(root: ET.Element) -> dict:
    """child_link -> (joint_type, parent_link, T_parent_child at angle 0)."""
    m = {}
    for j in root.findall("joint"):
        child = j.find("child")
        parent = j.find("parent")
        if child is None or parent is None:
            continue
        m[child.get("link")] = (j.get("type"), parent.get("link"), _origin_to_T(j.find("origin")))
    return m


def find_flange_link(urdf_path: str, camera_link: str) -> str:
    """The link the camera is rigidly mounted to — i.e. walk up through FIXED joints
    until the connecting joint is non-fixed (the last moving link). For the xArm+ZED rig
    `ZEDX_Mini` -> `link6`. If the camera traces to the root through only fixed joints,
    that root is returned (a static / eye-to-hand camera)."""
    root = ET.parse(urdf_path).getroot()
    m = _parent_joint_map(root)
    cur = camera_link
    seen = set()
    while cur in m and cur not in seen:
        seen.add(cur)
        jtype, parent, _ = m[cur]
        if jtype != "fixed":
            return cur                      # cur's own parent joint moves -> cur is the flange
        cur = parent
    return cur                              # reached the root via fixed joints (static camera)


def link_chain_transform(urdf_path: str, parent_link: str, child_link: str) -> np.ndarray:
    """Compose joint origins from `parent_link` (ancestor) down to `child_link`
    (descendant), revolute joints taken at angle 0 — the nominal rigid transform."""
    root = ET.parse(urdf_path).getroot()
    m = _parent_joint_map(root)
    hops = []
    cur = child_link
    seen = set()
    while cur != parent_link and cur in m and cur not in seen:
        seen.add(cur)
        _, p, T = m[cur]
        hops.append(T)
        cur = p
    if cur != parent_link:
        raise ValueError(f"{child_link} is not a descendant of {parent_link}")
    out = np.eye(4)
    for T in reversed(hops):
        out = out @ T
    return out


def chain_from_urdf(urdf_path: str, flange_link: str):
    """Derive the revolute kinematic Chain base->flange from the URDF (so the bundle's
    FK correction runs on the real robot). Returns (Chain, joint_names, base_link):
      * joint_names is the ordered movable-joint list — the order the bridge must report
        joint states in, so chain.fk(joints) lines up with the controller.
      * leading fixed joints (the world mounting above the first movable joint) are
        trimmed; fixed joints *between* movable joints are folded into the next origin.
    For the xArm rig this yields 6 joints (joint1..joint6), base_link=link_base."""
    root = ET.parse(urdf_path).getroot()
    by_child = {}
    for j in root.findall("joint"):
        child = j.find("child")
        parent = j.find("parent")
        if child is None or parent is None:
            continue
        axis_el = j.find("axis")
        axis = [float(v) for v in (axis_el.get("xyz", "0 0 1").split())] if axis_el is not None else [0.0, 0.0, 1.0]
        by_child[child.get("link")] = {
            "type": j.get("type"), "parent": parent.get("link"),
            "T": _origin_to_T(j.find("origin")), "axis": axis, "name": j.get("name"),
        }
    # walk flange -> root, then reverse to base -> flange
    path = []
    cur = flange_link
    seen = set()
    while cur in by_child and cur not in seen:
        seen.add(cur)
        path.append(by_child[cur])
        cur = by_child[cur]["parent"]
    path.reverse()

    first = next((i for i, j in enumerate(path) if j["type"] != "fixed"), None)
    if first is None:
        raise ValueError(f"no movable joint between the base and {flange_link}")
    base_link = path[first]["parent"]

    origins, axes, names = [], [], []
    pending = np.eye(4)
    for j in path[first:]:
        if j["type"] == "fixed":
            pending = pending @ j["T"]
            continue
        origins.append(pending @ j["T"])
        axes.append(j["axis"])
        names.append(j["name"])
        pending = np.eye(4)
    return Chain(origins, axes), names, base_link


def read_nominal_optical(urdf_path: str, camera_link: str) -> np.ndarray:
    """Nominal camera-body -> optical-frame transform from the URDF (4x4)."""
    root = ET.parse(urdf_path).getroot()
    joint = _find_optical_joint(root, camera_link)
    if joint is None:
        return np.eye(4)
    return _origin_to_T(joint.find("origin"))


def ensure_optical_frame(urdf_path: str, camera_link: str, out_path: Optional[str] = None) -> str:
    """Guarantee `<camera_link>_optical_frame` + its fixed joint exist (identity offset
    if newly added). Returns the path written."""
    tree = ET.parse(urdf_path)
    root = tree.getroot()
    optical = f"{camera_link}_optical_frame"
    if _find_optical_joint(root, camera_link) is None:
        link = ET.SubElement(root, "link")
        link.set("name", optical)
        joint = ET.SubElement(root, "joint")
        joint.set("name", f"{camera_link}_to_optical")
        joint.set("type", "fixed")
        ET.SubElement(joint, "parent").set("link", camera_link)
        ET.SubElement(joint, "child").set("link", optical)
        org = ET.SubElement(joint, "origin")
        org.set("xyz", "0 0 0")
        org.set("rpy", "0 0 0")
    dst = out_path or urdf_path
    tree.write(dst, encoding="unicode" if dst == "-" else "utf-8", xml_declaration=False)
    return dst


def write_calibrated_optical(
    urdf_path: str,
    camera_link: str,
    T_optical: np.ndarray,
    out_path: Optional[str] = None,
    *,
    provenance: str = "measured",
) -> str:
    """Write the calibrated camera-body -> optical-frame transform back into the URDF.
    Creates the optical link/joint first if needed. `provenance` ('sdk' | 'measured' |
    'assumed') is recorded as an attribute so downstream consumers know whether the
    optical model was SDK-given, measured here, or only assumed from the mesh centre
    (the last being the case where metric depth is NOT guaranteed). Returns the path."""
    ensure_optical_frame(urdf_path, camera_link, out_path=out_path or urdf_path)
    dst = out_path or urdf_path
    tree = ET.parse(dst)
    root = tree.getroot()
    joint = _find_optical_joint(root, camera_link)
    assert joint is not None  # ensured above
    origin = joint.find("origin")
    if origin is None:
        origin = ET.SubElement(joint, "origin")
    _T_to_origin(origin, T_optical)
    joint.set("remoroo_optical_provenance", provenance)
    tree.write(dst, encoding="utf-8", xml_declaration=False)
    return dst
