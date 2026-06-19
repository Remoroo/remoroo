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
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
        lim_el = j.find("limit")
        limit = ([float(lim_el.get("lower")), float(lim_el.get("upper"))]
                 if lim_el is not None and lim_el.get("lower") is not None and lim_el.get("upper") is not None
                 else None)  # None = continuous / unspecified → callers treat as wide-open
        by_child[child.get("link")] = {
            "type": j.get("type"), "parent": parent.get("link"),
            "T": _origin_to_T(j.find("origin")), "axis": axis, "name": j.get("name"), "limit": limit,
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

    origins, axes, names, limits, types = [], [], [], [], []
    pending = np.eye(4)
    for j in path[first:]:
        if j["type"] == "fixed":
            pending = pending @ j["T"]
            continue
        origins.append(pending @ j["T"])
        axes.append(j["axis"])
        names.append(j["name"])
        limits.append(j.get("limit"))
        types.append("prismatic" if j["type"] == "prismatic" else "revolute")
        pending = np.eye(4)
    return Chain(origins, axes, limits=limits, types=types), names, base_link


# --------------------------------------------------------------------------- #
# The CANONICAL ARM MAP — the single source of truth tying arm name ↔ side ↔   #
# base/ee/flange links ↔ ordered joints ↔ camera. Consumed by cuRobo (per-arm  #
# base/ee/cspace), the Bridge (arm→joint order), calibration, and the Studio.  #
# --------------------------------------------------------------------------- #
@dataclass
class ArmSpec:
    """One arm of the cell, derived from the URDF (operator-verifies `side`)."""
    name: str                       # cell.yaml arm name when known, else provisional
    side: str                       # "left" | "right" | "center" — a GEOMETRIC GUESS to verify
    base_link: str                  # the SHARED planning root (cuRobo base_link)
    mount_link: str                 # first link of this arm's subtree (under the root)
    ee_link: str                    # end-effector (cuRobo ee_link for Cartesian planning)
    flange_link: str                # the camera flange (calibration binds its chain here)
    joint_names: List[str]          # this arm's movable joints, URDF ORDER (the bridge contract)
    camera: str                     # the URDF camera link on this arm ("" if none)
    dof: int

    def to_dict(self) -> dict:
        return asdict(self)


def _children_map(root: ET.Element) -> Dict[str, list]:
    m: Dict[str, list] = {}
    for j in root.findall("joint"):
        c, p = j.find("child"), j.find("parent")
        if c is None or p is None:
            continue
        m.setdefault(p.get("link"), []).append((c.get("link"), j.get("type")))
    return m


def _ee_below(root: ET.Element, flange: str) -> str:
    """The best end-effector link reachable from `flange` through FIXED joints: prefer a name
    that looks like a tool tip (tcp/tool/ee/grip), else the deepest fixed non-camera leaf. So
    a wrist camera on the flange isn't mistaken for the end-effector."""
    ch = _children_map(root)
    PREF = ("tcp", "tool", "_ee", "grip", "hand")
    best, best_score = flange, -1.0
    stack, seen = [(flange, 0)], {flange}
    while stack:
        link, depth = stack.pop()
        low = link.lower()
        cam = low.endswith("_optical_frame") or (
            any(h in low for h in CAMERA_HINTS) and not any(h in low for h in NON_CAMERA_HINTS))
        score = depth + (100 if any(p in low for p in PREF) else 0)
        if not cam and score > best_score:
            best_score, best = score, link
        for (c, t) in ch.get(link, []):
            if t == "fixed" and c not in seen:
                seen.add(c)
                stack.append((c, depth + 1))
    return best


def enumerate_arms(urdf_path: str, camera_to_arm: Optional[Dict[str, str]] = None) -> List[ArmSpec]:
    """Derive the canonical arm map from the URDF. Each maximal movable chain under the shared
    root is one arm; camera-bearing chains are matched first (so the wrist camera binds its arm),
    then any camera-less movable chains. `side` is a GEOMETRIC guess from the mount's lateral (Y)
    offset — the operator VERIFIES it (the live-mirror wiggle test) since a URDF can have L/R
    swapped. `camera_to_arm` (cell.yaml cameras[].name→attached_to) names the arms; absent →
    provisional names. Pure stdlib + the existing chain derivation; fully testable off-robot."""
    root = ET.parse(urdf_path).getroot()
    children = {j.find("child").get("link") for j in root.findall("joint") if j.find("child") is not None}
    all_links = [l.get("name") for l in root.findall("link")]
    roots = [l for l in all_links if l not in children]
    base_link = roots[0] if roots else (all_links[0] if all_links else "world")

    arms: List[ArmSpec] = []
    used: set = set()

    def _add(flange: str, camera: str) -> None:
        try:
            _, names, mount = chain_from_urdf(urdf_path, flange)
        except ValueError:
            return
        if not names or set(names) & used:
            return
        used.update(names)
        arms.append(ArmSpec(name="", side="", base_link=base_link, mount_link=mount,
                            ee_link=_ee_below(root, flange), flange_link=flange,
                            joint_names=list(names), camera=camera, dof=len(names)))

    for cam in find_camera_links(urdf_path):       # camera-anchored arms first
        _add(find_flange_link(urdf_path, cam), cam)

    pmap = _parent_joint_map(root)
    cmap = _children_map(root)
    for link in all_links:                          # camera-less movable-chain tips
        pj = pmap.get(link)
        if not pj or pj[0] == "fixed":
            continue
        if any(t != "fixed" for (_, t) in cmap.get(link, [])):
            continue                                # has a movable child → not a tip
        _add(link, "")

    # side: lateral (Y) offset of the mount in the base frame; split L/R about the midline.
    ys: List[float] = []
    for a in arms:
        try:
            ys.append(float(link_chain_transform(urdf_path, base_link, a.mount_link)[1, 3]))
        except Exception:  # noqa: BLE001
            ys.append(0.0)
    mid = (max(ys) + min(ys)) / 2.0 if ys else 0.0
    cam2arm = camera_to_arm or {}
    for i, a in enumerate(arms):
        a.side = "center" if len(arms) == 1 else ("left" if ys[i] > mid else "right")
        a.name = cam2arm.get(a.camera) or a.camera or f"arm_{i + 1}"
    return arms


def arm_map_dict(urdf_path: str, camera_to_arm: Optional[Dict[str, str]] = None) -> dict:
    """The canonical arm map as a JSON/YAML-friendly dict (what gets written to
    robot_model/arms.yaml and shipped to the Studio)."""
    arms = enumerate_arms(urdf_path, camera_to_arm)
    return {"base_link": arms[0].base_link if arms else "", "arms": [a.to_dict() for a in arms]}


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
