"""Build a cuRoboV2 `robot_cfg` from the canonical arm map — PURE dicts, no cuRobo/torch import,
so the structure is unit-tested off-GPU for 1 / 2 / many TCPs.

This is the V2 sibling of `calib_engine/curobo_cfg.build_robot_cfg` (which emits the CLASSIC
shape: a single `ee_link`). V2 plans over **`tool_frames`** — a LIST of end-effector links driven
synchronously over one combined cspace — so the count-agnostic design falls straight out of the
arm map:

    tool_frames        = the ee_link of every ACTIVE arm (the TCPs this plan will drive)
    cspace.joint_names = the UNION of those arms' joints (URDF order, de-duplicated)
    lock_joints        = every OTHER arm's joints, held fixed (still collision-checked via spheres)

Driving one arm, two arms, or a humanoid's many limbs is the SAME code path — you just pass a
different `active_arms`. The MotionStack builds one config (and caches one planner) per requested
TCP set; nothing here ever special-cases "dual arm".
"""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence


def _dedup(seq: Sequence[str]) -> List[str]:
    """Preserve first-seen order while removing duplicates (shared joints across limbs)."""
    seen: set = set()
    out: List[str] = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def active_tool_frames(config: dict, active_groups: Optional[Sequence[str]] = None) -> List[str]:
    """The tip_link(s) of each active group — the `tool_frames` a `GoalToolPose` can target."""
    groups = config.get("groups") or []
    names = list(active_groups) if active_groups else [g["name"] for g in groups]
    by_name = {g["name"]: g for g in groups}
    return _dedup([t for n in names if n in by_name for t in (by_name[n].get("tip_links") or [])])


def build_v2_robot_cfg(
    config: dict,
    spheres_by_link: Dict[str, List[dict]],
    *,
    active_groups: Optional[Sequence[str]] = None,
    urdf_path: str = "robot.urdf",
    sphere_buffer: float = 0.005,
    limits: Optional[dict] = None,
    default_joint_position: Optional[Dict[str, float]] = None,
    self_collision_ignore: Optional[Dict[str, List[str]]] = None,
) -> dict:
    """A cuRoboV2 `robot_cfg` planning the `active_groups` (default: ALL groups in the config).

    `base_link` is the shared root; `tool_frames` = each active group's `tip_links`;
    `cspace.joint_names` = the union of those groups' joints; every other group's joints are
    `lock_joints` (held, still collision). `collision_spheres` cover ALL links so locked limbs and
    the body are obstacles. `limits` (from `safety.py`) caps acceleration/jerk so the planner's
    trajectory is dynamics-safe BY CONSTRUCTION. Morphology-agnostic: arm/leg/wheel/head are the
    same code path. Raises on an empty config (no chain = no plan)."""
    groups = config.get("groups") or []
    if not groups:
        raise ValueError("config has no groups — cannot build a V2 robot_cfg (no kinematic chain)")
    by_name = {g["name"]: g for g in groups}
    active_names = list(active_groups) if active_groups else [g["name"] for g in groups]
    missing = [n for n in active_names if n not in by_name]
    if missing:
        raise ValueError(f"unknown group(s) {missing}; config has {list(by_name)}")

    base_link = config.get("base_link") or groups[0].get("base_link")
    tool_frames = _dedup([t for n in active_names for t in (by_name[n].get("tip_links") or [])])
    # cuRobo convention (cf. franka.yml): `cspace.joint_names` = ALL actuated joints; `lock_joints`
    # holds the INACTIVE ones; cuRobo derives the ACTIVE set as joint_names MINUS lock_joints. Sizing
    # the cspace to only the active subset (and locking joints that aren't IN the cspace) left cuRobo
    # with a malformed cspace → 'NoneType' object has no attribute 'copy' at planner build.
    all_joints = _dedup([j for g in groups for j in g["joint_names"]])
    active_set = {j for n in active_names for j in by_name[n]["joint_names"]}
    defaults = dict(default_joint_position or {})
    locked = {j: float(defaults.get(j, 0.0)) for j in all_joints if j not in active_set}

    lim = limits or {}
    cspace = {
        "joint_names": all_joints,
        "default_joint_position": [float(defaults.get(j, 0.0)) for j in all_joints],
        "null_space_weight": [1.0] * len(all_joints),
        "cspace_distance_weight": [1.0] * len(all_joints),
        "max_acceleration": float(lim.get("max_acceleration", 15.0)),
        "max_jerk": float(lim.get("max_jerk", 500.0)),
    }

    return {
        "robot_cfg": {
            "kinematics": {
                "format_version": 2.0,
                "urdf_path": urdf_path,
                "base_link": base_link,
                "tool_frames": tool_frames,
                "collision_link_names": list(spheres_by_link.keys()) or None,
                "collision_spheres": spheres_by_link,
                "collision_sphere_buffer": float(sphere_buffer),
                # Both default to None in cuRobo's loader and are accessed UNCONDITIONALLY when
                # building the collision model (`self_collision_buffer.copy()`,
                # `self_collision_ignore.keys()`), so a missing field crashes planner build. Provide
                # both explicitly: an empty per-link buffer, and the parent↔child ADJACENCY ignore
                # (the loader path does NOT auto-generate it — without it every config self-collides).
                "self_collision_buffer": {},
                "self_collision_ignore": dict(self_collision_ignore or {}),
                "lock_joints": locked or None,
                "cspace": cspace,
            }
        }
    }
