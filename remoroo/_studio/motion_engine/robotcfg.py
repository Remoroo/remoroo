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


def active_tool_frames(arm_map: dict, active_arms: Optional[Sequence[str]] = None) -> List[str]:
    """The ee_link of each active arm — the `tool_frames` a `GoalToolPose` can target."""
    arms = arm_map.get("arms") or []
    names = list(active_arms) if active_arms else [a["name"] for a in arms]
    by_name = {a["name"]: a for a in arms}
    return _dedup([by_name[n]["ee_link"] for n in names if n in by_name])


def build_v2_robot_cfg(
    arm_map: dict,
    spheres_by_link: Dict[str, List[dict]],
    *,
    active_arms: Optional[Sequence[str]] = None,
    urdf_path: str = "robot.urdf",
    sphere_buffer: float = 0.005,
    limits: Optional[dict] = None,
    default_joint_position: Optional[Dict[str, float]] = None,
) -> dict:
    """A cuRoboV2 `robot_cfg` planning the `active_arms` (default: ALL arms in the map).

    `base_link` is the shared root; `tool_frames` = each active arm's `ee_link`;
    `cspace.joint_names` = the union of those arms' joints; every other arm's joints are
    `lock_joints` (held, still collision). `collision_spheres` cover ALL links so locked limbs and
    the body are obstacles. `limits` (from `safety.py`) caps acceleration/jerk so the planner's
    trajectory is dynamics-safe BY CONSTRUCTION. Raises on an empty arm map (no chain = no plan)."""
    arms = arm_map.get("arms") or []
    if not arms:
        raise ValueError("arm map has no arms — cannot build a V2 robot_cfg (no kinematic chain)")
    by_name = {a["name"]: a for a in arms}
    active_names = list(active_arms) if active_arms else [a["name"] for a in arms]
    missing = [n for n in active_names if n not in by_name]
    if missing:
        raise ValueError(f"unknown arm(s) {missing}; map has {list(by_name)}")

    base_link = arm_map.get("base_link") or arms[0]["base_link"]
    tool_frames = _dedup([by_name[n]["ee_link"] for n in active_names])
    planned = _dedup([j for n in active_names for j in by_name[n]["joint_names"]])
    planned_set = set(planned)
    # everything that belongs to a NON-active arm AND isn't also an active joint → locked
    defaults = dict(default_joint_position or {})
    locked = {
        j: float(defaults.get(j, 0.0))
        for a in arms if a["name"] not in active_names
        for j in a["joint_names"] if j not in planned_set
    }

    lim = limits or {}
    cspace = {
        "joint_names": planned,
        "default_joint_position": [float(defaults.get(j, 0.0)) for j in planned],
        "null_space_weight": [1.0] * len(planned),
        "cspace_distance_weight": [1.0] * len(planned),
        "max_acceleration": float(lim.get("max_acceleration", 15.0)),
        "max_jerk": float(lim.get("max_jerk", 500.0)),
    }
    if lim.get("max_velocity") is not None:
        cspace["max_velocity"] = float(lim["max_velocity"])

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
                "lock_joints": locked or None,
                "cspace": cspace,
            }
        }
    }
