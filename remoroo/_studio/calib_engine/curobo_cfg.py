"""Build cuRobo configs from the canonical arm map — PURE dicts, no cuRobo import, so the
structure is unit-tested off-GPU. This is the fix for the empty robot_cfg the operator found:
the old writer emitted only collision_spheres (no base_link / ee_link / cspace.joint_names), so
cuRobo had no kinematic chain and motion_gen silently fell back to "approximate". Here we emit a
PROPER per-arm robot_cfg (base_link, ee_link, the arm's cspace joints, the other arm LOCKED) and
a proper world (cuboids/meshes) for static obstacles.

cuRobo plans Cartesian goals for ONE ee_link at a time over cspace.joint_names; the rest of a
multi-arm robot is held via lock_joints (still collision-checked via its spheres). So a dual-arm
cell gets one config per arm; you load the one for the arm you're planning.
"""
from __future__ import annotations

from typing import Dict, List, Optional


def build_robot_cfg(
    arm_map: dict,
    spheres_by_link: Dict[str, List[dict]],
    *,
    plan_arm: Optional[str] = None,
    urdf_path: str = "robot.urdf",
    sphere_buffer: float = 0.005,
) -> dict:
    """A cuRobo `robot_cfg` for planning the arm `plan_arm` (default: the first). base_link is the
    shared root; ee_link + cspace.joint_names are that arm's; every OTHER arm's joints are
    lock_joints (held at 0, still collision). collision_spheres cover ALL links so the locked arm
    and the rest of the body are obstacles. Raises on an empty arm map (no chain = the old bug)."""
    arms = arm_map.get("arms") or []
    if not arms:
        raise ValueError("arm map has no arms — cannot build a cuRobo robot_cfg (no kinematic chain)")
    base_link = arm_map.get("base_link") or arms[0]["base_link"]
    target = next((a for a in arms if a["name"] == plan_arm), arms[0])
    planned = list(target["joint_names"])
    locked = {j: 0.0 for a in arms if a is not target for j in a["joint_names"]}
    return {
        "robot_cfg": {
            "kinematics": {
                "urdf_path": urdf_path,
                "base_link": base_link,
                "ee_link": target["ee_link"],
                "collision_link_names": list(spheres_by_link.keys()) or None,
                "collision_spheres": spheres_by_link,
                "collision_sphere_buffer": float(sphere_buffer),
                "lock_joints": locked or None,
                "cspace": {
                    "joint_names": planned,
                    "retract_config": [0.0] * len(planned),
                    "null_space_weight": [1.0] * len(planned),
                    "cspace_distance_weight": [1.0] * len(planned),
                    "max_acceleration": 15.0,
                    "max_jerk": 500.0,
                },
            }
        }
    }


def build_world_cfg(obstacles: Optional[List[dict]]) -> dict:
    """A cuRobo world-collision dict from the cell's static obstacles. Each obstacle:
    {name, type: cuboid|cylinder|mesh, pose:[x,y,z,qw,qx,qy,qz], dims:[...] | file_path}.
    cuRobo `WorldConfig.from_dict` consumes {"cuboid": {...}, "mesh": {...}}. The proper cuRobo
    way to model a table/wall — separate from the robot, not faked as URDF links."""
    cuboid: Dict[str, dict] = {}
    mesh: Dict[str, dict] = {}
    for o in obstacles or []:
        name = str(o.get("name") or f"obs_{len(cuboid) + len(mesh)}")
        pose = list(o.get("pose") or [0, 0, 0, 1, 0, 0, 0])
        kind = str(o.get("type") or "cuboid")
        if kind == "mesh" and o.get("file_path"):
            mesh[name] = {"file_path": o["file_path"], "pose": pose}
        elif kind == "cylinder":
            # cuRobo has no cylinder primitive in the simple world dict — approximate with a
            # tight cuboid bounding box (radius→half-width), the conservative (safe) choice.
            r, h = float(o.get("radius", 0.05)), float(o.get("height", 0.1))
            cuboid[name] = {"dims": [2 * r, 2 * r, h], "pose": pose}
        else:
            cuboid[name] = {"dims": list(o.get("dims") or [0.1, 0.1, 0.1]), "pose": pose}
    return {"cuboid": cuboid, "mesh": mesh}
