"""SiblingEnv (COMP-22 contract half) — the second backend of the SAME Env, proving DEC-03
backend parity mechanically: the identical actor program runs here and on the cell because
both are a GenericEnv over a stack/bridge pair.

This module ships the behavioral kinematic world (attach-on-grasp, move-with-carrier,
release-drops) used by CI and as the SiblingEnv contract reference. The Isaac Lab / Newton
worker (sibling_worker image, our cloud) implements this same seam with real physics; it is
the M5 cloud deliverable, not part of the wheel.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from ..env import GenericEnv
from ..envelope import Envelope
from ..scene.state import Entity, FeatureChannel, SceneState


@dataclass
class SimWorld:
    """Ground-truth object state. In the real Isaac worker this is the physics scene; here it
    is the minimal behavior that makes transport tasks REAL: objects attach when grasped near
    enough, ride the carrier, and drop where released."""
    objects: Dict[str, List[float]] = field(default_factory=dict)   # id -> [x,y,z]
    grasp_radius: float = 0.03
    attached: Dict[str, str] = field(default_factory=dict)          # tcp -> object id

    def nearest(self, xyz: List[float]) -> Optional[str]:
        best, best_d = None, 1e9
        for oid, p in self.objects.items():
            d = math.dist(xyz[:2], p[:2]) + abs(xyz[2] - p[2])
            if d < best_d:
                best, best_d = oid, d
        return best if best_d <= self.grasp_radius else None


class SimStack:
    """Same surface as MotionStack's motion verbs; moving a tcp drags its attached object."""

    def __init__(self, world: SimWorld) -> None:
        self.world = world
        self.pose: Dict[str, List[float]] = {}
        self.calls: List[dict] = []

    def _get(self, tcp: str) -> List[float]:
        return self.pose.setdefault(tcp, [0.3, 0.0, 0.3, 1.0, 0.0, 0.0, 0.0])

    def link_pose(self, tcp: str) -> List[float]:
        return list(self._get(tcp))

    def _move(self, tcp: str, pose: List[float]) -> None:
        self.pose[tcp] = list(pose)
        oid = self.world.attached.get(tcp)
        if oid:
            self.world.objects[oid] = list(pose[:3])

    def move_to_pose(self, tcp: str, pose: Any, **kw: Any):
        self.calls.append({"kind": "to", "tcp": tcp})
        self._move(tcp, list(pose))
        return type("R", (), {"ok": True})()

    def move_through_poses(self, tcp: str, poses: Any, **kw: Any):
        self.calls.append({"kind": "through", "tcp": tcp, "n": len(list(poses))})
        for p in poses:
            self._move(tcp, list(p))
        return type("R", (), {"ok": True})()

    def move_to_poses(self, targets: Dict[str, Any], **kw: Any):
        self.calls.append({"kind": "multi"})
        for t, p in targets.items():
            self._move(t, list(p))
        return type("R", (), {"ok": True})()


class SimBridge:
    """Same surface as the Bridge atoms touch. Grasp physics: closing near an object attaches
    it; the gripper report IS the sim's proprioceptive truth (so grasp evidence works the same
    in both worlds)."""

    def __init__(self, world: SimWorld, stack: SimStack) -> None:
        self.world = world
        self.stack = stack
        self._estop = False
        self._open: Dict[str, float] = {}

    def set_gripper(self, arm: str, width: float) -> None:
        self._open[arm] = width
        if width < 0.03:                                       # closing
            oid = self.world.nearest(self.stack.link_pose(arm)[:3])
            if oid is not None:
                self.world.attached[arm] = oid
        else:                                                  # opening: drop where it is
            self.world.attached.pop(arm, None)

    def gripper_state(self, arm: str) -> dict:
        held = arm in self.world.attached
        return {"closed_width": 0.011 if held else 0.0, "holding": held}

    def stop_signal(self, kind: str, tcp: str) -> bool:
        return False

    def estop(self) -> None:
        self._estop = True

    def estop_tripped(self) -> bool:
        return self._estop

    def camera_frame(self, camera: str) -> Any:
        return {"camera": camera, "depth": None, "rgb": None, "t_capture": 0.0}


def build_sibling_env(*, objects: Dict[str, List[float]],
                      goal_region: Dict[str, List[float]],
                      judge_fn: Optional[Callable] = None,
                      envelope: Optional[Envelope] = None,
                      home_poses: Optional[Dict[str, Any]] = None) -> GenericEnv:
    """Compose a sibling env: exact-robot half is the converted cell (cell_to_sim, cloud);
    here the seam is exercised with the behavioral world. Ground truth is used ONLY as the
    sibling's perception (rehearsal); it never certifies (DEC-12)."""
    world = SimWorld(objects=dict(objects))
    stack = SimStack(world)
    bridge = SimBridge(world, stack)

    def in_region(p: List[float]) -> bool:
        for axis, i in (("x", 0), ("y", 1)):
            lo, hi = goal_region[axis]
            if not (lo <= p[i] <= hi):
                return False
        return True

    def perceive() -> SceneState:
        ents = []
        for oid, p in world.objects.items():
            ents.append(Entity(entity_id=oid, label="object " + oid,
                               pose=list(p) + [1.0, 0.0, 0.0, 0.0],
                               features={"in_goal": FeatureChannel.scalar(
                                   1.0 if in_region(p) else 0.0)}))
        return SceneState(entities_list=ents, perception_program_id="sibling-truth@1")

    if judge_fn is None:
        from ..judge.v0 import EnsembleJudge

        def geometric(pre: SceneState, post: SceneState) -> float:
            vals = [e.feature("in_goal").values[0] for e in post.entities("object")]
            return sum(vals) / len(vals) if vals else 0.0

        judge_fn = EnsembleJudge(geometric_fn=geometric, judge_version="v0@sibling").judge

    return GenericEnv(backend="sibling", stack=stack, bridge=bridge,
                      envelope=envelope or Envelope(),
                      perceive_fn=perceive, judge_fn=judge_fn,
                      perception_version="sibling-truth@1",
                      home_poses=home_poses or {"arm_a": [0.3, 0.0, 0.4]})
