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

from .. import effector as _eff
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
    dims: Dict[str, List[float]] = field(default_factory=dict)      # id -> [dx,dy,dz]
    # honest grasp seam: the in-hand pose is unknowable offline, so attach with a
    # sampled offset (active-perception note 2026-07-21); {} disables (exact weld)
    grasp_noise: Dict[str, Any] = field(default_factory=dict)
    seed: int = 0
    offsets: Dict[str, List[float]] = field(default_factory=dict)   # tcp -> obj - tcp

    def graspable_width(self, oid: str) -> float:
        d = self.dims.get(oid) or [0.04, 0.04, 0.04]
        return min(float(d[0]), float(d[1]))

    def attach_offset(self, tcp: str, oid: str, tcp_xyz: List[float]) -> List[float]:
        """The held object's pose relative to the tcp: the TRUE relative pose at
        grasp time plus (when grasp_noise is set) a sampled perturbation — a blind
        fixed-offset program fails here as on the rig."""
        p = self.objects[oid]
        off = [p[i] - float(tcp_xyz[i]) for i in range(3)]
        gn = self.grasp_noise
        if gn:
            import random
            rng = random.Random(f"{self.seed}:{tcp}:{oid}")
            st = float(gn.get("sigma_t_m", 0.015))
            if self.dims.get(oid) and max(self.dims[oid]) < 0.05:
                st *= float(gn.get("soft_scale", 2.0))   # small/soft = more snap
            off = [off[i] + rng.gauss(0.0, st) for i in range(3)]
        return off

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
            off = self.world.offsets.get(tcp) or [0.0, 0.0, 0.0]
            self.world.objects[oid] = [float(pose[i]) + off[i] for i in range(3)]

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
    """Same surface as the Bridge atoms touch. Grasp physics: the commanded NATIVE
    value is interpreted through the cell's EFFECTOR DECLARATION (task_engine.effector)
    — never a private width literal (incident b12f5aa1: `width < 0.03` attached while
    the real fraction-bridge never closed). Fingers commanded to an aperture SMALLER
    than the object's graspable width squeeze -> hold; opened past it -> drop."""

    def __init__(self, world: SimWorld, stack: SimStack,
                 effectors: Optional[Dict[str, dict]] = None) -> None:
        self.world = world
        self.stack = stack
        self._estop = False
        self._native: Dict[str, float] = {}
        self._effectors = effectors or {}

    def effector_of(self, tcp: str) -> dict:
        return _eff.normalize(self._effectors.get(tcp))

    def set_gripper(self, tcp: str, native: float) -> None:
        eff = self.effector_of(tcp)
        self._native[tcp] = float(native)
        ap = _eff.aperture_of_native(eff, native)
        held = self.world.attached.get(tcp)
        if held is not None:
            if ap >= self.world.graspable_width(held):     # opened past the payload
                self.world.attached.pop(tcp, None)
                self.world.offsets.pop(tcp, None)
            return
        tcp_xyz = self.stack.link_pose(tcp)[:3]
        oid = self.world.nearest(tcp_xyz)
        if oid is not None and ap < self.world.graspable_width(oid):
            self.world.attached[tcp] = oid                 # fingers squeeze -> hold
            self.world.offsets[tcp] = self.world.attach_offset(tcp, oid, tcp_xyz)

    def actuate_effector(self, tcp: str, *, engage: bool, effector: Optional[dict] = None,
                         width: Optional[float] = None, **_: Any) -> None:
        eff = _eff.normalize(self._effectors.get(tcp) or effector)
        self.set_gripper(tcp, _eff.command_native(eff, width_m=width, engage=engage))

    def gripper_state(self, tcp: str) -> dict:
        eff = self.effector_of(tcp)
        native = self._native.get(tcp)
        ap = (_eff.aperture_of_native(eff, native) if native is not None
              else _eff.width_max(eff))
        held = tcp in self.world.attached
        # closed_width = the held object's width when holding (fingers stall on it)
        return {"closed_width": (self.world.graspable_width(self.world.attached[tcp])
                                 if held else ap),
                "holding": held}

    def effector_state(self, tcp: str) -> dict:
        return self.gripper_state(tcp)

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
                      home_poses: Optional[Dict[str, Any]] = None,
                      effectors: Optional[Dict[str, dict]] = None,
                      dims: Optional[Dict[str, List[float]]] = None,
                      grasp_noise: Optional[Dict[str, Any]] = None,
                      seed: int = 0) -> GenericEnv:
    """Compose a sibling env: exact-robot half is the converted cell (cell_to_sim, cloud);
    here the seam is exercised with the behavioral world. Ground truth is used ONLY as the
    sibling's perception (rehearsal); it never certifies (DEC-12). `effectors` is the
    tcp-keyed cell.yaml groups[].effector declaration — the ONE gripper contract."""
    world = SimWorld(objects=dict(objects), dims=dict(dims or {}),
                     grasp_noise=dict(grasp_noise or {}), seed=int(seed))
    stack = SimStack(world)
    bridge = SimBridge(world, stack, effectors=effectors)

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
                      effector_of=bridge.effector_of,
                      home_poses=home_poses or {"arm_a": [0.3, 0.0, 0.4]})
