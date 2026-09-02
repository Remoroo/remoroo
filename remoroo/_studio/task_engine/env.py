"""Env (IFACE-02 / COMP-11) — one contract, two backends. The SAME candidate program runs
unchanged on the real cell and in the sibling because both are a GenericEnv wired to a
different stack/bridge pair; atoms only ever touch those two interfaces (DEC-03 backend
parity, the load-bearing reason atoms are shipped).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from .atoms import Ctx
from .effector import DEFAULT_EFFECTOR, normalize as _normalize_effector
from .envelope import Envelope
from .judge.v0 import Verdict
from .params import Knobs
from .scene.state import SceneState
from .trace import Trace


@dataclass
class GenericEnv:
    """reset -> the (authored) resetter; observe -> the (authored) acting perception;
    judge -> the verifier via the judging perception. All injected callables, all versioned.
    backend is a label stamped into every TrialRecord."""
    backend: str                                        # "real" | "sibling"
    stack: Any
    bridge: Any
    envelope: Envelope
    perceive_fn: Callable[[], SceneState]
    # judge_fn optional (robotics_program, 2026-07-22): success is decided by the
    # program's own image evaluation, not an authored judge. None = no verdict.
    judge_fn: Optional[Callable[[SceneState, SceneState, Trace], Verdict]] = None
    reset_fn: Optional[Callable[[Ctx], bool]] = None
    perception_version: str = "v0"
    arm_of: Callable[[str], str] = lambda tcp: tcp
    home_poses: Dict[str, Any] = field(default_factory=dict)
    # embodiment frames (cell-declared; defaults = tabletop manipulator)
    up_of: Callable[[str], Any] = lambda tcp: (0.0, 0.0, 1.0)
    approach_of: Callable[[str], Any] = lambda tcp: (0.0, 0.0, -1.0)
    # FULL default declaration — consumers convert units through it (b12f5aa1)
    effector_of: Callable[[str], Dict[str, Any]] = \
        lambda tcp: dict(DEFAULT_EFFECTOR)

    def new_ctx(self, knobs: Optional[Knobs] = None) -> Ctx:
        return Ctx(stack=self.stack, bridge=self.bridge, envelope=self.envelope,
                   trace=Trace(), knobs=knobs or Knobs(), arm_of=self.arm_of,
                   home_poses=dict(self.home_poses), up_of=self.up_of,
                   approach_of=self.approach_of, effector_of=self.effector_of)

    def reset(self, ctx: Ctx, seed: int = 0) -> bool:
        if self.reset_fn is None:
            return True                                  # opportunistic mode: no reset exists
        return bool(self.reset_fn(ctx))

    def observe(self, camera: Optional[str] = None) -> SceneState:
        """Perceive. camera=None uses the task's default view; passing a camera (e.g.
        the acting arm's eye-in-hand) enables PERCEIVE-IN-HAND / perceive-at-pose —
        re-observing the in-hand object after a grasp, which is the only honest way to
        know its runtime pose (active-perception note, 2026-07-21)."""
        if camera is None:
            return self.perceive_fn()
        try:
            return self.perceive_fn(camera)
        except TypeError:
            return self.perceive_fn()

    def judge(self, pre: SceneState, post: SceneState, trace: Trace) -> Optional[Verdict]:
        if self.judge_fn is None:
            return None                     # no authored judge: images decide, not code
        return self.judge_fn(pre, post, trace)


def resolvers_from_cell(cell: Dict[str, Any]):
    """Build (up_of, approach_of, effector_of) from cell.yaml — the agent-authored
    morphology. Per-group `approach_axis` + `effector`, cell-level or per-group
    `up_axis`. Every absent key falls back to the tabletop-manipulator default, so a
    cell that declares nothing behaves exactly as today. Keyed by the group/TCP name
    (the one-name contract)."""
    groups = {g.get("name"): g for g in (cell.get("groups") or [])}
    cell_up = tuple(cell.get("up_axis") or (0.0, 0.0, 1.0))

    def up_of(tcp):
        return tuple((groups.get(tcp) or {}).get("up_axis") or cell_up)

    def approach_of(tcp):
        return tuple((groups.get(tcp) or {}).get("approach_axis") or (0.0, 0.0, -1.0))

    def effector_of(tcp):
        # authored declaration MERGED over the full default — partial declarations
        # still yield a complete, unit-convertible contract (b12f5aa1)
        return _normalize_effector((groups.get(tcp) or {}).get("effector"))

    return up_of, approach_of, effector_of
