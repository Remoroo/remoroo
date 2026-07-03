"""Env (IFACE-02 / COMP-11) — one contract, two backends. The SAME candidate program runs
unchanged on the real cell and in the sibling because both are a GenericEnv wired to a
different stack/bridge pair; atoms only ever touch those two interfaces (DEC-03 backend
parity, the load-bearing reason atoms are shipped).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from .atoms import Ctx
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
    judge_fn: Callable[[SceneState, SceneState, Trace], Verdict]
    reset_fn: Optional[Callable[[Ctx], bool]] = None
    perception_version: str = "v0"
    arm_of: Callable[[str], str] = lambda tcp: tcp
    home_poses: Dict[str, Any] = field(default_factory=dict)

    def new_ctx(self, knobs: Optional[Knobs] = None) -> Ctx:
        return Ctx(stack=self.stack, bridge=self.bridge, envelope=self.envelope,
                   trace=Trace(), knobs=knobs or Knobs(), arm_of=self.arm_of,
                   home_poses=dict(self.home_poses))

    def reset(self, ctx: Ctx, seed: int = 0) -> bool:
        if self.reset_fn is None:
            return True                                  # opportunistic mode: no reset exists
        return bool(self.reset_fn(ctx))

    def observe(self) -> SceneState:
        return self.perceive_fn()

    def judge(self, pre: SceneState, post: SceneState, trace: Trace) -> Verdict:
        return self.judge_fn(pre, post, trace)
