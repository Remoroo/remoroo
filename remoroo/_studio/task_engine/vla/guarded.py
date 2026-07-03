"""GuardedExecutor (COMP-25, DEC-14) — EVERY policy action passes through the same clamps as
authored code: the envelope (via atoms), the irreversibility budget, the supervisor's state,
and an optional anticipation veto (the world model's predicted-next-obs divergence signal,
injected as a callable). The policy never certifies and never bypasses; a deterministic layer
under every learned one.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from ..atoms import Ctx
from ..envelope import EnvelopeViolation
from ..reset.reversibility import IrreversibleBudget, ReversibilityTable
from .adapter import ActionAdapter


@dataclass
class SessionResult:
    steps: int
    stopped_by: str                      # "done" | "max_steps" | "envelope" | "budget" |
                                         # "supervisor" | "anticipation"
    actions: List[Dict[str, Any]] = field(default_factory=list)
    ok_steps: int = 0


class GuardedExecutor:
    def __init__(self, ctx: Ctx, adapter: Optional[ActionAdapter] = None, *,
                 budget: Optional[IrreversibleBudget] = None,
                 reversibility: Optional[ReversibilityTable] = None,
                 supervisor_state: Optional[Callable[[], str]] = None,
                 anticipate_veto: Optional[Callable[[Any, Dict[str, Any]], bool]] = None) -> None:
        self.ctx = ctx
        self.adapter = adapter or ActionAdapter()
        self.budget = budget or IrreversibleBudget(units=20)
        self.reversibility = reversibility
        self.supervisor_state = supervisor_state or (lambda: "ok")
        self.anticipate_veto = anticipate_veto        # returns True to VETO the action

    def run(self, policy: Callable[[Any], Optional[Dict[str, Any]]],
            observe: Callable[[], Any], *, max_steps: int = 50) -> SessionResult:
        res = SessionResult(steps=0, stopped_by="max_steps")
        for _ in range(max_steps):
            if self.supervisor_state() != "ok":
                res.stopped_by = "supervisor"
                break
            if hasattr(self.ctx.bridge, "estop_tripped") and self.ctx.bridge.estop_tripped():
                res.stopped_by = "supervisor"
                break
            obs = observe()
            action = policy(obs)
            if action is None:
                res.stopped_by = "done"
                break
            if self.anticipate_veto is not None and self.anticipate_veto(obs, action):
                res.stopped_by = "anticipation"
                break
            action_class = f"vla.{'gripper' if 'gripper' in action else 'move'}"
            if not self.budget.try_spend(self.reversibility, action_class):
                res.stopped_by = "budget"
                break
            try:
                ok = self.adapter.apply(self.ctx, action)
            except EnvelopeViolation:
                res.stopped_by = "envelope"
                break
            res.actions.append(action)
            res.steps += 1
            if ok:
                res.ok_steps += 1
        return res
