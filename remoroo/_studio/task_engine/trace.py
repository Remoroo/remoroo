"""Trace — every atom call becomes one auditable step (the ASPIRE-style execution trace).

The trace is simultaneously: the judge's proprioceptive channel (milestone partial credit),
the mutation agent's debugging evidence, the bank's label source (a grasp outcome labels the
pose that predicted it), and part of the TrialRecord. One mechanism, four consumers.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class TraceStep:
    name: str
    args: Dict[str, Any]
    t_start: float
    t_end: Optional[float] = None
    ok: Optional[bool] = None
    stop_cause: Optional[str] = None
    evidence: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "name": self.name, "args": self.args, "t_start": self.t_start,
            "t_end": self.t_end, "ok": self.ok, "stop_cause": self.stop_cause,
            "evidence": self.evidence,
        }


class _Handle:
    def __init__(self, step: TraceStep) -> None:
        self._step = step

    def done(self, ok: bool, stop_cause: Optional[str] = None, **evidence: Any) -> None:
        s = self._step
        s.t_end = time.monotonic()
        s.ok = bool(ok)
        s.stop_cause = stop_cause
        s.evidence.update(evidence)


class Trace:
    def __init__(self) -> None:
        self.steps: List[TraceStep] = []

    def begin(self, name: str, **args: Any) -> _Handle:
        step = TraceStep(name=name, args=args, t_start=time.monotonic())
        self.steps.append(step)
        return _Handle(step)

    def finalize(self) -> None:
        """Any step never done() was interrupted: mark it, loudly, not silently."""
        for s in self.steps:
            if s.ok is None:
                s.ok = False
                s.stop_cause = s.stop_cause or "exception"
                s.t_end = s.t_end or time.monotonic()

    def by_name(self, name: str) -> List[TraceStep]:
        return [s for s in self.steps if s.name == name]

    def to_list(self) -> List[dict]:
        return [s.to_dict() for s in self.steps]
