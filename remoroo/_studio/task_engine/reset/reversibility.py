"""Reversibility (COMP-15 edge half, DATA-07) — measured, never assumed (ENG 5.2). Every
undo-paired probe and every reset attempt updates a per-action-class score; exploration is
routed by it (reversible = free, irreversible = budgeted). Mode SELECTION is brain-side
(reset_policy); this ships the measurement and the budget accounting.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional, Tuple


class ReversibilityTable:
    def __init__(self) -> None:
        self._counts: Dict[str, Tuple[int, int]] = {}       # class -> (undo_ok, attempts)

    def update(self, action_class: str, undo_ok: bool) -> None:
        ok, n = self._counts.get(action_class, (0, 0))
        self._counts[action_class] = (ok + (1 if undo_ok else 0), n + 1)

    def score(self, action_class: str) -> float:
        """Laplace-smoothed undo success rate; an unmeasured class scores 0.5 (unknown, not
        trusted): measure before relying."""
        ok, n = self._counts.get(action_class, (0, 0))
        return (ok + 1.0) / (n + 2.0)

    def attempts(self, action_class: str) -> int:
        return self._counts.get(action_class, (0, 0))[1]

    def is_reversible(self, action_class: str, *, threshold: float = 0.8,
                      min_attempts: int = 3) -> bool:
        return self.attempts(action_class) >= min_attempts and \
            self.score(action_class) >= threshold

    def to_dict(self) -> dict:
        return {k: list(v) for k, v in self._counts.items()}

    @classmethod
    def from_dict(cls, d: dict) -> "ReversibilityTable":
        t = cls()
        t._counts = {k: (int(v[0]), int(v[1])) for k, v in d.items()}
        return t

    def save(self, path: str) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(self.to_dict()))

    @classmethod
    def load(cls, path: str) -> "ReversibilityTable":
        p = Path(path)
        return cls.from_dict(json.loads(p.read_text())) if p.exists() else cls()


class IrreversibleBudget:
    """Actions whose class is not measured-reversible draw from this, like damage. Exhausted
    means the session stops spending irreversible actions; it does NOT stop learning (mode 3/4,
    ENG 5.2)."""

    def __init__(self, units: int) -> None:
        self._initial = int(units)
        self._left = int(units)

    def try_spend(self, table: Optional[ReversibilityTable], action_class: str) -> bool:
        if table is not None and table.is_reversible(action_class):
            return True                                       # reversible actions are free
        if self._left <= 0:
            return False
        self._left -= 1
        return True

    @property
    def remaining(self) -> int:
        return self._left
