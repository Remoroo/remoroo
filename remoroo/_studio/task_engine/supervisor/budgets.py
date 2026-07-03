"""Damage budget (COMP-05) — a night has a finite allowance of bad events; exhaustion is a
deterministic stop, not a judgment call."""
from __future__ import annotations


class DamageBudget:
    def __init__(self, units: float) -> None:
        self._initial = float(units)
        self._left = float(units)

    def debit(self, cost: float, reason: str = "") -> float:
        self._left -= abs(float(cost))
        return self._left

    @property
    def remaining(self) -> float:
        return self._left

    @property
    def exhausted(self) -> bool:
        return self._left <= 0.0
