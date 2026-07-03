"""Knobs — the declared, bounded, searchable parameters of a program (IFACE-01 half).

A program declares each tunable number at its call site:

    depth = ctx.knobs.declare("pick.depth", default=0.012, lo=0.008, hi=0.025)

The declaration IS the search space: after one run the registry knows every knob's name and
range; the tuner (brain-side, R0) proposes override vectors; `bind()` produces the next run's
Knobs. Overrides are clamped into the declared range — the tuner can never exceed what the
program declared safe.
"""
from __future__ import annotations

from typing import Dict, Optional, Tuple


class Knobs:
    def __init__(self, overrides: Optional[Dict[str, float]] = None) -> None:
        self._overrides: Dict[str, float] = dict(overrides or {})
        self._ranges: Dict[str, Tuple[float, float, float]] = {}   # name -> (default, lo, hi)
        self._values: Dict[str, float] = {}                        # values used THIS run

    def declare(self, name: str, default: float, lo: float, hi: float) -> float:
        if lo > hi:
            raise ValueError(f"knob {name!r}: lo {lo} > hi {hi}")
        if not (lo <= default <= hi):
            raise ValueError(f"knob {name!r}: default {default} outside [{lo}, {hi}]")
        self._ranges[name] = (float(default), float(lo), float(hi))
        raw = self._overrides.get(name, default)
        value = min(max(float(raw), lo), hi)
        self._values[name] = value
        return value

    def vector(self) -> Dict[str, float]:
        """The knob values actually used this run (theta, for the (theta, score) table)."""
        return dict(self._values)

    def ranges(self) -> Dict[str, Tuple[float, float]]:
        return {k: (lo, hi) for k, (_d, lo, hi) in self._ranges.items()}

    def defaults(self) -> Dict[str, float]:
        return {k: d for k, (d, _lo, _hi) in self._ranges.items()}

    def bind(self, overrides: Dict[str, float]) -> "Knobs":
        """A fresh Knobs for the next run, with new override values (merged over current)."""
        merged = dict(self._overrides)
        merged.update(overrides)
        return Knobs(merged)
