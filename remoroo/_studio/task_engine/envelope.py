"""Envelope — the tighten-only clamp every atom call passes through (CON-03).

Reads the cell's declared safety + instrumentation facts (cell.yaml). Instrumentation decides
which guarded-stop conditions an atom may use (PROB-17): a cell without force sensing simply
cannot request an external_ft stop; absence of a declaration is treated as the MOST restrictive
reading (fielded cells predate these keys).
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Sequence, Set

STOP_TIMEOUT = "timeout"
STOP_Z = "z_reached"
STOP_CURRENT = "current_spike"
STOP_FT = "external_ft"


class EnvelopeViolation(Exception):
    pass


class Envelope:
    def __init__(self, *, max_speed_frac: float = 0.3,
                 workspace: Optional[Dict[str, Sequence[float]]] = None,
                 instrumentation: Optional[Dict[str, Any]] = None) -> None:
        self.max_speed_frac = float(max_speed_frac)
        self.workspace = workspace or {}          # {"x": [lo,hi], "y": [...], "z": [...]}
        self.instrumentation = instrumentation or {}

    @classmethod
    def from_cell(cls, cell: Dict[str, Any]) -> "Envelope":
        safety = cell.get("safety") or {}
        return cls(
            max_speed_frac=float(safety.get("max_speed_frac", 0.3)),
            workspace=safety.get("workspace") or {},
            instrumentation=cell.get("instrumentation") or {},
        )

    # speed ------------------------------------------------------------------
    def clamp_speed(self, frac: Optional[float]) -> float:
        if frac is None:
            return self.max_speed_frac
        return min(max(float(frac), 0.01), self.max_speed_frac)

    # guarded stops ------------------------------------------------------------
    def allowed_stops(self) -> Set[str]:
        allowed = {STOP_TIMEOUT, STOP_Z}
        if self.instrumentation.get("current_sense"):
            allowed.add(STOP_CURRENT)
        if self.instrumentation.get("ft_sensor"):
            allowed.add(STOP_FT)
        return allowed

    def require_stop(self, cond: str) -> None:
        if cond not in self.allowed_stops():
            raise EnvelopeViolation(
                f"stop condition {cond!r} not available on this cell "
                f"(instrumentation={self.instrumentation!r}); allowed={sorted(self.allowed_stops())}"
            )

    # workspace ------------------------------------------------------------------
    def check_xyz(self, xyz: Sequence[float]) -> None:
        for axis, v in zip(("x", "y", "z"), xyz):
            bounds = self.workspace.get(axis)
            if bounds and not (bounds[0] <= float(v) <= bounds[1]):
                raise EnvelopeViolation(f"{axis}={v} outside workspace {list(bounds)}")

    def check_points(self, points: Iterable[Sequence[float]]) -> None:
        for p in points:
            self.check_xyz(p[:3])
