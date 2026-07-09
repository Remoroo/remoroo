"""Phase spine (COMP-10, ART-05, DATA-07) — the engine owns the order of events
(DEC-08). Six phases; only acceptance evidence advances; regressions carry a reason.
The ledger file is the whole truth the cockpit renders; nothing else may write "done".

Phases: startup -> looking -> sim -> search -> real -> cert
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

PHASES = ["startup", "looking", "sim", "search", "real", "cert"]


class PhaseLedger:
    def __init__(self, path: str, task_slug: str = "") -> None:
        self.path = Path(path)
        self.task_slug = task_slug
        self.phase = "startup"
        self.run_home: Dict[str, Any] = {}          # joints captured at startup (COMP-09)
        self.evidence: Dict[str, dict] = {}         # phase -> acceptance evidence
        self.transitions: List[dict] = []
        self.instruments: Dict[str, Any] = {}       # what preflight found (cameras, vla, sibling)

    @classmethod
    def load(cls, path: str, task_slug: str = "") -> "PhaseLedger":
        led = cls(path, task_slug)
        p = Path(path)
        if p.exists():
            d = json.loads(p.read_text(encoding="utf-8"))
            led.task_slug = d.get("task_slug", task_slug)
            led.phase = d.get("phase", "startup")
            led.run_home = d.get("run_home", {})
            led.evidence = d.get("evidence", {})
            led.transitions = d.get("transitions", [])
            led.instruments = d.get("instruments", {})
        return led

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({
            "task_slug": self.task_slug, "phase": self.phase,
            "run_home": self.run_home, "evidence": self.evidence,
            "transitions": self.transitions[-200:], "instruments": self.instruments,
            "t": time.time()}, indent=1, default=str), encoding="utf-8")

    # ---- the only mutations ------------------------------------------------------
    def set_run_home(self, joints: Dict[str, float]) -> None:
        self.run_home = {"joints": joints, "t": time.time()}
        self.save()

    def advance(self, to_phase: str, evidence: dict) -> dict:
        """Forward, one phase at a time, WITH evidence — or a stated refusal."""
        if to_phase not in PHASES:
            return {"error": f"unknown phase {to_phase!r}"}
        cur, nxt = PHASES.index(self.phase), PHASES.index(to_phase)
        if nxt != cur + 1:
            return {"error": f"can't jump {self.phase} -> {to_phase}; phases advance "
                             "one at a time (regress() to go back)"}
        if not evidence:
            return {"error": f"advance to {to_phase!r} needs acceptance evidence; "
                             "nothing advances on a claim"}
        self.evidence[self.phase] = evidence
        self.transitions.append({"from": self.phase, "to": to_phase,
                                 "kind": "advance", "t": time.time()})
        self.phase = to_phase
        self.save()
        return {"ok": True, "phase": self.phase}

    def regress(self, to_phase: str, reason: str) -> dict:
        """Backward, any distance, always with the routed reason (DEC-10)."""
        if to_phase not in PHASES or PHASES.index(to_phase) >= PHASES.index(self.phase):
            return {"error": f"regress goes backward; {self.phase} -> {to_phase} isn't"}
        if not reason:
            return {"error": "a regression carries its classified reason"}
        self.transitions.append({"from": self.phase, "to": to_phase,
                                 "kind": "regress", "reason": reason, "t": time.time()})
        self.phase = to_phase
        self.save()
        return {"ok": True, "phase": self.phase, "reason": reason}

    def status(self) -> dict:
        return {"task_slug": self.task_slug, "phase": self.phase,
                "run_home_set": bool(self.run_home),
                "evidence_phases": sorted(self.evidence),
                "transitions": self.transitions[-10:],
                "instruments": self.instruments}
