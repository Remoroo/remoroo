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

# ---- the machine's own acceptance bars (audit 2026-07-13: "any non-empty dict
# advances" made the six phases a diary; these preconditions are computed by the
# ENGINE from its own stores — the agent's evidence dict is an annotation) -----------
FITNESS_FLOOR = 0.5          # search exit: a promoted candidate must clear this
CERT_MIN_TRIALS = 3          # real exit: at least this many ritual trials ...
CERT_MIN_OK = 2              # ... of which at least this many succeeded
CERT_EXPECT_RATE = 0.7       # ... with declared expectations passing at this rate

# Requirements per transition, keyed by the phase being EXITED. Each entry:
# (fact key, predicate over facts, the human requirement printed on refusal).
# `facts` is assembled by the service from the record / ground-truth library /
# trial store (measured, edge-owned) and from structured tool-result documents
# (reported: the sibling worker and the evolution archive live off-cell, so their
# results arrive as task_sibling/task_evolve output — shape-checked here and
# labeled "reported" on the certificate; hardware-truth exits are never reported).
REQUIREMENTS = {
    "startup": [
        ("run_home_set", lambda f: bool(f.get("run_home_set")),
         "phase_start must complete (RUN HOME joints captured)"),
    ],
    "looking": [
        ("vla_disposition",
         lambda f: bool(f.get("vla_day0")) or bool(f.get("vla_waived")),
         "a DAY-ZERO VLA attempt of the task text (vla_rollout — THE FIRST ACT when a "
         "VLA serves: the pretrained policy tries the task itself, guarded and "
         "recorded) OR a stated waiver (phase_waive {phase: vla, reason}) when the "
         "server cannot be made to serve this run — fixing YOUR integration configs "
         "counts as this run's work (seed vla_serving rung 10) before any waiver"),
        ("actionable", lambda f: int(f.get("actionable", 0)) >= 1,
         "at least 1 ACTIONABLE object (two looks or a touch, graded by the record)"),
        ("gt_cases", lambda f: int(f.get("gt_cases", 0)) >= 1,
         "at least 1 ground-truth case (one probe writes one)"),
    ],
    "sim": [
        ("sim_ranked", lambda f: int(f.get("sim_ranked", 0)) >= 2,
         "a ranked sibling table with >=2 scored candidates (task_sibling output: "
         "evidence.ranked = [{ref, mean}, ...]); if the sibling deployment cannot "
         "run cousins, phase_waive {phase: sim, reason} states it on the cert"),
    ],
    "search": [
        ("promoted_fitness",
         lambda f: isinstance(f.get("promoted_fitness"), (int, float))
         and f["promoted_fitness"] >= FITNESS_FLOOR,
         f"a promoted candidate with archive fitness >= {FITNESS_FLOOR} "
         "(evidence.promoted = {ref, fitness}); waivable like sim"),
    ],
    "real": [
        ("trials_total", lambda f: int(f.get("trials_total", 0)) >= CERT_MIN_TRIALS,
         f"at least {CERT_MIN_TRIALS} ritual trials recorded"),
        ("trials_ok", lambda f: int(f.get("trials_ok", 0)) >= CERT_MIN_OK,
         f"at least {CERT_MIN_OK} successful ritual trials"),
        ("expect_rate", lambda f: (int(f.get("expect_declared", 0)) > 0
                                   and f.get("expect_passed", 0)
                                   / max(f.get("expect_declared", 1), 1)
                                   >= CERT_EXPECT_RATE),
         f"declared expectations passing at >= {CERT_EXPECT_RATE:.0%} "
         "(declare predict= in skills; zero declared expectations never certifies)"),
    ],
}
# sim/search: off-cell rehearsal phases. "vla": not a phase but a CAPABILITY waiver —
# the day-zero attempt requirement on the looking exit. Hardware truth never waives.
WAIVABLE = {"sim", "search", "vla"}


class PhaseLedger:
    def __init__(self, path: str, task_slug: str = "") -> None:
        self.path = Path(path)
        self.task_slug = task_slug
        self.phase = "startup"
        self.run_home: Dict[str, Any] = {}          # joints captured at startup (COMP-09)
        self.evidence: Dict[str, dict] = {}         # phase -> acceptance evidence
        self.transitions: List[dict] = []
        self.instruments: Dict[str, Any] = {}       # what preflight found (cameras, vla, sibling)
        self.waivers: Dict[str, str] = {}           # phase -> stated reason (cert prints these)

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
            led.waivers = d.get("waivers", {})
        return led

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({
            "task_slug": self.task_slug, "phase": self.phase,
            "run_home": self.run_home, "evidence": self.evidence,
            "transitions": self.transitions[-200:], "instruments": self.instruments,
            "waivers": self.waivers,
            "t": time.time()}, indent=1, default=str), encoding="utf-8")

    # ---- the only mutations ------------------------------------------------------
    def set_run_home(self, joints: Dict[str, float]) -> None:
        self.run_home = {"joints": joints, "t": time.time()}
        self.save()

    def unmet(self, facts: Dict[str, Any]) -> List[str]:
        """The CURRENT phase's unmet exit requirements, humanly stated. Empty when the
        exit is earned (or the phase is waived). This list is the agent's work list."""
        if self.phase in self.waivers:
            return []
        return [req for _key, pred, req in REQUIREMENTS.get(self.phase, [])
                if not pred(facts or {})]

    def waive(self, phase: str, reason: str) -> dict:
        """Waive an off-cell phase with a stated reason — LOUD, never silent: the
        waiver is stamped into the ledger and printed on the certificate. Hardware-
        truth phases (looking, real) can never be waived."""
        if phase not in WAIVABLE:
            return {"error": f"{phase!r} is not waivable (only {sorted(WAIVABLE)}; "
                             "hardware truth has no waivers)"}
        if not reason:
            return {"error": "a waiver carries its stated reason"}
        self.waivers[phase] = reason
        self.save()
        return {"ok": True, "waived": phase, "reason": reason,
                "note": "the certificate will print this waiver"}

    def advance(self, to_phase: str, evidence: dict,
                facts: Optional[Dict[str, Any]] = None) -> dict:
        """Forward, one phase at a time — and ONLY when the current phase's exit
        requirements hold over engine-computed facts. The evidence dict is stored as
        the agent's annotation; it never substitutes for the facts (audit 2026-07-13:
        any non-empty dict used to reach cert)."""
        if to_phase not in PHASES:
            return {"error": f"unknown phase {to_phase!r}"}
        cur, nxt = PHASES.index(self.phase), PHASES.index(to_phase)
        if nxt != cur + 1:
            return {"error": f"can't jump {self.phase} -> {to_phase}; phases advance "
                             "one at a time (regress() to go back)"}
        missing = self.unmet(facts or {})
        if missing:
            return {"error": f"{self.phase} exit not earned — unmet: " + "; ".join(missing),
                    "unmet": missing,
                    "note": "this list IS the work; phase_status repeats it"}
        self.evidence[self.phase] = dict(evidence or {})
        if self.phase in self.waivers:
            self.evidence[self.phase]["waived"] = self.waivers[self.phase]
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

    def status(self, facts: Optional[Dict[str, Any]] = None) -> dict:
        out = {"task_slug": self.task_slug, "phase": self.phase,
               "run_home_set": bool(self.run_home),
               "evidence_phases": sorted(self.evidence),
               "transitions": self.transitions[-10:],
               "instruments": self.instruments,
               "waivers": self.waivers}
        if facts is not None:
            nxt = (PHASES[PHASES.index(self.phase) + 1]
                   if self.phase != PHASES[-1] else None)
            out["next"] = {"to": nxt, "unmet": self.unmet(facts)}
        return out
