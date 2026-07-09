"""Step expectations (COMP-07, IFACE-05, DATA-06) — every program step may declare
what should be true afterwards; the engine checks that what we expected actually
happened. Scores come from checked expectations, not from one camera's opinion
(rule 7). M0 ships the contract + checker; M5 makes authored skills declare them.

Usage inside a skill:
    h = ctx.trace.begin("grasp_obj3", predict={"gripper_holding": True})
    ... act ...
    verify(h, {"gripper_holding": bridge_says_holding})

The predicted/observed/pass triple lands in the SAME trace step (args carry the
prediction, evidence carries the check), so TrialRecords, replay, and the cockpit all
see it with zero new plumbing.
"""
from __future__ import annotations

from typing import Any, Dict


def verify(handle: Any, observed: Dict[str, Any]) -> bool:
    """Compare a step's declared prediction to what was measured; write both into the
    trace step; return pass/fail. Missing observations FAIL (unknown is not success)."""
    predicted = dict((getattr(handle, "_step").args or {}).get("predict") or {})
    if not predicted:                              # nothing declared: nothing to check
        handle.done(ok=True, expectation="none_declared")
        return True
    results = {}
    ok = True
    for key, want in predicted.items():
        got = observed.get(key, "__missing__")
        hit = got == want
        results[key] = {"predicted": want, "observed": got, "pass": hit}
        ok = ok and hit
    handle.done(ok=ok, stop_cause=None if ok else "expectation_failed",
                expectation=results)
    return ok


def summarize(trace_steps: list) -> Dict[str, Any]:
    """Per-trial expectation stats (feeds the cert audit bundle, ART-06).
    Consumes TraceStep dicts: expectation lives in step['evidence']."""
    declared = passed = 0
    failures = []
    for s in trace_steps:
        exp = (s.get("evidence") or {}).get("expectation")
        if exp in (None, "none_declared"):
            continue
        declared += 1
        if all(v.get("pass") for v in exp.values()):
            passed += 1
        else:
            failures.append({"step": s.get("name"), "expectation": exp})
    return {"declared": declared, "passed": passed, "failures": failures[:10]}
