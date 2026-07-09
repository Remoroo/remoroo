"""The real-trial ritual (COMP-08, COMP-09, DEC-09) — every real motion is the same
procedure, engine-owned, never authored:

  home -> look (is the scene the way trials expect? PROB-09/14) -> act with per-step
  expectation checks -> home -> look -> compare to the candidate's own sim prediction.

Home = RUN HOME, the joints captured at startup (the operator's determinism order).
The camera is never blocked at scoring time BY PROCEDURE (the 0.75 lie is dead here).
"""
from __future__ import annotations

import math
import time
from typing import Any, Dict, List, Optional

from .params import Knobs
from .run_trial import TrialBudget, run_trial

START_DRIFT_TOL_M = 0.04     # object displaced beyond this vs the record = drifted start


def _goto_home(env: Any, run_home: Dict[str, Any]) -> Dict[str, Any]:
    joints = (run_home or {}).get("joints") or {}
    if not joints:
        return {"ok": False, "reason": "no RUN HOME captured (startup phase sets it)"}
    goto = getattr(env.stack, "goto_joints", None)
    if not callable(goto):
        return {"ok": True, "skipped": "stack has no goto_joints (sim backend)"}
    res = goto(joints)
    return {"ok": bool(getattr(res, "ok", True))}


def _start_drift(record: Any, scene: Any) -> List[dict]:
    """Objects the record knows, displaced in the pre-look beyond tolerance."""
    drifted = []
    known = {o.object_id: o for o in record.entities(min_proof="corroborated")}
    for e in scene.entities():
        for oid, o in known.items():
            d = math.dist(o.pose[:3], e.pose[:3])
            if d < 0.15 and d > START_DRIFT_TOL_M:      # same object, moved
                drifted.append({"object_id": oid, "moved_m": round(d, 3)})
    return drifted


def run_ritual_trial(env: Any, actor: Any, *, task_slug: str, program_ref: str,
                     record: Any, run_home: Dict[str, Any],
                     sim_prediction: Optional[float] = None,
                     knobs: Optional[Knobs] = None, seed: int = 0,
                     budget: Optional[TrialBudget] = None, store: Any = None,
                     emit: Any = None) -> Dict[str, Any]:
    """Returns {record: TrialRecord, ritual: {...}} — the ritual report travels with
    the trial so trial_get shows procedure facts next to the outcome."""
    ritual: Dict[str, Any] = {"t0": time.time(), "steps": []}

    def _emit(stage: str, **kw: Any) -> None:
        ritual["steps"].append({"stage": stage, **kw})
        if emit:
            emit({"kind": "ritual", "stage": stage, "task": task_slug, **kw})

    # 1. home, so the pre-look sees the table and not our own arms
    h = _goto_home(env, run_home)
    _emit("home_pre", **h)

    # 2. pre-look: the scene must be the way trials are supposed to start
    pre = env.observe()
    drift = _start_drift(record, pre)
    if drift:
        _emit("start_drift", drifted=drift)
        pre = env.observe()                       # one re-look: transient vs real
        drift = _start_drift(record, pre)
        if drift:
            _emit("refused", reason="scene drifted from recorded start")
            return {"record": None,
                    "ritual": {**ritual, "outcome": "start_drift", "drift": drift,
                               "note": "someone or something moved the scene; "
                                       "re-ground the region before spending a trial"}}

    # 3. the trial itself (run_trial owns judge/record; reset stays the authored one)
    rec = run_trial(env, actor, task_slug=task_slug, program_ref=program_ref,
                    knobs=knobs, seed=seed, budget=budget, store=store)
    _emit("acted", outcome=rec.outcome)

    # 4. home again — the post-observation inside run_trial already happened, so the
    # ritual adds its OWN clear-camera look and prefers it for the comparison
    h2 = _goto_home(env, run_home)
    _emit("home_post", **h2)
    post_clear = env.observe()
    _emit("post_look", entities=len(post_clear.entities()))

    # 5. compare with the candidate's own sim prediction (COMP-20 pairing input)
    real_score = (rec.verdict or {}).get("score")
    gap = (None if sim_prediction is None or real_score is None
           else round(real_score - float(sim_prediction), 3))
    ritual.update({"outcome": rec.outcome, "real_score": real_score,
                   "sim_prediction": sim_prediction, "sim_real_gap": gap,
                   "t1": time.time()})
    _emit("compared", gap=gap)
    return {"record": rec, "ritual": ritual}
