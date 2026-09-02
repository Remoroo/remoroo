"""run_trial (COMP-12) — the deterministic trial executor: reset, observe, act, observe,
judge, record. Every exception becomes a STATED outcome; the trace is finalized so an
interrupted atom is marked, never silent. The runner (brain-side) owns scheduling and money;
this owns one trial done honestly.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

from .atoms import Ctx
from .envelope import EnvelopeViolation
from .env import GenericEnv
from .params import Knobs
from .record import TrialRecord, TrialStore, new_trial_id


@dataclass
class TrialBudget:
    max_seconds: float = 300.0
    max_steps: int = 200


def run_trial(env: GenericEnv, actor: Callable[[Any, Ctx], None], *,
              task_slug: str, program_ref: str,
              knobs: Optional[Knobs] = None, seed: int = 0,
              budget: Optional[TrialBudget] = None,
              store: Optional[TrialStore] = None,
              judge_version: str = "v0",
              skip_reset: bool = False) -> TrialRecord:
    budget = budget or TrialBudget()
    ctx = env.new_ctx(knobs)
    t0 = time.monotonic()
    outcome = "completed"
    reset_ok: Optional[bool] = None
    anomalies = []

    if not skip_reset:
        reset_ok = env.reset(ctx, seed=seed)
        if reset_ok is False:
            outcome = "reset_failed"

    pre = env.observe()

    if outcome == "completed":
        try:
            actor(env, ctx)
        except EnvelopeViolation as e:
            outcome = "envelope_violation"
            anomalies.append(f"envelope: {e}")
        except Exception as e:                          # noqa: BLE001 - stated, not silent
            outcome = "actor_exception"
            anomalies.append(f"{type(e).__name__}: {e}")
            # The traceback TAIL travels in the record: trial_get shows the crashing
            # file:line immediately (a live run spent turns hand-instrumenting the
            # actor to recover exactly this).
            import traceback
            anomalies.append("traceback: "
                             + " | ".join(traceback.format_exc().splitlines()[-8:]))

    ctx.trace.finalize()
    if hasattr(env.bridge, "estop_tripped") and env.bridge.estop_tripped():
        outcome = "supervisor_halt"

    # POST-MOTION territory from here on: NOTHING may lose the record (b12f5aa1's
    # sibling incident: a judge exception after 279s of real motion propagated out and
    # store.write never ran — all evidence lost). observe/judge crashes become STATED
    # outcomes on a record that still writes.
    import traceback as _tb
    try:
        post = env.observe()
    except Exception as e:                              # noqa: BLE001 - stated, not lost
        post = None
        if outcome == "completed":
            outcome = "post_observe_exception"
        anomalies.append(f"post_observe: {type(e).__name__}: {e}")
        anomalies.append("traceback: "
                         + " | ".join(_tb.format_exc().splitlines()[-8:]))
    # Universal honesty flag, judge-independent: a post observation far below the pre
    # one (arm occluding the camera, perception dropout) makes every outcome channel
    # unreliable — the anomaly travels in the record so trial_get and the TrialsPanel
    # show it even when an authored judge gets fooled.
    post_conf = getattr(getattr(post, "confidence", None), "overall", None)
    if post_conf is not None and post_conf < 0.5:
        anomalies.append(f"low_confidence_post_observation ({post_conf:.2f}) — "
                         "clear the camera view before the trial ends")
    try:
        verdict = env.judge(pre, post, ctx.trace)
    except Exception as e:                              # noqa: BLE001 - stated, not lost
        if outcome == "completed":
            outcome = "judge_exception"
        anomalies.append(f"judge: {type(e).__name__}: {e}")
        anomalies.append("traceback: "
                         + " | ".join(_tb.format_exc().splitlines()[-8:]))
        from .judge.v0 import Verdict as _V
        verdict = _V(score=0.0, ok=False, confidence=0.0,
                     judge_version="judge-crashed",
                     flags=["judge raised — fix perception_judge.py; the trial "
                            "record and trace are preserved"])
    if verdict is None:
        # No authored judge (robotics_program 2026-07-22): the trial record carries
        # the trace and scenes; success is decided by images after homing, not here.
        from .judge.v0 import Verdict as _V
        verdict = _V(score=0.0, ok=False, confidence=0.0, judge_version="no-judge",
                     flags=["no authored judge — success is evaluated from camera "
                            "images by the program loop, never from this record"])
    # COERCE, don't crash (live ritual_0004, 2026-07-18: an authored judge returned a
    # bare float score — 196s of REAL trial then 'float has no attribute to_dict' and
    # the record was LOST). A number becomes a stated minimal Verdict; anything else
    # non-conforming becomes a zero-confidence one with the contract in flags.
    if not hasattr(verdict, "to_dict"):
        from .judge.v0 import Verdict as _V
        if isinstance(verdict, (int, float)):
            verdict = _V(score=float(verdict), ok=bool(float(verdict) >= 0.5),
                         confidence=0.5, judge_version="coerced-float",
                         flags=["judge returned a bare number — return judge.v0.Verdict "
                                "(score, ok, confidence, channels) for full credit"])
        else:
            verdict = _V(score=0.0, ok=False, confidence=0.0,
                         judge_version="coerced-invalid",
                         flags=[f"judge returned {type(verdict).__name__!r}, not a "
                                "Verdict — fix perception_judge.py"])
    t1 = time.monotonic()

    if (t1 - t0) > budget.max_seconds:
        anomalies.append("time_budget_exceeded")
    if len(ctx.trace.steps) > budget.max_steps:
        anomalies.append("step_budget_exceeded")

    rec = TrialRecord(
        trial_id=new_trial_id(), task_slug=task_slug, backend=env.backend,
        program_ref=program_ref, knobs=ctx.knobs.vector(), knob_ranges=ctx.knobs.ranges(),
        judge_version=verdict.judge_version or judge_version,
        perception_version=env.perception_version, seed=seed,
        t_start=t0, t_end=t1, wall_time=time.time(),
        scene_pre=(pre.to_dict() if hasattr(pre, "to_dict") else
                   {"coerced": str(pre)[:200]}),
        scene_post=(post.to_dict() if hasattr(post, "to_dict") else
                    {"coerced": str(post)[:200]} if post is not None else
                    {"error": "post observe raised (see anomaly_flags)"}),
        trace=ctx.trace.to_list(), verdict=verdict.to_dict(), outcome=outcome,
        reset_ok=reset_ok, anomaly_flags=anomalies,
    )
    if store is not None:
        store.write(rec)
    return rec
