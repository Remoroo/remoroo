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

    post = env.observe()
    # Universal honesty flag, judge-independent: a post observation far below the pre
    # one (arm occluding the camera, perception dropout) makes every outcome channel
    # unreliable — the anomaly travels in the record so trial_get and the TrialsPanel
    # show it even when an authored judge gets fooled.
    post_conf = getattr(getattr(post, "confidence", None), "overall", None)
    if post_conf is not None and post_conf < 0.5:
        anomalies.append(f"low_confidence_post_observation ({post_conf:.2f}) — "
                         "clear the camera view before the trial ends")
    verdict = env.judge(pre, post, ctx.trace)
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
        scene_pre=pre.to_dict(), scene_post=post.to_dict(),
        trace=ctx.trace.to_list(), verdict=verdict.to_dict(), outcome=outcome,
        reset_ok=reset_ok, anomaly_flags=anomalies,
    )
    if store is not None:
        store.write(rec)
    return rec
