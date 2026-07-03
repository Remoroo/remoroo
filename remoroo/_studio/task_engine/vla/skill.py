"""vla(...) as a callable skill inside actor programs (ENG 8.3 role 3). Programs keep the
spine (atoms for transport); the policy carries fragments the program delegates, per family,
with earned authority — and everything it does is traced like any atom.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from ..atoms import AtomResult, Ctx
from .adapter import ActionAdapter
from .guarded import GuardedExecutor


def make_vla_skill(runtime: Any, *, adapter: Optional[ActionAdapter] = None,
                   observe_of: Optional[Callable[[Any], Callable[[], Any]]] = None):
    """runtime: policy handle with .policy(instruction) -> (obs -> action|None).
    Returns vla(env, ctx, instruction, max_steps=...) usable inside actor code."""

    def vla(env: Any, ctx: Ctx, instruction: str, *, max_steps: int = 30,
            **guard_kw: Any) -> AtomResult:
        h = ctx.trace.begin("vla", instruction=instruction, max_steps=max_steps)
        observe = (observe_of(env) if observe_of else env.observe)
        execu = GuardedExecutor(ctx, adapter, **guard_kw)
        session = execu.run(runtime.policy(instruction), observe, max_steps=max_steps)
        ok = session.stopped_by == "done"
        h.done(ok=ok, stop_cause=None if ok else session.stopped_by,
               steps=session.steps, ok_steps=session.ok_steps)
        return AtomResult(ok=ok, stop_cause=None if ok else session.stopped_by,
                          evidence={"steps": session.steps, "stopped_by": session.stopped_by})

    return vla
