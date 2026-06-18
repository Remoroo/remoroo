"""The supervised step family — eye_in_hand, eye_to_hand, and static all run the same
`CalibSession` executor; the observation model is derived inside it from the step's kind +
board_source (a moving-link camera, a world-fixed camera the arm presents a board to, or a
world-fixed camera with a hand-placed board). Three registrations, one executor — the
shared math is the proven `CalibSession`, not three copies.

A world-fixed camera with a hand-placed board has no moving chain, so it gets an empty
`Chain`; everything else walks the URDF chain to the bound flange.
"""
from __future__ import annotations

from ..geometry import Chain
from ..session import CalibSession
from .base import StepContext, register


def _is_static(item) -> bool:
    # The agent may author kind="static" directly, or "eye_to_hand" with a hand-placed board
    # (board_source != "arm") which IS static. A robot-PRESENTED board (board_source="arm")
    # is the moving eye_to_hand bundle, not static.
    if item.kind == "static":
        return True
    return item.kind == "eye_to_hand" and getattr(item, "board_source", "handheld") != "arm"


def _build(item, ctx: StepContext) -> CalibSession:
    chain = Chain([], []) if _is_static(item) else ctx.chain_provider(item.flange_link)
    bridge = ctx.bridge_factory(item)
    return CalibSession(item, ctx.target, ctx.K, chain, bridge, wh=ctx.wh,
                        accept_heldout_px=ctx.accept_heldout_px, accept_tip_mm=ctx.accept_tip_mm)


register("eye_in_hand", "supervised")(_build)
register("eye_to_hand", "supervised")(_build)
register("static", "supervised")(_build)
