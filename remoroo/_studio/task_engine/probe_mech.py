"""Probe MECHANICS (COMP-28 edge half, per DEC-21). The curriculum ORDER lives brain-side
(probe_policy); this ships the graded executors and the undo pairing. Every probe returns the
same evidence bundle: what moved, what the gripper felt, and whether the undo restored things,
so one probe feeds the bank, the reversibility table, the dynamics stream, and the judge
corpus at once (ENG 0.2).
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Sequence

from . import atoms
from .atoms import Ctx


def probe_touch(ctx: Ctx, tcp: str, at_xyz: Sequence[float], *,
                hover_h: float = 0.05) -> Dict[str, Any]:
    """The gentlest question: go there, come back. Proves reachability + clearance."""
    x, y, z = [float(v) for v in at_xyz[:3]]
    r1 = atoms.reach(ctx, tcp, [x, y, z + hover_h])
    r2 = atoms.descend(ctx, tcp, z)
    r3 = atoms.reach(ctx, tcp, [x, y, z + hover_h])
    return {"kind": "touch", "ok": bool(r1.ok and r2.ok and r3.ok),
            "undo_attempted": True, "undo_ok": bool(r3.ok)}


def probe_push(ctx: Ctx, tcp: str, at_xyz: Sequence[float], *, dist_m: float = 0.01,
               observe: Optional[Callable[[], Any]] = None) -> Dict[str, Any]:
    """Push a little, push back: the undo IS the mirrored push. undo_ok is judged by the
    caller from before/after observations when an observer is supplied."""
    x, y, z = [float(v) for v in at_xyz[:3]]
    before = observe() if observe else None
    r1 = atoms.sweep(ctx, tcp, [x, y, z], [x + dist_m, y, z], press_z=z)
    r2 = atoms.sweep(ctx, tcp, [x + dist_m, y, z], [x, y, z], press_z=z)
    after = observe() if observe else None
    return {"kind": "push", "ok": bool(r1.ok and r2.ok), "undo_attempted": True,
            "undo_ok": bool(r2.ok), "before": before, "after": after}


def probe_lift_place(ctx: Ctx, tcp: str, at_xyz: Sequence[float], *,
                     grasp_width: float = 0.02, lift_h: float = 0.06) -> Dict[str, Any]:
    """Lift it, put it back where it was: measures graspability AND reversibility of a
    pick in one motion. The grasp evidence doubles as a free perception label (bank)."""
    x, y, z = [float(v) for v in at_xyz[:3]]
    atoms.reach(ctx, tcp, [x, y, z + lift_h])
    atoms.descend(ctx, tcp, z)
    g = atoms.grasp(ctx, tcp, width=grasp_width)
    undo_ok = False
    if g.ok:
        atoms.reach(ctx, tcp, [x, y, z + lift_h])
        atoms.descend(ctx, tcp, z)
        r = atoms.release(ctx, tcp)
        undo_ok = bool(r.ok)
    else:
        atoms.release(ctx, tcp)
    atoms.reach(ctx, tcp, [x, y, z + lift_h])
    return {"kind": "lift_place", "ok": bool(g.ok), "grasp": g.evidence,
            "undo_attempted": bool(g.ok), "undo_ok": undo_ok}


PROBE_EXECUTORS: Dict[str, Any] = {
    "touch": probe_touch,
    "push": probe_push,
    "lift_place": probe_lift_place,
}
