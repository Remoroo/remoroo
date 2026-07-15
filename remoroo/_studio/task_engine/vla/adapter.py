"""ActionAdapter (COMP-25, PROB-12) — maps a policy's action space onto THIS cell through the
same two interfaces everything else uses (stack + Bridge). v1 action dialect: per-step dicts

    {"tcp": str, "delta_xyz": [dx,dy,dz]}                relative end-effector move
    {"tcp": str, "gripper": width}                       gripper command
    {"tcp": str, "pose": [x,y,z,(quat)]}                 absolute pose

The base checkpoint is third-party (fetched at setup); this adapter is the per-arm mapping
table, and it is deliberately dumb: clamping/vetoes belong to the GuardedExecutor.
"""
from __future__ import annotations

from typing import Any, Dict

from .. import atoms
from ..atoms import Ctx


class ActionAdapter:
    def __init__(self, *, max_step_m: float = 0.05, direct: bool = True) -> None:
        self.max_step_m = float(max_step_m)
        # DIRECT execution (2026-07-15 audit): a reactive policy's 1-5cm deltas must
        # NOT pay a full cuRobo plan each ("too slow, pauses between motions" — and
        # per-step planning breaks the closed-loop timing the policy was trained on).
        # Direct = the authored Bridge's planner-free cartesian move, bounded by the
        # clamp below + the envelope; the same trust model as calib-routine replay.
        # Falls back to the planned path — STATED ONCE — when the Bridge lacks it.
        self.direct = bool(direct)
        self._direct_warned = False

    def clamp_delta(self, delta) -> list:
        return [max(-self.max_step_m, min(self.max_step_m, float(v))) for v in delta[:3]]

    def apply(self, ctx: Ctx, action: Dict[str, Any]) -> bool:
        tcp = action["tcp"]
        if "gripper" in action:
            if float(action["gripper"]) < 0.02:
                return atoms.grasp(ctx, tcp, width=float(action["gripper"])).ok
            return atoms.release(ctx, tcp, open_width=float(action["gripper"])).ok
        if "delta_xyz" in action:
            # atoms._live_pose normalizes the stack's (xyz, wxyz) tuple to a FLAT
            # 7-vector — hand-indexing link_pose here got the tuple and crashed the
            # first live day-zero rollout (2026-07-15: list + float TypeError; the
            # same tuple-vs-flat FK class as the 2026-07-10 trial-ritual bug).
            cur = atoms._live_pose(ctx, tcp)
            d = self.clamp_delta(action["delta_xyz"])
            tgt = [cur[0] + d[0], cur[1] + d[1], cur[2] + d[2]] + list(cur[3:7])
            move_direct = getattr(ctx.bridge, "move_direct_pose", None)
            if self.direct and callable(move_direct):
                ctx.envelope.check_xyz(tgt[:3])          # the box still binds
                h = ctx.trace.begin("vla_direct", tcp=tcp, tgt=tgt[:3])
                try:
                    move_direct(ctx.arm_of(tcp), tgt)
                except Exception as e:                   # noqa: BLE001 - stated
                    h.done(ok=False, stop_cause="direct_failed", error=str(e))
                    return False
                h.done(ok=True)
                return True
            if self.direct and not self._direct_warned:
                self._direct_warned = True
                print("[vla] Bridge has no move_direct_pose(arm, xyzq) — falling back "
                      "to PLANNED moves (slow, breaks policy timing). Author it in "
                      "remoroo_cell/primitives.py (the robot SDK's cartesian move) "
                      "for real VLA motion.", flush=True)
            return atoms.reach(ctx, tcp, tgt).ok
        if "pose" in action:
            return atoms.reach(ctx, tcp, action["pose"]).ok
        raise ValueError(f"unknown action dialect: {sorted(action)}")
