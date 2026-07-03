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
    def __init__(self, *, max_step_m: float = 0.05) -> None:
        self.max_step_m = float(max_step_m)

    def clamp_delta(self, delta) -> list:
        return [max(-self.max_step_m, min(self.max_step_m, float(v))) for v in delta[:3]]

    def apply(self, ctx: Ctx, action: Dict[str, Any]) -> bool:
        tcp = action["tcp"]
        if "gripper" in action:
            if float(action["gripper"]) < 0.02:
                return atoms.grasp(ctx, tcp, width=float(action["gripper"])).ok
            return atoms.release(ctx, tcp, open_width=float(action["gripper"])).ok
        if "delta_xyz" in action:
            cur = ctx.stack.link_pose(tcp)
            d = self.clamp_delta(action["delta_xyz"])
            tgt = [cur[0] + d[0], cur[1] + d[1], cur[2] + d[2]] + list(cur[3:7])
            return atoms.reach(ctx, tcp, tgt).ok
        if "pose" in action:
            return atoms.reach(ctx, tcp, action["pose"]).ok
        raise ValueError(f"unknown action dialect: {sorted(action)}")
