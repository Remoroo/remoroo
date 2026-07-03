"""The kit's reference actors (edge half of COMP-13): the do-nothing and the random flailer.
The kit PROTOCOL (which of these to run, what must score ~0, the cheat adversary) is
brain-side; these run as ordinary trials through the same Env."""
from __future__ import annotations

import random
from typing import Any

from .. import atoms
from ..atoms import Ctx


def null_actor(env: Any, ctx: Ctx) -> None:
    """Does nothing. An honest verifier must score this ~0."""
    atoms.hold(ctx, next(iter(ctx.home_poses), "arm_a"))


def make_random_actor(*, workspace: dict, seed: int = 0, moves: int = 5):
    """Random flailing inside the safety workspace. An honest verifier must score this ~0
    (up to proprio partial credit if it accidentally grasps something)."""
    lo_hi = {a: workspace.get(a, [0.2, 0.5]) for a in ("x", "y", "z")}

    def random_actor(env: Any, ctx: Ctx) -> None:
        rng = random.Random(seed)
        tcp = next(iter(ctx.home_poses), "arm_a")
        for _ in range(moves):
            p = [rng.uniform(*lo_hi["x"]), rng.uniform(*lo_hi["y"]), rng.uniform(*lo_hi["z"])]
            atoms.reach(ctx, tcp, p)
            if rng.random() < 0.4:
                atoms.grasp(ctx, tcp, width=rng.uniform(0.005, 0.04))
                atoms.release(ctx, tcp)

    return random_actor
