"""Norm-stats bootstrap (2026-07-15) — REAL statistics for the vendor normalizer
without teleop: a short guarded joint-space tour around home, packing the profile's
observation at every stop, then per-dimension stats over the visited states.

Identity stats proved the wire but de-normalize the model's outputs into
semantically-aimless motion (the first day-zero rollout: slow, small, approached
nothing). Bootstrap stats are honest about what they are: the distribution of THIS
cell's reachable states around home — and CRITICALLY, they are the policy's
reachable envelope: denormalization is z*std+mean, so the model can only command
poses within a few sigma of what the tour visited (live 2026-07-15: a 0.12-rad
tour trapped the policy in a centimeter bubble — "completed, zero movement").
Default span/poses raised so the tour SWEEPS the workspace; still NOT a substitute
for episode statistics (the finetune pipeline overwrites this same file).

Action features use the state's own distribution for absolute-target groups
(subtract_state False: an absolute EE-pose action lives in the same space as the
EE-pose state)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import numpy as np


def _block(arr: np.ndarray) -> Dict[str, list]:
    """The vendor's norm-stats entry shape for one feature: per-dim moments over
    samples. std floors at 1e-3 so a barely-moving dim can't explode the divide."""
    return {"mean": arr.mean(axis=0).tolist(),
            "std": np.maximum(arr.std(axis=0), 1e-3).tolist(),
            "q01": np.quantile(arr, 0.01, axis=0).tolist(),
            "q99": np.quantile(arr, 0.99, axis=0).tolist(),
            "min": arr.min(axis=0).tolist(),
            "max": arr.max(axis=0).tolist()}


GRIPPER_PRIOR = {"mean": [0.02], "std": [0.02], "q01": [0.0], "q99": [0.05],
                 "min": [0.0], "max": [0.05]}   # a parallel gripper's opening, stated


def stats_from_states(profile: Any, states: List[np.ndarray]) -> Dict[str, Any]:
    """states: packed flat state vectors (profile.state_dim) from the tour."""
    S = np.stack([np.asarray(s, dtype=float) for s in states])
    out: Dict[str, Dict[str, list]] = {}
    for group in dict.fromkeys(s.group for s in profile.state):
        cols = np.concatenate([S[:, s.start:s.end]
                               for s in profile.state if s.group == group], axis=1)
        if group == "effector.position" and float(np.abs(cols).max()) < 1e-9:
            # no gripper feedback on this cell: a stated physical prior beats
            # floored zeros (zeros made the decode threshold always fire "close")
            dims = cols.shape[1]
            out[f"observation.state.{group}"] = {
                k: v * dims for k, v in GRIPPER_PRIOR.items()}
            continue
        out[f"observation.state.{group}"] = _block(cols)
    for group in dict.fromkeys(s.group for s in profile.actions):
        src = [s for s in profile.state if s.group == group]
        if not src:                              # action group with no state twin:
            continue                             # stays identity via the file's absence
        cols = np.concatenate([S[:, s.start:s.end] for s in src], axis=1)
        if group == "effector.position" and float(np.abs(cols).max()) < 1e-9:
            dims = cols.shape[1]
            out[f"action.{group}"] = {k: v * dims for k, v in GRIPPER_PRIOR.items()}
            continue
        out[f"action.{group}"] = _block(cols)
    return {"norm_stats": out}


def bootstrap_tour(profile: Any, *, stack: Any, bridge: Any, packer: Any,
                   n_poses: int = 16, joint_span: float = 0.35,
                   seed: int = 0) -> List[np.ndarray]:
    """Visit n_poses small joint-space offsets around the CURRENT config — every move
    collision-checked through goto_joints (the planner IS the right tool here: this
    is transport, not policy execution) — packing the observation at each stop, and
    return home. Refuses without a stack (no blind motion)."""
    if stack is None:
        raise RuntimeError("norm-stats bootstrap needs the motion stack (guarded "
                           "joint moves) — run it on the cell via the edge")
    rng = np.random.default_rng(seed)
    obs0 = bridge.get_observation()
    home = {str(n): float(v) for n, v in dict(obs0.joint_positions).items()}
    arm_joints = [n for n in home if "finger" not in n and "knuckle" not in n]
    first = packer()
    # REFUSE blindness (live 2026-07-15: a fully-degraded packer averaged zeros into
    # the stats file, which then scaled every policy action by 0.001 — "almost no
    # motion"). Arm/EE degradation is fatal here; a missing gripper reading alone is
    # tolerated and STATED (its stats get a physical prior downstream).
    fatal = [d for d in first.get("degraded", [])
             if not d.startswith("state:effector")]
    if fatal:
        raise RuntimeError(
            "the observation packer is DEGRADED — stats over blindness poison the "
            f"normalizer. Broken sources: {fatal}. Fix the packer/bridge seam "
            "(cell groups joint mapping, stack FK) before bootstrapping.")
    states: List[np.ndarray] = [np.asarray(first["observation.state"], dtype=float)]
    try:
        for _ in range(int(n_poses)):
            goal = dict(home)
            for j in rng.choice(arm_joints, size=min(3, len(arm_joints)),
                                replace=False):
                goal[j] = home[j] + float(rng.uniform(-joint_span, joint_span))
            res = stack.goto_joints(goal)
            if not getattr(res, "ok", False):
                continue                         # an unreachable sample is skipped, stated by count
            states.append(np.asarray(packer()["observation.state"], dtype=float))
    finally:
        stack.goto_joints(home)                  # ALWAYS end at home: cameras clear
    return states
