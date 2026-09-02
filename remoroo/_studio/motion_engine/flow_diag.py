#!/usr/bin/env python3
"""Categorise WHY flow waypoints fail validation — NO MOTION.

`validate` returns one boolean covering bounds + world + self, which is useless for deciding what to
fix. This splits the failing frames into those three causes and names the offending obstacle, so the
fix is aimed at the real problem instead of guessed at.

    python3 -m motion_engine.flow_diag --smooth 25
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import Counter
from pathlib import Path

import numpy as np

try:
    from motion_engine import MotionStack
except Exception:  # noqa: BLE001
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from motion_engine import MotionStack

from motion_engine.demo_curobo_showcase import (  # noqa: E402
    _apply, _flow_path, _world_culprits, connect_bridge, scenes, smooth_joints, validate_flow,
)
from motion_engine.trajectory import Trajectory  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="categorise flow validation failures (NO MOTION)")
    ap.add_argument("--cell", default=os.environ.get("REMOROO_CELL", "remoroo_cell"))
    ap.add_argument("--smooth", type=int, default=25)
    ap.add_argument("--accel", type=float, default=1.5)
    ap.add_argument("--step", type=float, default=0.006)
    ap.add_argument("--dt", type=float, default=0.025)
    ap.add_argument("--tol", type=float, default=0.005)
    ap.add_argument("--sep", type=float, default=0.06)
    ap.add_argument("--close", type=float, default=0.20)
    ap.add_argument("--stagger", type=float, default=0.18)
    ap.add_argument("--depth", type=float, default=0.08)
    ap.add_argument("--samples", type=int, default=6)
    args = ap.parse_args()
    cell = str(Path(args.cell).resolve())

    import torch
    from curobo.motion_retargeter import MotionRetargeter, MotionRetargeterCfg, SequenceGoalToolPose
    from curobo.types import ToolPoseCriteria

    bridge = connect_bridge(cell)
    stack = MotionStack.from_cell(cell, bridge=bridge)
    stack.prewarm()
    arms = list(stack._groups.keys())
    cvp = stack._planner_for(arms)
    home = {a: stack.current_tool_pose(a) for a in arms}
    seq = scenes(home, arms, approach=args.sep, close=args.close, stagger=args.stagger,
                 depth=args.depth)
    keys = {a: [list(home[a][0])] for a in arms}
    for _n, off in seq:
        t = _apply(home, arms, off, 1.0)
        for a in arms:
            keys[a].append([float(v) for v in t[a]])
    for a in arms:
        keys[a].append(list(home[a][0]))

    # Which keypose is nearest each failing frame? Print the keypose table with x (the wall is a thin
    # slab at x=-0.33 BEHIND the robot, so the most-negative x keyposes are the suspects).
    kp_names = ["home"] + [n for n, _ in seq] + ["home"]
    print("\n  keypose x per arm (wall obs_5_wall is a 5cm slab at x=-0.326):")
    for i, nm in enumerate(kp_names):
        xs = "  ".join("%s x=%+.3f" % (a, keys[a][i][0]) for a in arms)
        print("    %2d %-10s %s" % (i, nm, xs))

    dense, S, N, T = _flow_path(keys, arms, args.step / args.dt, args.accel, args.dt)
    # frame -> nearest keypose (by arc position of the first arm)
    a0 = arms[0]
    kxyz = np.asarray(keys[a0], float)
    dxyz = dense[a0]
    near = np.argmin(((dxyz[:, None, :] - kxyz[None, :, :]) ** 2).sum(-1), axis=1)
    tool_frames = [stack._tip(a) for a in arms]
    pos = np.stack([dense[a] for a in arms], axis=1)
    cfg = MotionRetargeterCfg.create(
        robot=stack._robot_cfg_for(arms),
        tool_pose_criteria={tf: ToolPoseCriteria.track_position() for tf in tool_frames},
        use_mpc=False, self_collision_check=True, scene_model=stack.world.scene,
        optimization_dt=args.dt, position_tolerance=args.tol,
    )
    rt = MotionRetargeter(cfg)
    dev = cfg.device_cfg.device
    pt = torch.tensor(pos, dtype=torch.float32).view(N, 1, len(arms), 1, 3)
    qt = torch.zeros(N, 1, len(arms), 1, 4, dtype=torch.float32)
    qt[..., 0] = 1.0
    res = rt.solve_sequence(SequenceGoalToolPose(tool_frames=list(tool_frames),
                                                 position=pt.to(dev), quaternion=qt.to(dev)))
    names = list(res.joint_state.joint_names)
    raw = res.joint_state.position[0].detach().cpu().numpy()

    for win in ({1, int(args.smooth)}):
        q = smooth_joints(raw, win)
        fl = Trajectory(names, q, args.dt)
        v = validate_flow(stack, arms, fl)
        bad = v.get("bad", [])
        print(f"\n{'=' * 78}\nsmooth={win}: {len(bad)}/{v.get('n')} waypoints fail validate")
        if v.get("error"):
            print("  validate error:", v["error"])
            continue
        if not bad:
            print("  ALL CLEAN")
            continue
        # 1) BOUNDS — pure numpy against the planner's own limits
        lims = cvp.joint_limits()
        qn = {n: q[:, i] for i, n in enumerate(names)}
        oob = Counter()
        for n, (lo, hi) in lims.items():
            if n in qn:
                bad_mask = (qn[n] < lo - 1e-6) | (qn[n] > hi + 1e-6)
                if bad_mask.any():
                    oob[n] = int(bad_mask.sum())
        print(f"  BOUNDS: {'none' if not oob else dict(oob)}")
        # 2/3) WORLD vs SELF on a sample of the failing frames
        from collections import Counter as _C
        hit = _C(kp_names[int(near[f])] for f in bad if f < len(near))
        print(f"  failing frames cluster at keyposes: {hit.most_common(6)}")
        print(f"  frame range: {min(bad)}..{max(bad)} of {v.get('n')}")
        idx = [bad[i] for i in np.linspace(0, len(bad) - 1, min(args.samples, len(bad))).astype(int)]
        for f in idx:
            seed = {n: float(q[f, i]) for i, n in enumerate(names)}
            info = cvp.explain_start_collision(seed)
            cul = _world_culprits(stack, cvp, seed)
            names_hit = ", ".join(f"{c[0]}({c[1]})" for c in cul[:3]) or "-"
            print(f"    frame {f:4d}: {str(info.get('summary'))[:88]}")
            print(f"                world-AABB culprits: {names_hit}")
    try:
        bridge.disconnect()
    except Exception:  # noqa: BLE001
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
