#!/usr/bin/env python3
"""Sweep flow parameterisations on the rig — NO MOTION, ever.

Why this exists: every `demo_curobo_showcase --flow --dry-run` pays ~70 s of planner build plus a full
solve, so comparing parameterisations one run at a time is hopeless (an MPC run took 8.5 min). This
builds the stack AND the retargeter ONCE, then solves many candidate paths through the same solver, so
the only thing that varies between rows is the thing under test.

It answers one question with data: which (solver, accel-cap, step) gives a flow that is DYNAMICALLY
FEASIBLE for this rig — peak joint |v|,|a|,|j| within the arm's own limits — while staying fast enough
to look good. The retargeter does NOT enforce those limits (trajopt does, which is why the per-pose
pipeline is smooth), so we have to shape the input path and verify the output.

    python3 -m motion_engine.flow_sweep --accels 0.6,1.0,1.5,2.5        # IK sweep
    python3 -m motion_engine.flow_sweep --mpc --accels 1.0,1.5          # MPC (slow: ~1.4 s/target)
"""
from __future__ import annotations

import argparse
import math
import os
import sys
import time
from pathlib import Path
from typing import Dict, List

import numpy as np

try:
    from motion_engine import MotionStack
except Exception:  # noqa: BLE001
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from motion_engine import MotionStack

from motion_engine.demo_curobo_showcase import (  # noqa: E402
    _SeedTracker, _apply, _flow_path, connect_bridge, generate_poses, max_speedup, scenes,
    smooth_joints, tcp_tracking_error, validate_flow,
)
from motion_engine.trajectory import Trajectory  # noqa: E402

URDF_VEL, MAX_ACC, MAX_JERK = 3.14, 15.0, 500.0        # the rig's own limits (URDF + safety)


def build_keys(stack, arms, args):
    """The dance keyposes (absolute TCP xyz per arm), starting and ending at the current pose."""
    home = {a: stack.current_tool_pose(a) for a in arms}
    if any(v is None for v in home.values()):
        raise RuntimeError("could not FK current tool poses")
    if args.poses > 0:
        seq = generate_poses(home, arms, args.poses, close=args.close, stagger=args.stagger,
                             depth=args.depth)
    else:
        seq = scenes(home, arms, approach=args.sep, close=args.close, stagger=args.stagger,
                     depth=args.depth)
    keys: Dict[str, List[list]] = {a: [list(home[a][0])] for a in arms}
    for _n, off in seq:
        tgt = _apply(home, arms, off, 1.0)
        for a in arms:
            keys[a].append([float(v) for v in tgt[a]])
    for a in arms:
        keys[a].append(list(home[a][0]))
    return keys, len(seq) + 2


def measure(pos: np.ndarray, dt: float):
    """Peak joint |v|,|a|,|j| and the time-stretch k that would make the path feasible.
    A pure time stretch by k scales v by 1/k, a by 1/k^2, j by 1/k^3."""
    v = np.diff(pos, axis=0) / dt
    a = np.diff(v, axis=0) / dt
    j = np.diff(a, axis=0) / dt
    pv, pa, pj = float(np.abs(v).max()), float(np.abs(a).max()), float(np.abs(j).max())
    k = max(1.0, pv / URDF_VEL, math.sqrt(pa / MAX_ACC), (pj / MAX_JERK) ** (1.0 / 3.0))
    return pv, pa, pj, k


def main() -> int:
    ap = argparse.ArgumentParser(description="flow parameterisation sweep (NO MOTION)")
    ap.add_argument("--cell", default=os.environ.get("REMOROO_CELL", "remoroo_cell"))
    ap.add_argument("--accels", default="0.6,1.0,1.5,2.5", help="comma list of Cartesian accel caps m/s^2")
    ap.add_argument("--steps-list", default="", help="comma list of --flow-step values (m) to cross with accels")
    ap.add_argument("--step", type=float, default=0.006)
    ap.add_argument("--dt", type=float, default=0.025)
    ap.add_argument("--mpc", action="store_true")
    ap.add_argument("--mpc-steps", type=int, default=4)
    ap.add_argument("--tol", type=float, default=0.005)
    ap.add_argument("--vreg", type=float, default=-1.0)
    ap.add_argument("--areg", type=float, default=-1.0)
    ap.add_argument("--closes", default="", help="comma list of --close values (m) for the close encounter")
    ap.add_argument("--staggers", default="", help="comma list of --stagger values (m)")
    ap.add_argument("--cads", default="", help="comma list of collision_activation_distance (m)")
    ap.add_argument("--smooths", default="1", help="comma list of joint smoothing window sizes (1=off)")
    ap.add_argument("--tols", default="", help="comma list of position_tolerance (m) — rebuilds solver each")
    ap.add_argument("--vregs", default="", help="comma list of velocity_regularization_weight (-1=default)")
    ap.add_argument("--aregs", default="", help="comma list of acceleration_regularization_weight (-1=default)")
    ap.add_argument("--poses", type=int, default=0)
    ap.add_argument("--sep", type=float, default=0.06)
    ap.add_argument("--close", type=float, default=0.20)
    ap.add_argument("--stagger", type=float, default=0.18)
    ap.add_argument("--depth", type=float, default=0.08)
    args = ap.parse_args()
    cell = str(Path(args.cell).resolve())

    import torch
    from curobo.motion_retargeter import MotionRetargeter, MotionRetargeterCfg, SequenceGoalToolPose
    from curobo.types import ToolPoseCriteria

    torch.manual_seed(0); np.random.seed(0)   # reproducible global-IK seeding
    print("connecting bridge...")
    bridge = connect_bridge(cell)
    try:
        _SeedTracker(bridge)
    except Exception:  # noqa: BLE001
        pass
    print("building + warming the motion stack (one time)...")
    stack = MotionStack.from_cell(cell, bridge=bridge)
    stack.prewarm()

    arms = list(stack._groups.keys())
    tool_frames = [stack._tip(a) for a in arms]
    dt = float(args.dt)
    mult = args.mpc_steps if args.mpc else 1

    accels = [float(x) for x in args.accels.split(",") if x.strip()]
    stepl = [float(x) for x in args.steps_list.split(",") if x.strip()] or [float(args.step)]
    tols = [float(x) for x in args.tols.split(",") if x.strip()] or [float(args.tol)]
    vregs = [float(x) for x in args.vregs.split(",") if x.strip()] or [float(args.vreg)]
    aregs = [float(x) for x in args.aregs.split(",") if x.strip()] or [float(args.areg)]
    smooths = [int(x) for x in args.smooths.split(",") if x.strip()] or [1]
    cads = [float(x) for x in args.cads.split(",") if x.strip()] or [0.01]
    closes = [float(x) for x in args.closes.split(",") if x.strip()] or [float(args.close)]
    staggers = [float(x) for x in args.staggers.split(",") if x.strip()] or [float(args.stagger)]

    print(f"\ndance: arms={arms} · dt={dt * 1e3:.0f}ms · "
          f"solver={'MPC x' + str(args.mpc_steps) if args.mpc else 'IK'}")
    print(f"rig limits: |v|<={URDF_VEL} rad/s  |a|<={MAX_ACC} rad/s^2  |j|<={MAX_JERK} rad/s^3")
    print("(the retargeter enforces NONE of these — trajopt does, which is why per-pose is smooth)\n")
    hdr = ("close", "stag", "smth", "wp", "dur_s", "pk|a|", "pk|j|", "tcp_mm",
           "SPEEDUP", "fast_s", "collide")
    print("%6s %6s %5s %5s %6s %8s %9s %7s %8s %7s %9s" % hdr)

    rows = []
    # Solver params live in the CFG, so each combination needs its own retargeter build (~75 s).
    # Path params (accel/step) only reshape the input, so they sweep cheaply inside one build.
    for tol in tols:
      for cad in cads:
        for vreg in vregs:
            for areg in aregs:
                cfg = MotionRetargeterCfg.create(
                    robot=stack._robot_cfg_for(arms),
                    tool_pose_criteria={tf: ToolPoseCriteria.track_position() for tf in tool_frames},
                    use_mpc=bool(args.mpc),
                    self_collision_check=True,
                    scene_model=stack.world.scene,
                    optimization_dt=dt,
                    position_tolerance=float(tol),
                    collision_activation_distance=float(cad),
                    velocity_regularization_weight=(None if vreg < 0 else float(vreg)),
                    acceleration_regularization_weight=(None if areg < 0 else float(areg)),
                    steps_per_target=int(args.mpc_steps),
                )
                retargeter = MotionRetargeter(cfg)
                dev = cfg.device_cfg.device
                for cl in closes:
                  for sg_ in staggers:
                    args.close, args.stagger = cl, sg_
                    keys, n_keys = build_keys(stack, arms, args)
                    for st in stepl:
                      for acc in accels:
                        eff = st * mult
                        dense, S, N, T = _flow_path(keys, arms, eff / dt, acc, dt * mult)
                        pos = np.stack([dense[a] for a in arms], axis=1)
                        pt = torch.tensor(pos, dtype=torch.float32).view(N, 1, len(arms), 1, 3)
                        qt = torch.zeros(N, 1, len(arms), 1, 4, dtype=torch.float32)
                        qt[..., 0] = 1.0
                        sg = SequenceGoalToolPose(tool_frames=list(tool_frames),
                                                  position=pt.to(dev), quaternion=qt.to(dev))
                        t1 = time.perf_counter()
                        res = retargeter.solve_sequence(sg)
                        solve_s = time.perf_counter() - t1
                        src = res.trajectory if (args.mpc and res.trajectory is not None) else res.joint_state
                        q_raw = src.position[0].detach().cpu().numpy()
                        for win in smooths:
                            q = smooth_joints(q_raw, win)
                            dev = float(np.abs(q - q_raw).max())      # how far the filter moved the path
                            pv, pa, pj, k = measure(q, dt)
                            ok = pv <= URDF_VEL and pa <= MAX_ACC and pj <= MAX_JERK
                            verdict = ("FEASIBLE" if ok else
                                       ("near %.2fx" % k if k < 1.5 else "OVER %.1fx" % k))
                            fl = Trajectory(list(src.joint_names), q, dt)
                            vres = validate_flow(stack, arms, fl)
                            coll = ("clean" if vres.get("ok") else
                                    ("ERR:" + vres["error"][:18] if vres.get("error")
                                     else "%d bad" % len(vres.get("bad", []))))
                            terr = tcp_tracking_error(stack, arms, fl, pos)
                            tmean = terr.get("mean", float("nan")) * 1e3 if terr.get("ok") else float("nan")
                            tmax = terr.get("max", float("nan")) * 1e3 if terr.get("ok") else float("nan")
                            kfast = max_speedup(fl, dt, (URDF_VEL, MAX_ACC, MAX_JERK), margin=0.95)
                            fast_s = ((len(q) - 1) * dt) / kfast
                            print("%6.2f %6.2f %5d %5d %6.1f %8.1f %9.0f %7.1f %8.2f %7.1f %9s"
                                  % (cl, sg_, win, len(q), (len(q) - 1) * dt, pa, pj,
                                     tmean, kfast, fast_s, coll))
                            sys.stdout.flush()
                            rows.append({"tol": tol, "vreg": vreg, "areg": areg, "win": win,
                                         "step": st, "accel": acc, "targets": N, "wp": len(q),
                                         "dur": (len(q) - 1) * dt, "solve": solve_s, "v": pv, "a": pa,
                                         "j": pj, "k": k, "ok": ok, "dev": dev,
                                         "kfast": kfast, "fast_s": fast_s,
                                         "cad": cad, "close": cl, "stag": sg_,
                                         "n_bad": len(vres.get("bad", [])),
                                         "coll_ok": bool(vres.get("ok")),
                                         "tcp_mean": tmean, "tcp_max": tmax})
                del retargeter

    print("\n── ranking: collision-clean, FASTEST achievable duration after time-compression ──")
    good = [r for r in rows if r["coll_ok"]]
    for r in sorted(good, key=lambda r: r["fast_s"])[:8]:
        print("  smooth=%d close=%.2f stag=%.2f → %.1fs at %.2fx (from %.1fs) · TCP %.1fmm · clean"
              % (r["win"], r["close"], r["stag"], r["fast_s"], r["kfast"], r["dur"], r["tcp_mean"]))
    if not good:
        print("  (none both feasible AND collision-clean)")
        for r in sorted(rows, key=lambda r: (r["k"],))[:5]:
            print("    close=%.2f stagger=%.2f smooth=%d → |a|%.1f |j|%.0f · %s"
                  % (r["close"], r["stag"], r["win"], r["a"], r["j"],
                     "collision-CLEAN" if r["coll_ok"] else "%d bad wp" % r["n_bad"]))
    try:
        bridge.disconnect()
    except Exception:  # noqa: BLE001
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
