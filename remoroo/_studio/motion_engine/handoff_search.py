#!/usr/bin/env python3
"""Where can the two arms MEET for a handoff? — NO MOTION.

A mid-air handoff needs both grippers at the same place at the same time. That is not free on this rig:
the close-encounter sweep showed the arms' ELBOWS swing backward into `obs_5_wall` (a 2m x 5cm slab at
x=-0.326 behind the robot) once each arm reaches more than ~0.16 m inward — even though the TCPs stay
far in front. So before building any handoff we answer, from data, whether a collision-free meeting
pose exists at all, and where.

Method: grid over (forward offset, height offset, gripper separation) around the midpoint of the two
arms' current TCPs, and for each candidate ask the REAL planner (`move_to_poses`, execute=False) to
drive both TCPs there. plan_pose succeeding proves three things at once: both poses are reachable, the
meeting configuration is collision-free (trajopt treats collision as a CONSTRAINT), and a safe path
exists from where the arms are now. Failure is equally informative — it is the planner refusing.

    python3 -m motion_engine.handoff_search
    python3 -m motion_engine.handoff_search --seps 0.04,0.08 --fwd -0.05,0.05,0.15
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import numpy as np

try:
    from motion_engine import MotionStack
except Exception:  # noqa: BLE001
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from motion_engine import MotionStack

from motion_engine.demo_curobo_showcase import _sorted_by_y, connect_bridge  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="search for a collision-free two-arm handoff pose (NO MOTION)")
    ap.add_argument("--cell", default=os.environ.get("REMOROO_CELL", "remoroo_cell"))
    ap.add_argument("--fwd", default="-0.05,0.05,0.15,0.25",
                    help="forward (x) offsets from the TCP midpoint — the wall is BEHIND, so positive is safer")
    ap.add_argument("--up", default="-0.05,0.10,0.25", help="height (z) offsets from the TCP midpoint")
    ap.add_argument("--seps", default="0.04,0.08,0.12",
                    help="gripper separation (m) at the meeting point — the object spans this")
    ap.add_argument("--attempts", type=int, default=1)
    args = ap.parse_args()
    cell = str(Path(args.cell).resolve())

    bridge = connect_bridge(cell)
    stack = MotionStack.from_cell(cell, bridge=bridge)
    stack.prewarm()
    arms = list(stack._groups.keys())
    home = {a: stack.current_tool_pose(a) for a in arms}
    if any(v is None for v in home.values()):
        print("could not FK current tool poses")
        return 2
    ys = _sorted_by_y(home, arms)                       # ys[0] = lower y, ys[1] = higher y
    if len(ys) < 2:
        print("handoff needs two arms")
        return 2
    c = np.mean([np.asarray(home[a][0], float) for a in arms], axis=0)   # midpoint of the two TCPs
    gap = float(np.linalg.norm(np.asarray(home[ys[0]][0], float) - np.asarray(home[ys[1]][0], float)))
    print(f"\narms={arms}  TCP midpoint={[round(float(x), 3) for x in c]}  current TCP gap={gap * 100:.0f} cm")
    print("obs_5_wall is a 5 cm slab at x=-0.326 BEHIND the robot; each arm reaching >0.16 m inward")
    print("swings an elbow into it, so we search FORWARD and HIGH of the midpoint.\n")

    fwd = [float(x) for x in args.fwd.split(",") if x.strip()]
    up = [float(x) for x in args.up.split(",") if x.strip()]
    seps = [float(x) for x in args.seps.split(",") if x.strip()]

    print("%6s %6s %6s %9s %8s  %s" % ("fwd_m", "up_m", "sep_m", "reach_cm", "plan_ms", "verdict"))
    ok_rows = []
    for dz in up:
        for dx in fwd:
            for d in seps:
                m = c + np.array([dx, 0.0, dz])
                t0 = {ys[0]: list(m + np.array([0.0, -d / 2.0, 0.0])),
                      ys[1]: list(m + np.array([0.0, +d / 2.0, 0.0]))}
                reach = max(float(np.linalg.norm(np.asarray(t0[a]) - np.asarray(home[a][0], float)))
                            for a in ys)
                t1 = time.perf_counter()
                try:
                    mv = stack.move_to_poses(t0, execute=False, max_attempts=int(args.attempts))
                    ok = bool(getattr(mv, "ok", False)) and getattr(mv, "trajectory", None) is not None
                    msg = (getattr(mv, "message", "") or "")[:44]
                except Exception as e:  # noqa: BLE001
                    ok, msg = False, f"{type(e).__name__}: {e}"[:44]
                ms = (time.perf_counter() - t1) * 1e3
                print("%6.2f %6.2f %6.2f %9.1f %8.0f  %s"
                      % (dx, dz, d, reach * 100, ms, "MEETS" if ok else "no plan: " + msg))
                sys.stdout.flush()
                if ok:
                    ok_rows.append({"fwd": dx, "up": dz, "sep": d, "reach": reach, "ms": ms,
                                    "pos": [round(float(v), 3) for v in m]})

    print("\n── feasible handoff poses (tightest separation first = best object transfer) ──")
    if not ok_rows:
        print("  NONE — no collision-free meeting pose in this grid. A mid-air handoff is not")
        print("  achievable at these offsets; widen --fwd/--up or reconsider the handoff.")
    for r in sorted(ok_rows, key=lambda r: (r["sep"], -r["fwd"]))[:10]:
        print("  fwd=%+.2f up=%+.2f sep=%.2fm at %s · each arm travels %.0fcm"
              % (r["fwd"], r["up"], r["sep"], r["pos"], r["reach"] * 100))
    try:
        bridge.disconnect()
    except Exception:  # noqa: BLE001
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
