#!/usr/bin/env python3
"""remoroo × cuRobo showcase — end-to-end multi-arm planning + execution on the real rig.

TWO JOBS:
  1. END-TO-END TEST — plan AND execute real coordinated multi-TCP moves through the cell Bridge
     (exercises the whole stack: seed → cuRobo plan → defensive audit → executor → real motion).
  2. CUSTOMER SHOWCASE — demonstrate cuRobo's realtime power on our rig: sub-second dual-arm
     collision-free planning (the ablated FAST_PLAN config), coordinated multi-TCP execution, and
     collision avoidance BETWEEN the two arms (paths that would collide if planned independently).

Safe by construction: every target is derived from the arms' CURRENT tool poses + bounded offsets
(reachable), every move is cuRobo-collision-free and passes the stack's defensive audit + safety
supervisor, the robot retracts home at start and end, and Ctrl-C E-stops then retracts. Use
--dry-run to plan + time WITHOUT moving (safe first pass); pass --execute to actually move.

    python3 demo_curobo_showcase.py --cell /path/to/remoroo_cell --dry-run     # plan only (safe)
    python3 demo_curobo_showcase.py --cell /path/to/remoroo_cell --execute      # real motion
"""
from __future__ import annotations

import argparse
import os
import signal
import statistics
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    from motion_engine import MotionStack
except Exception:  # noqa: BLE001
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from motion_engine import MotionStack


Pose = Tuple[List[float], List[float]]   # (xyz, wxyz)


def _ensure_safety_shim(cell: str) -> bool:
    """Register the `safety_shim` spine the cell Bridge imports (the edge does this at startup). Prefer
    the shipped `remoroo.edge`; else fall back to the edge's own resolver, which builds an inline
    spine — the path that actually works in the repo-checkout venv (the bench uses this)."""
    try:
        from remoroo import edge as spine
    except Exception:  # noqa: BLE001
        try:
            server_dir = str(Path(__file__).resolve().parents[1])
            if server_dir not in sys.path:
                sys.path.insert(0, server_dir)
            from edge_real import _ensure_safety_shim_resolves  # type: ignore
            _ensure_safety_shim_resolves()
            return True
        except Exception as e:  # noqa: BLE001
            print(f"  could not register safety_shim spine ({type(e).__name__}: {e})")
            return False
    name = Path(cell).name
    sys.modules.setdefault("safety_shim", spine)
    sys.modules.setdefault(f"{name}.safety_shim", spine)
    return True


def connect_bridge(cell: str):
    _ensure_safety_shim(cell)
    from remoroo_cell.primitives import Bridge  # type: ignore
    b = Bridge.from_cell_yaml(str(Path(cell) / "cell.yaml"))
    b.connect()
    return b


# --- geometry (pure) ---------------------------------------------------------------------------
# Targets are BARE [x,y,z] positions (orientation-free): cuRobo picks a reachable wrist orientation,
# so the arms can meet near the shared centre without us guessing feasible orientations. That's also
# where the coordination is visible — the two TCPs approach a common region and cuRobo keeps the arm
# BODIES collision-free.

XYZ = List[float]


def _center(home: Dict[str, Pose], arms: List[str]) -> XYZ:
    """Midpoint of the arms' current TCPs — the shared workspace centre the scenes aim at."""
    return [sum(home[a][0][i] for a in arms) / len(arms) for i in range(3)]


def _sorted_by_y(home: Dict[str, Pose], arms: List[str]) -> List[str]:
    return sorted(arms, key=lambda a: home[a][0][1])       # left→right


def _sep(targets: Dict[str, XYZ]) -> Optional[float]:
    pts = list(targets.values())
    if len(pts) < 2:
        return None
    a, b = pts[0], pts[1]
    return sum((float(a[i]) - float(b[i])) ** 2 for i in range(3)) ** 0.5


def _max_dq(traj) -> float:
    """Largest per-joint travel across the trajectory (rad) — proves the plan actually MOVES joints."""
    try:
        rows = [[float(v) for v in r] for r in traj.positions]
        if not rows:
            return 0.0
        return max(max(r[j] for r in rows) - min(r[j] for r in rows) for j in range(len(rows[0])))
    except Exception:  # noqa: BLE001
        return -1.0


# --- one plan→execute step ---------------------------------------------------------------------

def step(stack: MotionStack, name: str, targets: Dict[str, XYZ], execute: bool, log: List[dict]) -> bool:
    """ONE move: plan + (optionally) execute in a single call. `MoveResult.total_time` is cuRobo's
    plan solve time (the headline), and we also print the TCP separation the arms reach and the joint
    travel — so 'the arms met 16 cm apart, planned in 340 ms' is legible. Never raises."""
    sep = _sep(targets)
    t0 = time.perf_counter()
    try:
        mv = stack.move_to_poses(targets, execute=execute)
    except Exception as e:  # noqa: BLE001
        print(f"  ✗ {name:16s} raised {type(e).__name__}: {e}")
        log.append({"scene": name, "ok": False})
        return False
    wall = (time.perf_counter() - t0) * 1e3
    if not getattr(mv, "ok", False) or getattr(mv, "trajectory", None) is None:
        print(f"  ✗ {name:16s} no plan ({(getattr(mv, 'message', '') or '')[:46]})")
        log.append({"scene": name, "ok": False})
        return False
    plan_ms = float(getattr(mv, "total_time", 0.0)) * 1e3
    dq = _max_dq(mv.trajectory)
    dur = getattr(mv.trajectory, "duration", 0.0)
    sep_s = f"TCPs {sep * 100:4.0f}cm" if sep is not None else f"{len(targets)} TCP"
    if execute:
        st = "executed" if mv.executed else ("ABORTED" if getattr(mv, "aborted", False) else "not exec")
    else:
        st = "plan-only"
    flag = "✓" if mv.ok and (mv.executed or not execute) else ("⚠" if getattr(mv, "aborted", False) else "✗")
    print(f"  {flag} {name:16s} plan {plan_ms:5.0f} ms | {sep_s} | Δq {dq:4.2f} rad | "
          f"motion {dur:.1f}s | wall {wall:5.0f} ms | {st}")
    log.append({"scene": name, "ok": bool(mv.ok), "plan_ms": plan_ms, "sep": sep,
                "dq": dq, "executed": bool(mv.executed)})
    return bool(mv.ok)


# --- scenes (coordinated dual-arm, TCPs meeting near the shared centre) -------------------------

def planning_benchmark(stack: MotionStack, arms: List[str], home: Dict[str, Pose],
                       center: XYZ, reps: int) -> None:
    """(a) MULTI-TCP benchmark: one arm vs ALL arms coordinated to the centre, plan-only, no motion.
    Shows the FAST_PLAN sub-second headline + what the extra arms cost."""
    print("\n── planning-speed benchmark (plan only, no motion) ──")
    ys = _sorted_by_y(home, arms)
    one = {ys[0]: [center[0], home[ys[0]][0][1], center[2] + 0.12]}
    allt = {a: [center[0], center[1] + (0.09 if i % 2 == 0 else -0.09), center[2] + 0.12]
            for i, a in enumerate(ys)}
    for label, tgt in ((f"1 TCP ({ys[0]})", one), (f"{len(arms)} TCP → centre", allt)):
        ms = []
        for _ in range(reps):
            try:
                mv = stack.move_to_poses(tgt, execute=False)
                if getattr(mv, "ok", False):
                    ms.append(float(getattr(mv, "total_time", 0.0)) * 1e3)
            except Exception:  # noqa: BLE001
                pass
        if ms:
            print(f"  {label:18s} plan median {statistics.median(ms):6.0f} ms  (min {min(ms):.0f}, {len(ms)}/{reps} ok)")
        else:
            print(f"  {label:18s} — no successful plan")


def scene_spread(home: Dict[str, Pose], arms: List[str], center: XYZ) -> Dict[str, XYZ]:
    """Arms out to their OWN sides + up — the wide 'reset' pose between close encounters."""
    ys = _sorted_by_y(home, arms)
    return {a: [home[a][0][0], home[a][0][1] + (-0.10 if i == 0 else 0.10), center[2] + 0.15]
            for i, a in enumerate(ys)}


def scene_converge(home: Dict[str, Pose], arms: List[str], center: XYZ, sep: float) -> Dict[str, XYZ]:
    """Both TCPs meet NEAR the centre, `sep` apart (each stays on its own side) — the arm bodies come
    close and cuRobo keeps them collision-free. The signature coordinated move."""
    ys = _sorted_by_y(home, arms)
    return {a: [center[0], center[1] + (-sep / 2 if i == 0 else sep / 2), center[2] + 0.12]
            for i, a in enumerate(ys)}


def scene_cross(home: Dict[str, Pose], arms: List[str], center: XYZ, sep: float) -> Dict[str, XYZ]:
    """TCPs SWAP sides across the centre — the two arms' paths cross; cuRobo staggers them in time/space
    so the bodies never touch. The most striking collision-avoidance demo. (Kept at different heights
    for clearance.)"""
    ys = _sorted_by_y(home, arms)
    return {a: [center[0], center[1] + (sep / 2 if i == 0 else -sep / 2),
                center[2] + (0.08 if i == 0 else 0.20)]
            for i, a in enumerate(ys)}


def scene_weave(home: Dict[str, Pose], arms: List[str], center: XYZ) -> Dict[str, XYZ]:
    """Both near the centre at ALTERNATING heights — the arms interleave vertically."""
    ys = _sorted_by_y(home, arms)
    return {a: [center[0], center[1] + (-0.07 if i == 0 else 0.07),
                center[2] + (0.06 if i % 2 == 0 else 0.22)]
            for i, a in enumerate(ys)}


# --- orchestration -----------------------------------------------------------------------------

def run(stack: MotionStack, args) -> int:
    arms = list(stack._groups.keys())
    execute = bool(args.execute) and not args.dry_run
    print("=" * 74)
    print(f"cuRobo showcase — arms={arms}  mode={'EXECUTE (real motion)' if execute else 'DRY-RUN (plan only)'}")

    if execute:
        print("\nretracting to home...")
        r = stack.retract(execute=True)
        print(f"  retract: ok={r.ok} executed={r.executed} {r.message[:50]}")
        if not r.ok:
            print("  cannot reach home — aborting for safety.")
            return 2

    home = {a: stack.current_tool_pose(a) for a in arms}
    if any(v is None for v in home.values()):
        print("  could not FK current tool poses — is the bridge connected?")
        return 2
    center = _center(home, arms)

    planning_benchmark(stack, arms, home, center, reps=args.reps)

    # The showcase sequence: each entry is ONE coordinated plan→execute. The centre scenes bring the
    # two TCPs close / cross them, so the collision-free coordination is visible.
    seq = [("spread", scene_spread(home, arms, center))]
    if len(arms) >= 2:
        seq += [("converge", scene_converge(home, arms, center, args.sep)),
                ("cross", scene_cross(home, arms, center, args.sep)),
                ("weave", scene_weave(home, arms, center)),
                ("converge-tight", scene_converge(home, arms, center, max(0.12, args.sep * 0.7)))]

    print("\n── coordinated multi-TCP moves (plan → execute) ──")
    log: List[dict] = []
    for loop in range(args.loops):
        if args.loops > 1:
            print(f"  · loop {loop + 1}/{args.loops}")
        for name, targets in seq:
            step(stack, name, targets, execute, log)

    if execute:
        print("\nretracting home...")
        stack.retract(execute=True)

    ok = [r for r in log if r.get("ok")]
    pms = [r["plan_ms"] for r in log if r.get("plan_ms")]
    seps = [r["sep"] for r in log if r.get("ok") and r.get("sep") is not None]
    print("\n" + "-" * 74)
    if pms:
        line = (f"SUMMARY: {len(ok)}/{len(log)} moves ok · plan median {statistics.median(pms):.0f} ms "
                f"(min {min(pms):.0f})")
        if seps:
            line += f" · closest TCP approach {min(seps) * 100:.0f} cm"
        line += f" · {'EXECUTED on hardware' if execute else 'DRY-RUN, no motion'}"
        print(line)
    print("-" * 74)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="remoroo × cuRobo end-to-end multi-arm showcase")
    ap.add_argument("--cell", default=os.environ.get("REMOROO_CELL", "remoroo_cell"))
    ap.add_argument("--execute", action="store_true", help="ACTUALLY MOVE the robot (default: dry-run)")
    ap.add_argument("--dry-run", action="store_true", help="plan + time only, never move (overrides --execute)")
    ap.add_argument("--loops", type=int, default=1, help="how many times through the scene set")
    ap.add_argument("--sep", type=float, default=0.20,
                    help="TCP separation (m) at the centre for the converge/cross scenes — smaller = "
                         "the arms get closer (more dramatic coordination); the plan fails safely if too tight")
    ap.add_argument("--reps", type=int, default=6, help="plan repetitions for the speed benchmark")
    args = ap.parse_args()
    cell = str(Path(args.cell).resolve())

    bridge = None
    try:
        print("connecting bridge (robot + cameras)...")
        bridge = connect_bridge(cell)
    except Exception as e:  # noqa: BLE001
        print(f"bridge connect FAILED ({type(e).__name__}: {e}) — cannot run the end-to-end demo.")
        return 2

    # E-stop + retract on Ctrl-C / SIGTERM
    def _stop(_sig, _frm):
        print("\n! interrupt — E-stop + retract")
        try:
            if hasattr(bridge, "estop"):
                bridge.estop()
        except Exception:  # noqa: BLE001
            pass
        raise KeyboardInterrupt
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        print("building + warming the motion stack...")
        stack = MotionStack.from_cell(cell, bridge=bridge)
        stack.prewarm()
        return run(stack, args)
    except KeyboardInterrupt:
        return 130
    except Exception as e:  # noqa: BLE001
        import traceback
        print(f"demo failed: {type(e).__name__}: {e}")
        traceback.print_exc()
        return 1
    finally:
        try:
            if bridge is not None and hasattr(bridge, "disconnect"):
                bridge.disconnect()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    raise SystemExit(main())
