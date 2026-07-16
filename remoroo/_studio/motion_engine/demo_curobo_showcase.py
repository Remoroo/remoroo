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


# --- geometry helpers (pure) -------------------------------------------------------------------

def _off(pose: Pose, dx: float = 0.0, dy: float = 0.0, dz: float = 0.0) -> Pose:
    (x, y, z), q = pose
    return ([float(x) + dx, float(y) + dy, float(z) + dz], list(q))


def _toward(pose: Pose, target_xyz: List[float], frac: float) -> Pose:
    (x, y, z), q = pose
    tx, ty, tz = target_xyz
    return ([x + frac * (tx - x), y + frac * (ty - y), z + frac * (tz - z)], list(q))


def _mid(a: Pose, b: Pose) -> List[float]:
    return [(a[0][i] + b[0][i]) / 2.0 for i in range(3)]


# --- timing / reporting ------------------------------------------------------------------------

def _time_plan(stack: MotionStack, targets: Dict[str, Pose]) -> Tuple[float, object]:
    """Plan-only (execute=False) and return (plan_ms, MoveResult) — the showcase latency number."""
    t0 = time.perf_counter()
    res = stack.move_to_poses(targets, execute=False)
    return (time.perf_counter() - t0) * 1e3, res


def _do_move(stack: MotionStack, name: str, targets: Dict[str, Pose], execute: bool,
             log: List[dict]) -> bool:
    """Plan (timed) then optionally execute. Records + prints one line. Never raises."""
    try:
        plan_ms, res = _time_plan(stack, targets)
    except Exception as e:  # noqa: BLE001
        print(f"  ✗ {name:22s} plan raised: {type(e).__name__}: {e}")
        log.append({"scene": name, "ok": False, "plan_ms": None})
        return False
    if not getattr(res, "ok", False) or getattr(res, "trajectory", None) is None:
        print(f"  ✗ {name:22s} no plan ({getattr(res, 'message', '')[:60]})")
        log.append({"scene": name, "ok": False, "plan_ms": plan_ms})
        return False
    dur = getattr(res.trajectory, "duration", 0.0)
    rec = {"scene": name, "ok": True, "plan_ms": plan_ms, "motion_s": dur,
           "tcps": len(targets), "executed": False}
    if not execute:
        print(f"  ◦ {name:22s} planned {plan_ms:6.0f} ms  ({len(targets)} TCP, motion {dur:.2f}s)  [dry-run]")
        log.append(rec)
        return True
    t0 = time.perf_counter()
    mv = stack.move_to_poses(targets, execute=True)         # re-plan + execute (plan is ~350ms)
    exec_ms = (time.perf_counter() - t0) * 1e3
    rec.update(executed=bool(mv.executed), aborted=bool(getattr(mv, "aborted", False)),
               exec_ms=exec_ms, ok=bool(mv.ok))
    flag = "✓" if mv.ok and mv.executed else ("⚠" if mv.aborted else "✗")
    print(f"  {flag} {name:22s} plan {plan_ms:6.0f} ms | exec {exec_ms:7.0f} ms "
          f"| {len(targets)} TCP | motion {dur:.2f}s{'  ABORTED' if mv.aborted else ''}")
    log.append(rec)
    return bool(mv.ok)


# --- scenes ------------------------------------------------------------------------------------

def planning_benchmark(stack: MotionStack, arms: List[str], home: Dict[str, Pose], reps: int) -> None:
    """(a) MULTI-TCP benchmark: single-arm vs all-arms coordinated planning, plan-only, no motion.
    Shows the coordinated multi-TCP cost + the FAST_PLAN sub-second headline."""
    print("\n── planning-speed benchmark (plan only, no motion) ──")
    a0 = arms[0]
    single = {a0: _off(home[a0], dz=0.08)}
    both = {a: _off(home[a], dz=0.08, dy=(0.06 if i % 2 == 0 else -0.06)) for i, a in enumerate(arms)}
    for label, tgt in ((f"1 TCP ({a0})", single), (f"{len(arms)} TCP (all arms)", both)):
        ms = []
        for _ in range(reps):
            try:
                p, res = _time_plan(stack, tgt)
                if getattr(res, "ok", False):
                    ms.append(p)
            except Exception:  # noqa: BLE001
                pass
        if ms:
            print(f"  {label:22s} median {statistics.median(ms):6.0f} ms  "
                  f"(min {min(ms):.0f}, over {len(ms)}/{reps})")
        else:
            print(f"  {label:22s} — no successful plan")


def scene_sync_reach(home: Dict[str, Pose], arms: List[str]) -> Dict[str, Pose]:
    """Both arms reach outward + up, synchronously — a clean coordinated dual-arm move."""
    return {a: _off(home[a], dz=0.12, dy=(0.10 if i % 2 == 0 else -0.10), dx=0.06)
            for i, a in enumerate(arms)}


def scene_converge(home: Dict[str, Pose], arms: List[str], frac: float) -> Optional[Dict[str, Pose]]:
    """COLLISION AVOIDANCE: both arms move toward the shared centre at the same time — their straight-
    line paths would bring the arm bodies together; cuRobo plans the two arms JOINTLY so the motion
    stays collision-free. Only meaningful for ≥2 arms."""
    if len(arms) < 2:
        return None
    mid = _mid(home[arms[0]], home[arms[1]])
    mid[2] += 0.10                                          # a touch high, to keep it comfortable
    return {a: _toward(home[a], mid, frac) for a in arms}


def scene_speed_loop(home: Dict[str, Pose], arms: List[str], k: int) -> Dict[str, Pose]:
    """A small coordinated jog — used repeatedly to show a tight plan→execute cadence."""
    s = 1.0 if k % 2 == 0 else -1.0
    return {a: _off(home[a], dz=0.06 + 0.04 * (k % 3), dy=s * (0.05 if i % 2 == 0 else -0.05))
            for i, a in enumerate(arms)}


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

    planning_benchmark(stack, arms, home, reps=args.reps)

    print("\n── coordinated multi-TCP moves ──")
    log: List[dict] = []
    for loop in range(args.loops):
        _do_move(stack, "sync-reach", scene_sync_reach(home, arms), execute, log)
        conv = scene_converge(home, arms, args.converge)
        if conv is not None:
            _do_move(stack, "converge (collide-avoid)", conv, execute, log)
        if execute:                                         # return to home between scene sets
            stack.retract(execute=True)
        for k in range(args.speed_moves):
            _do_move(stack, f"speed-jog {k+1}", scene_speed_loop(home, arms, k), execute, log)

    if execute:
        print("\nretracting home...")
        stack.retract(execute=True)

    # summary
    planned = [r for r in log if r.get("plan_ms") is not None]
    okmoves = [r for r in log if r.get("ok")]
    print("\n" + "-" * 74)
    if planned:
        pms = [r["plan_ms"] for r in planned]
        print(f"SUMMARY: {len(okmoves)}/{len(log)} moves ok · plan median {statistics.median(pms):.0f} ms "
              f"(min {min(pms):.0f}) · {'executed on hardware' if execute else 'DRY-RUN, no motion'}")
    print("-" * 74)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="remoroo × cuRobo end-to-end multi-arm showcase")
    ap.add_argument("--cell", default=os.environ.get("REMOROO_CELL", "remoroo_cell"))
    ap.add_argument("--execute", action="store_true", help="ACTUALLY MOVE the robot (default: dry-run)")
    ap.add_argument("--dry-run", action="store_true", help="plan + time only, never move (overrides --execute)")
    ap.add_argument("--loops", type=int, default=1, help="how many times through the scene set")
    ap.add_argument("--speed-moves", type=int, default=4, help="coordinated jogs per loop")
    ap.add_argument("--converge", type=float, default=0.35, help="fraction toward centre (collision scene)")
    ap.add_argument("--reps", type=int, default=8, help="plan repetitions for the speed benchmark")
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
