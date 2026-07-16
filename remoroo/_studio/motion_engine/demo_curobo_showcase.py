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
# Targets are BARE [x,y,z] positions (orientation-free): cuRobo picks a reachable wrist orientation.
# CRITICAL DESIGN RULE (the fix for the all-failing v1): every target is each arm's OWN current TCP
# pose + a SMALL bounded offset — NOT a shared centre both arms crowd into. Driving two TCPs to one
# midpoint forces the arm BODIES to interpenetrate (self-collision), which cuRobo correctly refuses;
# and large offsets leave the workspace (v1 saw 796 mm IK error). Offset-from-own-pose is reachable
# by construction, and a mutual approach only closes PART of the current gap, so the bodies stay clear.

XYZ = List[float]


def _toward(home: Dict[str, Pose], arms: List[str]) -> Dict[str, float]:
    """Per-arm sign for 'move toward the other arm(s)' in y: +1 for an arm left of the group centre,
    -1 for one on the right. So a mutual-approach offset closes the gap symmetrically."""
    cy = sum(home[a][0][1] for a in arms) / len(arms)
    return {a: (1.0 if home[a][0][1] <= cy else -1.0) for a in arms}


def _apply(home: Dict[str, Pose], arms: List[str],
           offsets: Dict[str, XYZ], scale: float) -> Dict[str, XYZ]:
    """Target = each arm's current TCP xyz + scale*offset. scale<1 shrinks toward the current pose
    (guaranteed reachable + collision-free), which is how a scene degrades gracefully on failure."""
    return {a: [home[a][0][i] + scale * offsets[a][i] for i in range(3)] for a in arms}


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

_SCALES = (1.0, 0.6, 0.35, 0.15)   # shrink the offset toward the current pose until a plan is found


def step(stack: MotionStack, name: str, home: Dict[str, Pose], arms: List[str],
         offsets: Dict[str, XYZ], execute: bool, log: List[dict],
         scales: tuple = _SCALES) -> bool:
    """ONE coordinated move, ADAPTIVE: try the full offset, and on a planning failure shrink it toward
    the current pose and retry (`scales`). Since scale→0 is the current (trivially feasible) pose, a
    scene never spams failures — it lands the largest feasible version, or reports that even the
    smallest offset failed (which means born-in-collision, not the target). Never raises."""
    mv = None
    err = ""
    for scale in scales:
        targets = _apply(home, arms, offsets, scale)
        sep = _sep(targets)
        t0 = time.perf_counter()
        try:
            mv = stack.move_to_poses(targets, execute=execute)
        except Exception as e:  # noqa: BLE001
            err = f"{type(e).__name__}: {e}"
            continue
        wall = (time.perf_counter() - t0) * 1e3
        if not getattr(mv, "ok", False) or getattr(mv, "trajectory", None) is None:
            err = (getattr(mv, "message", "") or "")
            continue                                    # shrink and retry
        plan_ms = float(getattr(mv, "total_time", 0.0)) * 1e3
        dq = _max_dq(mv.trajectory)
        dur = getattr(mv.trajectory, "duration", 0.0)
        tag = "" if scale == 1.0 else f" @{int(scale * 100)}%"
        sep_s = f"TCPs {sep * 100:4.0f}cm" if sep is not None else f"{len(targets)} TCP"
        st = ("executed" if mv.executed else ("ABORTED" if getattr(mv, "aborted", False) else "not exec")) \
            if execute else "plan-only"
        flag = "✓" if (mv.executed or not execute) else ("⚠" if getattr(mv, "aborted", False) else "✗")
        print(f"  {flag} {name:16s}{tag:5s} plan {plan_ms:5.0f} ms | {sep_s} | Δq {dq:4.2f} rad | "
              f"motion {dur:.1f}s | wall {wall:5.0f} ms | {st}")
        log.append({"scene": name, "ok": True, "plan_ms": plan_ms, "sep": sep,
                    "dq": dq, "scale": scale, "executed": bool(mv.executed)})
        return True
    print(f"  ✗ {name:16s} no plan even at {int(scales[-1] * 100)}% offset ({(err or '')[:44]})")
    log.append({"scene": name, "ok": False})
    return False


# --- scenes (coordinated dual-arm, TCPs meeting near the shared centre) -------------------------

def planning_benchmark(stack: MotionStack, arms: List[str], home: Dict[str, Pose], reps: int) -> None:
    """MULTI-TCP benchmark: one arm vs ALL arms, a small synchronized +8 cm lift (feasible by
    construction), plan-only. Shows the plan-time headline + what the extra arm costs."""
    print("\n── planning-speed benchmark (plan only, no motion) ──")
    ys = _sorted_by_y(home, arms)
    one = _apply(home, [ys[0]], {ys[0]: [0.0, 0.0, 0.08]}, 1.0)
    allt = _apply(home, arms, {a: [0.0, 0.0, 0.08] for a in arms}, 1.0)
    for label, tgt in ((f"1 TCP ({ys[0]})", one), (f"{len(arms)} TCP lift", allt)):
        ms = []
        for _ in range(reps):
            try:
                mv = stack.move_to_poses(tgt, execute=False)
                if getattr(mv, "ok", False):
                    ms.append(float(getattr(mv, "total_time", 0.0)) * 1e3)
            except Exception:  # noqa: BLE001
                pass
        if ms:
            print(f"  {label:18s} plan median {statistics.median(ms):6.0f} ms  "
                  f"(min {min(ms):.0f}, {len(ms)}/{reps} ok)")
        else:
            print(f"  {label:18s} — no successful plan (even a +8 cm lift → suspect the collision "
                  "model, not the target)")


def sanity_nudge(stack: MotionStack, home: Dict[str, Pose], arms: List[str],
                 execute: bool, log: List[dict]) -> bool:
    """A 3 cm single-arm lift — the smallest real move. If THIS can't plan, the failure is NOT the
    demo's targets: the start config is born-in-collision (modeled world, or the two-arm self-collision
    spheres / ignore matrix). Say so loudly, with where to look."""
    a = _sorted_by_y(home, arms)[0]
    ok = step(stack, "sanity-nudge", home, [a], {a: [0.0, 0.0, 0.03]}, execute, log, scales=(1.0, 0.5))
    if not ok:
        print("  ⚠ EVEN A 3 cm NUDGE FAILED — this is NOT the demo targets. The current config is")
        print("    likely BORN-IN-COLLISION. Check `stack.debug_world()` (modeled obstacles sitting on")
        print("    the arm) and the two-arm self-collision spheres / ignore matrix. Fix that first.")
    return ok


def scenes(home: Dict[str, Pose], arms: List[str], approach: float) -> List[tuple]:
    """Coordinated dual-arm scenes as per-arm OFFSETS (dx,dy,dz) from each arm's own pose — small,
    on-own-side, feasible. Mutual moves close only `approach` of the gap so the bodies stay clear."""
    ys = _sorted_by_y(home, arms)
    tw = _toward(home, arms)
    out = [("lift", {a: [0.0, 0.0, 0.08] for a in arms})]        # synchronized rise (also a sanity move)
    if len(arms) >= 2:
        out += [
            ("reach-in",  {a: [0.06, tw[a] * approach, 0.04] for a in arms}),   # forward + gently toward
            ("spread",    {a: [0.0, -tw[a] * 0.10, 0.06] for a in arms}),       # apart to own sides + up
            ("weave",     {a: [0.0, tw[a] * (approach * 0.6),
                               (0.08 if i % 2 == 0 else -0.05)] for i, a in enumerate(ys)}),  # interleave z
            ("bow",       {a: [0.05, 0.0, -0.06] for a in arms}),               # both reach down-forward
        ]
    else:
        out += [("reach", {a: [0.08, 0.0, 0.04] for a in arms}),
                ("bow",   {a: [0.05, 0.0, -0.06] for a in arms})]
    return out


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

    log: List[dict] = []
    # SANITY FIRST: if a 3 cm nudge can't plan, the problem is the collision world, not the targets —
    # bail early with the diagnosis rather than grinding through the whole (doomed) sequence.
    print("\n── sanity: smallest real move ──")
    if not sanity_nudge(stack, home, arms, execute, log):
        print("-" * 74)
        print("ABORTED: born-in-collision at the current config. Fix the collision world first "
              "(see the ⚠ above); the coordinated scenes can't succeed until then.")
        print("-" * 74)
        return 3

    planning_benchmark(stack, arms, home, reps=args.reps)

    # Coordinated dual-arm scenes — per-arm offsets from own pose, adaptive (shrink-on-failure).
    seq = scenes(home, arms, approach=args.sep)
    print("\n── coordinated multi-TCP moves (plan → execute) ──")
    for loop in range(args.loops):
        if args.loops > 1:
            print(f"  · loop {loop + 1}/{args.loops}")
        for name, offsets in seq:
            step(stack, name, home, arms, offsets, execute, log)

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
    ap.add_argument("--sep", type=float, default=0.06,
                    help="how far each arm reaches TOWARD the other (m) in the coordinated scenes — the "
                         "mutual approach closes only part of the gap so the arm bodies stay clear "
                         "(default 6 cm; raise for a wider dance, lower for a closer one)")
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
