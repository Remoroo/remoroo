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
import math
import os
import signal
import statistics
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

try:
    from motion_engine import MotionStack
    from motion_engine.trajectory import Trajectory
except Exception:  # noqa: BLE001
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from motion_engine import MotionStack
    from motion_engine.trajectory import Trajectory


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

_SCALES = (1.0, 0.6, 0.3)          # shrink the offset toward the current pose until a plan is found
_MAX_ATTEMPTS = 1                  # attempt-0 + the lean escalate ladder only — NO slow PRM retry (~1s
#                                    cap per pose vs ~16s); a pose that would need PRM is just skipped


def step(stack: MotionStack, name: str, home: Dict[str, Pose], arms: List[str],
         offsets: Dict[str, XYZ], execute: bool, log: List[dict],
         scales: tuple = _SCALES, start: Optional[Dict[str, float]] = None):
    """ONE coordinated move, ADAPTIVE: try the full offset, and on a planning failure shrink it toward
    the current pose and retry (`scales`). `start` plans from a given config (for chaining a sequence
    off-line) instead of the live joints. Returns the MoveResult (with the trajectory) or None. Never
    raises."""
    mv = None
    err = ""
    for scale in scales:
        targets = _apply(home, arms, offsets, scale)
        sep = _sep(targets)
        t0 = time.perf_counter()
        try:
            mv = stack.move_to_poses(targets, execute=execute, start=start, max_attempts=_MAX_ATTEMPTS)
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
        return mv                                          # the MoveResult (carries the trajectory)
    print(f"  ✗ {name:16s} no plan even at {int(scales[-1] * 100)}% offset")
    if err:
        print(f"      {err[:200]}")                        # includes the START-CONFIG CHECK cause now
    log.append({"scene": name, "ok": False})
    return None


# --- scenes (coordinated dual-arm, TCPs meeting near the shared centre) -------------------------

def planning_benchmark(stack: MotionStack, arms: List[str], home: Dict[str, Pose], reps: int) -> None:
    """MULTI-TCP benchmark: one arm vs ALL arms, a small synchronized +8 cm lift (feasible by
    construction), plan-only. Shows the plan-time headline + what the extra arm costs."""
    print("\n── planning-speed benchmark (plan only, no motion) ──")
    ys = _sorted_by_y(home, arms)
    # "1 arm moves": lift ys[0] +8 cm while the OTHER arm(s) hold their CURRENT pose — goaled
    # EXPLICITLY (both tracked). We do NOT use the single-tip path (which leaves the idle arm on a
    # zero-cost null-space hold): on this bimanual rig that path drifts the idle arm into collision and
    # fails even a trivial lift. Goaling both is the honest "one arm moves, one holds" measurement.
    one = {a: [home[a][0][0], home[a][0][1], home[a][0][2] + (0.08 if a == ys[0] else 0.0)] for a in arms}
    allt = _apply(home, arms, {a: [0.0, 0.0, 0.08] for a in arms}, 1.0)
    for label, tgt in ((f"1 arm moves ({ys[0]})", one), (f"{len(arms)} arms lift", allt)):
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


def retract_gate(stack: MotionStack, execute: bool) -> tuple:
    """Establish a clean start (and prove the current config is escapable): plan a retract to HOME. In
    execute mode PERFORM it, so the dance begins from a known spread pose — this is what the working
    plan→execute demo did. In dry-run it plans only (no motion). Returns (planned_ok, moved).

    Why this instead of a 3 cm nudge: a nudge keeps the arms as crowded as they are now (so it collides
    if they're close), which says nothing useful. A retract SEPARATES them — if even THAT can't plan,
    the current config is genuinely stuck (born-in-collision / over-conservative two-arm self-collision),
    which is a real world/model bug, not a demo-target problem."""
    print("\n── gate: plan a retract to home (clean start) ──")
    t0 = time.perf_counter()
    try:
        r = stack.retract(execute=execute)
    except Exception as e:  # noqa: BLE001
        print(f"  ✗ retract raised {type(e).__name__}: {e}")
        return False, False
    ms = float(getattr(r, "total_time", 0.0)) * 1e3
    ok = bool(getattr(r, "ok", False)) and getattr(r, "trajectory", None) is not None
    moved = bool(getattr(r, "executed", False))
    wall = (time.perf_counter() - t0) * 1e3
    if ok:
        tail = "and EXECUTED → arm now at home" if moved else "PLAN ONLY — the arm did NOT move"
        print(f"  ✓ retract plans OK ({ms:.0f} ms plan | wall {wall:.0f} ms) — {tail}")
    else:
        print(f"  ✗ retract could NOT plan ({(getattr(r, 'message', '') or '')[:56]})")
    return ok, moved


def scenes(home: Dict[str, Pose], arms: List[str], approach: float, close: float,
           stagger: float, depth: float) -> List[tuple]:
    """A flowing ~20-move coordinated routine with a CLOSE-ENCOUNTER climax. Most moves keep each arm
    on its own side; the close scenes bring the two grippers NEAR each other but STAGGERED IN HEIGHT
    (one high, one low) so the arm bodies pass at different levels and stay collision-free — the real
    collision-avoidance showcase. Every move is a per-arm OFFSET from home; the adaptive shrink finds
    the closest feasible approach, so a too-tight `close` backs off gracefully and never breaks."""
    ys = _sorted_by_y(home, arms)
    tw = _toward(home, arms)                          # +1 = toward centre (inward), -1 = outward, per arm
    IN = float(approach)                              # small inward reach — stays own-side (default 6 cm)
    CLOSE = float(close)                              # bigger inward for the close encounter (default 20 cm)
    STAG = float(stagger)                             # vertical (z) stagger for the close encounter
    DEEP = float(depth)                               # depth (x) stagger — one arm reaches deep, the other
    #                                                   shallow. TWO clearance axes (height + depth) let the
    #                                                   grippers converge closer in y before a body touches.
    OUT, UP, DN, FWD, BAK = 0.10, 0.09, -0.07, 0.08, -0.06

    def per(fn) -> Dict[str, XYZ]:                    # build {arm: [dx,dy,dz]} from a per-(index,arm) fn
        return {a: [float(v) for v in fn(i, a)] for i, a in enumerate(ys)}

    seq = [
        ("wake",      per(lambda i, a: (0,          0,                 UP))),          # both rise
        ("reach",     per(lambda i, a: (FWD,        0,                 UP * 0.5))),    # both forward
        ("open",      per(lambda i, a: (FWD * 0.5, -tw[a] * OUT,       UP * 0.3))),    # spread to own sides
        ("sway-L",    per(lambda i, a: (FWD * 0.5, -tw[a] * OUT,       UP if i % 2 == 0 else DN))),  # A up/B down
        ("sway-R",    per(lambda i, a: (FWD * 0.5, -tw[a] * OUT,       DN if i % 2 == 0 else UP))),  # swap
        ("gather",    per(lambda i, a: (FWD * 0.3,  tw[a] * IN,        UP * 0.4))),    # gentle inward (own-side)
    ]
    if len(arms) >= 2:
        # CLOSE ENCOUNTER — grippers come near, staggered in HEIGHT (`stagger`) so the bodies clear.
        # A BIGGER stagger is what lets them approach closer; the adaptive shrink backs `close` off only
        # if it's STILL too tight, and prints the separation reached. `clasp` is the closest: most inward
        # AND most stagger.
        seq += [
            # A reaches DEEP+HIGH, B reaches SHALLOW+LOW (then swap) — bodies pass at different depth AND
            # height, so the grippers can converge closer in y. `clasp` is the tightest: most inward.
            ("approach",  per(lambda i, a: (FWD * 0.4 + (DEEP if i % 2 == 0 else -DEEP),  tw[a] * CLOSE,        STAG if i % 2 == 0 else -STAG))),
            ("near-pass", per(lambda i, a: (FWD * 0.4 + (-DEEP if i % 2 == 0 else DEEP),  tw[a] * CLOSE,       -STAG if i % 2 == 0 else STAG))),
            ("clasp",     per(lambda i, a: (FWD * 0.6 + (DEEP if i % 2 == 0 else -DEEP),  tw[a] * CLOSE * 1.2,  STAG * 1.25 if i % 2 == 0 else -STAG * 1.25))),
            ("part",      per(lambda i, a: (FWD * 0.2, -tw[a] * OUT,          UP * 0.4))),  # ease back apart
        ]
    seq += [
        ("bow",       per(lambda i, a: (FWD,        0,                 DN))),          # both dip forward-down
        ("rise",      per(lambda i, a: (BAK,        0,                 UP))),          # both up and back
        ("ripple-1",  per(lambda i, a: (0,         -tw[a] * IN,        UP if i % 2 == 0 else DN * 0.3))),
        ("ripple-2",  per(lambda i, a: (0,         -tw[a] * IN,        DN * 0.3 if i % 2 == 0 else UP))),
        ("present",   per(lambda i, a: (FWD,       -tw[a] * IN,        UP * 0.6))),    # offer pose
        ("tip-L",     per(lambda i, a: (FWD * 0.6, -tw[a] * OUT,       UP if i % 2 == 0 else DN * 0.5))),
        ("tip-R",     per(lambda i, a: (FWD * 0.6, -tw[a] * OUT,       DN * 0.5 if i % 2 == 0 else UP))),
        ("float",     per(lambda i, a: (0,          0,                 UP))),          # both hover high
        ("settle",    per(lambda i, a: (0,          0,                 DN * 0.3))),    # ease back toward home
    ]
    return seq


def generate_poses(home: Dict[str, Pose], arms: List[str], n: int,
                   close: float, stagger: float, depth: float) -> List[tuple]:
    """Procedurally generate N flowing coordinated poses (deterministic — parametric, no RNG). Each arm
    traces a smooth OWN-SIDE path; every ~5th pose is a CLOSE ENCOUNTER (grippers converge, staggered in
    height + depth so the bodies clear). `--poses N` = an arbitrarily long dance."""
    ys = _sorted_by_y(home, arms)
    tw = _toward(home, arms)
    OUT, UP, FWD = 0.10, 0.09, 0.08
    seq: List[tuple] = []
    for k in range(max(1, int(n))):
        t = 2.0 * math.pi * k / max(6, int(n))
        if len(arms) >= 2 and k % 5 == 4:                              # periodic close encounter
            hi = (k % 10) < 5                                          # alternate which arm is high
            off = {a: [FWD * 0.5 + ((depth if (i % 2 == 0) == hi else -depth)),
                       tw[a] * close,
                       (stagger if (i % 2 == 0) == hi else -stagger)] for i, a in enumerate(ys)}
            name = f"close-{k:02d}"
        else:                                                         # own-side flow (dy always outward)
            off = {a: [FWD * 0.7 * math.sin(t + i * 0.7),
                       -tw[a] * OUT * (0.4 + 0.6 * abs(math.sin(t))),
                       UP * math.sin(1.5 * t + i * math.pi)] for i, a in enumerate(ys)}
            name = f"flow-{k:02d}"
        seq.append((name, {a: [float(v) for v in off[a]] for a in arms}))
    return seq


# --- FLOW: ONE multi-waypoint cuRobo trajectory (the "right way") ------------------------------
# Instead of N per-pose plans stitched together (each cuRobo traj decelerates to a full STOP at its
# last waypoint → a pause at every join, and the chained per-pose seeds can drift into an obstacle and
# waste failing retries), we hand cuRobo's MotionRetargeter the WHOLE dance as one dense sequence of
# TCP targets and get back ONE continuous, velocity-linked, collision-free joint trajectory. The
# retargeter tracks a target-per-frame: global IK on frame 0, then each frame warm-starts (and is
# velocity-limited by `optimization_dt`) off the previous → smooth motion THROUGH the keyposes, not TO
# each one. No per-pose stops, no chaining, no drift, no retries — the pauses and the wasted plan time
# both dissolve because there is exactly one solve.


_URDF_VEL_LIMIT = 3.14      # rad/s — every revolute joint on this rig declares velocity="3.14"


def _flow_path(keys: Dict[str, List[XYZ]], arms: List[str], v_cruise: float, a_max: float,
               dt: float, m: int = 200, ds: float = 0.001):
    """Time-parameterise the dance so the TCPs never exceed `a_max` Cartesian acceleration — the
    CURVATURE-AWARE parameterisation (TOPP-style), which is what actually removes jerk.

    Two wrong parameterisations were measured before landing here:
      1. uniform-in-spline-parameter (the original): segments differ ~20x in length (443 mm beside
         26 mm), so the TCP crawled through short segments and sprinted through long ones — a ~57x
         speed swing, and every speed change is an acceleration spike.
      2. constant arc-length speed: fixes the swing but is WORSE — it drives the tight corners at full
         cruise, and speed through a sharp corner IS acceleration (v^2 * curvature). Measured peak
         Cartesian accel got ~10x higher.

    So speed must be bounded BY CURVATURE: v <= sqrt(a_max / |P''(s)|), taking the tightest bound
    across both arms (they share one clock, so both must be satisfied). Forward/backward passes then
    bound the TANGENTIAL acceleration, and pinning v=0 at both ends starts and stops at rest with no
    ad-hoc ramp. Result: slow through corners, cruise on the straights, smooth throughout."""
    fine = {a: _catmull_rom(np.asarray(keys[a], float), m) for a in arms}
    L = len(next(iter(fine.values())))
    step = np.zeros(L - 1)
    for a in arms:                                        # shared arc length = the busiest arm
        step = np.maximum(step, np.linalg.norm(np.diff(fine[a], axis=0), axis=1))
    s_fine = np.concatenate([[0.0], np.cumsum(step)])
    S = float(s_fine[-1])
    if S < 1e-6:
        return {a: np.asarray(keys[a], float) for a in arms}, 0.0, len(next(iter(keys.values()))), 0.0

    n = max(8, int(S / ds) + 1)                           # uniform arc-length grid
    s = np.linspace(0.0, S, n)
    h = float(s[1] - s[0])
    P = {a: np.stack([np.interp(s, s_fine, fine[a][:, k]) for k in range(3)], axis=1) for a in arms}

    vlim = np.full(n, float(v_cruise))                    # curvature limit: v <= sqrt(a_max/|P''|)
    for a in arms:
        d2 = np.zeros_like(P[a])
        d2[1:-1] = (P[a][2:] - 2.0 * P[a][1:-1] + P[a][:-2]) / (h * h)
        vlim = np.minimum(vlim, np.sqrt(float(a_max) / np.maximum(np.linalg.norm(d2, axis=1), 1e-9)))
    vlim = np.minimum(vlim, float(v_cruise))

    v = vlim.copy()
    v[0] = v[-1] = 0.0                                    # start and end at REST
    for i in range(1, n):                                 # forward: bound tangential accel
        v[i] = min(v[i], math.sqrt(max(v[i - 1] ** 2 + 2.0 * a_max * h, 0.0)))
    for i in range(n - 2, -1, -1):                        # backward: bound tangential decel
        v[i] = min(v[i], math.sqrt(max(v[i + 1] ** 2 + 2.0 * a_max * h, 0.0)))

    t = np.concatenate([[0.0], np.cumsum(h / np.maximum((v[1:] + v[:-1]) * 0.5, 1e-6))])
    T = float(t[-1])
    N = max(4, int(round(T / dt)) + 1)
    tq = np.linspace(0.0, T, N)                           # resample on the CONTROL clock
    out = {a: np.stack([np.interp(tq, t, P[a][:, k]) for k in range(3)], axis=1) for a in arms}
    return out, S, N, T


def _catmull_rom(pts: np.ndarray, per_seg) -> np.ndarray:
    """Dense C1-smooth path PASSING THROUGH every keypose in `pts` (K,3). `per_seg` is the sample count
    PER SEGMENT (one int per segment, so frames can be allocated by arc length → constant speed).
    Endpoints are clamped (duplicated) so the path starts/ends at REST (zero tangent) but keeps a
    continuous, non-zero velocity THROUGH the interior keyposes — that continuity is what removes the
    per-pose pause. Pure numpy (no scipy), one spline per axis."""
    pts = np.asarray(pts, float)
    if len(pts) < 2:
        return pts.copy()
    per_seg = [int(max(2, n)) for n in np.atleast_1d(per_seg)]
    if len(per_seg) == 1:
        per_seg = per_seg * (len(pts) - 1)
    P = np.vstack([pts[0], pts, pts[-1]])          # clamp: tangent≈0 at the very start and end
    out: List[np.ndarray] = []
    for s in range(len(pts) - 1):
        p0, p1, p2, p3 = P[s], P[s + 1], P[s + 2], P[s + 3]
        last = s == len(pts) - 2
        for t in np.linspace(0.0, 1.0, per_seg[s], endpoint=last):
            t2, t3 = t * t, t * t * t
            out.append(0.5 * ((2 * p1) + (-p0 + p2) * t
                              + (2 * p0 - 5 * p1 + 4 * p2 - p3) * t2
                              + (-p0 + 3 * p1 - 3 * p2 + p3) * t3))
    return np.asarray(out)


def smooth_joints(q: np.ndarray, win: int) -> np.ndarray:
    """Low-pass the SOLVED joint trajectory (Hann kernel, edge-padded so the endpoints hold).

    Per-frame IK jitter is HIGH-frequency — the solver lands on a slightly different null-space branch
    each frame — while the dance itself is low-frequency. Rig sweeps proved NO solver parameter reaches
    that jitter (position_tolerance 10x tighter and velocity_regularization 500x higher both left peak
    accel at ~250 rad/s^2, and reshaping the Cartesian path did nothing either). Acceleration and jerk
    are 2nd/3rd differences, so they are dominated by exactly the frequencies this filter removes.
    Deviation from the solved path is reported and every waypoint is re-validated (`validate_flow`)."""
    win = int(win)
    if win <= 1:
        return q
    if win % 2 == 0:
        win += 1
    k = np.hanning(win + 2)[1:-1]
    k = k / k.sum()
    pad = win // 2
    qp = np.pad(q, ((pad, pad), (0, 0)), mode="edge")
    return np.stack([np.convolve(qp[:, i], k, mode="valid") for i in range(q.shape[1])], axis=1)


def validate_flow(stack: MotionStack, arms: List[str], flow, chunk: int = 512) -> dict:
    """Collision-validate EVERY waypoint against the modeled world + self + bounds, via cuRobo's
    canonical `RobotCollisionChecker.validate` (batched, so the whole path costs one or two GPU calls).

    This gate is NOT optional for the flow: the retargeter treats collision as a soft COST rather than
    a constraint (unlike trajopt, which is why the per-pose pipeline is safe by construction), and we
    additionally low-pass the solved joints — so nothing upstream guarantees the executed path is
    collision-free. Modeled obstacles only, not the live ESDF."""
    import torch
    try:
        cvp = stack._planner_for(arms)
        checker = cvp._collision_checker()
        names = list(cvp.planner.joint_names)
        idx = {n: i for i, n in enumerate(flow.joint_names)}
        missing = [n for n in names if n not in idx]
        if missing:
            return {"ok": False, "error": f"trajectory is missing planner joints {missing[:4]}"}
        q_all = np.ascontiguousarray(np.asarray(flow.positions, float)[:, [idx[n] for n in names]])
        bad: List[int] = []
        for s in range(0, len(q_all), chunk):
            q = torch.as_tensor(np.ascontiguousarray(q_all[s:s + chunk]), dtype=torch.float32,
                                device=cvp._device).unsqueeze(1).contiguous()   # [B, 1, dof]
            v = checker.validate(q).view(-1).detach().cpu().numpy().astype(bool)
            bad.extend((s + np.nonzero(~v)[0]).tolist())
        return {"ok": not bad, "bad": bad, "n": int(len(q_all))}
    except Exception as e:  # noqa: BLE001 — a checker failure must READ as unverified, never as "safe"
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def tcp_tracking_error(stack: MotionStack, arms: List[str], flow, targets: np.ndarray,
                       chunk: int = 2048) -> dict:
    """How far the EXECUTED TCP path strays from the dance we ASKED for (metres), after solving and
    smoothing. Smoothing trades fidelity for feasibility, so this is the number that says whether the
    choreography still looks like itself — jerk figures alone can be 'fixed' by smoothing the dance
    into mush. `targets` is (frames, links, 3); it is resampled if the solver emitted a different
    frame count (MPC emits steps_per_target per input target)."""
    import torch
    from curobo.types import JointState
    try:
        cvp = stack._planner_for(arms)
        names = list(cvp.planner.joint_names)
        idx = {n: i for i, n in enumerate(flow.joint_names)}
        q_all = np.ascontiguousarray(np.asarray(flow.positions, float)[:, [idx[n] for n in names]])
        T = len(q_all)
        tg = np.asarray(targets, float)
        if len(tg) != T:                                   # align target count to solver output
            src = np.linspace(0.0, 1.0, len(tg))
            dst = np.linspace(0.0, 1.0, T)
            tg = np.stack([np.stack([np.interp(dst, src, tg[:, li, k]) for k in range(3)], axis=1)
                           for li in range(tg.shape[1])], axis=1)
        tips = [stack._tip(a) for a in arms]
        per_link = {tf: [] for tf in tips}
        for s in range(0, T, chunk):
            t = torch.as_tensor(np.ascontiguousarray(q_all[s:s + chunk]),
                                dtype=torch.float32, device=cvp._device).contiguous()
            st = cvp.planner.compute_kinematics(JointState.from_position(t, joint_names=names))
            for li, tf in enumerate(tips):
                xyz = st.tool_poses.get_link_pose(tf).position.detach().cpu().numpy().reshape(-1, 3)
                per_link[tf].append(np.linalg.norm(xyz - tg[s:s + chunk, li, :], axis=1))
        e = np.concatenate([np.concatenate(v) for v in per_link.values()])
        return {"ok": True, "max": float(e.max()), "mean": float(e.mean()),
                "p95": float(np.percentile(e, 95))}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def max_speedup(flow, dt: float, limits, margin: float = 0.95) -> float:
    """The largest time-compression this trajectory can take and still respect the rig's limits.

    Compressing time by k leaves the GEOMETRY untouched (so a collision-clean path stays clean — speed
    cannot create a collision) while scaling velocity by k, acceleration by k^2 and jerk by k^3. So the
    ceiling is whichever limit binds first, times a safety margin."""
    p = np.asarray(flow.positions, float)
    if len(p) < 4:
        return 1.0
    v = np.diff(p, axis=0) / dt
    a = np.diff(v, axis=0) / dt
    j = np.diff(a, axis=0) / dt
    pv, pa, pj = float(np.abs(v).max()), float(np.abs(a).max()), float(np.abs(j).max())
    lv, la, lj = (float(x) for x in limits)
    k = min(lv / max(pv, 1e-9), math.sqrt(la / max(pa, 1e-9)), (lj / max(pj, 1e-9)) ** (1.0 / 3.0))
    return max(1.0, k * float(margin))


def retime(flow, speed: float):
    """Same path, new clock: dt/speed, with velocities refreshed to match the new cadence."""
    if abs(speed - 1.0) < 1e-6:
        return flow
    dt_new = flow.dt / float(speed)
    pos = np.asarray(flow.positions, float)
    vel = np.zeros_like(pos)
    vel[1:] = (pos[1:] - pos[:-1]) / dt_new
    meta = dict(flow.meta or {})
    meta["speed"] = round(float(speed), 3)
    return Trajectory(list(flow.joint_names), pos, dt_new, velocities=vel, meta=meta)


def flow_smoothness(flow, dt: float, limits=None, top: int = 5) -> dict:
    """MEASURE where a flow trajectory is rough — no motion, pure numpy on the solved path. Jerk you
    FEEL on the arm is acceleration discontinuity, so we report per-joint peak |v|,|a|,|j| plus a
    CHATTER count (acceleration sign flips): smooth motion flips sign a few times per move, while
    per-frame IK noise flips it constantly. High chatter + low peak |a| ⇒ solver wobble (tighten
    position_tolerance / raise the regularization); low chatter + high peak |a| ⇒ genuinely aggressive
    motion at a few frames (slow the path down)."""
    p = np.asarray(flow.positions, float)
    if len(p) < 4:
        return {}
    v = np.diff(p, axis=0) / dt
    a = np.diff(v, axis=0) / dt
    j = np.diff(a, axis=0) / dt
    flips = np.sum(np.diff(np.sign(a), axis=0) != 0, axis=0)          # per joint: accel sign changes
    names = list(flow.joint_names)
    rows = [{"joint": names[k], "v": float(np.max(np.abs(v[:, k]))), "a": float(np.max(np.abs(a[:, k]))),
             "j": float(np.max(np.abs(j[:, k]))), "flips": int(flips[k]),
             "worst_frame": int(np.argmax(np.abs(a[:, k])))} for k in range(p.shape[1])]
    rows.sort(key=lambda r: -r["j"])
    print(f"\n  ── smoothness (jerk diagnosis · {len(p)} waypoints @ dt={dt*1e3:.0f}ms) ──")
    print(f"    {'joint':12s} {'peak|v|':>9s} {'peak|a|':>10s} {'peak|jerk|':>11s} {'chatter':>8s}  {'%frames':>7s}")
    for r in rows[:top]:
        pct = 100.0 * r["flips"] / max(len(a) - 1, 1)
        print(f"    {r['joint']:12s} {r['v']:9.2f} {r['a']:10.1f} {r['j']:11.0f} {r['flips']:8d}  {pct:6.1f}%")
    worst = rows[0]
    chat = 100.0 * worst["flips"] / max(len(a) - 1, 1)
    # NOTE: chatter alone is NOT a verdict — it counts sign flips without weighting by MAGNITUDE, so a
    # smooth path hovering near zero accel flips often at tiny amplitude and scores "worse" than a
    # violent one that swings hard a few times. The feasibility numbers below are the real verdict.
    print(f"    → worst: {worst['joint']} (jerk {worst['j']:.0f} rad/s³ at frame {worst['worst_frame']}); "
          f"accel sign-flips on {chat:.0f}% of frames (magnitude, not this %, is the verdict)")

    out = {"rows": rows, "chatter_pct": chat}
    if limits:
        pv = max(r["v"] for r in rows); pa = max(r["a"] for r in rows); pj = max(r["j"] for r in rows)
        lv, la, lj = float(limits[0]), float(limits[1]), float(limits[2])
        # A pure TIME STRETCH by k scales v by 1/k, a by 1/k^2, j by 1/k^3 (same geometric path).
        k = max(1.0, pv / lv, math.sqrt(pa / la), (pj / lj) ** (1.0 / 3.0))
        print(f"\n    ── dynamic feasibility vs the rig's OWN limits ──")
        print(f"    {'':10s} {'peak':>10s} {'limit':>10s} {'over':>8s}")
        nwp = len(p)                                     # p is the positions array (T, dof)
        for nm, pk, lm in (("velocity", pv, lv), ("accel", pa, la), ("jerk", pj, lj)):
            print(f"    {nm:10s} {pk:10.1f} {lm:10.1f} {pk / lm:7.1f}x{'  OK' if pk <= lm else '  ** OVER **'}")
        if k > 1.001:
            print(f"    → the retargeter does NOT enforce these (trajopt does — that is why the per-pose "
                  f"path is smooth).\n      Time-stretch {k:.2f}x (dt {dt:.4f} → {dt * k:.4f}, "
                  f"{(nwp - 1) * dt:.1f}s → {(nwp - 1) * dt * k:.1f}s) would make it feasible.")
        else:
            print("    → within every limit: dynamically feasible as-is.")
        out.update({"peak": (pv, pa, pj), "stretch": k})
    return out


def run_flow(stack: MotionStack, args, tracker: Optional["_SeedTracker"] = None) -> int:
    """Plan + (optionally) execute the dance as ONE continuous cuRobo trajectory via MotionRetargeter.
    Two-part like the rest of the demo: build the flow (no motion), GATE on ENTER, then move to the
    flow's start config and REPLAY the single trajectory. Camera-independent after one seed read."""
    import torch
    from curobo.motion_retargeter import (
        MotionRetargeter, MotionRetargeterCfg, SequenceGoalToolPose,
    )
    from curobo.types import ToolPoseCriteria

    arms = list(stack._groups.keys())
    execute = bool(args.execute) and not args.dry_run
    dt = float(args.flow_dt)
    print("=" * 74)
    print(f"cuRobo showcase · FLOW (one multi-waypoint trajectory) — arms={arms}  "
          f"mode={'EXECUTE (real motion)' if execute else 'DRY-RUN (plan only)'}  "
          f"dt={dt * 1e3:.0f}ms  solver={'MPC' if args.flow_mpc else 'IK'}")

    home = {a: stack.current_tool_pose(a) for a in arms}
    if any(v is None for v in home.values()):
        print("  could not FK current tool poses — is the bridge connected?")
        return 2

    # 1) the SAME choreography, as discrete keyposes (curated dance or --poses generator) --------
    if args.poses and int(args.poses) > 0:
        seq = generate_poses(home, arms, int(args.poses), close=args.close,
                             stagger=args.stagger, depth=args.depth)
    else:
        seq = scenes(home, arms, approach=args.sep, close=args.close,
                     stagger=args.stagger, depth=args.depth)
    keys: Dict[str, List[XYZ]] = {a: [list(home[a][0])] for a in arms}          # start AT the current pose
    for _name, offsets in seq:
        tgt = _apply(home, arms, offsets, 1.0)
        for a in arms:
            keys[a].append([float(v) for v in tgt[a]])
    for a in arms:
        keys[a].append(list(home[a][0]))                                        # …and return home

    # 2) densify into a shared, synchronized frame count (both arms hit keypose k at the same frame) -
    # MPC consumes `steps_per_target` control steps per target, and EACH step advances real time by dt —
    # so at the same target density the dance would run steps× slower (1200 targets × 4 = 120 s). Space
    # the targets steps× further apart to hold the tempo; MPC's horizon smooths between them, which is
    # exactly what a horizon is for. IK is 1 step per target, so it is unaffected (mult = 1).
    steps = max(1, int(args.flow_steps))
    mult = steps if args.flow_mpc else 1
    eff_step = float(args.flow_step) * mult
    seg = np.stack([np.linalg.norm(np.diff(np.asarray(keys[a], float), axis=0), axis=1) for a in arms])
    seg_max = seg.max(axis=0)
    # `eff_step/dt` is the CRUISE speed we'd like; `--flow-accel` caps Cartesian acceleration so the
    # path slows itself through corners instead of spiking. Target dt is the MPC-compensated clock.
    v_cruise = eff_step / dt
    dense, S, N, T = _flow_path(keys, arms, v_cruise, float(args.flow_accel), dt * mult)
    out_wp = N * mult                                                   # waypoints the solver will emit
    print(f"  path: {len(seq) + 2} keyposes → {N} targets  (arc {S * 1e3:.0f} mm · "
          f"segments {seg_max.min() * 1e3:.0f}-{seg_max.max() * 1e3:.0f} mm · "
          f"cruise {v_cruise:.2f} m/s capped at {float(args.flow_accel):.1f} m/s² → {T:.1f}s of path)")
    print(f"        {'MPC ' + str(steps) + ' steps/target' if args.flow_mpc else 'IK 1 step/target'} "
          f"→ ~{out_wp} waypoints ≈ {(out_wp - 1) * dt:.1f}s of motion")

    tool_frames = [stack._tip(a) for a in arms]
    pos = np.stack([dense[a] for a in arms], axis=1)                            # (N, L, 3)
    pos_t = torch.tensor(pos, dtype=torch.float32).view(N, 1, len(arms), 1, 3)
    quat_t = torch.zeros(N, 1, len(arms), 1, 4, dtype=torch.float32)
    quat_t[..., 0] = 1.0                                                        # identity wxyz (ignored: position-only)

    # 3) build the retargeter solver from OUR robot cfg + modeled world (self + world collision) ----
    # cuRobo's global IK draws 64 RANDOM seeds, so an unseeded solve lands in a different
    # null-space branch every run — same targets, different elbows, different collisions (measured:
    # the same config validated clean once and 29-bad the next). Seed it so a validated dry-run
    # actually predicts the next run.
    torch.manual_seed(int(args.flow_seed)); np.random.seed(int(args.flow_seed))
    print(f"  building retargeter solver ({'IK+MPC' if args.flow_mpc else 'warm-started IK'} · "
          "self+world collision)... (one-time)")
    try:
        cfg = MotionRetargeterCfg.create(
            robot=stack._robot_cfg_for(arms),
            tool_pose_criteria={tf: ToolPoseCriteria.track_position() for tf in tool_frames},
            use_mpc=bool(args.flow_mpc),
            self_collision_check=True,
            scene_model=stack.world.scene,
            optimization_dt=dt,                                                 # velocity limit rides THIS dt
            # NOTE: the default 0.005 (5 mm) is ~the size of --flow-step (6 mm) — the solver's accepted
            # slop is comparable to the step, so consecutive frames can wobble. Tighten to smooth.
            position_tolerance=float(args.flow_tol),
            velocity_regularization_weight=(None if args.flow_vreg < 0 else float(args.flow_vreg)),
            steps_per_target=steps,
            # The acceleration term is what kills jerk — and it is only truly live in MPC mode
            # (in IK mode the retargeter never populates _prev_velocity, so (v-v_prev)/dt has no v_prev).
            acceleration_regularization_weight=(None if args.flow_areg < 0 else float(args.flow_areg)),
        )
        retargeter = MotionRetargeter(cfg)
        dev_t = cfg.device_cfg.device
        seq_goal = SequenceGoalToolPose(tool_frames=list(tool_frames),
                                        position=pos_t.to(dev_t), quaternion=quat_t.to(dev_t))
    except Exception as e:  # noqa: BLE001
        import traceback
        print(f"  ✗ retargeter FAILED to build/solve: {type(e).__name__}: {e}")
        traceback.print_exc()
        return 1

    # The rig's OWN declared dynamics (same numbers the stack prints at boot) — velocity from the URDF,
    # accel/jerk from safety.planner_limits(). trajopt treats these as constraints; the retargeter does not.
    try:
        lim = dict(getattr(stack, "_limits", None) or {})
        limits = (_URDF_VEL_LIMIT, float(lim.get("max_acceleration", 15.0)), float(lim.get("max_jerk", 500.0)))
    except Exception:  # noqa: BLE001 — diagnostics must never block the run
        limits = (_URDF_VEL_LIMIT, 15.0, 500.0)

    # ── SOLVE → VALIDATE → RE-SOLVE ────────────────────────────────────────────────────────────────
    # The solve is NOT reproducible: identical inputs produced a collision-clean path on one run and 40
    # colliding waypoints on the next (TCP error moved 3.4mm → 9.4mm too). cuRobo's IK `random_seed` is
    # already fixed at 123 and torch.manual_seed changes nothing, so this is GPU float non-determinism
    # at frame 0 amplified through hundreds of warm-started frames into a different null-space branch —
    # different elbows, different collisions. Chasing determinism is the wrong fight. Instead gate on
    # the ACTUAL trajectory and re-solve until one passes, so what executes is always a verified path.
    win = int(args.flow_smooth)
    tries = max(1, int(args.flow_tries))
    flow = vres = best = best_v = None
    for attempt in range(1, tries + 1):
        retargeter.reset()                              # fresh global IK, new branch
        t0 = time.perf_counter()
        result = retargeter.solve_sequence(seq_goal)
        solve_ms = (time.perf_counter() - t0) * 1e3
        # IK: one velocity-limited solve per frame → `joint_state` IS the path (frames are `dt` apart).
        # MPC: `joint_state` holds only per-frame ENDPOINTS (steps_per_target apart); the fine path at
        # `optimization_dt` is in `result.trajectory` — use it so the replay cadence stays correct.
        src = result.trajectory if (args.flow_mpc and result.trajectory is not None) else result.joint_state
        names = list(src.joint_names)
        raw = src.position[0].detach().cpu().numpy()                           # (T, dof)
        positions = smooth_joints(raw, win)                                    # kill the IK jitter
        moved = float(np.abs(positions - raw).max()) if win > 1 else 0.0
        vel = np.zeros_like(positions)
        vel[1:] = (positions[1:] - positions[:-1]) / dt                        # feed-forward matches dt
        cand = Trajectory(names, positions, dt, velocities=vel,
                          meta={"flow": True, "frames": int(len(positions)), "smooth": win,
                                "solver": "mpc" if args.flow_mpc else "ik", "attempt": attempt})
        v = validate_flow(stack, arms, cand)
        nbad = len(v.get("bad") or [])
        tag = ("CLEAN" if v.get("ok") else
               (f"UNVERIFIED ({str(v.get('error'))[:38]})" if v.get("error") else f"{nbad} colliding wp"))
        print(f"  attempt {attempt}/{tries}: {cand.summary()} in {solve_ms:.0f} ms · "
              f"smoothing moved joints {math.degrees(moved):.1f}° · {tag}")
        if best is None or (nbad < len(best_v.get("bad") or [0] * 10 ** 6)):
            best, best_v = cand, v
        if v.get("ok"):
            flow, vres = cand, v
            break
    if flow is None:                                    # nothing clean — keep the least-bad to REPORT
        flow, vres = best, best_v
        print(f"  → no collision-free solve in {tries} attempts (best: "
              f"{len(vres.get('bad') or [])} bad waypoints)")

    print(f"  ✓ flow: {flow.summary()} · Δq {_max_dq(flow):.2f} rad")
    flow_smoothness(flow, dt, limits=limits)

    # ── SPEED. Time-compression leaves the geometry alone (so it can never create a collision) and
    # scales v by k, a by k^2, j by k^3 — so we can run right up to the rig's own envelope, measured.
    speed = float(args.flow_speed)
    if args.flow_fit:
        speed = max_speedup(flow, dt, limits, margin=float(args.flow_margin))
        print(f"\n  ── speed fit: {speed:.2f}x within limits (margin {float(args.flow_margin):.2f}) "
              f"→ {flow.duration:.1f}s becomes {flow.duration / speed:.1f}s ──")
    if abs(speed - 1.0) > 1e-6:
        flow = retime(flow, speed)
        dt = flow.dt
        print(f"  retimed: dt {dt * 1e3:.1f}ms · {flow.duration:.1f}s · {len(flow)} waypoints")
        flow_smoothness(flow, dt, limits=limits)

    # HARD GATE (already computed per attempt above — time-compression cannot change it, since it
    # leaves the geometry untouched). Reported here so the verdict sits next to the execute decision.
    print("\n  ── collision validation (every waypoint · modeled world + self + bounds) ──")
    if vres.get("error"):
        print(f"    ⚠ could NOT validate ({vres['error']}) — treating as UNVERIFIED, refusing to execute.")
    elif vres["ok"]:
        print(f"    ✓ all {vres['n']} waypoints collision-free")
    else:
        b = vres["bad"]
        print(f"    ✗ {len(b)}/{vres['n']} waypoints in collision (first at frame {b[0]}) — refusing to execute.")

    # Fidelity: smoothing buys feasibility by moving joints, so confirm the CHOREOGRAPHY survived —
    # a jerk number can always be "fixed" by smoothing the dance into mush.
    terr = tcp_tracking_error(stack, arms, flow, pos)
    if terr.get("ok"):
        print(f"    TCP tracking vs the intended dance: mean {terr['mean'] * 1e3:.1f} mm · "
              f"p95 {terr['p95'] * 1e3:.1f} mm · max {terr['max'] * 1e3:.1f} mm")
    else:
        print(f"    (TCP tracking check unavailable: {terr.get('error', '?')})")

    if not execute:
        print("-" * 74)
        print(f"DRY-RUN done (one {flow.duration:.1f}s flow, {len(flow)} waypoints planned + saved). "
              "Re-run with --flow --execute to run it.")
        print("-" * 74)
        return 0

    # 4) GATE, then approach the flow's start config and REPLAY the single trajectory ---------------
    if not vres.get("ok"):
        print("\nNOT executing: the flow did not pass collision validation (see above).")
        return 1
    q0 = {n: float(v) for n, v in zip(names, positions[0])}
    try:
        input(f"\n>>> Press ENTER to (1) move to the flow start, then (2) run the {flow.duration:.1f}s "
              "CONTINUOUS dance (Ctrl-C aborts): ")
    except (EOFError, KeyboardInterrupt):
        print("\naborted before motion — nothing moved.")
        return 0

    print("\n── approach · move to the flow start config (collision-checked) ──")
    ap = stack.goto_joints(q0, execute=True)
    if not (getattr(ap, "ok", False) and getattr(ap, "executed", False)):
        print(f"  ✗ could not reach the flow start ({(getattr(ap, 'message', '') or '')[:90]}) — NOT running the flow.")
        return 1
    if tracker is not None and getattr(ap, "trajectory", None) is not None:
        tracker.advance(ap.trajectory)

    print(f"\n── FLOW · replaying ONE {flow.duration:.1f}s trajectory ({len(flow)} waypoints, no stops) ──")
    res = stack.play_trajectory(flow)
    ok = bool(getattr(res, "ok", False) and getattr(res, "executed", False))
    if ok:
        if tracker is not None:
            tracker.advance(flow)
        print(f"  ✓ flow executed — one continuous dual-arm trajectory, no per-pose pauses")
    else:
        aborted = getattr(res, "aborted", False)
        print(f"  {'⚠' if aborted else '✗'} flow {'ABORTED' if aborted else 'FAILED'}: "
              f"{(getattr(res, 'message', '') or '')[:90]}")

    try:                                                                       # best-effort retract (non-fatal)
        print("\nretracting home (best-effort)...")
        r = stack.retract(execute=True)
        if not getattr(r, "ok", False):
            print("  retract couldn't plan from here — leaving the arms put.")
    except Exception as e:  # noqa: BLE001
        print(f"  retract skipped: {type(e).__name__}: {e}")

    print("\n" + "-" * 74)
    print("FLOW SUMMARY: one multi-waypoint cuRobo trajectory replayed on hardware."
          if ok else "FLOW did not complete on hardware.")
    print("-" * 74)
    return 0 if ok else 1


# --- orchestration -----------------------------------------------------------------------------

class _FakeObs:
    """A stand-in observation carrying only joint_positions — all the plan seed needs."""

    def __init__(self, joint_positions: Dict[str, float]) -> None:
        self.joint_positions = dict(joint_positions)


class _SeedTracker:
    """Keep the plan seed ALIVE and STOP a dying camera from freezing the run — a stop-gap for a flaky
    cable so the demo CONTINUES. Wraps `bridge.get_observation`: every good read caches the joints; but
    the moment a read FAILS or goes SLOW (the ZED grab hanging ~10 s and leaking Argus threads until the
    box freezes), it LATCHES to tracked-only — it NEVER grabs the camera again, and thereafter returns
    the last-known joints (advanced by each executed trajectory). So the planner always seeds from where
    the arm ACTUALLY is (never HOME → no fly-home) AND the camera errors can't infect the control loop.
    (Real fix is the cable + a joint read decoupled from the cameras.)"""

    SLOW_S = 2.0        # a read slower than this = the camera is struggling → latch (good reads are ~0.07 s)

    def __init__(self, bridge) -> None:
        self._real = bridge.get_observation
        self.tracked: Optional[Dict[str, float]] = None
        self.using_fallback = False
        self.camera_dead = False
        self.fallbacks = 0
        bridge.get_observation = self._get_observation      # monkeypatch in place

    def _latch(self, why: str) -> None:
        if not self.camera_dead:
            self.camera_dead = True
            print(f"      ⚠ {why} — SWITCHING to tracked-seed-only for the rest of the run: NO more camera "
                  "grabs (stops the Argus hang/freeze). Seed = last read, advanced by each executed move.")

    def _get_observation(self, *a, **k):
        # Once latched, never touch the (dying) camera again — return the tracked config immediately.
        if self.camera_dead and self.tracked is not None:
            self.using_fallback = True
            self.fallbacks += 1
            return _FakeObs(self.tracked)
        t0 = time.perf_counter()
        try:
            obs = self._real(*a, **k)
            dt = time.perf_counter() - t0
            jp = getattr(obs, "joint_positions", None)
            if jp:
                self.tracked = {str(n): float(v) for n, v in dict(jp).items()}
                self.using_fallback = False
                if dt > self.SLOW_S:                        # slow read = camera struggling → stop grabbing it
                    self._latch(f"camera read took {dt:.1f}s")
                return obs
        except Exception:  # noqa: BLE001 — camera/bridge read failed
            pass
        if self.tracked is not None:                        # failed read → latch + fall back
            self._latch("camera read failed")
            self.using_fallback = True
            self.fallbacks += 1
            return _FakeObs(self.tracked)
        raise RuntimeError("no joint reading available and no cached seed to fall back on")

    def advance(self, traj) -> None:
        """After an executed move the arm is at the trajectory's END — record it so the tracked seed
        stays accurate even while the camera read is down."""
        if self.tracked is not None and traj is not None:
            try:
                self.tracked.update({str(n): float(v) for n, v in zip(traj.joint_names, traj.final)})
            except Exception:  # noqa: BLE001
                pass


def _joints_readable(stack: MotionStack) -> bool:
    """True iff the robot's CURRENT joints can be read (a non-empty seed). If False, executing ANY
    plan is a FLY-HOME hazard: the planner seeds from its DEFAULTS (= home), not the arm's real pose,
    so the executed trajectory starts at the wrong config and the arm jumps violently to 'home'. On
    this rig `get_observation()` couples the ZED grab with the joint read, so a camera fault empties
    the seed — exactly the incident that needed an E-stop."""
    try:
        return bool(stack._seed_positions())
    except Exception:  # noqa: BLE001
        return False


def _world_culprits(stack: MotionStack, cvp, seed) -> list:
    """Which modeled cuboid(s) the robot's collision spheres penetrate at `seed` → (name, n_spheres,
    center, dims), worst first. ROTATION-AWARE via world.sphere_box_overlaps — the private AABB copy
    here read the 90°-yawed wall as a slab through the workspace (the obs_5_wall lie, 2026-07-20)."""
    try:
        spheres = cvp.robot_spheres(seed)
    except Exception:  # noqa: BLE001
        return []
    from motion_engine.world import sphere_box_overlaps
    scene = stack.world.scene or {}
    cuboids = scene.get("cuboid") or {}
    hits = []
    for name, v in sphere_box_overlaps(spheres, scene).items():
        if v["spheres_penetrating"] > 0:
            b = cuboids.get(name) or {}
            c = [float(x) for x in (list(b.get("pose") or [0, 0, 0])[:3])]
            d = [float(x) for x in (b.get("dims") or [0.1, 0.1, 0.1])]
            hits.append((name, int(v["spheres_penetrating"]), c, d))
    hits.sort(key=lambda h: -h[1])
    return hits


def diagnose_start(stack: MotionStack) -> int:
    """Interrogate the CURRENT config to explain 'Start in collision' — NO motion. Shows each joint vs
    its planner limit (out-of-bounds is the usual cause after a wrist wraps past a URDF limit), plus
    cuRobo's `validate` verdict, split into bounds / world / self."""
    cvp = stack._planner_for(list(stack._groups.keys()))
    names = list(cvp.planner.joint_names)
    seed = stack._seed_positions()
    lims = cvp.joint_limits()
    print("\n" + "=" * 72)
    print("START-CONFIG DIAGNOSIS — why every plan says 'Start in collision' (NO motion)")
    print(f"  {'joint':16s} {'current':>9s} {'min':>8s} {'max':>8s}   status")
    oob = []
    for n in names:
        v = float(seed.get(n, float("nan")))
        lo, hi = lims.get(n, (float("nan"), float("nan")))
        status = "OK"
        if v == v and lo == lo and (v < lo - 1e-6 or v > hi + 1e-6):
            status = "*** OUT OF LIMITS ***"
            oob.append((n, v, lo, hi))
        print(f"  {n:16s} {v:9.3f} {lo:8.3f} {hi:8.3f}   {status}")
    # DECISIVE born-collision ablation: a tiny (+3 cm) test plan WITH the modeled world vs WITHOUT it.
    # If clearing the world makes the start plannable, it's a MODELED OBSTACLE; if it still fails with
    # NO world, it's SELF-collision (world-independent). Uses the planner's own (working) plan path.
    arms = list(stack._groups.keys())

    def _tiny_ok() -> Optional[bool]:
        a = arms[0]
        p = stack.current_tool_pose(a)
        if p is None:
            return None
        res = stack.move_to_poses({a: [p[0][0], p[0][1], p[0][2] + 0.03]}, execute=False)
        return bool(getattr(res, "ok", False))

    print("\n  ablation: a +3 cm test plan WITH the modeled world vs WITHOUT it (no motion)...")
    with_world = _tiny_ok()
    no_world = None
    try:
        cvp.clear_world()
        no_world = _tiny_ok()
    finally:
        try:
            cvp.set_world(stack.world)
        except Exception:  # noqa: BLE001
            pass
    print(f"    plan WITH modeled world : {with_world}")
    print(f"    plan with world CLEARED : {no_world}")

    print("\n  VERDICT:")
    if oob:
        print(f"  >>> {len(oob)} joint(s) OUT OF LIMITS — bounds violation; unwrap/jog them back.")
    elif with_world:
        print("  >>> the start plans fine now — the config is OK; earlier failures were from a different")
        print("      (worse) pose. Re-run the demo.")
    elif no_world:
        print("  >>> WORLD collision — clearing the modeled obstacles fixed it.")
        culprits = _world_culprits(stack, cvp, seed)
        if culprits:
            print("      The arm's collision spheres sit INSIDE these modeled obstacle(s):")
            for name, n, c, d in culprits:
                print(f"        '{name}': {n} spheres inside  · center={[round(x, 3) for x in c]}"
                      f"  dims={[round(x, 3) for x in d]}")
            print("      → fix that obstacle's placement/size in cell.yaml, or clear the world for the demo.")
        else:
            print("      (couldn't pinpoint via AABB — the obstacle may be a mesh or a rotated box.)")
    else:
        print("  >>> SELF-collision — fails even with NO world. The two-arm self-collision model flags this")
        print("      config (over-conservative spheres / missing cross-arm ignore, or the arms are close).")
    print("=" * 72)
    return 0


def run(stack: MotionStack, args, tracker: Optional["_SeedTracker"] = None) -> int:
    if getattr(args, "flow", False):
        return run_flow(stack, args, tracker)

    arms = list(stack._groups.keys())
    execute = bool(args.execute) and not args.dry_run
    print("=" * 74)
    print(f"cuRobo showcase — arms={arms}  mode={'EXECUTE (real motion)' if execute else 'DRY-RUN (plan only)'}")

    if getattr(args, "diagnose", False):
        return diagnose_start(stack)

    # BEST-EFFORT retract: try to start the dance from the cell's cspace-home (a clean, spread base).
    # NON-fatal: if the big move to home can't be planned (it crosses a spot the collision model
    # rejects, or the model is conservative), we DON'T abort — the arms are wherever they are, and the
    # dance is small OFFSETS FROM THE CURRENT POSE, which plan fine on their own. Phase 1 (no motion)
    # confirms before anything executes. `home` below is read LIVE from the arm either way.
    ok, moved = retract_gate(stack, execute)
    if not ok:
        print("  retract to cspace-home couldn't plan — SKIPPING it and dancing from the CURRENT pose")
        print("  (the moves are small offsets from here; Phase 1 below plans them all with no motion).")

    home = {a: stack.current_tool_pose(a) for a in arms}
    if any(v is None for v in home.values()):
        print("  could not FK current tool poses — is the bridge connected?")
        return 2

    # Build the routine: the curated ~21-move dance, or `--poses N` procedurally-generated poses.
    if args.poses and int(args.poses) > 0:
        seq = generate_poses(home, arms, int(args.poses), close=args.close,
                             stagger=args.stagger, depth=args.depth)
    else:
        seq = scenes(home, arms, approach=args.sep, close=args.close,
                     stagger=args.stagger, depth=args.depth)

    # ── PHASE 1 · PLAN THE WHOLE CHAIN (no motion). Each pose plans from the PREVIOUS pose's END
    # (chained), and we SAVE the trajectory. Only ONE live-state read (the first seed); after that the
    # chain rides trajectory ends — so planning never re-reads the camera or drifts. A pose that can't
    # plan is SKIPPED (the chain continues from the last good config), so Phase 2 only replays validated
    # moves — no stuck re-planning, no drift into the wall, no out-of-range.
    print(f"\n── PHASE 1 · PLAN {len(seq)} poses (chained, no motion) ──")
    plan_log: List[dict] = []
    planned: List[tuple] = []                              # [(name, trajectory)] — the saved chain
    seed = stack._seed_positions()                        # the ONLY live read; then we chain off trajectories
    for name, offsets in seq:
        mv = step(stack, name, home, arms, offsets, execute=False, log=plan_log, start=seed)
        traj = getattr(mv, "trajectory", None) if mv is not None else None
        if traj is not None:
            planned.append((name, traj))
            seed = {n: float(v) for n, v in zip(traj.joint_names, traj.final)}   # chain from this pose's END
    pms = [r["plan_ms"] for r in plan_log if r.get("plan_ms")]
    seps = [r["sep"] for r in plan_log if r.get("ok") and r.get("sep") is not None]
    print(f"\n  PLAN complete: {len(planned)}/{len(seq)} poses planned + SAVED"
          + (f" · median {statistics.median(pms):.0f} ms" if pms else "")
          + (f" · closest TCP approach {min(seps) * 100:.0f} cm" if seps else ""))

    if not execute:
        print("-" * 74)
        print(f"DRY-RUN done ({len(planned)}/{len(seq)} planned + saved). Re-run with --execute to replay.")
        print("-" * 74)
        return 0
    if not planned:
        print("nothing planned — nothing to execute.")
        return 0

    # ── GATE · wait for the operator before ANY motion.
    try:
        input(f"\n>>> Press ENTER to EXECUTE (replay) these {len(planned)} saved trajectories "
              "(Ctrl-C aborts): ")
    except (EOFError, KeyboardInterrupt):
        print("\naborted before execution — no motion.")
        return 0

    # ── PHASE 2 · REPLAY the saved trajectories — NO re-planning, NO camera reads. The chain is only
    # valid while each move COMPLETES (the next traj starts where this one ends), so STOP on any abort.
    print(f"\n── PHASE 2 · EXECUTE {len(planned)} saved trajectories ──")
    done = 0
    for name, traj in planned:
        res = stack.play_trajectory(traj)
        if getattr(res, "ok", False) and getattr(res, "executed", False):
            print(f"  ✓ {name:16s} motion {float(getattr(traj, 'duration', 0.0)):.1f}s | executed")
            done += 1
        else:
            aborted = getattr(res, "aborted", False)
            print(f"  {'⚠' if aborted else '✗'} {name:16s} "
                  f"{'ABORTED' if aborted else 'FAILED'}: {(getattr(res, 'message', '') or '')[:70]}")
            print("  stopping — the arm didn't finish this move, so the saved chain no longer lines up.")
            break

    try:                                                  # best-effort retract (non-fatal)
        print("\nretracting home (best-effort)...")
        r = stack.retract(execute=True)
        if not getattr(r, "ok", False):
            print("  retract couldn't plan from here — leaving the arms put.")
    except Exception as e:  # noqa: BLE001
        print(f"  retract skipped: {type(e).__name__}: {e}")

    print("\n" + "-" * 74)
    print(f"SUMMARY: {done}/{len(planned)} saved trajectories replayed on hardware"
          + (f" · closest TCP approach {min(seps) * 100:.0f} cm" if seps else ""))
    if args.loops > 1:
        print("(--loops is ignored in replay mode: the saved chain only lines up from its own start.)")
    print("-" * 74)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="remoroo × cuRobo end-to-end multi-arm showcase")
    ap.add_argument("--cell", default=os.environ.get("REMOROO_CELL", "remoroo_cell"))
    ap.add_argument("--execute", action="store_true", help="ACTUALLY MOVE the robot (default: dry-run)")
    ap.add_argument("--dry-run", action="store_true", help="plan + time only, never move (overrides --execute)")
    ap.add_argument("--diagnose", action="store_true",
                    help="diagnostic, NO motion: show each joint vs its planner limit + cuRobo's validate "
                         "verdict to explain a 'Start in collision' abort (usually an out-of-bounds joint)")
    ap.add_argument("--loops", type=int, default=1, help="how many times through the routine (execute phase)")
    ap.add_argument("--poses", type=int, default=0,
                    help="generate N procedural flowing poses instead of the curated ~21-move routine "
                         "(own-side flow + a close encounter every ~5th pose). 0 = use the curated dance")
    ap.add_argument("--sep", type=float, default=0.06,
                    help="gentle inward reach (m) for the own-side scenes — closes only part of the gap "
                         "so the bodies stay clear (default 6 cm)")
    ap.add_argument("--close", type=float, default=0.20,
                    help="how far each arm reaches toward the other (m) in the CLOSE-ENCOUNTER scenes — "
                         "the grippers come near, staggered in height so the bodies clear; the adaptive "
                         "shrink backs off if it's too tight, so raising this only ever gets them as "
                         "close as is collision-free (default 20 cm)")
    ap.add_argument("--stagger", type=float, default=0.18,
                    help="vertical (height) offset (m) between the two grippers during the close encounter "
                         "— a lever for getting closer: the bodies pass at different levels (default 18 cm)")
    ap.add_argument("--depth", type=float, default=0.08,
                    help="depth (x) offset (m) between the two arms during the close encounter — one reaches "
                         "deep, the other shallow, so the bodies also pass at different distances; combined "
                         "with --stagger this opens both clearance axes so the grippers converge closer in y "
                         "(default 8 cm; raise it with --close to push nearer, the shrink stays safe)")
    ap.add_argument("--reps", type=int, default=6, help="plan repetitions for the speed benchmark")
    # ── FLOW: one continuous multi-waypoint cuRobo trajectory (the "right way") ──
    ap.add_argument("--flow", action="store_true",
                    help="plan the WHOLE dance as ONE continuous cuRobo trajectory (MotionRetargeter): "
                         "no per-pose stops, no chaining, no failing retries. Two-part like the rest: "
                         "builds the flow (no motion), waits for ENTER, then moves to the flow start and "
                         "replays the single trajectory. Combine with --execute / --poses N")
    ap.add_argument("--flow-dt", type=float, default=0.025,
                    help="seconds per frame for the flow (the replay cadence AND the retargeter's "
                         "velocity limit); matches the stack's ~25 ms interpolation dt (default 0.025)")
    ap.add_argument("--flow-step", type=float, default=0.006,
                    help="target TCP spacing (m) between consecutive flow frames — the density knob: "
                         "smaller = smoother + slower, larger = faster + coarser (default 6 mm)")
    ap.add_argument("--flow-tol", type=float, default=0.005,
                    help="retargeter position tolerance (m). The default 0.005 is ~the size of "
                         "--flow-step, so the solver's accepted slop is comparable to the step and "
                         "frames can wobble → jerk. Try 0.001 to smooth (costs solve time)")
    ap.add_argument("--flow-vreg", type=float, default=-1.0,
                    help="velocity regularization weight — penalizes (q-q_prev)/dt. -1 = cuRobo default "
                         "(0.001, weak). Raise (e.g. 0.05) for smoother joint velocity at the cost of "
                         "tracking accuracy")
    ap.add_argument("--flow-mpc", action="store_true",
                    help="use the retargeter's MPC solver (Level 3: smoothest — it carries velocity/"
                         "acceleration/JERK state across frames, which warm-started IK does not) instead "
                         "of IK. 2-4x slower to solve. Targets are auto-spaced --flow-steps× further "
                         "apart so the dance keeps its tempo (MPC's horizon smooths between them)")
    ap.add_argument("--flow-smooth", type=int, default=25,
                    help="low-pass window (frames) applied to the SOLVED joint trajectory — the only "
                         "lever measured to reach the retargeter's per-frame IK jitter (no solver "
                         "parameter did). 1 = off. Rig-measured at 25: peak accel 252->9.3 (limit 15) "
                         "and jerk 20091->75 (limit 500) for +2.5mm mean TCP error. Every waypoint is "
                         "re-validated for collision afterwards")
    ap.add_argument("--flow-tries", type=int, default=4,
                    help="re-solve up to N times until a collision-free trajectory is found. The solve "
                         "is NOT reproducible (GPU non-determinism picks a different null-space branch "
                         "each run), so this is what makes the demo reliable rather than lucky")
    ap.add_argument("--flow-seed", type=int, default=0,
                    help="RNG seed for cuRobo's global IK. Unseeded, every solve picks a different "
                         "null-space branch, so a clean dry-run does NOT predict the next run")
    ap.add_argument("--flow-fit", action="store_true",
                    help="RUN AS FAST AS THE RIG ALLOWS: after solving, time-compress the trajectory "
                         "until the first of velocity/accel/jerk hits its limit (times --flow-margin). "
                         "Time-compression cannot create a collision — it leaves the geometry untouched")
    ap.add_argument("--flow-speed", type=float, default=1.0,
                    help="manual time-compression multiplier (2.0 = twice as fast). Ignored when "
                         "--flow-fit is set")
    ap.add_argument("--flow-margin", type=float, default=0.95,
                    help="fraction of the rig's limits --flow-fit is allowed to reach (default 0.95)")
    ap.add_argument("--flow-accel", type=float, default=1.5,
                    help="Cartesian acceleration cap (m/s²) for the TCP path — THE smoothness/speed "
                         "dial. The path slows itself through corners to respect it and cruises on the "
                         "straights, so lower = smoother (and a bit longer), higher = snappier "
                         "(and jerkier). Default 1.5")
    ap.add_argument("--flow-steps", type=int, default=4,
                    help="MPC control steps per target (steps_per_target). More = smoother tracking but "
                         "each target costs steps×dt of motion time (auto-compensated by target spacing). "
                         "Ignored when --flow-mpc is off (default 4)")
    ap.add_argument("--flow-areg", type=float, default=-1.0,
                    help="acceleration regularization weight — penalizes (v-v_prev)/dt, THE anti-jerk "
                         "term. -1 = cuRobo default (0.01). Raise (e.g. 0.1) with --flow-mpc for the "
                         "smoothest motion")
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

    # Keep the plan seed alive across camera-read drops (flaky cable stop-gap) — wraps get_observation
    # BEFORE the stack is built so every stack read goes through it. Falls back to the last-known joints
    # (never HOME) so the demo continues instead of fly-home-aborting.
    tracker = None
    try:
        tracker = _SeedTracker(bridge)
    except Exception as e:  # noqa: BLE001 — never let the safety wrapper block the run
        print(f"  (seed tracker not installed: {type(e).__name__}: {e})")

    try:
        print("building + warming the motion stack...")
        stack = MotionStack.from_cell(cell, bridge=bridge)
        stack.prewarm()
        return run(stack, args, tracker)
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
