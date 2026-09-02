#!/usr/bin/env python3
"""Measure how fast the grippers actually actuate — JAWS ONLY, the arms never move.

A mid-motion handoff lives or dies on this number: the flow streams at a fixed dt, and we fire
`set_gripper` from a side thread at a chosen frame. If the jaws take 800 ms to travel, the "snatch"
has to become a slow transfer (or the arms must dwell near each other longer); if they take 150 ms,
a genuine mid-flight pass is on.

`set_gripper` is non-blocking (`set_gripper_position(..., wait=False)`), so we command it and poll the
drive joint until it stops changing. Reports command→first-motion latency and command→settled time,
for a full open/close and for partial travel.

    python3 -m motion_engine.gripper_timing              # both arms, 2 reps
    python3 -m motion_engine.gripper_timing --arm arm1
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

try:
    from motion_engine.demo_curobo_showcase import connect_bridge
except Exception:  # noqa: BLE001
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from motion_engine.demo_curobo_showcase import connect_bridge


def wait_feedback(drv, timeout: float = 3.0):
    """Block until the gripper poller lands its first sample.

    `read_gripper_drive` reads a value cached by a ~10 Hz background poller and returns None until the
    first poll arrives — and the FIRST call is what lazily starts that poller. So a single call always
    returns None; treating that as "no gripper" is wrong (it is what made this look unwired)."""
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout:
        v = drv.read_gripper_drive()
        if v is not None:
            return v
        time.sleep(0.02)
    return None


def travel(drv, target: float, *, settle_n: int = 6, tol: float = 2e-3, timeout: float = 4.0):
    """Command `target` (0=open..1=closed) and poll the drive joint until it stops moving.
    Returns (latency_to_first_motion_s, settled_s, start_rad, end_rad)."""
    start = wait_feedback(drv, timeout=1.0)
    t0 = time.perf_counter()
    drv.set_gripper(float(target))
    first = None
    last = start
    still = 0
    while time.perf_counter() - t0 < timeout:
        v = drv.read_gripper_drive()
        if v is None:
            time.sleep(0.005)
            continue
        if first is None and start is not None and abs(v - start) > tol:
            first = time.perf_counter() - t0
        if last is not None and abs(v - last) <= tol:
            still += 1
            if first is not None and still >= settle_n:
                return first, time.perf_counter() - t0, start, v
        else:
            still = 0
        last = v
        time.sleep(0.005)
    return first, time.perf_counter() - t0, start, last


def main() -> int:
    ap = argparse.ArgumentParser(description="measure gripper actuation time (JAWS ONLY, no arm motion)")
    ap.add_argument("--cell", default=os.environ.get("REMOROO_CELL", "remoroo_cell"))
    ap.add_argument("--arm", default="", help="single arm name (default: every arm)")
    ap.add_argument("--reps", type=int, default=2)
    args = ap.parse_args()

    print("=" * 74)
    print("GRIPPER TIMING — the JAWS move, the ARMS DO NOT. No trajectory is executed.")
    print("=" * 74)
    bridge = connect_bridge(str(Path(args.cell).resolve()))
    drivers = getattr(bridge, "_drivers", {}) or {}
    arms = [args.arm] if args.arm else list(drivers)
    if not arms:
        print("no drivers found on the bridge")
        return 2

    for a in arms:
        drv = drivers.get(a)
        if drv is None:
            print(f"  {a}: no driver")
            continue
        v0 = wait_feedback(drv)
        if v0 is None:
            print(f"  {a}: no gripper feedback after 3s of polling — skipping")
            continue
        print(f"\n── {a} ── (feedback live, drive={v0:.4f} rad)")
        print("  %-14s %10s %10s %12s" % ("move", "latency", "settled", "travel(rad)"))
        for r in range(max(1, args.reps)):
            for label, tgt in (("open (0.0)", 0.0), ("close (1.0)", 1.0),
                               ("half (0.5)", 0.5), ("close (1.0)", 1.0)):
                lat, tot, s0, s1 = travel(drv, tgt)
                span = abs((s1 or 0.0) - (s0 or 0.0))
                print("  %-14s %9s %9.3fs %12.4f"
                      % (label, ("%.3fs" % lat) if lat is not None else "  none", tot, span))
                sys.stdout.flush()
            if r == 0 and len(arms) > 1:
                pass
        drv.set_gripper(0.0)                      # leave it open (safe, ready to receive an object)
    print("\nleft both grippers OPEN.")
    try:
        bridge.disconnect()
    except Exception:  # noqa: BLE001
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
