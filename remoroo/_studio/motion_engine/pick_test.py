#!/usr/bin/env python3
"""Validate the ONE real risk of the table-clearing demo: can a gripper actually pick a soft toy?

Perceive (overhead SAM2) → pick a target toy → RETRACT → hover top-down above it → descend → close →
lift → read the gripper to judge whether it holds → move to a drop point → release. Everything is
operator-gated (ENTER between phases, Ctrl-C E-stops) and starts from a retract, so it is safe to probe.

Grasp success is read from the gripper: after closing on a real object the jaws STOP partway
(drive-joint > a few mm); closing on air runs to fully shut. That's the held/not-held signal.

    $PY -m motion_engine.pick_test                       # perceive + plan only, NO motion
    $PY -m motion_engine.pick_test --execute             # gated pick of the best-reachable toy
    $PY -m motion_engine.pick_test --execute --arm arm2 --index 0
"""
from __future__ import annotations

import argparse
import math
import os
import signal
import sys
import time
from pathlib import Path

import numpy as np

try:
    from motion_engine import MotionStack
    from motion_engine.demo_curobo_showcase import connect_bridge, _SeedTracker
    from motion_engine.perceive_table import detect_all, urdf_static_pose, K_of, annotate
except Exception:  # noqa: BLE001
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from motion_engine import MotionStack
    from motion_engine.demo_curobo_showcase import connect_bridge, _SeedTracker
    from motion_engine.perceive_table import detect_all, urdf_static_pose, K_of, annotate

OVERHEAD = "ZEDX_Mini_3"


def R_to_wxyz(R: np.ndarray):
    tr = R[0, 0] + R[1, 1] + R[2, 2]
    if tr > 0:
        s = 0.5 / math.sqrt(tr + 1.0)
        w, x, y, z = 0.25 / s, (R[2, 1] - R[1, 2]) * s, (R[0, 2] - R[2, 0]) * s, (R[1, 0] - R[0, 1]) * s
    elif R[0, 0] > R[1, 1] and R[0, 0] > R[2, 2]:
        s = 2.0 * math.sqrt(1.0 + R[0, 0] - R[1, 1] - R[2, 2])
        w, x, y, z = (R[2, 1] - R[1, 2]) / s, 0.25 * s, (R[0, 1] + R[1, 0]) / s, (R[0, 2] + R[2, 0]) / s
    elif R[1, 1] > R[2, 2]:
        s = 2.0 * math.sqrt(1.0 + R[1, 1] - R[0, 0] - R[2, 2])
        w, x, y, z = (R[0, 2] - R[2, 0]) / s, (R[0, 1] + R[1, 0]) / s, 0.25 * s, (R[1, 2] + R[2, 1]) / s
    else:
        s = 2.0 * math.sqrt(1.0 + R[2, 2] - R[0, 0] - R[1, 1])
        w, x, y, z = (R[1, 0] - R[0, 1]) / s, (R[0, 2] + R[2, 0]) / s, (R[1, 2] + R[2, 1]) / s, 0.25 * s
    q = np.array([w, x, y, z]); return (q / np.linalg.norm(q)).tolist()


def topdown_quat(yaw_deg: float) -> list:
    """TCP orientation for a top-down grasp: tool +Z points straight DOWN (approach from above),
    tool +X is the jaw-opening axis rotated to `yaw_deg` in the table plane. If the physical gripper
    opens along the other axis, add 90° (--yaw-offset); if it points sideways, the tool-Z assumption is
    wrong and we flip via --approach-axis."""
    a = math.radians(yaw_deg)
    c, s = math.cos(a), math.sin(a)
    R = np.array([[c, s, 0.0], [s, -c, 0.0], [0.0, 0.0, -1.0]])   # columns = tool x,y,z in world
    return R_to_wxyz(R)


def topup_quat(yaw_deg: float) -> list:
    """TCP orientation for approaching from BELOW: tool +Z points straight UP (world +Z). The OPPOSING
    gripper in a handoff — arm1 comes up under a toy that arm2 holds gripper-down, so the two grippers
    face each other with the toy between them (not parallel/side-by-side)."""
    a = math.radians(yaw_deg)
    c, s = math.cos(a), math.sin(a)
    R = np.array([[c, -s, 0.0], [s, c, 0.0], [0.0, 0.0, 1.0]])    # tool +Z = world +Z
    return R_to_wxyz(R)


def tilted_quat(tilt_deg: float, toward=(0.0, -1.0)) -> list:
    """Gripper approach axis (tool +Z) tilted `tilt` degrees from straight-DOWN, leaning toward the
    horizontal direction `toward` (default -Y, i.e. toward arm2). tilt=0 → straight down; 90 → fully
    horizontal; 180 → straight up. This is the flexible handoff receive: any angle 45–135° makes the
    receiving gripper OPPOSE the presenting one enough to grab the same toy, without demanding the
    (rarely reachable) exact 180°."""
    th = math.radians(tilt_deg)
    ty = float(toward[1]); tx = float(toward[0])
    n = (tx * tx + ty * ty) ** 0.5 or 1.0
    zt = np.array([math.sin(th) * tx / n, math.sin(th) * ty / n, -math.cos(th)])   # approach dir
    zt = zt / (np.linalg.norm(zt) or 1.0)
    xt = np.cross([0.0, 0.0, 1.0], zt)                            # jaw axis: horizontal, ⟂ approach
    xt = xt / (np.linalg.norm(xt) or 1.0)
    yt = np.cross(zt, xt)
    return R_to_wxyz(np.column_stack([xt, yt, zt]))


def _gate(msg: str) -> bool:
    try:
        input(msg)
        return True
    except (EOFError, KeyboardInterrupt):
        print("\naborted — no motion.")
        return False


def grab_overhead(bridge, tries: int = 8, pause: float = 0.6):
    """Grab the overhead frame, tolerating the GMSL/Argus transient ('Receive thread is not running',
    'ZED grab failed') that hits right after a daemon restart — retry instead of crashing the run."""
    last = None
    for i in range(tries):
        try:
            return bridge.grab_camera(OVERHEAD)
        except Exception as e:  # noqa: BLE001
            last = e
            print(f"  overhead grab retry {i + 1}/{tries} ({type(e).__name__})")
            time.sleep(pause)
    raise RuntimeError(f"overhead grab failed after {tries} tries: {last}")


def filter_detections(objs: list, cell: str, args, stack=None) -> list:
    """Drop PHANTOM detections — the 2026-07-21 'always picks empty space' bug: 1 real toy read as
    3 objects (one past the table edge, one = the robot's own gripper in frame). Three gates:
      1. TABLE FOOTPRINT: centroid inside the modeled table obstacle (cell.yaml) minus a margin.
      2. ROBOT SELF-MASK: centroid within `--robot-mask` of any live FK'd robot collision sphere
         (needs `stack` with a bridge) → it's the arm/gripper, not a toy.
      3. EXCLUDE ZONES: --exclude \"x,y,r\" (repeatable) — e.g. the drop bin.
    """
    import yaml
    keep = []
    tb = next((o for o in (yaml.safe_load((Path(cell) / "cell.yaml").read_text()).get("obstacles") or [])
               if o.get("name") == "table"), None)
    tc = list((tb or {}).get("pose") or [0.10, -0.33, -0.065])[:3]
    td = list((tb or {}).get("dims") or [0.8, 1.9, 0.04])
    sph = None
    if stack is not None:
        try:
            cvp = stack._planner_for(list(stack._groups.keys()))
            sph = cvp.robot_spheres(stack._seed_positions())
        except Exception as e:  # noqa: BLE001
            print(f"  (robot self-mask unavailable: {type(e).__name__} — footprint/exclude gates only)")
    zones = []
    for zs in (getattr(args, "exclude", None) or []):
        try:
            x, y, r = [float(v) for v in zs.split(",")]
            zones.append((x, y, r))
        except Exception:  # noqa: BLE001
            print(f"  (bad --exclude {zs!r} — want x,y,r)")
    rmask = float(getattr(args, "robot_mask", 0.09) or 0.09)
    for o in objs:
        x, y, _ = o["xyz"]
        if abs(x - tc[0]) > td[0] / 2 - 0.02 or abs(y - tc[1]) > td[1] / 2 - 0.02:
            print(f"  ✂ drop {o.get('name', '?')!r} ({x:+.2f},{y:+.2f}): OFF the table footprint")
            continue
        if sph is not None and len(sph):
            d = np.hypot(sph[:, 0] - x, sph[:, 1] - y)
            near = (d < (rmask + sph[:, 3])) & (sph[:, 2] > tc[2])
            if bool(near.any()):
                print(f"  ✂ drop {o.get('name', '?')!r} ({x:+.2f},{y:+.2f}): the ROBOT is there "
                      f"(self-mask, {int(near.sum())} spheres)")
                continue
        if any((x - zx) ** 2 + (y - zy) ** 2 < zr ** 2 for zx, zy, zr in zones):
            print(f"  ✂ drop {o.get('name', '?')!r} ({x:+.2f},{y:+.2f}): in an --exclude zone")
            continue
        keep.append(o)
    print(f"  detections: {len(objs)} raw → {len(keep)} after footprint/self-mask/exclude")
    return keep


def perceive(bridge, cell: str, args, stack=None) -> tuple:
    frame = grab_overhead(bridge)
    depth = np.asarray(frame["depth_m"], float)
    K = K_of(frame["intrinsics"])
    R, t = urdf_static_pose(str(Path(cell) / "robot_model" / "robot.urdf"), f"{OVERHEAD}_optical_frame")
    print("perceiving table (overhead SAM2)...")
    objs = detect_all(frame["rgb"], depth, R, t, K, args)
    objs = filter_detections(objs, cell, args, stack=stack)
    return objs, frame, depth, R, t, K


def main() -> int:
    ap = argparse.ArgumentParser(description="single-toy pick validation (gated)")
    ap.add_argument("--cell", default=os.environ.get("REMOROO_CELL", "remoroo_cell"))
    ap.add_argument("--execute", action="store_true", help="actually move (default: perceive + plan only)")
    ap.add_argument("--arm", default="", help="which arm picks (default: the one that can reach it)")
    ap.add_argument("--index", type=int, default=-1, help="which detected toy (default: best-reachable)")
    ap.add_argument("--grip-open", type=float, default=0.0, help="gripper open value 0..1 (0=fully open)")
    ap.add_argument("--grip-close", type=float, default=1.0, help="gripper close value 0..1 (1=fully closed)")
    ap.add_argument("--hover", type=float, default=0.12, help="hover height above the toy top (m)")
    ap.add_argument("--grasp-dz", type=float, default=-0.025, help="grasp depth below the toy top (m)")
    ap.add_argument("--lift", type=float, default=0.18, help="lift height after grasp (m)")
    ap.add_argument("--yaw-offset", type=float, default=90.0, help="add to the toy yaw for the jaw axis (deg)")
    ap.add_argument("--held-thresh", type=float, default=0.825,
                    help="drive-joint below this after closing = HOLDING. A SOFT toy compresses so the "
                         "jaws nearly fully close even when holding (measured 0.806 held vs ~0.838 air), "
                         "so this sits just under fully-shut. Ground truth = re-perceive that it left")
    ap.add_argument("--drop", default="0.40,0.30,0.20", help="drop point x,y,z (m) to release over")
    # SAM2 detection knobs (forwarded to detect_all)
    ap.add_argument("--weights", default=".remoroo/task/weights")
    ap.add_argument("--sam-points", type=int, default=48)
    ap.add_argument("--table-z", type=float, default=-0.045)
    ap.add_argument("--min-height", type=float, default=0.010)
    ap.add_argument("--min-x", type=float, default=0.15, help="drop objects behind the robot (world x floor, m)")
    ap.add_argument("--max-height", type=float, default=0.12)
    ap.add_argument("--min-pts", type=int, default=150)
    ap.add_argument("--dedup", type=float, default=0.06)
    ap.add_argument("--max-footprint", type=float, default=28.0)
    ap.add_argument("--exclude", action="append", default=[], help="phantom-free zone \"x,y,r\" (repeatable) — e.g. the drop bin")
    ap.add_argument("--robot-mask", type=float, default=0.09, help="drop detections within this of a live robot sphere (m)")
    args = ap.parse_args()
    cell = str(Path(args.cell).resolve())
    execute = bool(args.execute)

    bridge = connect_bridge(cell)
    try:
        _SeedTracker(bridge)
    except Exception:  # noqa: BLE001
        pass

    def _stop(_s, _f):
        try:
            if hasattr(bridge, "estop"):
                bridge.estop()
        except Exception:  # noqa: BLE001
            pass
        raise KeyboardInterrupt
    signal.signal(signal.SIGINT, _stop)

    print("building + warming the motion stack...")
    stack = MotionStack.from_cell(cell, bridge=bridge)
    stack.prewarm()
    arms = list(stack._groups.keys())

    objs, frame, depth, R, t, K = perceive(bridge, cell, args, stack=stack)
    if not objs:
        print("no toys detected — nothing to pick."); return 1
    annotate(frame, depth, objs, R, t, K,
             str(Path(os.environ.get("SCRATCH", "/tmp")) / "pick_test.png"))

    # choose the target toy + which arm reaches it (plan-only reachability of the hover pose)
    def hover_pose(o):
        q = topdown_quat(o["yaw"] + args.yaw_offset)
        return ([o["xyz"][0], o["xyz"][1], o["xyz"][2] + args.hover], q)

    print("\nreachability (plan-only, top-down hover) — which arm can reach each toy:")
    reach = {}
    for i, o in enumerate(objs):
        who = []
        for a in ([args.arm] if args.arm else arms):
            try:
                r = stack.move_to_poses({a: hover_pose(o)}, execute=False, max_attempts=1)
                if getattr(r, "ok", False) and getattr(r, "trajectory", None) is not None:
                    who.append(a)
            except Exception:  # noqa: BLE001
                pass
        reach[i] = who
        print(f"  obj {i}: {o['label']:6s} XY=({o['xyz'][0]:+.3f},{o['xyz'][1]:+.3f}) "
              f"z={o['xyz'][2]:+.3f} yaw {o['yaw']:+.0f}° → reachable by {who or 'NEITHER'}")

    if args.index >= 0:
        idx = args.index
    else:
        idx = next((i for i, o in enumerate(objs) if reach[i]), None)
    if idx is None or not reach.get(idx):
        print("\nno reachable toy with the current settings."); return 1
    o = objs[idx]
    arm = args.arm or reach[idx][0]
    hp = hover_pose(o)
    print(f"\nTARGET: obj {idx} ({o['label']}) with {arm}  ·  hover {[round(v,3) for v in hp[0]]}  "
          f"quat {[round(v,3) for v in hp[1]]}")

    if not execute:
        print("\nplan-only done (no motion). Re-run with --execute to pick.")
        try: bridge.disconnect()
        except Exception: pass  # noqa: E722
        return 0

    # ---- GATED PICK ------------------------------------------------------------------------------
    if not _gate("\n>>> ENTER to RETRACT both arms to a clean start (Ctrl-C aborts): "):
        return 0
    stack.retract(execute=True)
    try:
        bridge.set_gripper(arm, float(args.grip_open))                # open before approach
    except Exception as e:  # noqa: BLE001
        print(f"  gripper open skipped: {e}")

    if not _gate(f"\n>>> ENTER to move {arm} to a HIGH hover above the toy — CONFIRM the gripper points "
                 "straight DOWN before continuing (Ctrl-C aborts): "):
        return 0
    high = ([hp[0][0], hp[0][1], o["xyz"][2] + max(0.22, args.hover)], hp[1])
    r = stack.move_to_poses({arm: high}, execute=True, max_attempts=3)
    if not (getattr(r, "ok", False) and getattr(r, "executed", False)):
        print(f"  ✗ could not reach the high hover: {(getattr(r,'message','') or '')[:80]}"); return 1

    if not _gate("\n>>> gripper pointing down? ENTER to DESCEND + GRASP + LIFT (Ctrl-C aborts): "):
        return 0
    gz = o["xyz"][2] + args.grasp_dz                                 # grasp height
    grasp_pose = ([hp[0][0], hp[0][1], gz], hp[1])
    r = stack.move_to_poses({arm: grasp_pose}, execute=True, max_attempts=3)
    if not (getattr(r, "ok", False) and getattr(r, "executed", False)):
        print(f"  ✗ could not descend to grasp: {(getattr(r,'message','') or '')[:80]}"); return 1
    drv = bridge._drivers.get(arm)
    bridge.set_gripper(arm, float(args.grip_close))                  # CLOSE on the toy
    time.sleep(1.2)                                                  # let the jaws travel + settle
    held_pos = drv.read_gripper_drive() if drv is not None else None
    lifted = stack.move_to_poses({arm: ([hp[0][0], hp[0][1], o["xyz"][2] + args.lift], hp[1])},
                                 execute=True, max_attempts=3)
    # HELD signal (drive-joint: 0 = open, ~0.85 = fully shut). An object BLOCKS the jaws partway, so
    # held ⇔ they did NOT reach (near) fully-closed. Closing on air runs to ~0.84.
    held = held_pos is not None and held_pos < float(args.held_thresh)
    print(f"\n  gripper drive after close: {held_pos}  (fully-shut≈0.84)  →  "
          f"{'HOLDING something' if held else 'likely EMPTY (closed on air)'}")

    if held and _gate(f"\n>>> lifted. ENTER to carry to the drop point {args.drop} and release: "):
        dx, dy, dz = (float(v) for v in args.drop.split(","))
        stack.move_to_poses({arm: ([dx, dy, dz], hp[1])}, execute=True, max_attempts=3)
        bridge.set_gripper(arm, float(args.grip_open))
    try:
        print("\nretracting (best-effort)...")
        stack.retract(execute=True)
    except Exception as e:  # noqa: BLE001
        print(f"  retract skipped: {e}")
    try: bridge.disconnect()
    except Exception: pass  # noqa: E722
    print("\n" + "-" * 60)
    print(f"PICK TEST: {o['label']} with {arm} — {'HELD' if held else 'grasp did NOT hold'}")
    print("-" * 60)
    return 0 if held else 1


if __name__ == "__main__":
    raise SystemExit(main())
