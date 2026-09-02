#!/usr/bin/env python3
"""RESUME a handoff from the frozen 'golden state' — arm2 already PRESENTING the toy at the meet,
arm1 parked. Skips perception and the pick entirely: reads the REAL joints via the xArm SDK, FKs
arm2's TRUE TCP through the robot planner (camera-immune), then runs the remaining gated legs:

  arm1 open → STAGE (+y back) → FINAL (recv offsets below arm2's pinch) → TCP-gap print
       → TRANSFER (arm1 close, arm2 open) → arm2 retreat + arm1 lift → optional box → retract

Built for the 2026-07-21 freeze (mid-run Argus death → stale seed aimed the receive under the
table). Restart the camera daemons first if the bridge won't connect:
    sudo systemctl restart nvargus-daemon zed_x_daemon

    $PY -m motion_engine.handoff_resume --execute --recv-yaw 90 [--box x,y,z]
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
    from motion_engine.pick_test import topdown_quat, tilted_quat, _gate
    from motion_engine.handoff_test import _roll_about_tool, _drive, _move
except Exception:  # noqa: BLE001
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from motion_engine import MotionStack
    from motion_engine.demo_curobo_showcase import connect_bridge, _SeedTracker
    from motion_engine.pick_test import topdown_quat, tilted_quat, _gate
    from motion_engine.handoff_test import _roll_about_tool, _drive, _move

ARM_IPS = {"arm1": "192.168.10.214", "arm2": "192.168.10.228"}


def real_joints() -> dict:
    from xarm.wrapper import XArmAPI
    out = {}
    for name, ip in ARM_IPS.items():
        suf = "_1" if name == "arm2" else ""
        a = XArmAPI(ip); time.sleep(0.3)
        code, ang = a.get_servo_angle(is_radian=True)
        if code != 0:
            raise RuntimeError(f"servo read failed for {name}: {code}")
        for i in range(6):
            out[f"joint{i + 1}{suf}"] = float(ang[i])
        a.disconnect()
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="resume a frozen handoff (arm2 presenting)")
    ap.add_argument("--cell", default=os.environ.get("REMOROO_CELL", "remoroo_cell"))
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--pick-arm", default="arm2")
    ap.add_argument("--recv-arm", default="arm1")
    ap.add_argument("--recv-dx", type=float, default=0.0)
    ap.add_argument("--recv-dy", type=float, default=0.0)
    ap.add_argument("--recv-dz", type=float, default=-0.025)
    ap.add_argument("--recv-yaw", type=float, default=90.0)
    ap.add_argument("--recv-tilt", type=float, default=90.0)
    ap.add_argument("--approach-back", type=float, default=0.06)
    ap.add_argument("--box", default="", help="arm1 drop point x,y,z (empty = skip)")
    ap.add_argument("--grip-open", type=float, default=0.0)
    ap.add_argument("--grip-close", type=float, default=1.0)
    ap.add_argument("--held-thresh", type=float, default=0.825)
    args = ap.parse_args()
    cell = str(Path(args.cell).resolve())
    pick_arm, recv_arm = args.pick_arm, args.recv_arm

    rj = real_joints()
    print("frozen joints read (SDK, no cameras).")

    bridge = connect_bridge(cell)
    try:
        _SeedTracker(bridge)
    except Exception:  # noqa: BLE001
        pass
    signal.signal(signal.SIGINT, lambda *_: (getattr(bridge, "estop", lambda: None)(), sys.exit(130)))

    print("building + warming the motion stack...")
    stack = MotionStack.from_cell(cell, bridge=bridge)
    stack.prewarm()

    # arm2's TRUE TCP from the REAL joints, FK'd through the planner — no camera, no seed
    cvp = stack._planner_for(list(stack._groups.keys()))
    for n in cvp.planner.joint_names:
        rj.setdefault(n, 0.0)
    state = cvp.planner.compute_kinematics(cvp._start_js(rj))
    frames = list(getattr(state, "tool_frames", None) or cvp.planner.tool_frames)
    pos = state.tool_poses.position.detach().cpu().numpy().reshape(-1, 3)
    a2 = [float(v) for v in pos[frames.index(stack._tip(pick_arm))]]
    print(f"{pick_arm} TRUE TCP (FK of real joints): ({a2[0]:+.3f},{a2[1]:+.3f},{a2[2]:+.3f})")
    if a2[2] < 0.15:
        print("!! arm2's TCP is near the table — this is NOT a presenting pose; aborting (wrong state to resume)")
        return 1

    recv_xyz = [a2[0] + args.recv_dx, a2[1] + args.recv_dy, a2[2] + args.recv_dz]
    recv_q = _roll_about_tool(tilted_quat(args.recv_tilt), args.recv_yaw)
    stage_xyz = [recv_xyz[0], recv_xyz[1] + abs(args.approach_back), recv_xyz[2]]
    print(f"receive: stage={[round(v, 3) for v in stage_xyz]} final={[round(v, 3) for v in recv_xyz]}")

    if not args.execute:
        r = stack.move_to_poses({recv_arm: (stage_xyz, recv_q)}, execute=False, max_attempts=3)
        print(f"plan-only: stage {'OK' if getattr(r, 'ok', False) else 'FAILED — ' + (getattr(r, 'message', '') or '')[:60]}")
        return 0

    if not _gate(f"\n>>> ENTER: open {recv_arm} gripper + bring it to the STAGE point (Ctrl-C aborts): "):
        return 0
    bridge.set_gripper(recv_arm, args.grip_open)
    if not _move(stack, recv_arm, stage_xyz, recv_q, label="receive stage"):
        return 1
    if not _move(stack, recv_arm, recv_xyz, recv_q, label="receive final (slow contact)"):
        return 1
    a1p = stack.current_tool_pose(recv_arm)
    if a1p:
        gap = 1000 * float(np.linalg.norm(np.array(a1p[0][:3]) - np.array(a2)))
        print(f"  TCP gap NOW: {gap:.0f}mm (designed {1000 * math.hypot(args.recv_dx, args.recv_dy, args.recv_dz):.0f}mm)")

    if not _gate("\n>>> both grippers on the SAME toy? ENTER to TRANSFER (arm1 close → arm2 open): "):
        return 0
    bridge.set_gripper(recv_arm, args.grip_close); time.sleep(1.4)
    got = (_drive(bridge, recv_arm) or 1.0) < args.held_thresh
    bridge.set_gripper(pick_arm, args.grip_open); time.sleep(1.0)
    print(f"  transfer: {recv_arm} drive={_drive(bridge, recv_arm)} → {'HOLDING' if got else 'did NOT catch'}")

    if not _gate("\n>>> ENTER: arm2 retreats + arm1 lifts the toy clear: "):
        return 0
    _move(stack, pick_arm, [a2[0], a2[1] - 0.10, a2[2] + 0.10], topdown_quat(-90), label="pick retreat")
    _move(stack, recv_arm, [recv_xyz[0], recv_xyz[1] + 0.12, recv_xyz[2] + 0.10], recv_q, label="recv lift")

    if args.box and got:
        bx = [float(v) for v in args.box.split(",")]
        if _gate(f"\n>>> ENTER: {recv_arm} carries to the box {bx} and releases: "):
            if _move(stack, recv_arm, bx, topdown_quat(0.0), label="to box"):
                bridge.set_gripper(recv_arm, args.grip_open); time.sleep(0.8)
                print("  dropped.")

    try:
        print("\nretracting both (best-effort)...")
        stack.retract(execute=True)
    except Exception as e:  # noqa: BLE001
        print(f"  retract skipped: {e}")
    print("\n" + "-" * 60)
    print(f"HANDOFF RESUME: transfer {'OK' if got else 'FAILED'}")
    print("-" * 60)
    return 0 if got else 1


if __name__ == "__main__":
    raise SystemExit(main())
