#!/usr/bin/env python3
"""Validate the mid-air HANDOFF: arm2 picks a toy → hands it to arm1 → arm1 drops it (in the box).

The last unproven mechanism of the table-clearing demo. Every phase is operator-gated (ENTER, Ctrl-C
E-stops) and the meeting geometry is fully tunable so we can dial it in live:

  PICK(arm2)  →  PRESENT(arm2 lifts the toy to the meet point, gripper down, toy hanging below)
              →  RECEIVE(arm1 comes to meet+offset, gripper open)     [planned with arm2 HELD still]
              →  TRANSFER(arm1 closes, then arm2 opens — timed)
              →  arm2 retreats, arm1 carries to the box and releases

arm1 receives at arm2's ACHIEVED grasp point (live FK, not the nominal meet), XY-coincident but
~2.5cm BELOW in z: the pinch point is physically arm2's fingertips (at -0.005 arm1 gripped arm2's
finger together with the toy — live 2026-07-20), so arm1 closes on the hanging toy body just under
them. The planner tracks the goal mm-perfect (sweep-proven), so the offset is exactly what you get.
Tune --recv-dz live while watching; the script prints the achieved TCP gap (expect ≈25mm).

    $PY -m motion_engine.handoff_test --execute --meet 0.45,-0.15,0.45 --box 0.30,0.30,0.20
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
    from motion_engine.pick_test import perceive, topdown_quat, tilted_quat, _gate, OVERHEAD
except Exception:  # noqa: BLE001
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from motion_engine import MotionStack
    from motion_engine.demo_curobo_showcase import connect_bridge, _SeedTracker
    from motion_engine.pick_test import perceive, topdown_quat, tilted_quat, _gate, OVERHEAD


def _drive(bridge, arm):
    d = bridge._drivers.get(arm)
    return d.read_gripper_drive() if d is not None else None


def _roll_about_tool(q, yaw_deg):
    """Rotate a tool quat about ITS OWN approach axis (+Z): spins the jaw/camera/CABLE side while
    still facing the same way. q ∘ Rz(yaw) — right-multiply = tool-frame rotation."""
    h = math.radians(yaw_deg) / 2.0
    w2, z2 = math.cos(h), math.sin(h)
    w1, x1, y1, z1 = [float(v) for v in q]
    return [w1 * w2 - z1 * z2,
            x1 * w2 + y1 * z2,
            y1 * w2 - x1 * z2,
            w1 * z2 + z1 * w2]


def _move(stack, arm, xyz, quat, *, attempts=3, label=""):
    """PLAN + EXECUTE one pose. cuRobo's IK/collision is a HARD constraint — it never returns a
    colliding trajectory — so there is nothing to re-validate; the un-goaled limb holds via
    minimal-motion. Same call shape the showcase uses."""
    r = stack.move_to_poses({arm: (list(xyz), list(quat))}, execute=True, max_attempts=attempts)
    ok = bool(getattr(r, "ok", False) and getattr(r, "executed", False))
    if not ok:
        print(f"  ✗ {label or arm}: {(getattr(r, 'message', '') or '')[:90]}")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description="mid-air handoff validation (gated)")
    ap.add_argument("--cell", default=os.environ.get("REMOROO_CELL", "remoroo_cell"))
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--index", type=int, default=-1, help="which toy (default: best arm2-reachable)")
    ap.add_argument("--pick-arm", default="arm2")
    ap.add_argument("--recv-arm", default="arm1")
    # meeting geometry
    ap.add_argument("--meet", default="0.44,-0.34,0.40", help="handoff point x,y,z (arm2's TCP goes here)")
    ap.add_argument("--present-yaw", type=float, default=-30.0, help="arm2 wrist yaw at present (deg). Keeps the WRIST CAMERA (offset along tool +x) OUT of arm1's +y approach corridor; -30 = camera toward -y/+x AND trajopt-feasible under the 2026-07-22 full camera/holder collision model (-90/-60/-120 became infeasible once the camera volume was truly modeled)")
    ap.add_argument("--recv-dx", type=float, default=0.0, help="arm1 TCP offset from arm2's grasp, x (m)")
    ap.add_argument("--recv-dy", type=float, default=0.0, help="arm1 TCP offset from arm2's grasp, y (m, toward arm1)")
    ap.add_argument("--recv-dz", type=float, default=-0.025, help="arm1 TCP offset from arm2's grasp, z (m). XY must COINCIDE (aim at the toy's axis) but Z must clear arm2's FINGERTIPS: the pinch point IS where arm2's finger tips physically are (at -0.005 arm1 grabbed arm2's finger with the toy, live 2026-07-20), so close ~2.5cm below on toy body only")
    ap.add_argument("--recv-yaw", type=float, default=0.0, help="arm1 wrist ROLL about the tool axis at receive (deg). Rotates which side of the gripper (jaws/camera/CABLE TUBE) faces the toy — use ±90/180 to keep arm1's wrist-cam cable clear of the pinch (err31 root cause was the cable tube, live 2026-07-20; joint6 was at -139°)")
    ap.add_argument("--recv-tilt", type=float, default=90.0, help="arm1 gripper tilt from straight-down (deg); 90=horizontal toward arm2, any 45-135 opposes")
    ap.add_argument("--approach-back", type=float, default=0.06, help="receive is TWO-STAGE: stage this far back (+y, arm1's side) then a short slow final move — first toy contact at crawl speed, not full flight (arm1 tripped collision-protection err31 ramming the anchored plush, live 2026-07-20)")
    ap.add_argument("--sweep", action="store_true", help="find meet points reachable by both arms, then exit")
    ap.add_argument("--box", default="", help="arm1 drop point x,y,z (empty = skip the drop)")
    # pick geometry (mirror pick_test)
    ap.add_argument("--hover", type=float, default=0.12)
    ap.add_argument("--grasp-dz", type=float, default=-0.025)
    ap.add_argument("--lift", type=float, default=0.20)
    ap.add_argument("--yaw-offset", type=float, default=90.0)
    ap.add_argument("--grip-open", type=float, default=0.0)
    ap.add_argument("--grip-close", type=float, default=1.0)
    ap.add_argument("--held-thresh", type=float, default=0.825)
    # detection (forwarded to detect_all via perceive)
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
    pick_arm, recv_arm = args.pick_arm, args.recv_arm
    meet = [float(v) for v in args.meet.split(",")]

    bridge = connect_bridge(cell)
    try:
        _SeedTracker(bridge)
    except Exception:  # noqa: BLE001
        pass
    signal.signal(signal.SIGINT, lambda *_: (getattr(bridge, "estop", lambda: None)(), sys.exit(130)))

    print("building + warming the motion stack...")
    stack = MotionStack.from_cell(cell, bridge=bridge)
    stack.prewarm()

    objs, frame, depth, R, t, K = perceive(bridge, cell, args, stack=stack)
    if not objs:
        print("no toys detected."); return 1

    def hover_pose(o):
        return ([o["xyz"][0], o["xyz"][1], o["xyz"][2] + args.hover], topdown_quat(o["yaw"] + args.yaw_offset))

    # pick target: --index, else the best pick-arm-reachable toy
    def reachable(o, arm):
        try:
            r = stack.move_to_poses({arm: hover_pose(o)}, execute=False, max_attempts=1)
            return bool(getattr(r, "ok", False) and getattr(r, "trajectory", None) is not None)
        except Exception:  # noqa: BLE001
            return False
    def graspable(o):
        # a jaw-spannable dimension (< ~8cm) + a solid detection (not a sliver/fragment)
        return 2.0 <= min(o["footprint_cm"]) <= 8.5 and o["n"] >= 800
    if args.index >= 0:
        idx = args.index
    else:
        cand = [i for i, o in enumerate(objs) if reachable(o, pick_arm)]
        good = [i for i in cand if graspable(objs[i])]
        pool = good or cand
        idx = max(pool, key=lambda i: objs[i]["n"]) if pool else None   # most-solid graspable toy
    if idx is None:
        print(f"no {pick_arm}-reachable toy."); return 1
    o = objs[idx]
    print(f"\nTARGET: obj {idx} ({o['label']}) XY=({o['xyz'][0]:+.3f},{o['xyz'][1]:+.3f}) z={o['xyz'][2]:+.3f}")
    print(f"handoff meet={meet}  arm1 receive offset=({args.recv_dx:+.2f},{args.recv_dy:+.2f},{args.recv_dz:+.2f})")

    def can(arm, xyz, q):
        try:
            r = stack.move_to_poses({arm: (list(xyz), list(q))}, execute=False, max_attempts=1)
            return bool(getattr(r, "ok", False) and getattr(r, "trajectory", None) is not None)
        except Exception:  # noqa: BLE001
            return False

    if args.sweep:
        # find meet points reachable by BOTH arms (arm2 present top-down, arm1 receive top-down+offset)
        print("\n── meet-point sweep (present arm2 + receive arm1, both top-down) ──")
        good = []
        for x in (0.38, 0.44, 0.50, 0.56):
            for y in (-0.40, -0.34, -0.28, -0.22):
                for z in (0.34, 0.40, 0.46):
                    m = [x, y, z]
                    if not can(pick_arm, m, topdown_quat(args.present_yaw)):
                        continue
                    rc = [m[0] + args.recv_dx, m[1] + args.recv_dy, m[2] + args.recv_dz]
                    tilts = [tl for tl in (60, 75, 90, 105, 120, 135) if can(recv_arm, rc, tilted_quat(tl))]
                    if tilts:
                        good.append((m, tilts))
                        print(f"  meet={m}  present({pick_arm})=OK  receive({recv_arm}) tilts OK={tilts}")
        print(f"\n{len(good)} meet point(s) reachable by BOTH.")
        for m, tilts in sorted(good, key=lambda g: abs(g[0][1] + 0.30))[:6]:
            print(f"    meet={m}  works at tilts {tilts}  (90=horizontal)")
        try: bridge.disconnect()
        except Exception: pass  # noqa: E722
        return 0

    present_q = topdown_quat(args.present_yaw)
    recv_xyz = [meet[0] + args.recv_dx, meet[1] + args.recv_dy, meet[2] + args.recv_dz]
    recv_q = _roll_about_tool(tilted_quat(args.recv_tilt), args.recv_yaw)   # tilt to OPPOSE arm2, roll to keep the cable side clear
    print(f"present({pick_arm}) TCP={meet}   receive({recv_arm}) TCP={[round(v,3) for v in recv_xyz]}")

    # plan-only reachability of the meeting poses (arm2 at meet, arm1 at receive)
    for arm, xyz, q, nm in ((pick_arm, meet, present_q, "present"), (recv_arm, recv_xyz, recv_q, "receive")):
        try:
            r = stack.move_to_poses({arm: (xyz, q)}, execute=False, max_attempts=1)
            ok = bool(getattr(r, "ok", False) and getattr(r, "trajectory", None) is not None)
        except Exception:  # noqa: BLE001
            ok = False
        print(f"  reachability {nm} ({arm}): {'OK' if ok else 'NOT reachable — adjust --meet/--recv-*'}")

    if not args.execute:
        print("\nplan-only done. Re-run with --execute to run the handoff.")
        try: bridge.disconnect()
        except Exception: pass  # noqa: E722
        return 0

    # ---- PICK (pick_arm) -------------------------------------------------------------------------
    if not _gate("\n>>> ENTER to RETRACT both arms + open grippers (Ctrl-C aborts): "):
        return 0
    stack.retract(execute=True)
    bridge.set_gripper(pick_arm, args.grip_open)
    bridge.set_gripper(recv_arm, args.grip_open)

    hp = hover_pose(o)
    if not _gate(f"\n>>> ENTER: {pick_arm} to HIGH hover — CONFIRM gripper DOWN (Ctrl-C aborts): "):
        return 0
    if not _move(stack, pick_arm, [hp[0][0], hp[0][1], o["xyz"][2] + max(0.22, args.hover)], hp[1], label="high hover"):
        return 1
    if not _gate("\n>>> gripper down? ENTER to DESCEND + GRASP + LIFT: "):
        return 0
    if not _move(stack, pick_arm, [hp[0][0], hp[0][1], o["xyz"][2] + args.grasp_dz], hp[1], label="descend"):
        return 1
    bridge.set_gripper(pick_arm, args.grip_close); time.sleep(1.2)
    held = (_drive(bridge, pick_arm) or 1.0) < args.held_thresh
    _move(stack, pick_arm, [hp[0][0], hp[0][1], o["xyz"][2] + args.lift], hp[1], label="lift")
    print(f"  pick: drive={_drive(bridge, pick_arm)} → {'HELD' if held else 'EMPTY'}")
    if not held and not _gate(">>> pick looks EMPTY. ENTER to continue anyway, Ctrl-C to abort: "):
        return 1

    # ---- PRESENT (pick_arm to the meet point) ----------------------------------------------------
    # FACE THE MEET first (2026-07-21): straight-from-rest IK put joint1_1 at world +215° for a
    # meet at azimuth +35° (Δ=179° — the arm arched BACKWARD over itself; operator: "strangely
    # behind the robot"). Rotating joint1 toward the meet azimuth first anchors minimal-motion IK
    # in the FACING branch. Collision-checked (goto_joints = plan_cspace), holds all other joints.
    if not _gate(f"\n>>> ENTER: {pick_arm} PRESENTS the toy at the meet point {meet}: "):
        return 0
    try:
        from motion_engine.perceive_table import urdf_static_pose as _usp
        sfx = "_1" if pick_arm.endswith("2") else ""
        _Rb, _tb = _usp(str(Path(cell) / "robot_model" / "robot.urdf"), f"link_base{sfx}")
        base_yaw = math.atan2(_Rb[1, 0], _Rb[0, 0])
        az = math.atan2(meet[1] - _tb[1], meet[0] - _tb[0])
        cur = stack._seed_positions().get(f"joint1{sfx}", 0.0)
        tgt = az - base_yaw
        tgt += round((cur - tgt) / (2 * math.pi)) * 2 * math.pi   # nearest 2π-equivalent to current
        if abs(tgt - cur) > math.radians(25):
            print(f"  facing the meet: joint1{sfx} {math.degrees(cur):+.0f}° → {math.degrees(tgt):+.0f}°")
            rf = stack.goto_joints({f"joint1{sfx}": float(tgt)})
            if not getattr(rf, "ok", False):
                print(f"  (face-turn didn't plan — presenting from here: {(getattr(rf, 'message', '') or '')[:60]})")
    except Exception as e:  # noqa: BLE001
        print(f"  (face-the-meet skipped: {type(e).__name__})")
    pres_ok = False
    pres_end = None
    # fallback yaws stay NEAR the camera-safe orientation (camera drifts back toward arm1's
    # corridor as |dyaw| grows — never fall back all the way to camera-in-corridor +90)
    for dyaw in (0.0, -30.0, 30.0, -60.0):
        q = topdown_quat(args.present_yaw + dyaw)
        r = stack.move_to_poses({pick_arm: (list(meet), list(q))}, execute=False, max_attempts=3)
        traj = getattr(r, "trajectory", None)
        if getattr(r, "ok", False) and traj is not None:
            if dyaw:
                print(f"  present: yaw fallback {args.present_yaw + dyaw:+.0f}° (base yaw was trajopt-marginal)")
            res = stack.play_trajectory(traj)
            pres_ok = bool(getattr(res, "ok", False) and getattr(res, "executed", False))
            pres_end = {n: float(traj.positions[-1][i]) for i, n in enumerate(traj.joint_names)}
            break
        print(f"  present yaw={args.present_yaw + dyaw:+.0f}°: {(getattr(r, 'message', '') or '')[:60]}")
    if not pres_ok:
        print("  ✗ present: no yaw variant planned — tune --meet")
        return 1

    # ---- RECEIVE (recv_arm to arm2's ACHIEVED grasp + offset, planned with pick_arm HELD) --------
    # goal from the PRESENT TRAJECTORY's end config, FK'd through the robot planner — NEVER
    # `current_tool_pose`: after a mid-run camera death the tracked seed goes STALE and live FK
    # reported the PICK pose (z=-0.03) as "achieved", aiming the receive 6cm UNDER the table
    # (2026-07-21 golden-state freeze). The planned end config is in-process and camera-immune.
    base = list(meet)
    try:
        _cvp = stack._planner_for(list(stack._groups.keys()))
        _js = _cvp._start_js(pres_end)
        _state = _cvp.planner.compute_kinematics(_js)
        _frames = list(getattr(_state, "tool_frames", None) or _cvp.planner.tool_frames)
        _pos = _state.tool_poses.position.detach().cpu().numpy().reshape(-1, 3)
        base = [float(v) for v in _pos[_frames.index(stack._tip(pick_arm))]]
    except Exception as e:  # noqa: BLE001
        print(f"  (trajectory-end FK unavailable ({type(e).__name__}) — using the nominal meet)")
    recv_xyz = [base[0] + args.recv_dx, base[1] + args.recv_dy, base[2] + args.recv_dz]
    print(f"  receive goal ← {pick_arm} ACHIEVED TCP {[round(v, 3) for v in base]} + offsets = "
          f"{[round(v, 3) for v in recv_xyz]}")
    if not _gate(f"\n>>> ENTER to bring {recv_arm} IN to the receive pose (open gripper): "):
        return 0
    stage_xyz = [recv_xyz[0], recv_xyz[1] + abs(args.approach_back), recv_xyz[2]]
    if not _move(stack, recv_arm, stage_xyz, recv_q, label="receive stage"):
        print("  (staging point unreachable — tune --recv-tilt / --recv-dz / --meet, then re-run)")
        return 1
    if not _move(stack, recv_arm, recv_xyz, recv_q, label="receive final (slow contact)"):
        print("  (final approach failed — tune --recv-dz / --approach-back, then re-run)")
        return 1
    a1, a2 = stack.current_tool_pose(recv_arm), stack.current_tool_pose(pick_arm)
    if a1 and a2:
        gap = 1000 * float(np.linalg.norm(np.array(a1[0][:3]) - np.array(a2[0][:3])))
        want = 1000 * math.hypot(args.recv_dx, args.recv_dy, args.recv_dz)
        print(f"  TCP gap NOW: {gap:.0f}mm (designed {want:.0f}mm)")

    # ---- TRANSFER (timed: recv closes, then pick opens) — arm1 is NOW at the toy ----------------
    if not _gate("\n>>> are the two grippers on the SAME toy now? ENTER to TRANSFER (arm1 close → arm2 open): "):
        return 0
    bridge.set_gripper(recv_arm, args.grip_close); time.sleep(1.4)     # arm1 grabs
    got = (_drive(bridge, recv_arm) or 1.0) < args.held_thresh
    bridge.set_gripper(pick_arm, args.grip_open); time.sleep(1.0)      # arm2 lets go
    print(f"  transfer: {recv_arm} drive={_drive(bridge, recv_arm)} → {'HOLDING' if got else 'did NOT catch'}")

    # ---- arm2 retreats, arm1 lifts the toy away --------------------------------------------------
    if not _gate("\n>>> ENTER: arm2 retreats + arm1 lifts the toy clear: "):
        return 0
    _move(stack, pick_arm, [meet[0], meet[1], meet[2] + 0.12], present_q, label="pick retreat")
    _move(stack, recv_arm, [recv_xyz[0], recv_xyz[1], recv_xyz[2] + 0.14], recv_q, label="recv lift")

    # ---- DROP in the box (recv_arm) -------------------------------------------------------------
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
    try: bridge.disconnect()
    except Exception: pass  # noqa: E722
    print("\n" + "-" * 60)
    print(f"HANDOFF: pick {'HELD' if held else 'empty'} → transfer {'OK' if got else 'FAILED'}")
    print("-" * 60)
    return 0 if got else 1


if __name__ == "__main__":
    raise SystemExit(main())
