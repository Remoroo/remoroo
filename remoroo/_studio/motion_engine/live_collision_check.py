#!/usr/bin/env python3
"""LIVE collision monitor — move the arm BY HAND, watch collisions fire in real time.

Reads both controllers' joints directly (no camera), FKs the robot spheres, and checks them against
the SAME modeled world + self-collision model the planner uses (`world_sphere_collision`). Prints a
live line each cycle: which modeled obstacle the spheres are inside, how many, and the authoritative
collision-free verdict. This is the ground-truth answer to "is the box actually being collision-checked
at the pose I put the arm in?" — no planning, no confounds.

    $PY -m motion_engine.live_collision_check              # read-only, you move via Studio free-drive
    $PY -m motion_engine.live_collision_check --manual     # ALSO puts the arms in drag/teach mode

Ctrl-C to stop (restores position mode).
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

try:
    from motion_engine import MotionStack
except Exception:  # noqa: BLE001
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from motion_engine import MotionStack

ARM_IPS = {"arm1": "192.168.10.214", "arm2": "192.168.10.228"}


def name_culprits(cvp, scene: dict, cfg: dict) -> list:
    """WHICH modeled obstacle(s) the robot is inside — per cuRobo's OWN checker, no home-rolled
    geometry (an AABB test here ignored obstacle ROTATION: the 90°-yawed wall read as a phantom slab
    through the workspace, '76 spheres in obs_5_wall' while genuinely CLEAR). Method: reload the
    checker's world with ONE obstacle at a time (`update_world`, public API) and count spheres cuRobo
    says penetrate it. Called only on a collision verdict; restores the full scene."""
    from curobo.scene import Scene
    checker = cvp._collision_checker()
    q = cvp._q_bhd(cfg)
    hits = []
    try:
        for name, spec in (scene.get("cuboid") or {}).items():
            checker.update_world(Scene.create({"cuboid": {name: spec}}))
            state = checker.get_kinematics(q)
            d = checker.get_collision_distance(state).detach().cpu().numpy().reshape(-1)
            cnt = int((d > 0).sum())
            if cnt:
                hits.append((name, cnt))
    finally:
        checker.update_world(Scene.create(scene))      # ALWAYS back to the full world
    return sorted(hits, key=lambda kv: -kv[1])


def main() -> int:
    ap = argparse.ArgumentParser(description="live collision monitor (move the arm by hand)")
    ap.add_argument("--cell", default=os.environ.get("REMOROO_CELL", "remoroo_cell"))
    ap.add_argument("--rate", type=float, default=5.0, help="check rate Hz")
    ap.add_argument("--manual", action="store_true", help="put arms in drag/teach mode (hand-movable)")
    args = ap.parse_args()
    cell = str(Path(args.cell).resolve())

    from xarm.wrapper import XArmAPI
    apis = {}
    for name, ip in ARM_IPS.items():
        a = XArmAPI(ip)
        time.sleep(0.3)
        if a.warn_code:
            a.clean_warn()
        if a.error_code:
            a.clean_error()
        apis[name] = a
    if args.manual:
        print("⚠  MANUAL/DRAG mode ON — the arms are hand-movable (gravity-comp). Support them.")
        for a in apis.values():
            try:
                a.motion_enable(True); a.set_mode(2); a.set_state(0)
            except Exception as e:  # noqa: BLE001
                print("  drag-mode enable failed:", e)

    print("building the collision model (planner spheres + modeled world)...")
    stack = MotionStack.from_cell(cell, bridge=None)
    stack.prewarm()
    cvp = stack._planner_for(list(stack._groups.keys()))
    names = list(cvp.planner.joint_names)
    scene = stack.world.scene or {}
    print(f"modeled obstacles: {list((scene.get('cuboid') or {}).keys())}")
    print(f"monitoring at {args.rate:.0f} Hz — move the arm; Ctrl-C to stop.\n")

    period = 1.0 / max(0.5, args.rate)
    try:
        while True:
            cfg = {}
            for name, a in apis.items():
                code, ang = a.get_servo_angle(is_radian=True)
                if code == 0 and ang:
                    suf = "" if name == "arm1" else "_1"
                    for i in range(6):
                        cfg[f"joint{i + 1}{suf}"] = float(ang[i])
            try:
                rep = cvp.world_sphere_collision(cfg)
                free = rep.get("collision_free")
            except Exception as e:  # noqa: BLE001
                print("  check error:", str(e)[:80]); time.sleep(period); continue
            if free:
                print(f"\r[{time.strftime('%H:%M:%S')}] CLEAR" + " " * 60, end="", flush=True)
            else:
                # name the culprit(s) with cuRobo's own checker (one-obstacle-at-a-time worlds)
                try:
                    hits = name_culprits(cvp, scene, cfg)
                except Exception as e:  # noqa: BLE001
                    hits = []
                    print("  naming error:", str(e)[:80])
                kind = ("WORLD: " + ", ".join(f"{n}({c})" for n, c in hits)) if hits \
                    else "SELF/BOUNDS (no modeled obstacle penetrated)"
                print(f"\r[{time.strftime('%H:%M:%S')}] *** COLLISION ***  {kind}" + " " * 20,
                      end="", flush=True)
            time.sleep(period)
    except KeyboardInterrupt:
        print("\nstopping.")
    finally:
        for a in apis.values():
            try:
                if args.manual:
                    a.set_mode(0); a.set_state(0)
                a.disconnect()
            except Exception:  # noqa: BLE001
                pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
