#!/usr/bin/env python3
"""⚠️  ONE-OFF DEMO — NOT PART OF THE ENGINE. NEVER IMPORT THIS. (operator, 2026-07-21)

This file is a hand-tuned table-toy perception DEMO for one specific rig snapshot.
Every number in it (table_z, table footprint bounds, size filters, SAM2 params, the
camera serial, colour names) was HUMAN-hardcoded for that one scene — the exact
anti-pattern the remoroo engine exists to eliminate. It must never be used by the
agent, the task engine, the sim, or any product path:

  - The agent grounds a real scene with view_image (its own eyes), not this.
  - Task perception programs are AUTHORED by the agent and EVOLVED by the sim
    until mature — never hand-tuned, and never assuming a "table" exists.

Kept only as a historical demo of the 2026-07-20 table-clearing session.

Detect the target toys on the table from the OVERHEAD camera — NO MOTION.

The scene is CLUTTERED (several toys, both robot arms, coiled hoses, floor). Pure "above-the-table"
geometry can't tell a toy from an elbow, so the default path is open-vocabulary text detection:

  GroundingDINO(caption) -> box  ->  SAM2(box) -> tight mask  ->  deproject masked depth -> robot XYZ

This reuses the exact chain task_engine/perceive already owns (detector + segmenter behind the
ModelRegistry, PerceptionOps.cloud for the pixels->robot seam). The only custom piece is the overhead
camera's extrinsic, which is a CONSTANT read from the URDF fixed-joint chain (so a dead wrist camera or
an absent bridge can't block it). Objects come back in the ROBOT/planning frame + an annotated image.

    python3 -m motion_engine.perceive_table                          # detect "grey toy . white toy"
    python3 -m motion_engine.perceive_table --prompt "grey toy . white toy . pink rabbit"
    python3 -m motion_engine.perceive_table --geometry               # model-free fallback (all clutter)
"""
from __future__ import annotations

import argparse
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np


def _rpy_to_R(rpy) -> np.ndarray:
    r, p, y = (float(v) for v in rpy)
    cr, sr, cp, sp, cy, sy = (np.cos(r), np.sin(r), np.cos(p), np.sin(p), np.cos(y), np.sin(y))
    Rx = np.array([[1, 0, 0], [0, cr, -sr], [0, sr, cr]])
    Ry = np.array([[cp, 0, sp], [0, 1, 0], [-sp, 0, cp]])
    Rz = np.array([[cy, -sy, 0], [sy, cy, 0], [0, 0, 1]])
    return Rz @ Ry @ Rx                                            # URDF rpy convention


def urdf_static_pose(urdf_path: str, target_link: str, root: str = "world"):
    """Compose the constant transform ROOT -> target_link over a chain of `fixed` joints (what a
    static/eye-to-hand camera mount always is). Returns (R 3x3, t 3) = robot_from_cam — no arm,
    no cuRobo, so a dead wrist camera or absent bridge cannot block it."""
    tree = ET.parse(urdf_path)
    child_joint = {}
    for j in tree.getroot().iter("joint"):
        c = j.find("child")
        if c is not None:
            child_joint[c.get("link")] = j
    chain, link = [], target_link
    while link != root:
        j = child_joint.get(link)
        if j is None:
            raise RuntimeError(f"no parent joint for {link!r} (chain to {root!r} broken)")
        if j.get("type") not in (None, "fixed"):
            raise RuntimeError(f"joint {j.get('name')!r} is {j.get('type')} — camera not static")
        chain.append(j)
        link = j.find("parent").get("link")
    R, t = np.eye(3), np.zeros(3)
    for j in reversed(chain):
        o = j.find("origin")
        xyz = np.array([float(v) for v in (o.get("xyz") or "0 0 0").split()])
        rpy = [float(v) for v in (o.get("rpy") or "0 0 0").split()]
        t = R @ xyz + t
        R = R @ _rpy_to_R(rpy)
    return R, t


def K_of(intr) -> np.ndarray:
    if isinstance(intr, dict):
        return np.array([[intr["fx"], 0, intr["cx"]], [0, intr["fy"], intr["cy"]], [0, 0, 1]], float)
    a = np.asarray(intr, float)
    return a.reshape(3, 3) if a.size == 9 else a


class _Calib:
    """The duck-typed calibration PerceptionOps expects: a fixed K and robot_from_cam 4x4."""
    def __init__(self, K, R, t):
        self._K = np.asarray(K, float)
        self._T = np.eye(4); self._T[:3, :3] = R; self._T[:3, 3] = t
    def intrinsics(self, camera): return self._K
    def robot_from_cam(self, camera): return self._T


class _Shim:
    """A bridge that serves the one overhead frame to PerceptionOps.capture."""
    def __init__(self, frame): self._f = dict(frame)
    def grab_camera(self, camera): return dict(self._f)


def table_mask(pts: np.ndarray, table_z: float, min_h: float, max_h: float,
               min_x: float = 0.0, max_x: float = 0.55, min_y: float = -1.25,
               max_y: float = 0.60) -> np.ndarray:
    """Keep points that sit above the modeled table and inside the REACHABLE forward zone. `min_x`
    (default 0.0) drops everything BEHIND the robot base — the wall/structure at negative x that
    surfaced as phantom pick targets the arm would swing back into."""
    return ((pts[:, 2] > table_z + min_h) & (pts[:, 2] < table_z + max_h)
            & (pts[:, 0] > min_x) & (pts[:, 0] < max_x)
            & (pts[:, 1] > min_y) & (pts[:, 1] < max_y))


def object_from_points(pts: np.ndarray) -> dict:
    c = np.median(pts[:, :2], axis=0)                             # robust XY centroid
    top = float(np.percentile(pts[:, 2], 90))                    # grasp height ~ object top
    xy = pts[:, :2] - c
    evals, evecs = np.linalg.eigh(xy.T @ xy)
    major = evecs[:, int(np.argmax(evals))]
    yaw = float(np.degrees(np.arctan2(major[1], major[0])))
    ext = pts.max(axis=0) - pts.min(axis=0)
    return {"xyz": [float(c[0]), float(c[1]), top], "yaw": yaw,
            "footprint_cm": [float(ext[0] * 100), float(ext[1] * 100)], "n": int(len(pts))}


_COLORS = {"white": (235, 235, 235), "grey": (140, 140, 140), "black": (30, 30, 30),
           "red": (200, 40, 40), "pink": (230, 150, 170), "blue": (50, 90, 200),
           "green": (60, 170, 80), "yellow": (225, 210, 70), "brown": (140, 100, 60)}


def color_name(rgb) -> str:
    r, g, b = (float(v) for v in rgb)
    mx, mn = max(r, g, b), min(r, g, b)
    if mx - mn < 28:                                             # low saturation → grey scale
        return "white" if mx > 190 else ("black" if mx < 70 else "grey")
    return min(_COLORS.items(), key=lambda kv: sum((a - c) ** 2 for a, c in zip(kv[1], (r, g, b))))[0]


def detect_all(rgb, depth, R, t, K, args) -> list:
    """CLASS-AGNOSTIC: SAM2 segments EVERY object, then we keep only table-top, toy-sized instances and
    name each by mean colour. Near-total recall on clutter — the foundation for 'clear the whole table'.
    Robot arms / hoses / the table itself are rejected by footprint + colour, not by a fragile prompt."""
    from sam2.build_sam import build_sam2
    from sam2.automatic_mask_generator import SAM2AutomaticMaskGenerator
    from task_engine.perceive.loaders import PINS, fetch_pinned
    pin = PINS["segmenter"]
    ckpt = fetch_pinned(pin["url"], pin["sha256"], args.weights)
    gen = SAM2AutomaticMaskGenerator(build_sam2(pin["name"], str(ckpt)),
                                     points_per_side=int(args.sam_points),
                                     pred_iou_thresh=0.80, stability_score_thresh=0.90,
                                     min_mask_region_area=400)
    rgb = np.asarray(rgb)[..., :3]
    masks = gen.generate(rgb)
    print(f"SAM2 auto-masks: {len(masks)} raw instances")
    fx, fy, cx, cy = K[0, 0], K[1, 1], K[0, 2], K[1, 2]
    objs = []
    for m in sorted(masks, key=lambda m: -m["area"]):
        seg = np.asarray(m["segmentation"], bool)
        vs, us = np.nonzero(seg)
        z = depth[vs, us]
        good = np.isfinite(z) & (z > 0.1) & (z < 3.0)
        if good.sum() < 60:
            continue
        us2, vs2, z = us[good], vs[good], z[good]
        pc = np.stack([(us2 - cx) * z / fx, (vs2 - cy) * z / fy, z], axis=1)
        P = pc @ R.T + t
        keep = table_mask(P, args.table_z, args.min_height, args.max_height, min_x=getattr(args,'min_x',0.0))
        if keep.sum() < args.min_pts:
            continue
        P = P[keep]
        o = object_from_points(P)
        fp = max(o["footprint_cm"])
        if fp < 2.0 or fp > args.max_footprint:                  # reject fragments + robot/table slabs
            continue
        if any(np.hypot(o["xyz"][0] - p["xyz"][0], o["xyz"][1] - p["xyz"][1]) < args.dedup for p in objs):
            continue
        col = rgb[vs2[keep], us2[keep]].mean(axis=0)
        o.update({"label": color_name(col), "score": float(m.get("predicted_iou", 0.0)),
                  "rgb": [int(v) for v in col],
                  "box": [float(us2[keep].min()), float(vs2[keep].min()),
                          float(us2[keep].max()), float(vs2[keep].max())]})
        objs.append(o)
    objs.sort(key=lambda o: (o["xyz"][0], o["xyz"][1]))
    for i, o in enumerate(objs):
        print(f"  obj {i}: {o['label']:6s} XY=({o['xyz'][0]:+.3f}, {o['xyz'][1]:+.3f})  "
              f"top z={o['xyz'][2]:+.3f}  {o['footprint_cm'][0]:.1f}x{o['footprint_cm'][1]:.1f}cm  "
              f"yaw {o['yaw']:+.0f}°  rgb{tuple(o['rgb'])}  ({o['n']} pts)")
    return objs


def detect_text(ops, frame, R, t, K, args) -> list:
    """Open-vocab detect -> segment -> deproject -> table-filter, one object per surviving detection."""
    dets = ops.detect(args.prompt, frame)
    dets = [d for d in dets if d["score"] >= args.score]
    dets.sort(key=lambda d: -d["score"])
    print(f"GroundingDINO('{args.prompt}') → {len(dets)} box(es) over score {args.score}")
    objs = []
    for d in dets:
        try:
            mask = ops.segment({"box": d["box"]}, frame)[0]
        except Exception as e:  # noqa: BLE001
            print(f"  {d['label']!r} score {d['score']:.2f}: segment failed ({e})"); continue
        pts = ops.cloud(np.asarray(mask, bool), frame)           # robot-frame Nx3
        keep = table_mask(pts, args.table_z, args.min_height, args.max_height, min_x=getattr(args,'min_x',0.0))
        pts = pts[keep]
        if len(pts) < args.min_pts:
            print(f"  {d['label']!r} score {d['score']:.2f}: only {len(pts)} on-table pts — skipped")
            continue
        # reject a box that overlaps one we already accepted (GroundingDINO doubles up)
        o = object_from_points(pts)
        if any(np.hypot(o["xyz"][0] - p["xyz"][0], o["xyz"][1] - p["xyz"][1]) < args.dedup for p in objs):
            continue
        o.update({"label": d["label"], "score": d["score"], "box": d["box"]})
        objs.append(o)
        print(f"  ✓ {d['label']!r:18s} score {d['score']:.2f} → XY=({o['xyz'][0]:+.3f}, "
              f"{o['xyz'][1]:+.3f})  top z={o['xyz'][2]:+.3f}  "
              f"{o['footprint_cm'][0]:.1f}x{o['footprint_cm'][1]:.1f}cm  yaw {o['yaw']:+.0f}°  ({o['n']} pts)")
    return objs


def annotate(frame, depth, objs, R, t, K, out) -> None:
    try:
        from PIL import Image, ImageDraw
        rgb = frame.get("rgb")
        img = (Image.fromarray(np.asarray(rgb)[..., :3].astype(np.uint8)) if rgb is not None
               else Image.fromarray((255 * depth / (np.nanmax(depth) or 1)).astype(np.uint8)).convert("RGB"))
        d = ImageDraw.Draw(img)
        fx, fy, cx, cy = K[0, 0], K[1, 1], K[0, 2], K[1, 2]
        for i, o in enumerate(objs):
            if o.get("box"):
                d.rectangle([float(v) for v in o["box"]], outline=(80, 220, 80), width=4)
            pc = (np.asarray(o["xyz"]) - t) @ R                  # base -> cam (R orthonormal)
            if pc[2] > 0:
                u, v = fx * pc[0] / pc[2] + cx, fy * pc[1] / pc[2] + cy
                d.ellipse([u - 10, v - 10, u + 10, v + 10], outline=(255, 60, 60), width=4)
                tag = f"{o.get('label', 'obj%d' % i)} {o.get('score', 0):.2f}"
                d.text((u + 12, v - 10), tag, fill=(255, 60, 60))
        img.save(out)
        print(f"\nannotated overhead image → {out}")
    except Exception as e:  # noqa: BLE001
        print(f"(annotation skipped: {type(e).__name__}: {e})")


def main() -> int:
    ap = argparse.ArgumentParser(description="detect toys on the table (overhead cam, NO MOTION)")
    ap.add_argument("--cell", default=os.environ.get("REMOROO_CELL", "remoroo_cell"))
    ap.add_argument("--prompt", default="grey toy . white toy",
                    help="GroundingDINO caption; period-separate multiple objects")
    ap.add_argument("--score", type=float, default=0.35, help="min detection confidence")
    ap.add_argument("--weights", default=".remoroo/task/weights")
    ap.add_argument("--geometry", action="store_true", help="model-free above-table clustering instead")
    ap.add_argument("--text", action="store_true", help="open-vocab GroundingDINO instead of SAM2-all")
    ap.add_argument("--sam-points", type=int, default=48, help="SAM2 auto-mask points-per-side (density)")
    ap.add_argument("--max-footprint", type=float, default=28.0, help="reject blobs larger than this (cm)")
    ap.add_argument("--optical-frame", default="", help="baked optical frame (auto if empty)")
    ap.add_argument("--table-z", type=float, default=-0.045, help="table TOP surface, world z (m)")
    ap.add_argument("--min-height", type=float, default=0.010, help="min height above table (m)")
    ap.add_argument("--min-x", type=float, default=0.15, help="drop objects behind the robot (world x floor, m)")
    ap.add_argument("--max-height", type=float, default=0.30, help="ignore points taller than this (m)")
    ap.add_argument("--min-pts", type=int, default=150, help="min on-table points to accept a detection")
    ap.add_argument("--dedup", type=float, default=0.06, help="merge detections closer than this (m)")
    ap.add_argument("--out", default="", help="annotated image path (default: scratchpad/perceive_table.png)")
    args = ap.parse_args()
    cell = str(Path(args.cell).resolve())

    # OVERHEAD-ONLY: open just the static overhead ZED and read its constant extrinsic from the URDF.
    import yaml
    from motion_engine.demo_curobo_showcase import _ensure_safety_shim
    _ensure_safety_shim(cell)
    from remoroo_cell.primitives import ZedCamera  # type: ignore

    cellcfg = yaml.safe_load((Path(cell) / "cell.yaml").read_text())
    ov = next((c for c in cellcfg.get("cameras", [])
               if c.get("name") == "overhead" or str(c.get("serial")) == "54687609"
               or c.get("mount") == "eye_to_hand"), None)
    if ov is None:
        print("no eye_to_hand / overhead camera in cell.yaml"); return 2
    link = args.optical_frame.replace("_optical_frame", "") or ov.get("link", "ZEDX_Mini_3")
    ofr = args.optical_frame or f"{link}_optical_frame"
    print(f"overhead camera: name={ov.get('name')!r} link={link!r} serial={ov.get('serial')}")

    zed = ZedCamera(ov); zed.start()
    frame = zed.grab()
    depth = np.asarray(frame["depth_m"], dtype=float)
    K = K_of(frame["intrinsics"])
    print(f"depth {depth.shape}  fx={K[0,0]:.1f} fy={K[1,1]:.1f} cx={K[0,2]:.1f} cy={K[1,2]:.1f}")

    R, t = urdf_static_pose(str(Path(cell) / "robot_model" / "robot.urdf"), ofr)
    print(f"robot_from_cam via URDF chain to {ofr!r}: camera at world {[round(float(v),3) for v in t]}\n")

    objs = []
    if args.geometry:
        objs = detect_geometry(frame, depth, R, t, K, args)
    elif args.text:
        from task_engine.perceive.serving import ModelRegistry
        from task_engine.perceive.loaders import register_default_models
        from task_engine.perceive.operators import PerceptionOps
        registry = ModelRegistry()
        register_default_models(registry, args.weights)
        ops = PerceptionOps(registry, _Calib(K, R, t), _Shim(frame))
        print("loading GroundingDINO + SAM2 (one-time)...")
        objs = detect_text(ops, ops.capture("overhead"), R, t, K, args)
    else:
        print("loading SAM2 automatic mask generator (one-time)...")   # DEFAULT: find every object
        objs = detect_all(frame["rgb"], depth, R, t, K, args)

    print(f"\n{len(objs)} object(s) accepted on the table.")
    out = args.out or str(Path(os.environ.get("SCRATCH", "/tmp")) / "perceive_table.png")
    annotate(frame, depth, objs, R, t, K, out)
    try:
        zed.stop()
    except Exception:  # noqa: BLE001
        pass
    return 0


# --- model-free fallback (kept for an empty table / no-GPU) ----------------------------------------
def detect_geometry(frame, depth, R, t, K, args) -> list:
    H, W = depth.shape
    vs, us = np.nonzero(np.isfinite(depth) & (depth > 0.1) & (depth < 3.0))
    z = depth[vs, us]
    fx, fy, cx, cy = K[0, 0], K[1, 1], K[0, 2], K[1, 2]
    p_cam = np.stack([(us - cx) * z / fx, (vs - cy) * z / fy, z], axis=1)
    P = p_cam @ R.T + t
    P = P[table_mask(P, args.table_z, args.min_height, args.max_height, min_x=getattr(args,'min_x',0.0))]
    if len(P) < 10:
        print("no object points above the table"); return []
    g = 0.01
    ix = np.floor((P[:, 0] - P[:, 0].min()) / g).astype(int)
    iy = np.floor((P[:, 1] - P[:, 1].min()) / g).astype(int)
    from task_engine.perceive.operators import _connected
    occ = np.zeros((iy.max() + 2, ix.max() + 2), dtype=bool); occ[iy, ix] = True
    objs = []
    for comp in sorted(_connected(occ), key=lambda m: -int(m.sum())):
        if comp.sum() < 4:
            continue
        sel = comp[iy, ix]
        objs.append(object_from_points(P[sel]))
    return objs


if __name__ == "__main__":
    raise SystemExit(main())
