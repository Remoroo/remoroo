#!/usr/bin/env python3
"""Overhead-camera TRUTH TEST — NO MOTION. Answers "is perception lying?" in one run.

The 2026-07-21 failure mode: the camera moved + was recalibrated, and picks went to empty space.
This verifies the WHOLE pixels→world seam (the exact chain the demo uses: URDF optical frame +
intrinsics + depth) against two ground truths the world itself provides:

  1. TABLE PLANE: unproject every depth pixel → world. The dominant plane under the workspace MUST
     land at the modeled table top (z ≈ --table-z, default -0.045) and be FLAT (normal ≈ +z).
     A shifted/tilted plane = wrong extrinsic (bad solve or unbaked move) — quantified in mm/deg.
  2. TOY SANITY: detect objects (same SAM2 path as the demo); each must sit ON the table
     (top_z ∈ [table, table+0.25]) and inside the modeled table's footprint.

    $PY -m motion_engine.verify_overhead            # PASS/FAIL + numbers
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np

try:
    from motion_engine.perceive_table import urdf_static_pose, K_of, detect_all
except Exception:  # noqa: BLE001
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from motion_engine.perceive_table import urdf_static_pose, K_of, detect_all

OVERHEAD_SERIAL = "54687609"


def world_points(depth: np.ndarray, K: np.ndarray, R: np.ndarray, t: np.ndarray, step: int = 4):
    h, w = depth.shape
    v, u = np.mgrid[0:h:step, 0:w:step]
    z = depth[::step, ::step]
    ok = np.isfinite(z) & (z > 0.1) & (z < 4.0)
    u, v, z = u[ok].astype(float), v[ok].astype(float), z[ok]
    x = (u - K[0, 2]) * z / K[0, 0]
    y = (v - K[1, 2]) * z / K[1, 1]
    cam = np.stack([x, y, z], axis=1)
    return cam @ R.T + t


def main() -> int:
    ap = argparse.ArgumentParser(description="overhead camera truth test (NO MOTION)")
    ap.add_argument("--cell", default=os.environ.get("REMOROO_CELL", "remoroo_cell"))
    ap.add_argument("--table-z", type=float, default=-0.045, help="modeled table-top height (m)")
    ap.add_argument("--tol-mm", type=float, default=15.0, help="PASS tolerance on plane height (mm)")
    ap.add_argument("--tol-deg", type=float, default=2.0, help="PASS tolerance on plane tilt (deg)")
    ap.add_argument("--skip-detect", action="store_true", help="plane check only (no SAM2)")
    # detection knobs (mirror the demo)
    ap.add_argument("--weights", default=".remoroo/task/weights")
    ap.add_argument("--sam-points", type=int, default=48)
    ap.add_argument("--min-height", type=float, default=0.010)
    ap.add_argument("--min-x", type=float, default=0.15)
    ap.add_argument("--max-height", type=float, default=0.12)
    ap.add_argument("--min-pts", type=int, default=150)
    ap.add_argument("--dedup", type=float, default=0.06)
    ap.add_argument("--max-footprint", type=float, default=28.0)
    args = ap.parse_args()
    cell = str(Path(args.cell).resolve())

    # --- camera + the EXACT extrinsic chain the demo uses -------------------------------------
    import yaml
    from motion_engine.demo_curobo_showcase import _ensure_safety_shim
    _ensure_safety_shim(cell)
    from remoroo_cell.primitives import ZedCamera  # type: ignore
    cellcfg = yaml.safe_load((Path(cell) / "cell.yaml").read_text())
    ov = next((c for c in cellcfg.get("cameras", [])
               if c.get("name") == "overhead" or str(c.get("serial")) == OVERHEAD_SERIAL), None)
    if ov is None:
        print("no overhead camera in cell.yaml"); return 2
    link = ov.get("link", "ZEDX_Mini_3")
    R, t = urdf_static_pose(str(Path(cell) / "robot_model" / "robot.urdf"), f"{link}_optical_frame")
    print(f"extrinsic: optical frame of {link!r} at world {[round(float(v), 3) for v in t]}")

    zed = ZedCamera(ov); zed.start()
    frame = zed.grab()
    depth = np.asarray(frame["depth_m"], float)
    K = K_of(frame["intrinsics"])

    # --- 1. TABLE PLANE ------------------------------------------------------------------------
    # table footprint from the modeled obstacle (generous margin), points 30cm above/below model
    obs = {o.get("name"): o for o in (cellcfg.get("obstacles") or [])}
    tb = obs.get("table") or {}
    tc = list(tb.get("pose") or [0.10, -0.33, -0.065])[:3]
    td = list(tb.get("dims") or [0.8, 1.9, 0.04])
    pts = world_points(depth, K, R, t)
    m = (np.abs(pts[:, 0] - tc[0]) < td[0] / 2 - 0.05) & (np.abs(pts[:, 1] - tc[1]) < td[1] / 2 - 0.05) \
        & (np.abs(pts[:, 2] - args.table_z) < 0.30)
    tab = pts[m]
    if len(tab) < 500:
        print(f"FAIL: only {len(tab)} depth points over the table footprint — camera not seeing the "
              f"table (gross extrinsic error, wrong serial, or depth outage)")
        return 1
    # robust plane: median height + PCA normal on the inlier slab around the median
    z_med = float(np.median(tab[:, 2]))
    slab = tab[np.abs(tab[:, 2] - z_med) < 0.02]
    c = slab.mean(0)
    _, _, vt = np.linalg.svd(slab - c, full_matrices=False)
    n = vt[2] / np.linalg.norm(vt[2])
    n = n if n[2] > 0 else -n
    tilt = float(np.degrees(np.arccos(np.clip(n[2], -1, 1))))
    dz_mm = 1000 * (z_med - args.table_z)
    ok_plane = abs(dz_mm) <= args.tol_mm and tilt <= args.tol_deg
    print(f"[1] table plane: z={z_med:+.4f} (model {args.table_z:+.3f}, off {dz_mm:+.1f}mm)  "
          f"tilt={tilt:.2f}°  inliers={len(slab)}/{len(tab)}  → {'PASS' if ok_plane else 'FAIL'}")

    # --- 2. TOY SANITY -------------------------------------------------------------------------
    ok_toys = True
    if not args.skip_detect:
        print("loading SAM2 (one-time)...")
        objs = detect_all(frame["rgb"], depth, R, t, K, args)
        if not objs:
            ok_toys = False
            print("[2] toys: NONE detected → FAIL (a toy is on the table)")
        for o in objs:
            x, y, z = o["xyz"]
            on_table = (abs(x - tc[0]) < td[0] / 2) and (abs(y - tc[1]) < td[1] / 2) \
                and (args.table_z - 0.01 < z < args.table_z + 0.25)
            ok_toys = ok_toys and on_table
            print(f"[2] toy {o.get('name', '?')!r}: world=({x:+.3f},{y:+.3f},{z:+.3f}) "
                  f"→ {'PASS (on table)' if on_table else 'FAIL (NOT on the table plane!)'}")

    print("-" * 64)
    verdict = ok_plane and ok_toys
    print("VERDICT:", "PASS — pixels→world chain is TRUE (extrinsic + intrinsics + depth agree "
                      "with the physical table)" if verdict else
          "FAIL — the pixels→world chain is LYING; do NOT trust picks until this passes")
    return 0 if verdict else 1


if __name__ == "__main__":
    raise SystemExit(main())
