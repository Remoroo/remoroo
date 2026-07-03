"""Grounding MECHANICS (COMP-09 edge half, per DEC-21). The six-step ground() PROCEDURE is
brain-side (remoroo_brain/robotics/ground_policy.py); this module ships only the mechanisms it
drives: multi-view capture sweeps, 3D candidate fusion, visibility-map assembly, and the
physical-nudge executor. No procedure, no ordering, no policy here.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Sequence

import numpy as np

from .. import atoms
from ..atoms import Ctx


def scan_capture(ctx: Ctx, ops: Any, camera: str, tcp: Optional[str],
                 viewpoints: Sequence[Any]) -> List[Dict[str, Any]]:
    """Fly a wrist camera through viewpoints (or a single static capture when tcp is None)
    and return the captured frames with their viewpoint tags."""
    out: List[Dict[str, Any]] = []
    if tcp is None:
        out.append({"viewpoint": None, "frame": ops.capture(camera)})
        return out
    for vp in viewpoints:
        r = atoms.look(ctx, camera, vp, tcp=tcp)
        if not r.ok:
            continue
        out.append({"viewpoint": list(vp), "frame": ops.capture(camera)})
    return out


def fuse_candidates(candidates: List[Dict[str, Any]], *, radius_m: float = 0.03,
                    min_support: int = 2) -> List[Dict[str, Any]]:
    """3D consistency fusion: candidates {"camera","xyz","label","score"} cluster by
    proximity; a cluster seen from fewer than min_support distinct cameras/views is a
    hallucination and dies here."""
    fused: List[Dict[str, Any]] = []
    for c in candidates:
        xyz = np.asarray(c["xyz"], dtype=float)
        home = None
        for f in fused:
            if np.linalg.norm(np.asarray(f["xyz"]) - xyz) <= radius_m:
                home = f
                break
        if home is None:
            fused.append({"xyz": xyz.tolist(), "labels": [c.get("label", "")],
                          "views": {c["camera"]}, "scores": [float(c.get("score", 1.0))],
                          "members": [c]})
        else:
            n = len(home["members"]) + 1
            home["xyz"] = ((np.asarray(home["xyz"]) * (n - 1) + xyz) / n).tolist()
            home["labels"].append(c.get("label", ""))
            home["views"].add(c["camera"])
            home["scores"].append(float(c.get("score", 1.0)))
            home["members"].append(c)
    confirmed = []
    for f in fused:
        if len(f["views"]) >= min_support:
            confirmed.append({
                "xyz": f["xyz"],
                "label": max(set(f["labels"]), key=f["labels"].count),
                "support": len(f["views"]),
                "score": float(np.mean(f["scores"])),
                "views": sorted(f["views"]),
            })
    return confirmed


def visibility_map(confirmed: List[Dict[str, Any]]) -> Dict[str, List[int]]:
    """camera -> indices of confirmed entities that camera sees. Runtime camera selection
    reads this; it is data, not a guess (ART-08 / DATA-11)."""
    vis: Dict[str, List[int]] = {}
    for i, ent in enumerate(confirmed):
        for cam in ent["views"]:
            vis.setdefault(cam, []).append(i)
    return vis


def nudge(ctx: Ctx, tcp: str, at_xyz: Sequence[float], *, dist_m: float = 0.005,
          press_z: Optional[float] = None,
          observe: Optional[Callable[[], Any]] = None) -> Dict[str, Any]:
    """Physical confirmation mechanics: approach, push dist_m sideways at the point, retreat;
    return before/after observations so the caller (brain policy) judges whether the scene
    changed. The gripper is the ultimate segmenter; this is its cheapest question."""
    x, y, z = [float(v) for v in at_xyz[:3]]
    pz = z if press_z is None else float(press_z)
    before = observe() if observe else None
    hover = [x, y, pz + 0.05]
    r1 = atoms.reach(ctx, tcp, hover)
    r2 = atoms.sweep(ctx, tcp, [x, y, pz], [x + dist_m, y, pz], press_z=pz)
    r3 = atoms.reach(ctx, tcp, hover)
    after = observe() if observe else None
    return {"ok": bool(r1.ok and r2.ok and r3.ok), "before": before, "after": after}


def attempt_lift(ctx: Ctx, tcp: str, at_xyz: Sequence[float], *, grasp_width: float = 0.02,
                 lift_h: float = 0.05) -> Dict[str, Any]:
    """The stronger physical question: try to pick it up. Returns the grasp evidence; a
    closed-on-air result kills a specular ghost AND writes a free negative label upstream."""
    x, y, z = [float(v) for v in at_xyz[:3]]
    atoms.reach(ctx, tcp, [x, y, z + 0.06])
    atoms.descend(ctx, tcp, z)
    g = atoms.grasp(ctx, tcp, width=grasp_width)
    if g.ok:
        atoms.reach(ctx, tcp, [x, y, z + lift_h])
        atoms.descend(ctx, tcp, z)
    atoms.release(ctx, tcp)
    atoms.reach(ctx, tcp, [x, y, z + 0.06])
    return {"held": bool(g.ok), "evidence": g.evidence}
