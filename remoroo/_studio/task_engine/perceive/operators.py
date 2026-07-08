"""Perception operators (COMP-07 / IFACE-08) — the small task-agnostic function set the
agent composes per-task perception from. Pure geometry is numpy here; learned models come
through the ModelRegistry (segmenter/detector handles are callables the tests fake).

Frames are plain dicts: {"rgb": HxWx3 uint8 | None, "depth": HxW float meters | None,
"camera": name, "t_capture": float}. Calibration is duck-typed: intrinsics(camera) -> K 3x3,
robot_from_cam(camera) -> T 4x4 (Setup owns the real one).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

import numpy as np

from .serving import ModelRegistry


class PerceptionOps:
    def __init__(self, registry: ModelRegistry, calib: Any, bridge: Any) -> None:
        self.registry = registry
        self.calib = calib
        self.bridge = bridge

    # capture ------------------------------------------------------------------
    def capture(self, camera: str) -> Dict[str, Any]:
        """Normalized frame dict from WHATEVER the authored Bridge returns: a rich
        grab_camera(camera) dict when the bridge has one, else camera_frame(camera) —
        which per the authored contract is the tuple (depth_m, K_3x3, t_capture); rgb
        may be absent (depth-only rigs are normal, the geometric ops still work)."""
        grab = getattr(self.bridge, "grab_camera", None)
        frame = grab(camera) if callable(grab) else self.bridge.camera_frame(camera)
        if frame is None:
            raise RuntimeError(f"camera {camera!r} returned no frame")
        if isinstance(frame, dict):
            # Key-normalize authored dialects: bridges commonly say depth_m /
            # intrinsics; the operator contract says depth / K.
            if "depth" not in frame and "depth_m" in frame:
                frame["depth"] = frame["depth_m"]
            if "K" not in frame and "intrinsics" in frame:
                frame["K"] = frame["intrinsics"]
            frame.setdefault("camera", camera)
            frame.setdefault("rgb", None)
            frame.setdefault("depth", None)
            frame.setdefault("t_capture", 0.0)
            return frame
        seq = tuple(frame)
        out: Dict[str, Any] = {
            "rgb": None,
            "depth": seq[0] if len(seq) > 0 else None,
            "camera": camera,
            "t_capture": float(seq[2]) if len(seq) > 2 and seq[2] is not None else 0.0,
        }
        if len(seq) > 1 and seq[1] is not None:
            out["K"] = seq[1]
        return out

    # learned proposals ------------------------------------------------------------
    def segment(self, prompt: Any, frame: Dict[str, Any]) -> List[np.ndarray]:
        return list(self.registry.get("segmenter")(frame, prompt))

    def detect(self, text: str, frame: Dict[str, Any]) -> List[Dict[str, Any]]:
        """-> [{"label", "score", "box" [x0,y0,x1,y1], "mask" optional}]"""
        return list(self.registry.get("detector")(frame, text))

    def track(self, frame: Dict[str, Any], mask: np.ndarray) -> Any:
        return self.registry.get("tracker")(frame, mask)

    # geometry ------------------------------------------------------------------
    def cloud(self, mask: np.ndarray, frame: Dict[str, Any]) -> np.ndarray:
        """Masked depth pixels -> Nx3 points in the ROBOT frame (through the calibration
        Setup owns). This is the pixels-to-world seam every channel rides."""
        depth = frame["depth"]
        cam = frame["camera"]
        K = np.asarray(self.calib.intrinsics(cam), dtype=float)
        T = np.asarray(self.calib.robot_from_cam(cam), dtype=float)
        ys, xs = np.nonzero(np.asarray(mask, dtype=bool))
        z = np.asarray(depth, dtype=float)[ys, xs]
        good = np.isfinite(z) & (z > 0)
        xs, ys, z = xs[good], ys[good], z[good]
        fx, fy, cx, cy = K[0, 0], K[1, 1], K[0, 2], K[1, 2]
        pts_cam = np.stack([(xs - cx) * z / fx, (ys - cy) * z / fy, z], axis=1)
        pts_h = np.concatenate([pts_cam, np.ones((len(pts_cam), 1))], axis=1)
        return (pts_h @ T.T)[:, :3]

    @staticmethod
    def mask_stats(mask: np.ndarray) -> Dict[str, Any]:
        m = np.asarray(mask, dtype=bool)
        ys, xs = np.nonzero(m)
        if len(xs) == 0:
            return {"area_px": 0, "centroid_px": None, "bbox": None, "hull_px": []}
        pts = np.stack([xs, ys], axis=1).astype(float)
        return {
            "area_px": int(m.sum()),
            "centroid_px": [float(xs.mean()), float(ys.mean())],
            "bbox": [int(xs.min()), int(ys.min()), int(xs.max()), int(ys.max())],
            "hull_px": _convex_hull(pts).tolist(),
        }

    @staticmethod
    def keypoints(polygon: Sequence[Sequence[float]], k: int = 4,
                  min_angle_deg: float = 25.0) -> List[List[float]]:
        """The k sharpest vertices of a polygon (interior turn furthest from straight).
        Pure geometry: no vocabulary, no model."""
        poly = np.asarray(polygon, dtype=float)
        n = len(poly)
        if n < 3:
            return poly.tolist()
        sharp = []
        for i in range(n):
            a, b, c = poly[(i - 1) % n], poly[i], poly[(i + 1) % n]
            v1, v2 = a - b, c - b
            cosang = float(np.dot(v1, v2) /
                           (np.linalg.norm(v1) * np.linalg.norm(v2) + 1e-12))
            ang = float(np.degrees(np.arccos(np.clip(cosang, -1.0, 1.0))))
            turn = 180.0 - ang                       # 0 = straight edge, high = sharp corner
            if turn >= min_angle_deg:
                sharp.append((turn, i))
        sharp.sort(reverse=True)
        idx = sorted(i for _t, i in sharp[:k])
        return [poly[i].tolist() for i in idx]

    @staticmethod
    def plane(points: np.ndarray) -> Dict[str, Any]:
        """Least-squares plane fit -> {"normal": [a,b,c], "d": d} with |normal| = 1
        (ax + by + cz + d = 0)."""
        p = np.asarray(points, dtype=float)
        centroid = p.mean(axis=0)
        _u, _s, vt = np.linalg.svd(p - centroid)
        normal = vt[-1]
        if normal[2] < 0:
            normal = -normal
        d = -float(np.dot(normal, centroid))
        return {"normal": normal.tolist(), "d": d}

    @staticmethod
    def above_plane_blobs(frame: Dict[str, Any], plane_z: float,
                          min_height_m: float = 0.005,
                          min_px: int = 20) -> List[np.ndarray]:
        """Geometry-first proposals: connected regions of depth closer than the support
        plane by min_height. No vocabulary at all (the detector-missed escape hatch)."""
        depth = np.asarray(frame["depth"], dtype=float)
        raised = np.isfinite(depth) & (depth > 0) & (depth < (plane_z - min_height_m))
        return [m for m in _connected(raised) if m.sum() >= min_px]


# --- small pure helpers --------------------------------------------------------------------
def _convex_hull(pts: np.ndarray) -> np.ndarray:
    """Monotone chain; pts Nx2 -> hull Mx2 counter-clockwise."""
    pts = np.unique(pts, axis=0)
    if len(pts) <= 2:
        return pts
    order = np.lexsort((pts[:, 1], pts[:, 0]))
    pts = pts[order]

    def cross2(o, a, b):
        return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])

    def half(seq):
        out: List[np.ndarray] = []
        for p in seq:
            while len(out) >= 2 and cross2(out[-2], out[-1], p) <= 0:
                out.pop()
            out.append(p)
        return out

    lower = half(pts)
    upper = half(pts[::-1])
    return np.array(lower[:-1] + upper[:-1])


def _connected(mask: np.ndarray) -> List[np.ndarray]:
    """4-connected components, BFS, numpy-only (no scipy dependency in the wheel)."""
    m = np.asarray(mask, dtype=bool)
    seen = np.zeros_like(m)
    comps: List[np.ndarray] = []
    H, W = m.shape
    for y0, x0 in zip(*np.nonzero(m & ~seen)):
        if seen[y0, x0]:
            continue
        stack = [(int(y0), int(x0))]
        comp = np.zeros_like(m)
        seen[y0, x0] = True
        while stack:
            y, x = stack.pop()
            comp[y, x] = True
            for dy, dx in ((1, 0), (-1, 0), (0, 1), (0, -1)):
                ny, nx = y + dy, x + dx
                if 0 <= ny < H and 0 <= nx < W and m[ny, nx] and not seen[ny, nx]:
                    seen[ny, nx] = True
                    stack.append((ny, nx))
        comps.append(comp)
    return comps
