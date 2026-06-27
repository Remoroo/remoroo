"""Ground-truth self-collision audit — does the SPHERE model agree with the MESH model? (CHECK 2).

cuRobo's self-collision verdict is sphere–sphere overlap over the NON-ignored link pairs. The question
"are the collision spheres right?" is answered by comparing that verdict to the EXACT mesh overlap at
the same joint configuration — the methodology the published `foam` sphere-approximation tool uses to
score itself ("percentage difference in collision-check validity between the [sphere] model and the
mesh"). Per non-ignored pair, per sampled config:

  • PHANTOM  spheres overlap, meshes DON'T  → the planner is blocked for nothing (false positive; the
             precision deficit — spheres are conservative). The defect we usually chase: over-fat
             spheres / a missing ignore pair / two chains modelled too close.
  • MISS     meshes overlap, spheres DON'T  → the planner is blind to a real collision (false negative;
             the recall deficit — UNSAFE). Should never happen for a safe model.

`phantom_rate` has a KNOWN-GOOD baseline of ~0 (true radii, faithful fit), so a nonzero value is itself
the signal — unlike an uncalibrated "free fraction". This module is PURE CPU and cuRobo-free: FK and
sphere overlap are numpy; mesh ground truth is FCL (`trimesh.collision`) when available, else a scipy
convex-hull LP fallback (exact for convex links, conservative for concave). So `audit_core` is
unit-testable off-robot with synthetic geometry. Mesh LOADING needs trimesh and is lazy (rig only).
"""
from __future__ import annotations

import itertools
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

# ============================================================================
# Forward kinematics — pure numpy (no trimesh), so FK is testable off-rig.
# ============================================================================

def _rpy_to_R(roll: float, pitch: float, yaw: float) -> np.ndarray:
    """URDF fixed-axis rpy → 3×3 rotation, R = Rz(yaw) · Ry(pitch) · Rx(roll) (tf 'sxyz')."""
    cr, sr = np.cos(roll), np.sin(roll)
    cp, sp = np.cos(pitch), np.sin(pitch)
    cy, sy = np.cos(yaw), np.sin(yaw)
    Rx = np.array([[1, 0, 0], [0, cr, -sr], [0, sr, cr]])
    Ry = np.array([[cp, 0, sp], [0, 1, 0], [-sp, 0, cp]])
    Rz = np.array([[cy, -sy, 0], [sy, cy, 0], [0, 0, 1]])
    return Rz @ Ry @ Rx


def _origin_T(xyz: Sequence[float], rpy: Sequence[float]) -> np.ndarray:
    T = np.eye(4)
    T[:3, :3] = _rpy_to_R(*rpy)
    T[:3, 3] = xyz
    return T


def _axis_angle_R(axis: np.ndarray, theta: float) -> np.ndarray:
    """Rodrigues rotation of `theta` about a (normalised) axis."""
    a = np.asarray(axis, dtype=float)
    n = np.linalg.norm(a)
    if n < 1e-12:
        return np.eye(3)
    a = a / n
    K = np.array([[0, -a[2], a[1]], [a[2], 0, -a[0]], [-a[1], a[0], 0]])
    return np.eye(3) + np.sin(theta) * K + (1 - np.cos(theta)) * (K @ K)


def _joint_motion(jtype: str, axis: Sequence[float], q: float) -> np.ndarray:
    """The 4×4 a movable joint applies for value `q` (about/along its axis), beyond its fixed origin."""
    T = np.eye(4)
    if jtype in ("revolute", "continuous"):
        T[:3, :3] = _axis_angle_R(axis, float(q))
    elif jtype == "prismatic":
        a = np.asarray(axis, dtype=float)
        a = a / (np.linalg.norm(a) or 1.0)
        T[:3, 3] = float(q) * a
    # fixed / unknown → identity
    return T


def parse_joints(urdf_path: str) -> List[dict]:
    """Every URDF joint → `{name, type, parent, child, T(origin 4×4), axis, lower, upper}`."""
    root = ET.parse(urdf_path).getroot()
    out: List[dict] = []
    for j in root.findall("joint"):
        org = j.find("origin")
        xyz = [float(v) for v in (org.get("xyz", "0 0 0").split())] if org is not None else [0, 0, 0]
        rpy = [float(v) for v in (org.get("rpy", "0 0 0").split())] if org is not None else [0, 0, 0]
        ax = j.find("axis")
        axis = [float(v) for v in ax.get("xyz", "0 0 1").split()] if ax is not None else [0.0, 0.0, 1.0]
        lim = j.find("limit")
        lower = float(lim.get("lower")) if (lim is not None and lim.get("lower") is not None) else None
        upper = float(lim.get("upper")) if (lim is not None and lim.get("upper") is not None) else None
        out.append({"name": j.get("name"), "type": j.get("type", "fixed"),
                    "parent": j.find("parent").get("link"), "child": j.find("child").get("link"),
                    "T": _origin_T(xyz, rpy), "axis": axis, "lower": lower, "upper": upper})
    return out


def link_fk(joints: List[dict], config: Dict[str, float]) -> Dict[str, np.ndarray]:
    """`{link: 4×4}` world pose of every link, given joint values (missing → 0)."""
    child_to_joint = {j["child"]: j for j in joints}
    links = {j["parent"] for j in joints} | {j["child"] for j in joints}
    cache: Dict[str, np.ndarray] = {}

    def fk(link: str) -> np.ndarray:
        if link in cache:
            return cache[link]
        j = child_to_joint.get(link)
        if j is None:                                   # a root link
            cache[link] = np.eye(4)
            return cache[link]
        T = fk(j["parent"]) @ j["T"] @ _joint_motion(j["type"], j["axis"], config.get(j["name"], 0.0))
        cache[link] = T
        return T

    return {ln: fk(ln) for ln in links}


# ============================================================================
# Sphere overlap — pure numpy (matches cuRobo's self-collision rule, true radii).
# ============================================================================

def _posed_spheres(spheres_link: List[dict], T: np.ndarray) -> np.ndarray:
    """Link-local spheres → world `[(x,y,z,r)]` under pose `T`."""
    out = []
    R, t = T[:3, :3], T[:3, 3]
    for s in (spheres_link or []):
        r = float(s.get("radius", 0.0))
        if r > 0:
            out.append(np.concatenate([R @ np.asarray(s["center"], dtype=float) + t, [r]]))
    return np.asarray(out, dtype=float).reshape(-1, 4)


def sphere_pair_penetration(sa: np.ndarray, sb: np.ndarray) -> float:
    """Max overlap depth (m) over the two links' sphere sets; ≤0 ⇒ clear (signed-distance min)."""
    if len(sa) == 0 or len(sb) == 0:
        return -np.inf
    ca, ra = sa[:, :3], sa[:, 3]
    cb, rb = sb[:, :3], sb[:, 3]
    d = np.linalg.norm(ca[:, None, :] - cb[None, :, :], axis=2) - ra[:, None] - rb[None, :]
    return float(-d.min())                              # >0 ⇒ overlapping by this much


# ============================================================================
# Mesh ground truth — FCL primary (lazy), scipy convex-hull LP fallback.
# ============================================================================

def _aabb(points: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    return points.min(0), points.max(0)


class _HullChecker:
    """Exact for convex links, conservative (over-reports) for concave — scipy only, so it runs
    everywhere FCL doesn't (incl. CI). Two convex hulls intersect iff a point satisfies BOTH hulls'
    half-spaces (an LP feasibility); an AABB broadphase skips the obvious non-overlaps first."""

    def __init__(self, verts: Dict[str, np.ndarray]) -> None:
        from scipy.spatial import ConvexHull, QhullError  # scipy is always present
        self._eq: Dict[str, Optional[np.ndarray]] = {}
        self._v: Dict[str, np.ndarray] = {}
        for link, v in verts.items():
            v = np.asarray(v, dtype=float).reshape(-1, 3)
            self._v[link] = v
            try:
                self._eq[link] = ConvexHull(v).equations          # [normal | offset], n·x + off ≤ 0
            except (QhullError, ValueError):
                self._eq[link] = None                              # degenerate (coplanar) → AABB only

    def _world_halfspaces(self, link: str, T: np.ndarray) -> Optional[np.ndarray]:
        eq = self._eq.get(link)
        if eq is None:
            return None
        R, t = T[:3, :3], T[:3, 3]
        # n·x_link + off ≤ 0, x_link = Rᵀ(x_world − t)  ⇒  (nᵀRᵀ)·x_world ≤ off·(−1) + (nᵀRᵀ)·t
        n_world = eq[:, :3] @ R.T
        b = -eq[:, 3] + n_world @ t
        return np.column_stack([n_world, b])                       # rows: n·x ≤ b

    def collide(self, link_T: Dict[str, np.ndarray], pairs: List[Tuple[str, str]]) -> set:
        from scipy.optimize import linprog
        world_v = {ln: (self._v[ln] @ link_T[ln][:3, :3].T + link_T[ln][:3, 3]) for ln in self._v}
        boxes = {ln: _aabb(world_v[ln]) for ln in world_v}
        hit = set()
        for a, b in pairs:
            if a not in world_v or b not in world_v:
                continue
            (lo_a, hi_a), (lo_b, hi_b) = boxes[a], boxes[b]
            if np.any(hi_a < lo_b) or np.any(hi_b < lo_a):         # AABB broadphase: disjoint
                continue
            Ha, Hb = self._world_halfspaces(a, link_T[a]), self._world_halfspaces(b, link_T[b])
            if Ha is None or Hb is None:                           # degenerate hull ⇒ trust the AABB
                hit.add((a, b)); continue
            A = np.vstack([Ha[:, :3], Hb[:, :3]])
            bv = np.concatenate([Ha[:, 3], Hb[:, 3]])
            res = linprog(c=np.zeros(3), A_ub=A, b_ub=bv, bounds=[(None, None)] * 3, method="highs")
            if res.success:                                        # a common point exists ⇒ hulls intersect
                hit.add((a, b))
        return hit


class _FclChecker:
    """Exact non-convex mesh collision via FCL (`trimesh.collision.CollisionManager`). Boolean only —
    we never ask FCL for penetration depth (unimplemented for non-convex; not needed, phantoms are the
    SEPARATED case). One `set_transform`+`in_collision_internal` per config (broadphase inside)."""

    def __init__(self, meshes: dict) -> None:
        from trimesh.collision import CollisionManager
        self._mgr = CollisionManager()
        self._links = []
        for link, mesh in meshes.items():
            self._mgr.add_object(link, mesh)
            self._links.append(link)

    def collide(self, link_T: Dict[str, np.ndarray], pairs: List[Tuple[str, str]]) -> set:
        for link in self._links:
            if link in link_T:
                self._mgr.set_transform(link, link_T[link])
        _, names = self._mgr.in_collision_internal(return_names=True, return_data=False)
        allowed = {tuple(sorted(p)) for p in pairs}
        return {tuple(sorted(p)) for p in names if tuple(sorted(p)) in allowed}


def make_mesh_checker(verts: Dict[str, np.ndarray], meshes: Optional[dict]):
    """FCL (exact) if trimesh meshes are supplied and importable, else the scipy hull fallback."""
    if meshes:
        try:
            return _FclChecker(meshes), "fcl"
        except Exception:  # noqa: BLE001 — no python-fcl / trimesh ⇒ degrade, never crash the gate
            pass
    return _HullChecker(verts), "convex_hull_fallback"


# ============================================================================
# Mesh loading — trimesh, LAZY (rig only); tests inject verts directly.
# ============================================================================

def load_link_meshes(urdf_path: str, pkg_root: str):
    """`(verts, meshes)` per link from the URDF collision geometry — mm→m scaled + collision-origin'd,
    concatenated per link. `meshes` are trimesh objects (for FCL); `verts` are vertex arrays (for the
    hull fallback / bbox). trimesh is imported here only (so this module loads off-rig)."""
    import trimesh

    def resolve(fn: str) -> str:
        if fn.startswith("package://"):
            fn = fn[len("package://"):].split("/", 1)[1]
        return str(Path(pkg_root) / fn)

    root = ET.parse(urdf_path).getroot()
    verts: Dict[str, np.ndarray] = {}
    meshes: Dict[str, object] = {}
    for link in root.findall("link"):
        parts = []
        for col in link.findall("collision"):
            geo = col.find("geometry")
            if geo is None:
                continue
            m = None
            mesh_el = geo.find("mesh")
            if mesh_el is not None:
                m = trimesh.load(resolve(mesh_el.get("filename")), force="mesh")
                m.apply_scale([float(s) for s in (mesh_el.get("scale") or "1 1 1").split()])
            elif geo.find("box") is not None:
                size = [float(s) for s in geo.find("box").get("size", "0 0 0").split()]
                m = trimesh.creation.box(extents=size)
            elif geo.find("cylinder") is not None:
                cyl = geo.find("cylinder")
                m = trimesh.creation.cylinder(radius=float(cyl.get("radius", 0)), height=float(cyl.get("length", 0)))
            elif geo.find("sphere") is not None:
                m = trimesh.creation.icosphere(radius=float(geo.find("sphere").get("radius", 0)))
            if m is None or m.is_empty:
                continue
            org = col.find("origin")
            xyz = [float(v) for v in (org.get("xyz", "0 0 0").split())] if org is not None else [0, 0, 0]
            rpy = [float(v) for v in (org.get("rpy", "0 0 0").split())] if org is not None else [0, 0, 0]
            m.apply_transform(_origin_T(xyz, rpy))
            parts.append(m)
        if parts:
            mesh = parts[0] if len(parts) == 1 else trimesh.util.concatenate(parts)
            meshes[link.get("name")] = mesh
            verts[link.get("name")] = np.asarray(mesh.vertices, dtype=float)
    return verts, meshes


# ============================================================================
# The audit.
# ============================================================================

def _pairs(links: List[str], ignore: Dict[str, List[str]]) -> List[Tuple[str, str]]:
    """All link pairs MINUS the self-collision ignore matrix — exactly the set cuRobo checks."""
    ig = {k: set(v or []) for k, v in (ignore or {}).items()}
    out = []
    for a, b in itertools.combinations(sorted(links), 2):
        if b in ig.get(a, set()) or a in ig.get(b, set()):
            continue
        out.append((a, b))
    return out


def sample_configs(joints: List[dict], n: int, seed: Dict[str, float], rng) -> List[Dict[str, float]]:
    """`n` configs sampled UNIFORMLY within each movable joint's URDF limits (continuous → [−π, π]),
    plus structured corners: home/seed, all-zero, and every joint pushed to each of its limits."""
    movable = [j for j in joints if j["type"] in ("revolute", "continuous", "prismatic")]

    def lo_hi(j):
        if j["lower"] is not None and j["upper"] is not None:
            return j["lower"], j["upper"]
        return (-np.pi, np.pi) if j["type"] == "continuous" else (-1.0, 1.0)

    configs = [dict(seed), {j["name"]: 0.0 for j in movable}]
    for j in movable:                                   # each joint at each extreme, others at home
        lo, hi = lo_hi(j)
        for q in (lo, hi):
            c = dict(seed); c[j["name"]] = float(q); configs.append(c)
    bounds = {j["name"]: lo_hi(j) for j in movable}
    for _ in range(max(0, n - len(configs))):
        configs.append({nm: float(rng.uniform(lo, hi)) for nm, (lo, hi) in bounds.items()})
    return configs


def audit_core(joints: List[dict], spheres: Dict[str, List[dict]], verts: Dict[str, np.ndarray],
               meshes: Optional[dict], ignore: Dict[str, List[str]], *, seed: Dict[str, float],
               n: int = 400, phantom_warn: float = 0.02, phantom_fail: float = 0.10,
               rng=None) -> dict:
    """Compare the SPHERE vs MESH self-collision verdict over many configs. Pure (numpy/scipy + an
    optional lazy FCL); `verts`/`meshes`/`joints`/`spheres` are injected ⇒ fully testable off-rig."""
    rng = rng or np.random.default_rng(0)
    links = sorted(set(spheres) & set(verts))           # need BOTH a sphere set and a mesh for a pair
    skipped = sorted(set(spheres) - set(verts))         # spheres but no loadable mesh → excluded
    pairs = _pairs(links, ignore)
    checker, backend = make_mesh_checker({k: verts[k] for k in links}, meshes)
    sph = {ln: spheres.get(ln, []) for ln in links}

    configs = sample_configs(joints, n, seed, rng)
    s_count = m_count = phantom_count = miss_count = 0
    agg: Dict[Tuple[str, str], dict] = {}
    home_sphere_pairs: List[Tuple[str, str]] = []

    for ci, cfg in enumerate(configs):
        T = link_fk(joints, cfg)
        posed = {ln: _posed_spheres(sph[ln], T[ln]) for ln in links}
        s_hits: Dict[Tuple[str, str], float] = {}
        for a, b in pairs:
            pen = sphere_pair_penetration(posed[a], posed[b])
            if pen > 0:
                s_hits[(a, b)] = pen
        m_hits = checker.collide(T, pairs)              # one broadphase'd query per config
        if ci == 0:
            home_sphere_pairs = list(s_hits.keys())     # config 0 is the seed/home pose
        for pair in pairs:
            s = pair in s_hits
            m = pair in m_hits
            s_count += s
            m_count += m
            if s and not m:                             # PHANTOM
                phantom_count += 1
                d = agg.setdefault(pair, {"links": list(pair), "phantom": 0, "miss": 0,
                                          "max_penetration_mm": 0.0, "worst_config": None})
                d["phantom"] += 1
                pen_mm = round(s_hits[pair] * 1000, 1)
                if pen_mm > d["max_penetration_mm"]:
                    d["max_penetration_mm"] = pen_mm
                    d["worst_config"] = {k: round(float(v), 4) for k, v in cfg.items()}
            elif m and not s:                           # MISS (unsafe)
                miss_count += 1
                d = agg.setdefault(pair, {"links": list(pair), "phantom": 0, "miss": 0,
                                          "max_penetration_mm": 0.0, "worst_config": None})
                d["miss"] += 1

    phantom_pairs = sorted((d for d in agg.values() if d["phantom"]), key=lambda d: -d["phantom"])
    miss_pairs = sorted((d for d in agg.values() if d["miss"]), key=lambda d: -d["miss"])
    per_link: Dict[str, int] = {}
    for d in phantom_pairs:
        for ln in d["links"]:
            per_link[ln] = per_link.get(ln, 0) + d["phantom"]

    phantom_rate = round(phantom_count / s_count, 4) if s_count else 0.0
    miss_rate = round(miss_count / m_count, 4) if m_count else 0.0
    home_free = len(home_sphere_pairs) == 0

    if not home_free or miss_rate > 0 or phantom_rate > phantom_fail:
        verdict = "fail"
    elif phantom_rate > phantom_warn:
        verdict = "warn"
    else:
        verdict = "pass"

    return {
        "verdict": verdict, "backend": backend, "n_configs": len(configs), "n_pairs": len(pairs),
        "home_free": home_free,
        "phantom_rate": phantom_rate, "miss_rate": miss_rate,
        "phantom_pairs": [dict(p) for p in phantom_pairs[:24]],
        "miss_pairs": [{"links": p["links"], "miss": p["miss"]} for p in miss_pairs[:24]],
        "per_link_phantom": dict(sorted(per_link.items(), key=lambda kv: -kv[1])),
        "n_spheres_only_links_skipped": len(skipped), "skipped_links": skipped[:12],
    }


def audit(urdf_path: str, spheres: Dict[str, List[dict]], ignore: Dict[str, List[str]], *,
          pkg_root: str, seed: Optional[Dict[str, float]] = None, n: int = 400,
          phantom_warn: float = 0.02, phantom_fail: float = 0.10) -> dict:
    """Load the link meshes (trimesh, rig) and run `audit_core`. Off-rig (no trimesh) it falls back to
    nothing to compare against and reports that, rather than crashing the gate."""
    joints = parse_joints(urdf_path)
    try:
        verts, meshes = load_link_meshes(urdf_path, pkg_root)
    except Exception as e:  # noqa: BLE001 — surface, never crash the gate
        return {"verdict": "warn", "error": f"mesh load failed ({type(e).__name__}: {e}); "
                "CHECK 2 needs the link meshes to ground-truth the spheres", "backend": None}
    return audit_core(joints, spheres, verts, meshes, ignore, seed=dict(seed or {}), n=n,
                      phantom_warn=phantom_warn, phantom_fail=phantom_fail)
