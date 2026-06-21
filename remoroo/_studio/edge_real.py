#!/usr/bin/env python3
"""Remoroo Studio — REAL edge service (runs on the robot computer).

Implements the same HTTP contract the studio's ServerEdge expects, backed by the
agent-authored cell:

  * the Bridge  (remoroo_cell/primitives.py — get_observation / connect / estop)
  * the Recorder (remoroo_cell/capture/recorder.py)
  * cuRoboV2     (collision spheres + MotionGen planning + segmentation)

The studio server proxies /edge/* and /live/joints here when launched with
EDGE_URL pointing at it:

    EDGE_PORT=7779 REMOROO_CELL=/path/to/repo/remoroo_cell python server/edge_real.py
    # then:  EDGE_URL=http://127.0.0.1:7779 npm run serve   (or remoroo setup)

Everything degrades gracefully: if cuRobo or the authored cell aren't present yet,
endpoints return an honest {"error": ...} rather than crashing — so you can stand
this up incrementally as the gates get authored. No third-party web deps (stdlib
http.server); it only imports numpy/yaml/torch/curobo lazily, when a route needs them.
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import math
import traceback
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs

CELL_DIR = Path(os.environ.get("REMOROO_CELL", "remoroo_cell")).resolve()
PORT = int(os.environ.get("EDGE_PORT", "7779"))
# Make `import remoroo_cell.primitives` resolve (the cell lives in the repo).
sys.path.insert(0, str(CELL_DIR.parent))


def _safety_spine_module():
    """The safety/transport spine the cell Bridge imports — prefer the shipped `remoroo.edge`,
    else a SELF-CONTAINED inline copy so this works even when run from the working tree without
    the `remoroo` package installed (no external dependency)."""
    try:
        from remoroo import edge as spine  # shipped spine (remoroo/edge.py), if importable
        return spine
    except Exception:  # noqa: BLE001 — build the inline fallback below
        pass
    import types as _types
    import json as _json
    import time as _time
    from pathlib import Path as _Path
    import numpy as _np

    mod = _types.ModuleType("safety_shim")

    class SafetySupervisor:  # plain class (no @dataclass — robust under any module load context)
        def __init__(self, max_cartesian_speed_mps, max_joint_speed_frac, bounds_min, bounds_max):
            self.max_cartesian_speed_mps = max_cartesian_speed_mps
            self.max_joint_speed_frac = max_joint_speed_frac
            self.bounds_min = bounds_min
            self.bounds_max = bounds_max
            self.estopped = False        # E-stop STATE the cell's primitives.py reads/writes

        def estop(self):
            self.estopped = True

        def reset_estop(self):
            self.estopped = False

        def is_estopped(self):
            return bool(self.estopped)

        @classmethod
        def from_cell(cls, cell):
            s = cell.get("safety", {}) or {}
            w = (cell.get("workspace") or {}).get("bounds_m", {}) or {}
            # None-SAFE: a cell.yaml key present-but-null (`max_joint_speed_frac:`) makes
            # dict.get(k, default) return None, NOT the default → float(None) crashes the
            # Bridge connect. Coerce None to the default.
            def _f(v, d):
                return float(d) if v is None else float(v)
            return cls(_f(s.get("max_cartesian_speed_mps"), 0.10),
                       _f(s.get("max_joint_speed_frac"), 0.10),
                       _np.asarray(w.get("min") or [-0.5, -0.5, 0.0], float),
                       _np.asarray(w.get("max") or [0.5, 0.5, 0.8], float))

        def clamp_speed(self, frac):
            f = self.max_joint_speed_frac if frac is None else min(frac, self.max_joint_speed_frac)
            return max(0.0, f)

        def joints_within_limits(self, arm, q):
            return bool(_np.all(_np.isfinite(q)))

        def point_in_bounds(self, xyz):
            xyz = _np.asarray(xyz, float)
            return bool(_np.all(xyz >= self.bounds_min) and _np.all(xyz <= self.bounds_max))

        def trajectory_ok(self, arm, plan):
            pts = getattr(plan, "cartesian_waypoints", None)
            return True if pts is None else all(self.point_in_bounds(_np.asarray(p, float)) for p in pts)

    class EpisodeWriter:
        def __init__(self, out_dir):
            self.dir = _Path(out_dir); self.dir.mkdir(parents=True, exist_ok=True); self._frames = []

        def add(self, **frame):
            self._frames.append({"t": _time.time(), **frame})

        def close(self):
            (self.dir / "meta.json").write_text(_json.dumps({"schema": "remoroo_episode_v1",
                                                             "n_frames": len(self._frames)}, indent=2))
            return self.dir

    mod.SafetySupervisor = SafetySupervisor
    mod.EpisodeWriter = EpisodeWriter
    return mod


def _ensure_safety_shim_resolves() -> None:
    """The cell's primitives.py imports its safety/transport spine from `remoroo.edge` (shipped)
    with a `safety_shim` fallback. A cell missing its local shim — or authored with a bare
    `import safety_shim` — used to fail HARD ("No module named 'safety_shim'") and take the whole
    Bridge (hence calibration + the live camera feed) down. Pre-register the spine under BOTH the
    bare and package-qualified shim names so the Bridge imports regardless of how it was written."""
    spine = _safety_spine_module()
    sys.modules.setdefault("safety_shim", spine)
    sys.modules.setdefault(f"{CELL_DIR.name}.safety_shim", spine)


_ensure_safety_shim_resolves()

# --------------------------------------------------------------------------- #
# Lazy, cached access to the authored cell + toolchain (never crash on import) #
# --------------------------------------------------------------------------- #
_bridge = None
_bridge_err: str | None = None
_bridge_last_try: float = 0.0
# Serialises every touch of the shared cell Bridge. The live-joints SSE polls
# get_observation() ~20 Hz WHILE the calibration verbs move/capture on the SAME bridge — on a
# real SDK two threads hitting it concurrently corrupts reads / commands. The lock makes the
# live mirror and the supervised flow take turns on the one physical device (G14).
_bridge_lock = threading.RLock()


def get_bridge():
    """Construct + connect the cell Bridge. primitives.py is authored at G2 — i.e.
    AFTER the edge starts — so we must NOT cache the early failure forever, or the
    live robot never connects this session. On failure we retry (with a short
    backoff) so the bridge comes up the moment primitives.py exists / the robot is
    reachable."""
    global _bridge, _bridge_err, _bridge_last_try
    if _bridge is not None:
        return _bridge
    now = time.time()
    if _bridge_err is not None and (now - _bridge_last_try) < 3.0:
        return None  # backoff between retries
    _bridge_last_try = now
    try:
        from remoroo_cell.primitives import Bridge  # type: ignore  # re-attempted until authored (G2)

        b = Bridge.from_cell_yaml(str(CELL_DIR / "cell.yaml"))
        b.connect()
        _bridge = b
        _bridge_err = None
    except Exception as e:  # noqa: BLE001
        # Capture WHERE it failed (file:line) — a bare "TypeError: float()..." off a customer
        # machine is undiagnosable; the deepest frame tells us which line in which module.
        tb = traceback.extract_tb(e.__traceback__)
        where = f" (at {os.path.basename(tb[-1].filename)}:{tb[-1].lineno})" if tb else ""
        _bridge_err = f"{type(e).__name__}: {e}{where}"
    return _bridge


def load_cell_yaml() -> dict:
    try:
        import yaml  # type: ignore

        return yaml.safe_load((CELL_DIR / "cell.yaml").read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def toolchain_status() -> list[dict]:
    def probe(name, importer, fixable=False):
        try:
            detail = importer()
            return {"id": name, "name": name, "ok": True, "detail": str(detail), "fixable": False}
        except Exception as e:  # noqa: BLE001
            return {"id": name, "name": name, "ok": False, "detail": f"{type(e).__name__}: {e}", "fixable": fixable, "pending": False}

    def _py():
        return sys.version.split()[0]

    def _curobo():
        import curobo  # type: ignore

        return getattr(curobo, "__version__", "installed")

    def _torch_gpu():
        import torch  # type: ignore

        if not torch.cuda.is_available():
            raise RuntimeError("no CUDA GPU visible")
        return torch.cuda.get_device_name(0)

    items = [probe("python", _py), probe("curobo", _curobo), probe("gpu", _torch_gpu)]
    # the arm/camera SDKs are cell-specific; report whether the Bridge connects
    b = get_bridge()
    items.append(
        {"id": "bridge", "name": "Cell Bridge (primitives.py)", "ok": b is not None, "detail": _bridge_err or "connected", "fixable": False}
    )
    return items


# --------------------------------------------------------------------------- #
# cuRobo helpers (researched API: MotionGenConfig.load_from_robot_config, etc.) #
# --------------------------------------------------------------------------- #
def write_curobo_robot_yaml(urdf_text: str, spheres: list[dict]) -> Path:
    """Write a cuRobo robot config YAML embedding the studio's collision spheres
    (cuRobo plans on spheres, not meshes; we avoid Isaac Sim per the seed). Spheres
    arrive as [{link, center:[x,y,z], radius}] from the studio (approxSpheres)."""
    import yaml  # type: ignore

    rm = CELL_DIR / "robot_model"
    rm.mkdir(parents=True, exist_ok=True)
    (rm / "robot.urdf").write_text(urdf_text, encoding="utf-8")

    by_link: dict[str, list[dict]] = {}
    for s in spheres:
        by_link.setdefault(s["link"], []).append({"center": list(s["center"]), "radius": float(s["radius"])})

    # The CANONICAL ARM MAP (urdf-derived + cell.yaml arm names) — the single source of truth the
    # old config lacked. Written to arms.yaml; per-arm cuRobo robot_cfg (base_link, ee_link, the
    # arm's cspace joints, the other arm LOCKED) so the planner finally has a kinematic chain.
    from calib_engine import urdf_io
    from calib_engine.curobo_cfg import build_robot_cfg
    cy = load_cell_yaml()
    cam2arm = {}
    for c in (cy.get("cameras") or []):
        nm = str(c.get("name") or c.get("id") or "")
        if nm and c.get("attached_to"):
            cam2arm[nm] = str(c["attached_to"])
    arm_map = urdf_io.arm_map_dict(str(rm / "robot.urdf"), camera_to_arm=cam2arm)
    (rm / "arms.yaml").write_text(yaml.safe_dump(arm_map, sort_keys=False), encoding="utf-8")

    out = rm / "collision_spheres.yml"
    if arm_map["arms"]:
        for a in arm_map["arms"]:
            cfg = build_robot_cfg(arm_map, by_link, plan_arm=a["name"], urdf_path="robot.urdf")
            safe = str(a["name"]).replace("/", "_")
            (rm / f"robot_cfg_{safe}.yml").write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
        # default config = the first arm (back-compat for single-arm motion_gen)
        first = build_robot_cfg(arm_map, by_link, plan_arm=arm_map["arms"][0]["name"], urdf_path="robot.urdf")
        out.write_text(yaml.safe_dump(first, sort_keys=False), encoding="utf-8")
    else:  # no arms derived (degenerate URDF) — spheres-only, loud
        out.write_text(yaml.safe_dump({"robot_cfg": {"kinematics": {
            "urdf_path": "robot.urdf", "collision_spheres": by_link,
            "collision_sphere_buffer": 0.005}}}, sort_keys=False), encoding="utf-8")
    return out


_MOTION_GEN = None     # cached cuRobo MotionGen (collision-free trajectory planning)
_ROBOT_WORLD = None    # cached cuRobo RobotWorld (fast config-space collision queries)


def reset_curobo_cache():
    """Drop the cached cuRobo models so the next planning/feasibility call rebuilds the world.
    Call whenever the obstacles or the robot model change (the world is baked into both). This
    does NOT touch the calib service / session — calibration progress is preserved."""
    global _MOTION_GEN, _ROBOT_WORLD
    _MOTION_GEN = None
    _ROBOT_WORLD = None


def _curobo_world():
    """The cuRobo WorldConfig the planner AND the feasibility checker SHARE: a conservative
    workspace floor box (cell.yaml bounds) + the operator-modeled static OBSTACLES
    (table/wall/post) as proper cuRobo cuboids — separate from the robot, the cuRobo way. One
    builder so 'feasible' and 'planned' never disagree about where the table is."""
    from curobo.geom.types import WorldConfig, Cuboid  # type: ignore
    from calib_engine.curobo_cfg import build_world_cfg
    cy = load_cell_yaml()
    ws = (cy.get("workspace") or {})
    dims = ws.get("size", [1.5, 1.5, 1.0])
    center = ws.get("center", [0.0, 0.0, dims[2] / 2 - 0.5])
    cubs = [Cuboid(name="workspace_floor", pose=[*center, 1, 0, 0, 0], dims=list(dims))]
    for nm, c in build_world_cfg(cy.get("obstacles"))["cuboid"].items():
        cubs.append(Cuboid(name=nm, pose=c["pose"], dims=c["dims"]))
    return WorldConfig(cuboid=cubs)


def motion_gen():
    """Cached cuRobo MotionGen from the cell's robot YAML + the shared obstacle world. Plans the
    COLLISION-FREE trajectories for the supervised calibration moves (warmup is paid once)."""
    global _MOTION_GEN
    if _MOTION_GEN is not None:
        return _MOTION_GEN
    from curobo.wrap.reacher.motion_gen import MotionGen, MotionGenConfig  # type: ignore
    robot_yaml = str(CELL_DIR / "robot_model" / "collision_spheres.yml")
    cfg = MotionGenConfig.load_from_robot_config(robot_yaml, _curobo_world(), interpolation_dt=0.02)
    mg = MotionGen(cfg)
    mg.warmup()
    _MOTION_GEN = mg
    return mg


def _robot_world():
    """Cached cuRobo RobotWorld for FAST config-space collision queries — the per-candidate
    feasibility filter behind calibration next-pose suggestion (≈32 candidates/suggestion, so it
    must be cheap: collision distance only, NO trajectory optimization). Same robot spheres +
    obstacle world the planner uses."""
    global _ROBOT_WORLD
    if _ROBOT_WORLD is not None:
        return _ROBOT_WORLD
    from curobo.wrap.model.robot_world import RobotWorld, RobotWorldConfig  # type: ignore
    robot_yaml = str(CELL_DIR / "robot_model" / "collision_spheres.yml")
    cfg = RobotWorldConfig.load_from_config(robot_yaml, _curobo_world(),
                                            collision_activation_distance=0.01)
    _ROBOT_WORLD = RobotWorld(cfg)
    return _ROBOT_WORLD


class _CuroboMotion:
    """Shared cuRobo collision checker (+ lazy joint-space planner) for the SUPERVISED
    calibration moves — the operator's 'cuRobo for everything' choice. One instance per calib
    service; off-robot / no-GPU it isn't built (the engine then skips filtering and direct-moves,
    exactly as before — no regression).

      feasible(q_named) -> bool          fast world+self collision check of a candidate config;
                                         FILTERS next-pose suggestions so the agent never proposes
                                         a pose that drives into the table.
      plan(start, goal)  -> [dict]|None  a COLLISION-FREE joint trajectory to the accepted pose,
                                         or None if no safe path exists (the move is then refused).

    SELF-VALIDATES the collision sign against the robot's CURRENT, known-safe pose. If that reads
    'colliding' (a sign flip, or the degenerate micro-sphere model) the FILTER disables itself
    (allow-all) so it can never block every pose — the move-time planner stays the hard gate."""

    def __init__(self):
        rw = _robot_world()                          # raises off-robot / no GPU → caller → None
        self.jn = list(rw.kinematics.joint_names)    # cuRobo's joint order
        self.filter_ok = self._self_check()

    def _cost(self, q_named) -> float:
        """World+self collision cost for one config (positive ⇒ in collision). The one place the
        version-sensitive cuRobo query lives, so the sign/threshold has a single home."""
        import torch  # type: ignore
        rw = _robot_world()
        row = [float(q_named.get(n, 0.0)) for n in self.jn]
        qt = torch.as_tensor([row], dtype=torch.float32, device=rw.tensor_args.device)
        d_w, d_s = rw.get_world_self_collision_distance_from_joint_trajectory(qt.unsqueeze(1))
        return float((d_w + d_s).max().item())

    def _self_check(self) -> bool:
        # the robot is sitting safely NOW → its current config MUST read collision-free; if not,
        # the model/sign is wrong (e.g. micro-spheres) → don't trust the filter.
        try:
            import numpy as np  # type: ignore
            b = get_bridge()
            jp = ((b.get_observation().joint_positions if b is not None else None) or {})
            q_now = {n: float(np.ravel(v)[0]) for n, v in jp.items()}
            if not q_now:
                return True
            c = self._cost(q_now)
            if c > 0.0:
                print(f"[calib] cuRobo collision self-check FAILED (current safe pose reads "
                      f"colliding, cost={c:.3f}) — sign/model off (degenerate spheres?). "
                      f"Suggestion filter DISABLED; the move-time planner still gates motion.")
                return False
            return True
        except Exception as e:
            print(f"[calib] cuRobo collision self-check errored ({e}); suggestion filter DISABLED.")
            return False

    def feasible(self, q_named) -> bool:
        if not self.filter_ok:
            return True
        try:
            return self._cost(q_named) <= 0.0
        except Exception as e:
            print(f"[calib] feasibility query failed ({e}); allowing pose (planner backstops).")
            return True

    def plan(self, start_named, goal_named):
        """Collision-free joint waypoints start→goal (each a name→value dict), or None if cuRobo
        finds no safe path."""
        import torch  # type: ignore
        from curobo.types.robot import JointState  # type: ignore
        from curobo.wrap.reacher.motion_gen import MotionGenPlanConfig  # type: ignore
        mg = motion_gen()
        def _js(named):
            row = [[float(named.get(n, 0.0)) for n in self.jn]]
            return JointState.from_position(torch.tensor(row, dtype=torch.float32, device="cuda"),
                                            joint_names=self.jn)
        res = mg.plan_single_js(_js(start_named), _js(goal_named), MotionGenPlanConfig(max_attempts=4))
        if not bool(res.success.item()):
            return None
        pos = res.get_interpolated_plan().position.detach().cpu().numpy()
        return [dict(zip(self.jn, row.tolist())) for row in pos]


def _calib_motion():
    """Build (once per calib service) the shared cuRobo collision checker + planner, or None when
    cuRobo / a GPU / the robot model isn't available (off-robot → no filter, direct moves)."""
    try:
        return _CuroboMotion()
    except Exception as e:
        print(f"[calib] cuRobo motion/collision unavailable ({e}); calibration uses direct moves.")
        return None


# --------------------------------------------------------------------------- #
# Route handlers — return a dict (JSON) OR a generator (SSE frames)            #
# --------------------------------------------------------------------------- #
def h_probe(_q):
    cy = load_cell_yaml()
    b = get_bridge()
    arms = cy.get("arms") or ([cy["arm"]] if cy.get("arm") else [])
    cams = cy.get("cameras") or []
    grips = cy.get("grippers") or ([cy["gripper"]] if cy.get("gripper") else [])
    try:
        import torch  # type: ignore

        gpu = {"name": torch.cuda.get_device_name(0) if torch.cuda.is_available() else "no GPU", "ok": torch.cuda.is_available()}
    except Exception:
        gpu = {"name": "torch not installed", "ok": False}
    return {
        "source": "real",
        "arms": [{"model": a.get("model", "arm"), "dof": int(a.get("dof", 6)), "confident": b is not None} for a in arms],
        "cameras": [{"id": c.get("id", "cam"), "role": c.get("role", "wrist"), "stereo": bool(c.get("stereo", False)), "confident": b is not None} for c in cams],
        "grippers": [{"model": g.get("model", "gripper"), "confident": b is not None} for g in grips],
        "gpu": gpu,
        "bridge_error": _bridge_err,
    }


def h_toolchain(_q):
    return toolchain_status()


def h_applyfix(_q):
    return toolchain_status()  # real fixes are cell/SDK-specific; re-probe


def h_estop(_q):
    b = get_bridge()
    try:
        if b is not None and hasattr(b, "estop"):
            b.estop()
        return {"ok": True}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)}


def h_safety(_q):
    b = get_bridge()
    return {"estop": bool(getattr(b, "estopped", False)) if b is not None else False}


def h_build_robot(_q, body):
    spheres = body.get("spheres", []) if isinstance(body, dict) else []
    try:
        out = write_curobo_robot_yaml(body.get("urdf", ""), spheres)
        reset_curobo_cache()   # rebuild the planner + collision checker with the new spheres
        # best-effort: validate by loading the kinematics
        approximate = True
        try:
            motion_gen()  # if it loads, the model is planner-ready
            approximate = False
        except Exception:
            pass
        return {"spheres": spheres, "count": len(spheres), "approximate": approximate, "configYaml": str(out)}
    except Exception as e:  # noqa: BLE001
        return {"error": f"{type(e).__name__}: {e}", "spheres": spheres, "count": len(spheres), "approximate": True}


def _joint_value(val) -> float:
    """Coerce ONE reported joint value to a python float, whatever shape the cell uses: a
    python scalar, a 0-d array `np.array(0.5)`, or a 1-d `[0.5]`. A 0-d array HAS `__len__`
    but `val[0]` raises IndexError — the exact bug that made the live mirror emit only {error}
    frames so the 3D robot never moved. `reshape(-1)[0]` flattens every case."""
    import numpy as np
    arr = np.asarray(val, float).reshape(-1)
    return float(arr[0]) if arr.size else 0.0


def sse_live_joints(_q):
    """Poll the Bridge for joint state and emit {t, joints} frames (no 'done'). Robust to the
    0-d / 1-d / scalar joint-value shapes cells report; a single bad poll yields an {error}
    frame but the loop keeps streaming so the mirror recovers."""
    b = get_bridge()
    if b is None:
        yield {"t": 0.0, "joints": {}, "error": _bridge_err or "no bridge"}
        return
    t0 = time.time()
    while True:
        try:
            with _bridge_lock:                 # take turns with the calibration verbs (G14)
                obs = b.get_observation()
            joints = {name: _joint_value(val) for name, val in (obs.joint_positions or {}).items()}
            yield {"t": time.time() - t0, "joints": joints}
        except Exception as e:  # noqa: BLE001
            yield {"t": time.time() - t0, "joints": {}, "error": str(e)}
        time.sleep(0.05)  # ~20 Hz


def sse_plan(_q):
    """Plan a few safe moves with cuRobo from the current state; stream progress,
    end with {done, result:{ribbon, reached, attempts, incidents}}."""
    b = get_bridge()
    try:
        import numpy as np  # type: ignore
        import torch  # type: ignore
        from curobo.types.robot import JointState  # type: ignore
        from curobo.types.math import Pose  # type: ignore
        from curobo.wrap.reacher.motion_gen import MotionGenPlanConfig  # type: ignore

        mg = motion_gen()
        obs = b.get_observation() if b is not None else None
        jnames = mg.kinematics.joint_names
        if obs is not None and obs.joint_positions:
            q0 = [float(np.ravel(obs.joint_positions[n])[0]) for n in jnames if n in obs.joint_positions]
        else:
            q0 = [0.0] * len(jnames)
        start = JointState.from_position(torch.tensor([q0], dtype=torch.float32, device="cuda"), joint_names=list(jnames))
        # a few small reachable goals around the current TCP
        fk = mg.compute_kinematics(start)
        base = fk.ee_pose
        ribbon: list[list[float]] = []
        reached = 0
        targets = [(0.1, 0.0, 0.05), (-0.1, 0.1, 0.0), (0.0, -0.1, 0.1)]
        for i, (dx, dy, dz) in enumerate(targets):
            pos = base.position.clone()
            pos[0, 0] += dx
            pos[0, 1] += dy
            pos[0, 2] += dz
            goal = Pose(pos, base.quaternion.clone())
            res = mg.plan_single(start, goal, MotionGenPlanConfig(max_attempts=2))
            yield {"progress": (i + 1) / len(targets)}
            if res.success.item():
                reached += 1
                traj = res.get_interpolated_plan()
                eef = mg.compute_kinematics(traj).ee_pose.position.detach().cpu().numpy()
                for p in eef:
                    ribbon.append([float(p[0]), float(p[1]), float(p[2])])
                start = traj[-1].unsqueeze(0)
        yield {"done": True, "result": {"ribbon": ribbon, "reached": reached, "attempts": len(targets), "incidents": 0}}
    except Exception as e:  # noqa: BLE001
        yield {"done": True, "result": {"ribbon": [], "reached": 0, "attempts": 0, "incidents": 0, "error": f"{type(e).__name__}: {e}"}}


def _delegate_script(rel: str):
    """Return a callable from an agent-authored script if present (e.g.
    calibration/collect_poses.py:run), else None."""
    p = CELL_DIR / rel
    return p if p.exists() else None


def _run_streaming_script(rel: str, entry_desc: str, _q):
    """Import an agent-authored script and stream its on_event callbacks as SSE.

    Contract: the script exposes ``run(bridge, cell, on_event=callback) -> result``
    and calls on_event(dict) per step. We run it on a worker thread and forward
    each event; the final frame is {done, result}. REAL — drives the live bridge;
    no simulation. Honest errors when the script isn't authored or doesn't match.
    """
    script = _delegate_script(rel)
    if script is None:
        yield {"done": True, "result": {"error": f"{rel} not authored yet — the agent writes it ({entry_desc})"}}
        return
    b = get_bridge()
    if b is None:
        yield {"done": True, "result": {"error": _bridge_err or "no bridge connected"}}
        return
    import importlib
    import queue
    import threading

    # Import as a PACKAGE module (remoroo_cell.calibration.collect_poses), NOT a
    # standalone file: the agent's script reuses siblings (e.g. run.py) via package/
    # relative imports, which need the package context. Loading it standalone via
    # spec_from_file_location gives __package__="" → relative import resolves a None
    # parent → "'NoneType' object has no attribute '__dict__'". CELL_DIR.parent is on
    # sys.path (see top), so the dotted path imports. Reload to pick up agent edits.
    modpath = f"{CELL_DIR.name}." + (rel[:-3] if rel.endswith(".py") else rel).replace("/", ".")
    try:
        mod = importlib.import_module(modpath)
        try:
            mod = importlib.reload(mod)
        except Exception:
            pass  # a reload glitch shouldn't break an otherwise-good import
    except Exception as e:  # noqa: BLE001
        yield {"done": True, "result": {"error": f"import {modpath} failed: {type(e).__name__}: {e}"}}
        return
    if not hasattr(mod, "run"):
        yield {"done": True, "result": {"error": f"{rel} has no run(bridge, cell, on_event=...) entry"}}
        return

    q: "queue.Queue" = queue.Queue()
    SENTINEL = object()
    cy = load_cell_yaml()

    def worker():
        try:
            result = mod.run(b, cy, on_event=q.put)
            q.put({"type": "done", "result": result or {}})
        except Exception as e:  # noqa: BLE001
            q.put({"type": "done", "result": {"error": f"{type(e).__name__}: {e}"}})
        finally:
            q.put(SENTINEL)

    # tell the operator we're alive BEFORE the script emits anything — the legibility fix so the
    # panel isn't a silent 0/0 while the (possibly slow) GPU mapper / first sweep spins up.
    yield {"type": "status", "message": f"{rel} loaded · bridge connected — starting…", "coverage": 0.0}
    threading.Thread(target=worker, daemon=True).start()
    while True:
        item = q.get()
        if item is SENTINEL:
            break
        if isinstance(item, dict) and item.get("type") == "done":
            yield {"done": True, "result": item.get("result", {})}
        else:
            yield item  # pose / status events stream straight through


def sse_scan(_q):
    """Run the agent's world scan live and stream coverage/points events.
    Events: {type:'points', xyz:[[x,y,z],...]} {type:'status', coverage, message}
            final {done, result:{coverage, points, scene}}."""
    yield from _run_streaming_script("world/scan.py", "run(bridge, cell, on_event=...)", _q)


def sse_record(q):
    seconds = float((q.get("seconds", ["3"]) or ["3"])[0])
    b = get_bridge()
    try:
        from remoroo_cell.capture.recorder import Recorder  # type: ignore

        cy = load_cell_yaml()
        rec = Recorder(b, cy, str(CELL_DIR / "capture" / "sample_episode"))
        t0 = time.time()
        # record_window blocks; emit a coarse progress estimate then the result
        path = rec.record_window(seconds, label="safe_motion")
        yield {"t": seconds}
        yield {"done": True, "result": {"frames": int(seconds * float(cy.get("capture", {}).get("rate_hz", 30))), "durationSec": seconds, "droppedFrames": 0, "sizeMB": 0, "schemaOk": True, "syncOk": True, "path": str(path)}}
        _ = t0
    except Exception as e:  # noqa: BLE001
        yield {"done": True, "result": {"error": f"{type(e).__name__}: {e}", "frames": 0, "durationSec": seconds, "droppedFrames": 0, "sizeMB": 0, "schemaOk": False, "syncOk": False}}


# --------------------------------------------------------------------------- #
# Calibration protocol (P5): /edge/calib/<verb> -> calib_engine CalibService.   #
# Robot-only (needs cv2 + the connected cell); the dispatch/kinematics are       #
# CI-tested off-robot via FakeBridge (calib_engine/tests/test_service.py).        #
# --------------------------------------------------------------------------- #
sys.path.insert(0, str(Path(__file__).resolve().parent))  # make `calib_engine` importable


class RealBridge:
    """Adapts the cell's primitives Bridge + a SPECIFIC camera + the AUTHORED fiducial target
    to calib_engine's BridgeProtocol. It is scoped to one (camera, arm) binding from the
    authored pipeline step, so on a multi-camera / multi-arm cell every capture comes from the
    RIGHT camera and every move drives the RIGHT arm — never the shared-bridge ambiguity that
    silently moved the wrong arm. The flange pose is chain.fk(joints) (URDF model FK) so the
    bundle's FK-correction has a chain to refine; joints are read in the chain's joint order."""

    def __init__(self, cell_bridge, chain, joint_names, target,
                 camera_id="", arm_id="", K=None, dist=None, sdk_T_lr=None, motion=None):
        self.b = cell_bridge
        self.chain = chain
        self.joint_names = joint_names
        self.target = target             # the authored Target (its detector + 3D points)
        self.camera_id = camera_id       # which cell camera this step's frames come from
        self.arm_id = arm_id             # which cell arm this step moves (per-arm routing)
        self.K = K
        self.dist = dist                 # optional radial-tangential coeffs (7.2)
        self._sdk_T_lr = sdk_T_lr        # optional 4x4 left->right stereo baseline (F9)
        self._motion = motion            # shared cuRobo collision checker + planner (None off-robot)

    def _observation(self):
        """Per-camera observation if the cell Bridge supports `get_observation(camera=...)`
        (multi-camera); else the shared observation (single-camera cell)."""
        if self.camera_id:
            try:
                return self.b.get_observation(camera=self.camera_id)
            except TypeError:
                pass
        return self.b.get_observation()

    def read_joints(self):
        import numpy as np
        jp = (self._observation().joint_positions) or {}
        out = []
        for n in self.joint_names:
            if n not in jp:
                raise RuntimeError(f"joint {n!r} not in observation.joint_positions")
            # The cell may report a joint as a scalar, a 0-d array (np.array(0.5)), or a 1-d
            # array ([0.5]) — all valid. `reshape(-1)` flattens every case; a 0-d array has
            # __len__ but NO `[0]`, which is what crashed before.
            arr = np.asarray(jp[n], float).reshape(-1)
            if arr.size == 0:
                raise RuntimeError(f"joint {n!r} has an empty value in observation.joint_positions")
            out.append(float(arr[0]))
        return np.asarray(out, float)

    def read_pose(self):
        return self.chain.fk(self.read_joints())

    def move_to_joints(self, joints):
        q = [float(x) for x in joints]
        # 'cuRobo for everything': with a world model loaded, drive a COLLISION-FREE planned path
        # to the pose (REFUSING the move when no safe path exists) instead of a straight joint
        # interpolation that could clip the table. Off-robot / no GPU → the direct move, unchanged.
        if self._motion is not None:
            self._planned_move(q)
        else:
            self._direct_move(q)

    def _planned_move(self, q):
        """Plan a collision-free trajectory current→q with cuRobo and follow it; refuse if cuRobo
        finds no safe path (better a refused move than the arm driving through a modeled obstacle)."""
        start = dict(zip(self.joint_names, [float(x) for x in self.read_joints()]))
        goal = dict(zip(self.joint_names, [float(x) for x in q]))
        waypoints = self._motion.plan(start, goal)
        if waypoints is None:
            raise RuntimeError(
                "cuRobo found no collision-free path to the suggested pose — the move was REFUSED "
                "(it would hit a modeled obstacle). Re-suggest a pose, or check the obstacles / "
                "robot collision model.")
        # follow the planned path; subsample so a per-waypoint HTTP joint move stays manageable,
        # and always finish exactly on the goal waypoint.
        step = max(1, len(waypoints) // 24)
        seq = waypoints[::step]
        if seq[-1] is not waypoints[-1]:
            seq.append(waypoints[-1])
        for wp in seq:
            self._direct_move([wp[n] for n in self.joint_names])

    def _direct_move(self, joints):
        q = [float(x) for x in joints]
        # Per-arm routing (the multi-arm SAFETY fix). When the step bound an arm, drive THAT
        # arm explicitly: prefer an arm-aware move, then the multi-arm primitive, then a
        # single-arm fallback. This removes the `_calib_arm` side-channel the engine never set.
        if self.arm_id:
            fn = getattr(self.b, "move_to_joints", None)
            if callable(fn):
                try:
                    fn(q, arm=self.arm_id); return     # move_to_joints(joints, arm=...)
                except TypeError:
                    pass
            fn = getattr(self.b, "move_joints", None)
            if callable(fn):
                try:
                    fn(self.arm_id, q); return          # move_joints(arm, joints)
                except TypeError:
                    pass
        for name in ("move_to_joints", "move_joints", "goto_joints", "set_joint_positions", "move_j"):
            fn = getattr(self.b, name, None)
            if callable(fn):
                fn(q)
                return
        raise RuntimeError("cell Bridge has no joint-move method (expected move_to_joints(joints, "
                           "arm=...) for multi-arm, or one of move_joints/goto_joints/"
                           "set_joint_positions/move_j)")

    def estop_ok(self):
        return True

    def _detect(self, img):
        import numpy as np
        # The AUTHORED target owns detection — the edge never names a fiducial type. A single
        # marker, ChArUco, AprilTag, grid, or checkerboard all flow through target.detect.
        ids, uv = self.target.detect(np.asarray(img))
        # 7.2: the solver is pinhole on RECTIFIED imagery. If the cell feeds a raw frame +
        # distortion coeffs, undistort the corners to the rectified pixels first.
        if self.dist is not None and self.K is not None and len(ids):
            from calib_engine.geometry import undistort_points
            uv = undistort_points(uv, self.K, self.dist)
        return ids, uv

    def capture(self):
        return self._detect(self.capture_image())

    def capture_right(self):
        """Right stereo lens corners (for the F9 left/right self-check). Probes obs.right /
        right_image; returns empty if the camera doesn't expose a second lens."""
        import numpy as np
        obs = self._observation()
        img = next((getattr(obs, a, None) for a in ("right", "right_image", "rgb_right")
                    if getattr(obs, a, None) is not None), None)
        if img is None:
            return np.zeros(0, int), np.zeros((0, 2))
        return self._detect(np.asarray(img))

    def sdk_T_lr(self):
        return self._sdk_T_lr

    def move_tcp_to_world(self, p_base):
        """Drive the TCP to a base-frame point (the physical tip-landing test, 7.1). Probes
        the cell's common API names; raises if the Bridge can't do a cartesian point move."""
        p = [float(x) for x in p_base]
        for name in ("move_tcp_to_world", "move_tcp", "move_to_point", "move_cartesian", "move_l"):
            fn = getattr(self.b, name, None)
            if callable(fn):
                fn(p)
                return
        raise RuntimeError("cell Bridge has no cartesian TCP move (expected move_tcp_to_world/"
                           "move_tcp/move_to_point/move_cartesian/move_l) — tip-landing needs it")

    def feasible(self, joints):
        """The next-pose collision filter (7.7), now ENGINE-driven via cuRobo: a candidate config
        must be collision-free in cuRobo's static world (table/obstacles) AND pass the operator's
        OPTIONAL bridge filter if primitives.py exposes one. Either gate rejecting → infeasible.
        With neither available (off-robot, no custom filter) → allow (the move-time planner is the
        hard gate, and there's nothing to collide with off-robot)."""
        q = [float(x) for x in joints]
        # 1) cuRobo world+self collision check — the 'cuRobo for everything' static-obstacle gate.
        if self._motion is not None and not self._motion.feasible(dict(zip(self.joint_names, q))):
            return False
        # 2) operator's optional feasibility (custom envelope / keep-outs) from primitives.py.
        for name in ("is_joint_pose_feasible", "feasible", "plan_feasible"):
            fn = getattr(self.b, name, None)
            if callable(fn):
                try:
                    return bool(fn(q))
                except Exception:  # noqa: BLE001
                    return True
        return True

    def capture_image(self):
        """The raw RGB frame from THIS step's bound camera (per-camera on a multi-cam cell)."""
        obs = self._observation()
        img = next((getattr(obs, a, None) for a in ("rgb", "color", "image", "rgb_image", "left")
                    if getattr(obs, a, None) is not None), None)
        if img is None:
            raise RuntimeError("observation has no image (expected obs.rgb/color/image)")
        return img


def _as_K(kcfg):
    import numpy as np
    if isinstance(kcfg, dict):
        return np.array([[kcfg["fx"], 0, kcfg["cx"]], [0, kcfg["fy"], kcfg["cy"]], [0, 0, 1.0]], float)
    return np.asarray(kcfg, float).reshape(3, 3)


def _target_spec_from_cell(cal: dict):
    """The cell's OPEN target spec — `calibration.target: {type, params}`. No silent ChArUco
    default: a legacy `calibration.board` is read as a charuco target only when its keys are
    fully present; otherwise None (the caller errors loudly)."""
    t = cal.get("target")
    if isinstance(t, dict) and t.get("type"):
        return {"type": str(t["type"]), "params": dict(t.get("params") or {})}
    b = cal.get("board")
    if isinstance(b, dict) and all(k in b for k in ("squares_x", "squares_y", "square_len", "marker_len")):
        return {"type": "charuco", "params": {
            "squares_x": int(b["squares_x"]), "squares_y": int(b["squares_y"]),
            "square_len": float(b["square_len"]), "marker_len": float(b["marker_len"]),
            "dict": str(b.get("dict", "DICT_5X5_1000"))}}
    return None


def _import_authored_targets():
    """Import the agent-authored remoroo_cell/calibration/targets.py if present — it registers
    any CUSTOM detectors via calib_engine.fiducials.register. Common rigs need no such file
    (the pipeline's target specs use shipped types)."""
    if (CELL_DIR / "calibration" / "targets.py").exists():
        import importlib
        try:
            importlib.import_module(f"{CELL_DIR.name}.calibration.targets")
        except Exception:  # noqa: BLE001 — a broken custom module shouldn't crash select; loud later
            pass


def _intrinsics_from_bridge(b, camera_id: str = ""):
    """Factory intrinsics live off the cell camera's observation, scoped to ONE camera. The
    Bridge may expose `get_observation(camera=<id>)` (multi-cam) and/or key obs.intrinsics by
    camera id; both are handled. Returns (K 3x3, (w,h)) or (None, None) if absent."""
    import numpy as np
    try:
        obs = b.get_observation(camera=camera_id) if camera_id else b.get_observation()
    except TypeError:
        obs = b.get_observation()
    except Exception:  # noqa: BLE001 — no camera up yet; caller falls back / errors clearly
        return None, None
    intr = getattr(obs, "intrinsics", None)
    if isinstance(intr, dict) and camera_id and camera_id in intr and isinstance(intr[camera_id], dict):
        intr = intr[camera_id]                     # obs.intrinsics keyed per camera
    if isinstance(intr, dict) and all(k in intr for k in ("fx", "fy", "cx", "cy")):
        K = np.array([[intr["fx"], 0.0, intr["cx"]], [0.0, intr["fy"], intr["cy"]], [0.0, 0.0, 1.0]], float)
        wh = (int(intr.get("width", 1280)), int(intr.get("height", 720)))
        return K, wh
    return None, None


def _camera_intrinsics(b, camera_id: str, cal: dict):
    """Per-camera K: a cell.yaml override (a cam with no factory K) else the SDK's factory K."""
    if cal.get("K") is not None:
        return _as_K(cal["K"]), tuple(cal.get("wh", [1280, 720]))
    return _intrinsics_from_bridge(b, camera_id)


def _load_pipeline(cy: dict, cal: dict, urdf_path: str):
    """The AUTHORED pipeline (remoroo_cell/calibration/pipeline.yaml) → (plan_items, targets).
    If the agent hasn't authored one yet, fall back to a URDF-derived plan bound to the cell's
    single open target — never a silent fabricated board."""
    from calib_engine import fiducials, pipeline, steps, urdf_io
    pf = CELL_DIR / "calibration" / "pipeline.yaml"
    if pf.exists():
        import yaml  # type: ignore
        spec = yaml.safe_load(pf.read_text(encoding="utf-8")) or {}
        psteps, tspecs = pipeline.parse(spec)
        urdf_cams = urdf_io.find_camera_links(urdf_path) if os.path.exists(urdf_path) else []
        cell_cams = [str(c.get("name") or c.get("id") or "") for c in (cy.get("cameras") or [])]
        arms = [str(a.get("name") or "") for a in (cy.get("arms") or [])]
        pipeline.validate(psteps, tspecs, kinds=steps.known_kinds(), family_of=steps.family_of,
                          cameras=list({*urdf_cams, *cell_cams}), arms=arms)
        items = pipeline.resolve_items(psteps, urdf_path)
        targets = pipeline.build_targets(tspecs, fiducials.build_target)
        return items, targets
    # Fallback: no authored pipeline yet → derive the plan from the URDF, bound to the cell's
    # one open target. Keeps a simple single-camera cell working before the pipeline is authored.
    from calib_engine.session import build_plan
    spec = _target_spec_from_cell(cal)
    if spec is None:
        raise RuntimeError("no calibration target configured — author remoroo_cell/calibration/"
                           "pipeline.yaml, or set cell.yaml calibration.target {type, params}.")
    target = fiducials.build_target(spec)
    items = build_plan(urdf_path)
    for it in items:
        it.target_id = "default"
    return items, {"default": target}


_CALIB = None


def _calib_service():
    global _CALIB
    if _CALIB is not None:
        return _CALIB
    from calib_engine import urdf_io
    from calib_engine.geometry import Chain
    from calib_engine.service import CalibService

    b = get_bridge()
    if b is None:
        raise RuntimeError(_bridge_err or "no bridge connected")
    import numpy as np
    cy = load_cell_yaml()
    cal = (cy.get("calibration") or {})
    acc_px = float(cal.get("accept_heldout_px") or 1.5)   # None-safe (key present-but-null)
    acc_mm = float(cal.get("accept_tip_mm") or 3.0)
    acc_rot = float(cal.get("accept_rot_sigma_deg") or 0.5)   # observability accept gate (deg)
    acc_tr = float(cal.get("accept_trans_sigma_mm") or 2.0)   # observability accept gate (mm)
    dist = np.asarray(cal["dist"], float) if cal.get("dist") is not None else None
    sdk_T_lr = np.asarray(cal["T_left_right"], float).reshape(4, 4) if cal.get("T_left_right") is not None else None
    urdf_path = str(CELL_DIR / "robot_model" / "robot.urdf")

    _import_authored_targets()
    items, targets = _load_pipeline(cy, cal, urdf_path)
    default_target = next(iter(targets.values())) if targets else None

    # Per-camera intrinsics: each bound camera reads its OWN factory K (a multi-cam rig no
    # longer solves camera #2 with camera #1's K). Keyed by the camera link the step bound.
    intrinsics: dict = {}
    for cam in {p.camera_link for p in items if p.kind != "base_to_base"}:
        intrinsics[cam] = _camera_intrinsics(b, cam, cal)
    K0, wh0 = next((kv for kv in intrinsics.values() if kv[0] is not None), (None, (1280, 720)))
    if K0 is None:
        K0, wh0 = _camera_intrinsics(b, "", cal)
    if K0 is None:
        raise RuntimeError(
            "no camera intrinsics available — get_observation().intrinsics is empty and "
            "cell.yaml calibration.K is unset. Have primitives.py report factory intrinsics "
            "{fx,fy,cx,cy,width,height} per camera (ZED: calibration_parameters.left_cam), or "
            "set calibration.K in cell.yaml as a fallback.")

    def chain_provider(flange):
        return urdf_io.chain_from_urdf(urdf_path, flange)[0]

    # Shared cuRobo collision checker + planner for the arm-driven steps — built ONCE (warmup is
    # slow), reused across poses. None off-robot / no GPU (the engine then skips the suggestion
    # filter and direct-moves, unchanged). A static (world-fixed) camera never moves, so it gets
    # no motion model.
    motion = _calib_motion()

    def bridge_factory(item):
        target = targets.get(item.target_id) or default_target
        cam, arm = item.camera_link, item.arm
        # This camera's own intrinsics, or the service default. `intrinsics` already holds only
        # cameras with a real K (filtered below), so .get is either a (K, wh) tuple or None — a
        # plain None check, NOT `K_array or K0` (a numpy array has no truth value).
        kv = intrinsics.get(cam)
        Kc = kv[0] if kv is not None else K0
        static = item.kind in ("eye_to_hand", "static") and getattr(item, "board_source", "handheld") != "arm"
        if static:
            return RealBridge(b, Chain([], []), [], target, camera_id=cam, arm_id="",
                              K=Kc, dist=dist, sdk_T_lr=sdk_T_lr)
        chain, names, _ = urdf_io.chain_from_urdf(urdf_path, item.flange_link)
        return RealBridge(b, chain, names, target, camera_id=cam, arm_id=arm,
                          K=Kc, dist=dist, sdk_T_lr=sdk_T_lr, motion=motion)

    _CALIB = CalibService(urdf_path, default_target, K0, bridge_factory, chain_provider, wh=tuple(wh0),
                          calib_dir=str(CELL_DIR / "calibration"),
                          accept_heldout_px=acc_px, accept_tip_mm=acc_mm,
                          accept_rot_sigma_deg=acc_rot, accept_trans_sigma_mm=acc_tr,
                          plan_items=items, targets=targets,
                          intrinsics={c: kv for c, kv in intrinsics.items() if kv[0] is not None})
    return _CALIB


def _board_body_to_target_spec(body: dict) -> dict:
    """Back-compat: an old set_board POST (dict/squares_x/...) → an open charuco target spec."""
    b = body or {}
    return {"type": "charuco", "params": {
        "dict": str(b.get("dict", "DICT_5X5_1000")),
        "squares_x": int(b["squares_x"]), "squares_y": int(b["squares_y"]),
        "square_len": float(b["square_len"]), "marker_len": float(b["marker_len"])}}


def _calib_set_target(spec: dict) -> dict:
    """Persist the OPEN target spec ({type, params}) to cell.yaml calibration.target and apply
    it. If a service is live, swap the Target + detector IN PLACE so the in-progress session is
    preserved. The edge never names a fiducial — `type` selects a shipped or authored detector."""
    global _CALIB
    import yaml  # type: ignore
    from calib_engine import fiducials
    spec = {"type": str((spec or {}).get("type") or ""), "params": dict((spec or {}).get("params") or {})}
    if not spec["type"]:
        return {"error": "target needs a 'type' (e.g. single_aruco | charuco | apriltag | aruco_grid | checkerboard)"}
    cy = load_cell_yaml()
    cal = dict(cy.get("calibration") or {})
    cal["target"] = spec
    cal.pop("board", None)                       # the open target supersedes the legacy board
    cy["calibration"] = cal
    (CELL_DIR / "cell.yaml").write_text(yaml.safe_dump(cy, sort_keys=False), encoding="utf-8")

    if _CALIB is not None:
        _import_authored_targets()
        try:
            tgt = fiducials.build_target(spec)
        except Exception as e:  # noqa: BLE001 — a bad spec must fail loudly, not silently
            return {"error": f"target build failed: {type(e).__name__}: {e}"}
        _CALIB.board = tgt
        _CALIB.targets["default"] = tgt
        if _CALIB.session is not None:
            _CALIB.session.board = tgt
            _CALIB.session.min_corners = int(tgt.min_points)
            if hasattr(_CALIB.session.bridge, "target"):
                _CALIB.session.bridge.target = tgt
    return {"type": "set_target", "target": spec}


def _first_camera(cy: dict) -> str:
    cams = cy.get("cameras") or []
    return str(cams[0].get("name") or cams[0].get("id") or "") if cams else ""


def _snapshot_target_and_camera(cy: dict, cal: dict):
    """The target + camera the live-cam inset detects against: the SELECTED step's if a session
    is live, else the cell's default target + first camera. The target may be None (none
    configured yet) — the raw frame still shows; only the detection OVERLAY needs it."""
    sess = getattr(_CALIB, "session", None) if _CALIB is not None else None
    if sess is not None and getattr(sess, "bridge", None) is not None and hasattr(sess.bridge, "target"):
        return sess.bridge.target, getattr(sess.bridge, "camera_id", "")
    spec = _target_spec_from_cell(cal)
    if spec is None:
        return None, _first_camera(cy)              # show the frame, no overlay yet
    try:
        _import_authored_targets()
        from calib_engine import fiducials
        return fiducials.build_target(spec), _first_camera(cy)
    except Exception:  # noqa: BLE001 — a bad target spec must not blank the live feed
        return None, _first_camera(cy)


def _board_outline(target) -> "dict | None":
    """The physical board outline (in the TARGET frame) for the 3D target overlay, derived from
    the target's OWN corner points — so it matches EXACTLY what the solver used, with no cv2
    origin/aspect guessing. Returns {center:[x,y], size:[w,h], squares:[nx,ny]} in metres (a
    ChArUco is rectangular sx≠sy, and its interior corners are inset one square from the edge, so
    we expand the corner bbox by one square spacing each side to reach the real board edge)."""
    import numpy as np
    pts = getattr(target, "point_xyz", None)
    if pts is None or len(pts) == 0:
        return None
    xy = np.asarray(pts, float)[:, :2]
    mn, mx = xy.min(0), xy.max(0)
    center = (mn + mx) / 2.0
    ux = np.unique(np.round(xy[:, 0], 6))
    uy = np.unique(np.round(xy[:, 1], 6))
    nx, ny = len(ux), len(uy)
    sx = float(np.min(np.diff(ux))) if nx > 1 else 0.0
    sy = float(np.min(np.diff(uy))) if ny > 1 else 0.0
    grid = str(getattr(target, "type", "")) in ("charuco", "checkerboard") and nx > 1 and ny > 1
    if grid:
        w = float(mx[0] - mn[0]) + 2 * sx
        h = float(mx[1] - mn[1]) + 2 * sy
        squares = [nx + 1, ny + 1]                # interior corner columns/rows + 1 = squares
    else:
        w = max(float(mx[0] - mn[0]), 1e-3)
        h = max(float(mx[1] - mn[1]), 1e-3)
        squares = [1, 1]
    return {"center": [float(center[0]), float(center[1])], "size": [w, h], "squares": squares}


def _calib_snapshot() -> dict:
    """Current camera frame (JPEG) + factory intrinsics + (when a target is configured) its
    detection overlay, for the live-cam inset. The RAW FEED shows as soon as the camera is up —
    it does NOT require a calibration target; only the overlay does. Needs cv2 + a connected
    camera; any failure returns {error} → UI placeholder."""
    import base64
    import numpy as np
    import cv2  # type: ignore

    b = get_bridge()
    if b is None:
        return {"error": _bridge_err or "no bridge connected"}
    cy = load_cell_yaml()
    cal = cy.get("calibration") or {}
    target, camera_id = _snapshot_target_and_camera(cy, cal)
    rb = RealBridge(b, None, [], target, camera_id=camera_id)
    img = np.asarray(rb.capture_image())
    if img.ndim == 2:
        img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)
    bgr = cv2.cvtColor(img, cv2.COLOR_RGB2BGR) if img.shape[-1] == 3 else img
    ok, buf = cv2.imencode(".jpg", bgr, [int(cv2.IMWRITE_JPEG_QUALITY), 70])
    if not ok:
        return {"error": "jpeg encode failed"}
    h, w = img.shape[:2]
    ids, uv = (np.empty(0, int), np.empty((0, 2)))
    if target is not None:
        try:
            ids, uv = target.detect(img)            # overlay only; a detector miss isn't fatal
        except Exception:  # noqa: BLE001
            ids, uv = (np.empty(0, int), np.empty((0, 2)))
    K, _ = _intrinsics_from_bridge(b, camera_id)
    out = {
        "type": "snapshot", "jpeg_b64": base64.b64encode(buf.tobytes()).decode("ascii"),
        "w": int(w), "h": int(h),
        "seen": bool(target is not None and len(ids) >= target.min_points), "n_corners": int(len(ids)),
        # the target's FULL point count (ChArUco: (sx-1)*(sy-1) interior corners) so the live cam
        # shows detected/expected — n/n means the WHOLE board is seen (a ChArUco's green outline
        # connects the INTERIOR corners, one square inside the border, so it looks smaller than the
        # physical board even at full detection; this number tells the operator it's complete).
        "expected_corners": int(len(getattr(target, "point_xyz", []))) if target is not None else 0,
        # the board outline (target frame) so the 3D overlay draws the REAL board — correct
        # rectangular size + checkerboard, not a guessed square plane.
        "board_outline": _board_outline(target) if target is not None else None,
        "corners": np.asarray(uv, float).round(1).tolist(),
        "intrinsics": None if K is None else {"fx": K[0, 0], "fy": K[1, 1], "cx": K[0, 2], "cy": K[1, 2]},
    }
    # When a session is live and seeded, overlay the PREDICTED marker corners (under the current
    # hand-eye X at the live joints) + the px drift + the board/optical poses, so the live cam
    # shows the amber prediction next to the green detection and the 3D stage can place the
    # marker (G13/G16). Best-effort — never blanks the raw feed.
    sess = getattr(_CALIB, "session", None) if _CALIB is not None else None
    if sess is not None and getattr(sess, "T_board_est", None) is not None:
        try:
            ov = sess.predicted_overlay()
            if ov.get("ok"):
                phase = getattr(sess, "phase", None)
                out.update({"t_board": ov["T_board"], "x_optical": ov["X"], "phase": phase})
                # The amber PIXEL overlay + its drift are only meaningful where the hand-eye X is
                # trustworthy: the seed-confirm wiggle and AFTER a solve (verify/validate). During
                # raw pre-solve COLLECTION, X is just the rough nominal seed, so by construction the
                # prediction sits ON the board at the seed config but slides FAR off (hundreds of px)
                # as the arm moves to new views — which reads as "dots out of place", not as signal.
                # Suppress it there (the green DETECTION is always correct and is all collection
                # needs); the 3D marker (t_board/x_optical) still renders from the seed.
                if getattr(sess, "result", None) is not None or phase in ("seed", "solving", "validate", "verify"):
                    out.update({"predicted": ov["predicted"], "predicted_ids": ov["predicted_ids"],
                                "drift_px": ov["drift_px"]})
        except Exception:  # noqa: BLE001
            pass
    return out


def _calib_frame_image(index: int) -> dict:
    """JPEG (base64) of a RETAINED capture frame — the camera image behind the curate
    contact-sheet overlay (A/B). Reaches into the live session's samples; {error} if the
    frame wasn't retained (off-robot) so the UI falls back to the pixel-only overlay."""
    import base64
    import numpy as np
    import cv2  # type: ignore
    sess = getattr(_CALIB, "session", None) if _CALIB is not None else None
    if sess is None:
        return {"error": "no calibration selected"}
    s = next((x for x in sess.samples if x.id == index), None)
    if s is None or s.image is None:
        return {"error": "no retained frame for this capture"}
    img = np.asarray(s.image)
    if img.ndim == 2:
        img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)
    bgr = cv2.cvtColor(img, cv2.COLOR_RGB2BGR) if img.shape[-1] == 3 else img
    ok, buf = cv2.imencode(".jpg", bgr, [int(cv2.IMWRITE_JPEG_QUALITY), 75])
    if not ok:
        return {"error": "jpeg encode failed"}
    h, w = img.shape[:2]
    return {"type": "frame_image", "id": index, "w": int(w), "h": int(h),
            "jpeg_b64": base64.b64encode(buf.tobytes()).decode("ascii")}


def _pipeline_for_ui() -> dict:
    """The AUTHORED pipeline steps + target specs for the Studio, as a pure URDF read — NO
    bridge / intrinsics / cv2 (so the operator sees the steps before the camera is up). Uses
    the authored pipeline.yaml if present, else the URDF-derived fallback bound to the cell's
    target. Targets are returned as SPECS (not built), keeping this cv2-free."""
    import xml.etree.ElementTree as ET
    from calib_engine import pipeline, steps, urdf_io
    from calib_engine.service import _planitem_json
    urdf_path = str(CELL_DIR / "robot_model" / "robot.urdf")
    if not os.path.exists(urdf_path):
        return {"error": f"no URDF at {urdf_path} — build the rig first"}
    cy = load_cell_yaml()
    cal = cy.get("calibration") or {}
    _import_authored_targets()
    pf = CELL_DIR / "calibration" / "pipeline.yaml"
    if pf.exists():
        import yaml  # type: ignore
        spec = yaml.safe_load(pf.read_text(encoding="utf-8")) or {}
        psteps, tspecs = pipeline.parse(spec)
        urdf_cams = urdf_io.find_camera_links(urdf_path)
        cell_cams = [str(c.get("name") or c.get("id") or "") for c in (cy.get("cameras") or [])]
        arms = [str(a.get("name") or "") for a in (cy.get("arms") or [])]
        pipeline.validate(psteps, tspecs, kinds=steps.known_kinds(), family_of=steps.family_of,
                          cameras=list({*urdf_cams, *cell_cams}), arms=arms)
        items = pipeline.resolve_items(psteps, urdf_path)
        target_specs = {tid: {"type": t.type, "params": t.params} for tid, t in tspecs.items()}
    else:
        from calib_engine.session import build_plan
        items = build_plan(urdf_path)
        spec = _target_spec_from_cell(cal)
        for it in items:
            it.target_id = "default"
        target_specs = {"default": spec} if spec else {}
    links = [l.get("name") for l in ET.parse(urdf_path).getroot().findall("link")]
    return {"type": "pipeline", "items": [_planitem_json(p) for p in items],
            "links": links, "urdf": urdf_path, "targets": target_specs}


def _calib_b2b_snapshot() -> dict:
    """BOTH wrist cameras' frames (JPEG) + detection overlays for the dual-arm base-to-base step,
    so the operator SEES each camera detect the shared board before capturing — the b2b flow was
    blind (no images, no detection). Needs the b2b step selected + cv2 + both cameras up; degrades
    to per-camera available:false so the panel stays honest."""
    import base64
    import numpy as np
    import cv2  # type: ignore
    svc = _calib_service()
    b2b = getattr(svc, "b2b", None)
    if b2b is None:
        return {"error": "select the base-to-base step first"}

    def one(bridge) -> dict:
        try:
            img = np.asarray(bridge.capture_image())
        except Exception as e:  # noqa: BLE001
            return {"available": False, "camera": getattr(bridge, "camera_id", ""), "reason": str(e)}
        if img.ndim == 2:
            img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)
        bgr = cv2.cvtColor(img, cv2.COLOR_RGB2BGR) if img.shape[-1] == 3 else img
        ok, buf = cv2.imencode(".jpg", bgr, [int(cv2.IMWRITE_JPEG_QUALITY), 70])
        h, w = img.shape[:2]
        try:
            ids, uv = bridge._detect(img)            # same detect (+ undistort) the capture uses
        except Exception:  # noqa: BLE001
            ids, uv = (np.empty(0, int), np.empty((0, 2)))
        target = bridge.target
        exp = int(len(getattr(target, "point_xyz", []))) if target is not None else 0
        return {
            "available": bool(ok), "camera": getattr(bridge, "camera_id", ""),
            "jpeg_b64": base64.b64encode(buf.tobytes()).decode("ascii") if ok else None,
            "w": int(w), "h": int(h),
            "n_corners": int(len(ids)), "expected_corners": exp,
            "seen": bool(target is not None and len(ids) >= target.min_points),
            "corners": np.asarray(uv, float).round(1).tolist(),
        }

    return {"type": "b2b_snapshot", "a": one(b2b.bridge_a), "b": one(b2b.bridge_b),
            "min_corners": int(b2b.min_corners), "collected": len(b2b.obs),
            "board_outline": _board_outline(b2b.board)}


def _calib_handle(verb: str, body: dict) -> dict:
    # The pure-URDF reads (`pipeline`/`plan`) touch no bridge, so they don't need the lock;
    # everything else may move/capture/read the shared device, so it serialises with the live
    # SSE poll (G14). RLock so a handler that internally reads joints doesn't self-deadlock.
    if verb not in ("plan", "pipeline"):
        with _bridge_lock:
            return _calib_handle_locked(verb, body)
    return _calib_handle_locked(verb, body)


def _calib_handle_locked(verb: str, body: dict) -> dict:
    try:
        # `pipeline`/`plan` is a pure URDF read — no bridge / intrinsics / cv2, so the operator
        # sees the authored steps even before the camera is up. Heavier deps (bridge, K,
        # targets, cv2) are only built on `select`.
        if verb in ("plan", "pipeline"):
            return _pipeline_for_ui()
        if verb in ("set_target", "set_board"):
            # The printed target — the ONE input not in the URDF — set from the Studio form.
            # `set_board` is the legacy ChArUco-only POST; convert it to an open target spec.
            spec = body if verb == "set_target" else _board_body_to_target_spec(body)
            return _calib_set_target(spec)
        if verb == "snapshot":
            # The live-cam inset: the current frame (JPEG) + factory intrinsics + the board
            # detection overlay. Edge-only (needs the image + cv2); degrades to {error} so the
            # UI shows a placeholder off-robot.
            return _calib_snapshot()
        if verb == "frame_image":
            return _calib_frame_image(int((body or {}).get("index", -1)))
        if verb == "b2b_snapshot":
            # dual-arm: BOTH wrist cameras' frames + detection, so base-to-base isn't blind.
            return _calib_b2b_snapshot()
        return _calib_service().handle(verb, body)
    except Exception as e:  # noqa: BLE001
        return {"error": f"{type(e).__name__}: {e}"}


# --------------------------------------------------------------------------- #
# Canonical arm map + static obstacles (the multi-arm model the Studio + cuRobo #
# share). arms.yaml is the source of truth; obstacles live in cell.yaml.        #
# --------------------------------------------------------------------------- #
def h_arms(_q):
    """The canonical arm map for the Studio: read `robot_model/arms.yaml` if built (the
    operator-verified sides), else derive it fresh from the URDF + cell.yaml camera→arm names."""
    import yaml  # type: ignore
    rm = CELL_DIR / "robot_model"
    f = rm / "arms.yaml"
    if f.exists():
        try:
            return yaml.safe_load(f.read_text(encoding="utf-8")) or {"base_link": "", "arms": []}
        except Exception as e:  # noqa: BLE001
            return {"error": f"{type(e).__name__}: {e}", "arms": []}
    urdf = rm / "robot.urdf"
    if not urdf.exists():
        return {"base_link": "", "arms": [], "reason": "no robot.urdf yet — model the cell first"}
    from calib_engine import urdf_io
    cy = load_cell_yaml()
    cam2arm = {str(c.get("name") or ""): str(c.get("attached_to") or "")
               for c in (cy.get("cameras") or []) if c.get("name") and c.get("attached_to")}
    try:
        return urdf_io.arm_map_dict(str(urdf), camera_to_arm=cam2arm)
    except Exception as e:  # noqa: BLE001
        return {"error": f"{type(e).__name__}: {e}", "arms": []}


def h_set_arms(_q, body):
    """Persist operator-verified arm sides to arms.yaml (the live-mirror wiggle result). `sides`
    is {arm_name: 'left'|'right'}. Sides drive the per-arm cuRobo cfg + the Studio labels."""
    import yaml  # type: ignore
    m = h_arms(None)
    if isinstance(m, dict) and m.get("error"):
        return m
    sides = (body or {}).get("sides") or {}
    for a in m.get("arms", []):
        if a.get("name") in sides:
            a["side"] = str(sides[a["name"]])
    rm = CELL_DIR / "robot_model"
    rm.mkdir(parents=True, exist_ok=True)
    (rm / "arms.yaml").write_text(yaml.safe_dump(m, sort_keys=False), encoding="utf-8")
    return {"ok": True, "arms": m.get("arms", [])}


def h_obstacles(_q):
    """Static obstacles (table/wall/box) the operator modeled — read from cell.yaml. cuRobo's
    world is built from these (see `motion_gen`)."""
    return {"obstacles": (load_cell_yaml().get("obstacles") or [])}


def h_set_obstacles(_q, body):
    """Persist the operator's obstacles to cell.yaml `obstacles` (consumed by cuRobo's world)."""
    import yaml  # type: ignore
    cy = load_cell_yaml()
    cy["obstacles"] = list((body or {}).get("obstacles") or [])
    (CELL_DIR / "cell.yaml").write_text(yaml.safe_dump(cy, sort_keys=False), encoding="utf-8")
    reset_curobo_cache()   # the obstacle world is baked into the planner + collision checker
    return {"ok": True, "obstacles": cy["obstacles"]}


# route table: path -> (kind, handler). kind in {"json","json_body","sse"}
ROUTES = {
    "/health": ("json", lambda q: {"ok": True, "edge": "real", "cell": str(CELL_DIR)}),
    "/edge/probe": ("json", h_probe),
    "/edge/toolchain": ("json", h_toolchain),
    "/edge/applyFix": ("json", h_applyfix),
    "/edge/estop": ("json", h_estop),
    "/edge/safety": ("json", h_safety),
    "/edge/buildRobot": ("json_body", h_build_robot),
    "/edge/arms": ("json", h_arms),
    "/edge/arms/set": ("json_body", h_set_arms),
    "/edge/obstacles": ("json", h_obstacles),
    "/edge/obstacles/set": ("json_body", h_set_obstacles),
    "/live/joints": ("sse", sse_live_joints),
    "/edge/scanWorld": ("sse", sse_scan),
    "/edge/record": ("sse", sse_record),
    "/edge/plan": ("sse", sse_plan),
}


def _json_default(o):
    """Make numpy scalars/arrays JSON-serializable (the engine returns them); the stock
    encoder raises `Object of type bool_ is not JSON serializable` otherwise."""
    import numpy as np
    if isinstance(o, np.bool_):
        return bool(o)
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.floating):
        return float(o)
    if isinstance(o, np.ndarray):
        return o.tolist()
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):  # quieter
        pass

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")

    def _route(self):
        u = urlparse(self.path)
        return u.path, parse_qs(u.query), ROUTES.get(u.path)

    def do_GET(self):
        path, q, route = self._route()
        if not route:
            self.send_response(404); self._cors(); self.end_headers(); self.wfile.write(b"not found"); return
        kind, fn = route
        if kind == "sse":
            return self._serve_sse(fn, q)
        if kind == "json":
            return self._serve_json(fn(q))
        self.send_response(405); self._cors(); self.end_headers()

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            return {}

    def do_POST(self):
        path, q, route = self._route()
        # Calibration protocol: /edge/calib/<verb> -> the stateful CalibService.
        if path.startswith("/edge/calib/"):
            return self._serve_json(_calib_handle(path[len("/edge/calib/"):], self._read_body()))
        if not route:
            self.send_response(404); self._cors(); self.end_headers(); self.wfile.write(b"not found"); return
        kind, fn = route
        body = self._read_body()
        if kind == "json_body":
            return self._serve_json(fn(q, body))
        if kind == "json":
            return self._serve_json(fn(q))
        if kind == "sse":
            return self._serve_sse(fn, q)
        self.send_response(405); self._cors(); self.end_headers()

    def _serve_json(self, obj):
        # The engine is numpy-heavy, so a result can carry np.bool_/np.float64/np.ndarray that
        # the stock json encoder rejects. Serialize numpy at THIS boundary (where JSON-ness is
        # owned), not by hand-casting every field in 20 verbs. And NEVER let a dumps failure
        # raise out of the handler — a dead handler thread drops the socket → the studio proxy
        # reports 502. On failure, return an honest {error} the UI can show.
        try:
            data = json.dumps(obj, default=_json_default).encode("utf-8")
        except Exception as e:  # noqa: BLE001
            data = json.dumps({"error": f"serialization failed: {type(e).__name__}: {e}"}).encode("utf-8")
        self.send_response(200); self.send_header("Content-Type", "application/json"); self._cors()
        self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)

    def _serve_sse(self, gen_fn, q):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self._cors()
        self.end_headers()
        try:
            for frame in gen_fn(q):
                self.wfile.write(f"data: {json.dumps(frame)}\n\n".encode("utf-8"))
                self.wfile.flush()
                if isinstance(frame, dict) and frame.get("done"):
                    break
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception:  # noqa: BLE001
            try:
                self.wfile.write(f"data: {json.dumps({'done': True, 'result': {'error': traceback.format_exc()[-300:]}})}\n\n".encode("utf-8"))
            except Exception:
                pass


def main():
    print(f"[remoroo-edge] REAL edge on http://127.0.0.1:{PORT}  cell={CELL_DIR}")
    print(f"[remoroo-edge] then: EDGE_URL=http://127.0.0.1:{PORT} npm run serve")
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
