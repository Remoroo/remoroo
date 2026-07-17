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
_bridge_err_logged: str | None = None  # last bridge error PRINTED (dedup the 3s-retry spam)
_bridge_last_try: float = 0.0
# Serialises every touch of the shared cell Bridge. The live-joints SSE polls
# get_observation() ~20 Hz WHILE the calibration verbs move/capture on the SAME bridge — on a
# real SDK two threads hitting it concurrently corrupts reads / commands. The lock makes the
# live mirror and the supervised flow take turns on the one physical device (G14).
_bridge_lock = threading.RLock()


class _LockedBridge:
    """A bridge proxy that takes `_bridge_lock` around EVERY call. The motion stack holds this
    (not the raw bridge) so its internal device touches — `_seed_positions` reads during a
    lock-free background warmup, the executor's streaming — are always serialized with operator
    traffic, without any caller having to hold the lock across long GPU work. RLock: handlers
    that already hold the lock re-enter for free, so nothing about the locked verbs changes."""

    def __init__(self, b):
        self._b = b

    def __getattr__(self, name):
        attr = getattr(self._b, name)
        if not callable(attr):
            return attr

        def locked(*a, **kw):
            with _bridge_lock:
                return attr(*a, **kw)

        return locked


# While a STANDALONE realtime.py subprocess is running, IT owns the devices (the ZED is single-open) and
# its own CUDA context. The edge yields: get_bridge() returns None so nothing here opens the camera/robot
# behind the child's back. `_realtime_proc` lets the E-stop terminate the child.
_realtime_active = False
_realtime_proc = None


def get_bridge():
    """Construct + connect the cell Bridge. primitives.py is authored at G2 — i.e.
    AFTER the edge starts — so we must NOT cache the early failure forever, or the
    live robot never connects this session. On failure we retry (with a short
    backoff) so the bridge comes up the moment primitives.py exists / the robot is
    reachable."""
    global _bridge, _bridge_err, _bridge_err_logged, _bridge_last_try
    if _realtime_active:
        return None              # the standalone realtime.py child owns the devices — don't touch them
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
        if _bridge_err_logged is not None:           # recovered after a prior failure → say so
            print("[remoroo-edge] ✅ robot bridge connected.", file=sys.stderr, flush=True)
            _bridge_err_logged = None
    except Exception as e:  # noqa: BLE001
        # Capture WHERE it failed (file:line) — a bare "TypeError: float()..." off a customer
        # machine is undiagnosable; the deepest frame tells us which line in which module.
        tb = traceback.extract_tb(e.__traceback__)
        where = f" (at {os.path.basename(tb[-1].filename)}:{tb[-1].lineno})" if tb else ""
        _bridge_err = f"{type(e).__name__}: {e}{where}"
        # SURFACE the FULL error to the console/log (deduped across the 3s retries). It used to live
        # ONLY in the live-mirror SSE frames → the truncated Studio header, never the terminal. Now
        # the complete reason + traceback land in the edge output (teed to the shell + .remoroo/edge.log).
        if _bridge_err != _bridge_err_logged:
            _bridge_err_logged = _bridge_err
            print(f"\n[remoroo-edge] ROBOT BRIDGE NOT CONNECTED: {_bridge_err}", file=sys.stderr)
            print("".join(traceback.format_exception(type(e), e, e.__traceback__)).rstrip(), file=sys.stderr)
            print("[remoroo-edge] (retrying every 3s; the live robot mirror stays home until this clears)\n",
                  file=sys.stderr, flush=True)
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


def _groups_as_arm_map(cfg: dict) -> dict:
    """Shim the AUTHORED config's `groups` into the `{base_link, arms:[{name, ee_link, joint_names,
    base_link, side}]}` shape `calib_engine.curobo_cfg.build_robot_cfg` expects (so calibration's
    classic motion_gen keeps working). ee_link = the group's first tip; side = its `side` tag."""
    groups = cfg.get("groups") or []
    base = (groups[0].get("base_link") if groups else "") or ""
    arms = [{
        "name": g.get("name"),
        "ee_link": (g.get("tip_links") or [""])[0],
        "joint_names": list(g.get("joint_names") or []),
        "base_link": g.get("base_link") or base,
        "side": (g.get("tags") or {}).get("side", ""),
    } for g in groups if g.get("joint_names")]
    return {"base_link": base, "arms": arms}


# --------------------------------------------------------------------------- #
# cuRobo helpers (researched API: MotionGenConfig.load_from_robot_config, etc.) #
# --------------------------------------------------------------------------- #
def _ensure_complete_urdf(urdf_path: str, prior_text) -> list:
    """Make the just-written `robot.urdf` COMPLETE for commission: every camera link keeps its
    `<cam>_optical_frame` (which carries the optical-convention rotation + the hand-eye calibration).
    Calibration writes that frame in place; the model/sphere gate overwrites robot.urdf with the raw
    editor URDF and would DROP it, so here we re-apply the calibrated optical-joint transform from the
    PRIOR robot.urdf (if any), else add an identity frame so the link at least exists. Returns the
    cameras whose CALIBRATED frame was preserved. This is the SINGLE complete artifact the motion stack
    loads — no consume-time patching."""
    import os
    import tempfile
    import numpy as _np
    from calib_engine import urdf_io

    cams = urdf_io.find_camera_links(urdf_path)
    prior_path = None
    if prior_text:
        fd, prior_path = tempfile.mkstemp(suffix=".urdf")
        os.close(fd)
        Path(prior_path).write_text(prior_text, encoding="utf-8")
    preserved: list = []
    try:
        for cam in cams:
            T = None
            if prior_path:
                try:
                    T = urdf_io.read_nominal_optical(prior_path, cam)   # prior body->optical (calibrated)
                except Exception:  # noqa: BLE001
                    T = None
            if T is not None and not _np.allclose(_np.asarray(T, float), _np.eye(4), atol=1e-9):
                urdf_io.write_calibrated_optical(urdf_path, cam, _np.asarray(T, float), provenance="measured")
                preserved.append(cam)
            else:
                urdf_io.ensure_optical_frame(urdf_path, cam)            # identity until calibrated
    finally:
        if prior_path:
            try:
                os.unlink(prior_path)
            except Exception:  # noqa: BLE001
                pass
    return preserved


def write_curobo_robot_yaml(urdf_text: str, spheres: list[dict]) -> Path:
    """Write `robot.urdf` + the cuRobo `collision_spheres.yml` (the sphere model calibration's
    motion_gen reads). The KINEMATIC config is NOT written here — it is the AUTHORED `cell.yaml:
    groups` (the single source of truth), read on demand via `urdf_io.robot_config`. No `arms.yaml`,
    no per-arm cfg files, no name reverse-mapping."""
    import yaml  # type: ignore
    from calib_engine import urdf_io
    from calib_engine.curobo_cfg import build_robot_cfg

    rm = CELL_DIR / "robot_model"
    rm.mkdir(parents=True, exist_ok=True)
    urdf_file = rm / "robot.urdf"
    prior = urdf_file.read_text(encoding="utf-8") if urdf_file.exists() else None
    urdf_file.write_text(urdf_text, encoding="utf-8")
    # KEEP THE URDF COMPLETE: re-apply each camera's calibrated `*_optical_frame` from the PRIOR
    # robot.urdf (the raw editor URDF dropped it). Without this, re-saving the model silently strips
    # calibration → commission loads an incomplete URDF → the live world has no valid camera pose.
    try:
        _ensure_complete_urdf(str(urdf_file), prior)
    except Exception as e:  # noqa: BLE001 — never block a model save on this; surface loudly in logs
        print(f"[model] could not preserve camera optical frames in robot.urdf: {type(e).__name__}: {e}")

    by_link: dict[str, list[dict]] = {}
    for s in spheres:
        by_link.setdefault(s["link"], []).append({"center": list(s["center"]), "radius": float(s["radius"])})

    cfg = urdf_io.robot_config(load_cell_yaml(), str(rm / "robot.urdf"))   # authored groups (or arms[] back-compat)
    arm_map = _groups_as_arm_map(cfg)
    out = rm / "collision_spheres.yml"
    if arm_map["arms"]:
        # classic robot_cfg for the DEFAULT group (base/ee/cspace + others locked) — calibration motion.
        first = build_robot_cfg(arm_map, by_link, plan_arm=arm_map["arms"][0]["name"], urdf_path="robot.urdf")
        out.write_text(yaml.safe_dump(first, sort_keys=False), encoding="utf-8")
    else:  # no kinematic groups yet (URDF modeled but groups not authored) — spheres-only, loud
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
    # If a STANDALONE realtime.py child is running, it owns the devices in its own process — terminate it
    # (its SIGTERM handler estops the robot; it also polls the physical E-stop). Then estop via our own
    # bridge if we hold it (we don't during realtime — get_bridge() returns None then).
    if _realtime_proc is not None and _realtime_proc.poll() is None:
        try:
            _realtime_proc.terminate()
        except Exception:  # noqa: BLE001
            pass
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
        reset_motion_stack()   # the unified motion stack bakes in the robot model too
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
    """The live mirror is a CONSUMER of the independent joint stream (one producer, many
    consumers — the mirror no longer polls the Bridge itself, so it neither competes with the
    calibration pipeline for the bridge lock nor pays for camera-bundled reads). Emits
    {t, joints} frames (no 'done'); frames without `joints` make the frontend hold the last
    real pose (a transient hiccup must not snap the mirror to the default)."""
    t0 = time.time()
    # The robot Bridge isn't ready until primitives.py is authored (G2) and the arm is reachable,
    # so EARLY in setup get_bridge() returns None. DON'T end the stream here: a closed SSE makes the
    # browser EventSource reconnect-loop (onerror), so the live dot shows a SILENT orange
    # "disconnected" with no reason — the exact failure operators hit at the calibrate gate. Instead
    # keep the stream OPEN, retry (get_bridge backs off internally), and HEARTBEAT the current
    # bridge error so the Studio can say "edge up · robot bridge not ready: <reason>".
    def _stream_or_reason():
        """The SSE must NEVER die on an internal error (a dead SSE reconnect-loops as a
        SILENT orange 'not connected'): any exception building the stream — a module missing
        from the bundle, a broken bridge attribute — becomes the heartbeat's visible reason."""
        try:
            return _joint_stream(), None
        except Exception as e:  # noqa: BLE001
            print(f"[edge] joint stream unavailable: {type(e).__name__}: {e}")
            return None, f"joint stream unavailable: {type(e).__name__}: {e}"

    js, reason = _stream_or_reason()
    while js is None:
        yield {"t": time.time() - t0, "joints": {}, "waiting": True,
               "error": reason or _bridge_err or "robot bridge not connected yet"}
        time.sleep(1.0)
        js, reason = _stream_or_reason()
    while True:
        evt = js.latest
        if evt is None:
            frame = {"t": time.time() - t0}
            if js.error:
                frame["error"] = js.error          # no `joints` → mirror keeps last pose
        else:
            frame = {"t": time.time() - t0, "joints": evt[1]}
        yield frame
        time.sleep(0.05)  # ~20 Hz consumer; the producer sets the actual sample rate


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


# --- the unified motion stack (shipped motion_engine) ----------------------
_MOTION_STACK = None


def reset_motion_stack():
    """Drop the cached MotionStack so the next commission/plan rebuilds it (the robot model,
    world, or safety changed). Paired with reset_curobo_cache()."""
    global _MOTION_STACK
    _MOTION_STACK = None


def motion_stack(force: bool = False):
    """The shipped `motion_engine.MotionStack`, built from ALL the cell artifacts (calibrated
    URDF + collision spheres + modeled obstacles + safety) and wired to the live bridge. This is the
    ONE unified autonomous-motion surface; the bridge supplies state + execute_trajectory. The
    collision world is the LIVE camera ESDF built at commission, not a stored scan."""
    global _MOTION_STACK
    if _MOTION_STACK is not None and not force:
        return _MOTION_STACK
    from motion_engine import MotionStack  # sibling of this file in server/, like calib_engine

    # The stack gets the LOCKING bridge proxy: its internal device reads (seed_positions during
    # a lock-free warmup) serialize per-call with operator traffic instead of requiring the
    # caller to hold _bridge_lock across minutes of GPU work.
    b = get_bridge()
    _MOTION_STACK = MotionStack.from_cell(str(CELL_DIR),
                                          bridge=(_LockedBridge(b) if b is not None else None))
    try:
        _joint_stream()   # the tracking watchdog's measured-joints tap (bridge.latest_joints)
    except Exception as e:  # noqa: BLE001 — watchdog degrades to post-move checks only
        print(f"[edge] joint stream unavailable for motion tracking: {type(e).__name__}: {e}")
    return _MOTION_STACK


def _preload_warp() -> None:
    """Initialise NVIDIA Warp ON THE MAIN THREAD before any worker thread builds cuRobo, and verify
    its torch interop is present. cuRoboV2 uses the TOP-LEVEL `wp.from_torch`/`wp.device_from_torch`
    (warp >= 1.0 exposes these directly — there is NO `warp.torch` submodule to import; importing one
    raises ModuleNotFoundError on current warp). We mirror the exact sequence that works in a plain
    shell (`import torch, warp; wp.init(); wp.from_torch(...)`) so this CANNOT spuriously fail on a
    healthy rig. Raises a clear, actionable error only if Warp's torch interop is genuinely missing
    (a toolchain / G1 issue), never for a cell-config problem."""
    try:
        import torch  # noqa: F401
        import warp as wp
        wp.init()
        if not hasattr(wp, "from_torch"):
            raise AttributeError("warp has no top-level from_torch (warp-lang too old or broken interop)")
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            "cuRoboV2 needs NVIDIA Warp's torch interop (wp.from_torch), which is unavailable "
            f"({type(e).__name__}: {e}). This is a TOOLCHAIN issue on the robot PC: install a "
            "cuRoboV2-compatible warp-lang (>= 1.0.0) for this torch+CUDA. Verify with: "
            "python -c \"import torch, warp as wp; wp.init(); print(wp.from_torch(torch.zeros(1, device='cuda')))\""
        ) from e


def sse_commission(_q):
    """Build the unified motion stack from every prior gate's output and VERIFY one collision-free
    plan+execute against the LIVE world (the cuRoboV2 ESDF built from the cameras + modeled
    obstacles). Streams each commission step (sphere health → planner
    build → plan → audit → executor replay); ends {done, result:<report>}. The motion is
    operator-gated in the Studio (default_proceed=false).

    The verification move is a REACHABLE target, never the cspace home (commonly inside the table):
    pass `tcp` + `x,y,z` to plan to an operator/agent-chosen point (keeping current orientation),
    else the stack auto-finds a small collision-free nudge from the live pose."""
    import queue as _queue
    import threading as _threading

    try:
        _preload_warp()                 # MAIN THREAD — load warp.torch before the build worker
        # REUSE the warm planner (build + cuRobo warmup is the costly step, done ONCE per bring-up).
        # Mutations that invalidate it already drop the cache (model/spheres → reset_motion_stack;
        # obstacles → hot-swapped in place). Pass ?rebuild=1 to force a fresh build after an upstream
        # artifact (safety/calib) was redone.
        st = motion_stack(force=(_q.get("rebuild", ["0"])[0] == "1"))
    except Exception as e:  # noqa: BLE001
        yield {"done": True, "result": {"ok": False, "message": f"{type(e).__name__}: {e}"}}
        return

    execute = (_q.get("execute", ["1"])[0] != "0")
    tcp = (_q.get("tcp", [None])[0]) or None
    target = None
    if all(k in _q for k in ("x", "y", "z")):
        try:
            target = [float(_q.get(k, ["0"])[0]) for k in ("x", "y", "z")]
        except (TypeError, ValueError):
            target = None
    q: "_queue.Queue" = _queue.Queue()
    out: dict = {}

    b = get_bridge()

    def run():
        with _bridge_lock:
            try:
                # Build the live frames ONCE up front + emit a per-camera verdict, so a 0-frame
                # outcome is visible (which camera/layer was missing) — never a silent fallback.
                live, reasons = ([], [])
                if b is not None:
                    try:
                        live, reasons = _live_frames(st, b)
                    except Exception as e:  # noqa: BLE001
                        reasons = [{"camera": None, "ok": False, "why": f"{type(e).__name__}: {e}"}]
                q.put({"step": "camera_inputs", "ok": bool(live), "n_frames": len(live), "cameras": reasons})
                out["report"] = st.commission(execute=execute, target=target, tcp=tcp,
                                               frames=lambda: live, progress=lambda s: q.put(s))
            except Exception as e:  # noqa: BLE001
                out["report"] = {"ok": False, "message": f"{type(e).__name__}: {e}"}
            finally:
                q.put(None)

    _threading.Thread(target=run, daemon=True).start()
    while True:
        item = q.get()
        if item is None:
            break
        yield item
    yield {"done": True, "result": out.get("report", {"ok": False, "message": "no report"})}


def sse_demo(_q):
    """The repeatable autonomous-motion DEMO (gate 12) — distinct from commission (prove ONE move).
    REUSES the already-commissioned WARM stack (no rebuild/re-warm), refreshes the LIVE ESDF before
    each leg (the robot re-plans against the current scene), moves through several reachable poses,
    then retracts. Streams `world`/`leg`/`retract` steps; ends {done, result}. Supervised in the
    Studio (E-stop). GET params: tcp, n (poses, default 3), execute ('0'=plan-only)."""
    import queue as _queue
    import threading as _threading

    try:
        _preload_warp()
        st = motion_stack()                          # REUSE the warm stack from commission
    except Exception as e:  # noqa: BLE001
        yield {"done": True, "result": {"ok": False, "message": f"{type(e).__name__}: {e}"}}
        return

    execute = (_q.get("execute", ["1"])[0] != "0")
    tcp = (_q.get("tcp", [None])[0]) or None
    try:
        n_poses = max(1, min(8, int(_q.get("n", ["3"])[0])))
    except (TypeError, ValueError):
        n_poses = 3
    b = get_bridge()
    q: "_queue.Queue" = _queue.Queue()
    out: dict = {}

    def run():
        with _bridge_lock:
            try:
                def frames():
                    try:
                        return _live_frames(st, b)[0] if b is not None else []
                    except Exception:  # noqa: BLE001
                        return []
                out["report"] = st.demo_run(n_poses=n_poses, execute=execute, tcp=tcp,
                                            frames=frames, progress=lambda s: q.put(s))
            except Exception as e:  # noqa: BLE001
                out["report"] = {"ok": False, "message": f"{type(e).__name__}: {e}"}
            finally:
                q.put(None)

    _threading.Thread(target=run, daemon=True).start()
    while True:
        item = q.get()
        if item is None:
            break
        yield item
    yield {"done": True, "result": out.get("report", {"ok": False, "message": "no report"})}


def sse_plan_move(_q):
    """Plan + execute a high-level move via the shipped stack (the demo gate + the operator's target
    gizmo exercise this). GET params:
      tcp     — group name (default: first)
      mode    — point | pose | retract   (default: point)
      x,y,z   — target position (point/pose)
      qw..qz  — orientation (pose only; OMIT to KEEP the current TCP orientation — far more reachable)
      execute — '0' = plan-only preview (no motion), '1' = plan + supervised execute (default)
    Streams phase, then {done, result:<MoveResult>}."""
    try:
        _preload_warp()                 # MAIN THREAD — load warp.torch before any build worker
        st = motion_stack()
    except Exception as e:  # noqa: BLE001
        yield {"done": True, "result": {"ok": False, "message": f"{type(e).__name__}: {e}"}}
        return

    groups = st.group_names
    tcp = (_q.get("tcp", [None])[0]) or (groups[0] if groups else None)
    mode = _q.get("mode", ["point"])[0]
    execute = (_q.get("execute", ["1"])[0] != "0")
    yield {"phase": "planning", "tcp": tcp, "mode": mode, "execute": execute}
    try:
        with _bridge_lock:
            if mode == "retract":
                res = st.retract(tcp, execute=execute)
            elif mode == "pose" and any(k in _q for k in ("qw", "qx", "qy", "qz")):
                xyz = [float(_q.get(k, ["0"])[0]) for k in ("x", "y", "z")]
                quat = [float(_q.get(k, [str(d)])[0]) for k, d in (("qw", 1.0), ("qx", 0.0), ("qy", 0.0), ("qz", 0.0))]
                res = st.move_to_pose(tcp, (xyz, quat), execute=execute)
            else:  # 'point' (or 'pose' without a quat): reach the point, KEEP current orientation
                xyz = [float(_q.get(k, ["0"])[0]) for k in ("x", "y", "z")]
                res = st.plan_to_point(tcp, xyz, execute=execute)
        yield {"done": True, "result": res.to_dict()}
    except Exception as e:  # noqa: BLE001
        yield {"done": True, "result": {"ok": False, "message": f"{type(e).__name__}: {e}"}}


def h_tcp_pose(_q):
    """The live pose of a TCP's tool frame in the base frame — FK of the current joints. The Studio
    spawns the target gizmo HERE so the operator drags from the real tool position. GET param: tcp
    (default: first group). Returns {ok, tcp, position:[x,y,z], quaternion:[w,x,y,z]}."""
    try:
        st = motion_stack()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    groups = st.group_names
    tcp = (_q.get("tcp", [None])[0]) or (groups[0] if groups else None)
    if not tcp:
        return {"ok": False, "error": "no TCP/group available"}
    try:
        with _bridge_lock:
            pose = st.current_tool_pose(tcp)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "tcp": tcp}
    if not pose:
        return {"ok": False, "error": "planner could not FK the current pose", "tcp": tcp}
    return {"ok": True, "tcp": tcp, "position": list(pose[0]), "quaternion": list(pose[1])}


def h_suggest_targets(_q):
    """STAGE 2 — remoroo reads the space: return reachable, collision-free points the TCP can move to
    (cuRobo plans each, keeping orientation). The Studio 'suggest points' button and the agent both
    consume this to propose a target the operator then confirms/nudges. GET params: tcp, n (default
    8). Returns {ok, tcp, targets:[{point,radius,distance,trajectory}]}."""
    try:
        st = motion_stack()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    groups = st.group_names
    tcp = (_q.get("tcp", [None])[0]) or (groups[0] if groups else None)
    if not tcp:
        return {"ok": False, "error": "no TCP/group available"}
    try:
        n = max(1, min(24, int(_q.get("n", ["8"])[0])))
    except (TypeError, ValueError):
        n = 8
    try:
        with _bridge_lock:
            targets = st.find_free_targets(tcp, n=n)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "tcp": tcp}
    return {"ok": True, "tcp": tcp, "targets": targets,
            "message": (f"{len(targets)} reachable point(s)" if targets
                        else "no reachable collision-free point found near the current pose")}


def h_validate_config(_q):
    """SOLID robot_cfg validation: structure (links/joints exist in URDF) + cuRobo's CANONICAL IK
    round-trip (ik.checks['0cm'] MUST succeed). Says whether the cfg is right or which field is
    wrong. GET param: tcp."""
    try:
        st = motion_stack()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    groups = st.group_names
    tcp = (_q.get("tcp", [None])[0]) or (groups[0] if groups else None)
    try:
        with _bridge_lock:
            rep = st.validate_config(tcp)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "tcp": tcp}
    rep["ok"] = True
    return rep


def h_motion_world(_q):
    """The CANONICAL collision world cuRobo plans against — the SAME world the clearance UI shows +
    clears: the cropped + SELF-MASKED cloud + every cuboid (obstacles, workspace cage, keep-out).
    Serving it finalizes the self-mask once (then cached). Returns {points, cuboids, voxel_size, meta}."""
    try:
        st = motion_stack()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    try:
        groups = st.group_names
        if groups:
            with _bridge_lock:
                st._planner_for([groups[0]])           # finalize (self-mask the cloud) once
        w = st.world
        import numpy as _np
        pts = _np.asarray(w.points, dtype=float).reshape(-1, 3) if w.n_points else _np.zeros((0, 3))
        return {"ok": True, "n_points": int(w.n_points), "voxel_size": float(w.voxel_size),
                "points": pts.tolist(), "cuboids": w.scene.get("cuboid") or {},
                "meta": w.meta, "world_warn": getattr(st, "_world_warn", None)}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def h_motion_esdf(_q):
    """The LIVE volumetric collision world — occupied voxel CENTRES of the cuRoboV2 ESDF the stack
    last built from the cameras (`stack.esdf_voxels()`). The Studio renders these as the live world.
    Returns {ok, n_voxels, voxels:[[x,y,z],...], bbox}. Empty until a commission/live build runs."""
    try:
        st = motion_stack()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    try:
        import numpy as _np
        v = _np.asarray(st.esdf_voxels(), dtype=float).reshape(-1, 3)
        bbox = ([v[:, i].min() for i in range(3)] + [v[:, i].max() for i in range(3)]) if len(v) else None
        vs = float(st.esdf_voxel_size()) if hasattr(st, "esdf_voxel_size") else 0.03
        return {"ok": True, "n_voxels": int(len(v)), "voxels": v.tolist(), "bbox": bbox, "voxel_size": vs}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def h_debug_world(_q):
    """SAVE + return the planner's collision world (the live ESDF occupied voxels + the modeled
    cuboids) as a binary STL — 'the world that gets into the planner', for offline inspection
    (MeshLab/Blender) and a one-click download. Writes <cell>/debug/planner_world.stl and returns the
    counts + base64 STL so the Studio can save it without SSH. The live in-stage cubes (/edge/motion/
    esdf) show the SAME world; this is the saveable artifact."""
    try:
        st = motion_stack()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    try:
        import base64
        out_path = str(Path(CELL_DIR) / "debug" / "planner_world.stl")
        info = st.save_debug_world(out_path)
        with open(out_path, "rb") as f:
            info["stl_base64"] = base64.b64encode(f.read()).decode("ascii")
        return info
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def sse_esdf_live(_q):
    """LIVE world stream: every ~`period`s, REBUILD the ESDF from the cameras and stream its occupied
    voxel centres (+ the mask stats), so the Studio shows the collision world the planner sees AND it
    STAYS ALIVE — it updates as the arm/scene moves (jog the arm and watch it fill). This also keeps
    the PLANNER's live world fresh (update_world_live applies the ESDF). Capped (`max_iters`) so a
    forgotten tab doesn't pin the GPU/bridge forever; the operator toggles it off when done."""
    import time as _t
    import numpy as _np
    try:
        st = motion_stack()
    except Exception as e:  # noqa: BLE001
        yield {"done": True, "result": {"ok": False, "error": f"{type(e).__name__}: {e}"}}
        return
    b = get_bridge()
    period = max(0.4, float((_q.get("period", ["1.2"])[0]) or 1.2))
    max_iters = int((_q.get("max_iters", ["1200"])[0]) or 1200)
    for i in range(max_iters):
        rec: dict = {"frame": i}
        try:
            with _bridge_lock:
                frames, reasons = (_live_frames(st, b) if b is not None else ([], []))
                if frames:
                    wr = st.update_world_live(frames)
                    for k in ("n_voxels", "n_live_voxels", "n_obstacle_voxels", "mask_fraction",
                              "n_robot_pixels_masked"):
                        rec[k] = wr.get(k)
                rec["n_cams"] = sum(1 for r in (reasons or []) if r.get("ok"))
                rec["cam_why"] = next((r.get("why") for r in (reasons or []) if not r.get("ok")), None)
                v = _np.asarray(st.esdf_voxels(), dtype=float).reshape(-1, 3)
                rec["voxels"] = v.tolist()
                rec["voxel_size"] = float(st.esdf_voxel_size())
        except Exception as e:  # noqa: BLE001 — never kill the stream on one bad cycle
            rec["error"] = f"{type(e).__name__}: {e}"
        yield rec
        _t.sleep(period)
    yield {"done": True, "result": {"ok": True, "frames": max_iters}}


def h_debug_dump(_q):
    """FULL live-world debug — SAVE (cell/debug/*.npz + *.json) + SHOW (returns the clouds for the
    Studio): camera poses, the RAW per-camera clouds, the robot-mask split (kept vs masked), the robot
    collision spheres at the seed, the world voxels + obstacles, and the born-collision verdict. Also
    PRINTS a concise before/after table to the edge console. The scientific 'is the cloud on the real
    scene, and does masking eat it?' view."""
    try:
        st = motion_stack()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    b = get_bridge()
    try:
        with _bridge_lock:
            frames, reasons = (_live_frames(st, b) if b is not None else ([], []))
            rep = st.debug_dump(frames, save_dir=str(CELL_DIR / "debug"))
    except Exception as e:  # noqa: BLE001
        import traceback
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc()[-600:]}
    rep["ok"] = True
    rep["camera_inputs"] = reasons
    # PRINTS — the operator asked for on-desk debug too. One honest line per camera + the verdict.
    print("\n===== COMMISSION DEBUG DUMP =====")
    print(f"seed joints: {rep.get('seed', {}).get('n_joints')}  robot spheres: {rep.get('n_spheres')}  "
          f"spheres bbox: {rep.get('spheres_bbox')}")
    for c in rep.get("cameras", []):
        print(f"  cam {c['name']}: pose xyz={c['pose_xyz']} wxyz={c['pose_wxyz']}  "
              f"valid_depth={c['valid_frac']*100:.0f}%  points={c['n_points']}  "
              f"KEPT(world)={c['n_kept']}  MASKED(robot)={c['n_masked']}  "
              f"OFFENDING(near robot)={c['n_offending']}  cloud_bbox={c['cloud_bbox']}")
    w = rep.get("world", {})
    print(f"  world: {w.get('n_voxels')} voxels @ {w.get('voxel_size')} m  bbox={w.get('voxels_bbox')}  "
          f"obstacles={len(w.get('obstacles') or [])}")
    sc = rep.get("start_collision", {})
    print(f"  start_collision: free={sc.get('collision_free')}  penetrating={sc.get('n_penetrating')}")
    print(f"  SAVED → {rep.get('saved')}\n=================================\n")
    return rep


def h_diagnose_plan(_q):
    """DECISIVE ablation for 'no collision-free move' once the world is ruled out: SELF-collision at the
    start? target reachable (pure IK)? joints in limits? does removing self-collision unblock it? Names
    the cause. `?fast=1` skips the warm plan-without-self probe (the slow part)."""
    try:
        st = motion_stack()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    tcp = (_q.get("tcp", [None])[0]) or None
    fast = (_q.get("fast", ["0"])[0] == "1")
    try:
        with _bridge_lock:
            rep = st.diagnose_plan(tcp, test_plan_no_self=not fast)
    except Exception as e:  # noqa: BLE001
        import traceback
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc()[-600:]}
    rep["ok"] = True
    print("\n===== PLAN DIAGNOSIS =====")
    print(f"  start_self_collision_free={rep.get('start_self_collision_free')}  "
          f"ik_reach={rep.get('ik_reach')}  limit_violations={list(rep.get('joint_limit_violations') or {})}")
    print(f"  self_collision_ignore={rep.get('self_collision_ignore')}")
    scp = rep.get("self_collision_pairs") or {}
    if scp.get("pairs"):
        print(f"  SELF-COLLIDING PAIRS ({scp.get('n_pairs')}, all_adjacent={scp.get('all_adjacent')}):")
        for p in scp["pairs"][:20]:
            print(f"    {p['links'][0]:>28s} ↔ {p['links'][1]:<28s}  {p['penetration_mm']:>6.1f} mm"
                  f"  {'adjacent' if p['adjacent'] else 'NON-adjacent'}")
    print(f"  plan_no_self_no_world={rep.get('plan_no_self_no_world')}")
    print(f"  VERDICT: {rep.get('verdict')}\n==========================\n")
    return rep


def h_calib_bake(_q):
    """APPLY SAVED CALIBRATION → URDF: re-bake every artifact in `calibration/` (per-camera optical
    frames + the base_to_base inter-arm transform) into `robot_model/robot.urdf`, then drop the planner
    caches so the motion stack reloads the complete URDF. Idempotent. This is the Studio's 'load +
    apply saved calibration' path — and the recovery after a model-gate rewrite dropped the frames.
    Returns the bake report {optical, base_to_base, errors}."""
    from calib_engine.bake import bake_calibration
    urdf = CELL_DIR / "robot_model" / "robot.urdf"
    rep = bake_calibration(str(urdf), str(CELL_DIR / "calibration"))
    try:
        reset_curobo_cache()
        reset_motion_stack()
    except Exception as e:  # noqa: BLE001
        rep.setdefault("errors", []).append(f"cache reset: {type(e).__name__}: {e}")
    rep["ok"] = not rep.get("errors")
    rep["applied"] = {"optical": len(rep.get("optical") or []),
                      "base_to_base": rep.get("base_to_base") is not None}
    return rep


def _obs_depth(obs):
    """Metric depth (H,W float) from a bridge observation, tolerant of the field name the cell's
    Bridge uses (`depth_m` preferred, then `depth` / `depth_image`). None if the cell exposes no
    depth (then the live world falls back to the modeled cuboids only)."""
    import numpy as _np
    for attr in ("depth_m", "depth", "depth_image"):
        d = getattr(obs, attr, None)
        if d is not None:
            a = _np.asarray(d, dtype=float)
            if a.ndim == 2 and a.size > 0:
                return a
    return None


def _grab_observation(b, keys):
    """One bridge observation via the FIRST camera key it accepts. Keys are tried in CONTRACT order
    (the URDF link — what calibration binds and the bridge keys `_cameras` on — before the friendly
    cell.yaml name). Tolerant of a single-cam bridge that ignores `camera=` (TypeError → no-arg) and
    of a multi-cam bridge that raises on an unknown key (KeyError/NameError → next key). Returns
    (observation, used_key) so intrinsics are read with the SAME key the depth came from."""
    for key in keys:
        try:
            obs = b.get_observation(camera=key)
            if obs is not None:
                return obs, key
        except TypeError:                       # bridge ignores camera= (single-cam)
            try:
                obs = b.get_observation()
                if obs is not None:
                    return obs, ""
            except Exception:  # noqa: BLE001
                return None, None
        except Exception:  # noqa: BLE001 — unknown key for THIS bridge; try the next
            continue
    return None, None


def _np_depth_stats(depth) -> dict:
    """Valid-depth fraction + metric range for one camera's depth — the honest read on whether the
    bridge is returning usable metric depth (vs mostly-invalid, which masks as 'robot') and in the
    right units (a max of ~1500 instead of ~1.5 means millimetres)."""
    import numpy as _np
    a = _np.asarray(depth, dtype=float)
    valid = _np.isfinite(a) & (a > 1e-3)
    n = int(valid.sum())
    if n == 0:
        return {"valid_frac": 0.0, "depth_min_m": None, "depth_max_m": None, "depth_median_m": None}
    v = a[valid]
    return {"valid_frac": round(n / a.size, 3),
            "depth_min_m": round(float(v.min()), 3), "depth_max_m": round(float(v.max()), 3),
            "depth_median_m": round(float(_np.median(v)), 3)}


def _live_frames(st, b):
    """Build one `DepthFrame` per cell camera from the BRIDGE's per-camera observation (general — it
    relies only on the bridge contract the agent fulfils: depth + intrinsics per camera) and the
    camera's calibrated optical-frame pose via `stack.link_pose`. Camera/morphology-agnostic.

    Returns (frames, reasons): `reasons` carries a per-camera verdict so a 0-frame outcome is NEVER
    silent — it says WHICH layer was missing (no depth on the obs / no intrinsics / FK link absent),
    making it unambiguous whether the gap is the bridge (the agent's primitives.py — risk #1) or the
    motion_engine↔cuRobo seam. When `frames` is empty, commission plans against the modeled cuboids."""
    from motion_engine import DepthFrame
    frames, reasons = [], []
    cams = load_cell_yaml().get("cameras") or []
    if not cams:
        reasons.append({"camera": None, "ok": False, "why": "no cameras in cell.yaml"})
    for cam in cams:
        name = cam.get("name") or cam.get("link") or ""
        link = cam.get("link") or name
        # CONTRACT order: the URDF link is the camera key calibration + the bridge use; the friendly
        # name is only a fallback. (Passing the name where the bridge keys by link silently drops the
        # camera — the first live-commission gap we hit on the bimanual ZED cell.)
        keys = [k for k in (link, name) if k]
        try:
            obs, used_key = _grab_observation(b, keys)
            if obs is None:
                reasons.append({"camera": name, "ok": False,
                                "why": f"bridge get_observation returned nothing for camera keys {keys}"})
                continue
            depth = _obs_depth(obs)
            K, _wh = _intrinsics_from_bridge(b, used_key if used_key is not None else "")
            if depth is None:
                reasons.append({"camera": name, "ok": False,
                                "why": "bridge observation exposes no depth (depth_m/depth/depth_image) — the agent's "
                                       "primitives.py must return per-camera metric depth for the live ESDF"})
                continue
            if K is None:
                reasons.append({"camera": name, "ok": False, "why": "no intrinsics for this camera from the bridge"})
                continue
            # The camera OPTICAL frame at the LIVE joints (eye-in-hand) — the FULL transform (the
            # optical-convention rotation + the hand-eye calibration) lives in robot.urdf's
            # `<cam>_optical_frame` joint, written by calibration and PRESERVED by the model-gate URDF
            # writer (`_ensure_complete_urdf`). REFUSE the bare body link: it carries no calibration and
            # mis-places the whole live cloud onto the robot. A missing optical frame means the URDF is
            # incomplete at commission — say so, loudly, with the fix.
            pose = st.link_pose(f"{link}_optical_frame")
            if pose is None:
                reasons.append({"camera": name, "ok": False,
                                "why": f"'{link}_optical_frame' is missing from robot.urdf — the URDF is INCOMPLETE "
                                       "at commission (calibration wasn't accepted, or an older model save dropped "
                                       "it). Re-run/accept calibration for this camera; the model-gate writer now "
                                       "preserves the optical frame. Refusing the un-calibrated body link."})
                continue
            frames.append(DepthFrame(depth=depth, intrinsics=K, cam_pose_in_base=pose, name=name))
            # DEPTH VALIDITY — the decisive number when 'robot px masked' is implausibly high: a zero/
            # invalid depth pixel back-projects to the camera origin (on the wrist) → within the segment
            # distance of the robot → masked AS robot. So a low valid fraction reads as '96% robot' and
            # the live world is empty. Surface valid% + the depth scale (catches metres-vs-mm too).
            a = _np_depth_stats(depth)
            reasons.append({"camera": name, "ok": True, "depth_hw": list(depth.shape), **a})
        except Exception as e:  # noqa: BLE001
            reasons.append({"camera": name, "ok": False, "why": f"{type(e).__name__}: {e}"})
    return frames, reasons


def h_world_collision_at_seed(_q):
    """cuRobo's DIRECT world-collision verdict at the seed (no IK) + analytic cross-check. GET params:
    tcp, cloud_only (default 1)."""
    try:
        st = motion_stack()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    groups = st.group_names
    tcp = (_q.get("tcp", [None])[0]) or (groups[0] if groups else None)
    cloud_only = (_q.get("cloud_only", ["1"])[0] != "0")
    try:
        with _bridge_lock:
            rep = st.world_collision_at_seed(tcp, cloud_only=cloud_only)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "tcp": tcp}
    rep["ok"] = True
    return rep


def h_world_alignment(_q):
    """Is the scanned cloud in the SAME frame as the robot + is the seed applied? GET param: tcp."""
    try:
        st = motion_stack()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    groups = st.group_names
    tcp = (_q.get("tcp", [None])[0]) or (groups[0] if groups else None)
    try:
        with _bridge_lock:
            rep = st.world_alignment(tcp)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "tcp": tcp}
    rep["ok"] = True
    return rep


def h_verify_start(_q):
    """RIGOROUS verification that the robot's START config is inside a world obstacle (and which one),
    via cuRobo IK + per-obstacle ablation. GET param: tcp."""
    try:
        st = motion_stack()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    groups = st.group_names
    tcp = (_q.get("tcp", [None])[0]) or (groups[0] if groups else None)
    try:
        with _bridge_lock:
            rep = st.verify_start_collision(tcp)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "tcp": tcp}
    rep["ok"] = True
    return rep


def h_diagnose_motion(_q):
    """Decisive collision diagnosis for 'no collision-free trajectory': plans the SAME small move WITH
    vs WITHOUT the world and reports what the start config overlaps — so we KNOW if it's world
    collision, self-collision, or reach, instead of guessing. GET params: tcp, optional x,y,z."""
    try:
        st = motion_stack()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    groups = st.group_names
    tcp = (_q.get("tcp", [None])[0]) or (groups[0] if groups else None)
    xyz = None
    if all(k in _q for k in ("x", "y", "z")):
        try:
            xyz = [float(_q.get(k, ["0"])[0]) for k in ("x", "y", "z")]
        except (TypeError, ValueError):
            xyz = None
    try:
        with _bridge_lock:
            rep = st.diagnose_motion(tcp, xyz)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "tcp": tcp}
    rep["ok"] = True
    return rep


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

    def read_joints_map(self):
        """EVERY joint the cell reports, by name — the motion check's crossed-binding diag
        (which arm actually moved) needs the whole rig, not just this step's chain."""
        import numpy as np
        jp = (self._observation().joint_positions) or {}
        out = {}
        for n, v in jp.items():
            arr = np.asarray(v, float).reshape(-1)
            if arr.size:
                out[str(n)] = float(arr[0])
        return out

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
        self._issue_direct(q)
        # ROUTINE recording: engine-commanded motion runs with _bridge_lock held for the WHOLE
        # move, so the edge's poll ticker is blind during it. The commanded waypoints ARE the
        # traversed path — report each one to the session's recorder (set as `on_move`), so the
        # recorded routine has the full safe passage, not a blind jump the replay guard refuses.
        hook = getattr(self, "on_move", None)
        if callable(hook):
            try:
                hook(q)
            except Exception:  # noqa: BLE001 — recording must never break a motion
                pass

    def _issue_direct(self, q):
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

    def move_direct(self, joints):
        """A DIRECT joint move that NEVER touches the planner — routine REPLAY re-follows the
        recorded, human-traversed path in dense tiny hops, so planning (cuRobo) is neither
        needed nor wanted (the world model isn't validated at calibration time)."""
        self._direct_move([float(x) for x in joints])

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


def _arm_flange_map(cy: dict, urdf_path: str) -> dict:
    """Map each authored arm/group → its flange (tip) URDF link, so an eye-to-hand
    `board_source="arm"` step resolves the PRESENTING arm's chain (not the fixed camera's,
    which has no movable joint). Empty when there's no URDF / no groups."""
    from calib_engine import urdf_io
    if not os.path.exists(urdf_path):
        return {}
    groups = urdf_io.robot_config(cy, urdf_path).get("groups", [])
    return {str(g.get("name")): str((g.get("tip_links") or [""])[0])
            for g in groups if (g.get("tip_links") or [])}


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
        items = pipeline.resolve_items(psteps, urdf_path, arm_flanges=_arm_flange_map(cy, urdf_path))
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


_JSTREAM = None


def _feed_calib_recorder(t: float, joints: dict) -> None:
    """The routine recorder rides the INDEPENDENT joint stream: whatever the stream sees, an
    armed recorder gets — recording is telemetry, never hostage to the calibration pipeline.
    The WHOLE map is recorded (every actuated joint, any morphology): a modeled + commissioned
    rig is ONE body — record all, so drive-to-start can restore all and the replayed chain's
    corridor is valid (the other limbs are where they were when it was traversed)."""
    sess = getattr(_CALIB, "session", None) if _CALIB is not None else None
    rec = getattr(sess, "recorder", None) if sess is not None else None
    if rec is None or not rec.armed:
        return
    try:
        rec.tick_map(joints, t=t)
    except Exception:  # noqa: BLE001 — a bad sample skips a waypoint, never kills telemetry
        pass


def _joint_stream():
    """Build (once) the single-producer joint-state stream for the connected cell Bridge.
    PUSH mode: the Bridge's `stream_joints(on_sample, should_stop)` — the CONTRACT
    (bridge_primitives.md): the cell subscribes to each controller's NATIVE report stream, so
    the rate is the SDK's own (an xArm pushes at 250 Hz). DEGRADED fallback for pre-contract
    cells: poll `get_observation()` under the bridge lock non-blocking at a few Hz (still ONE
    centralized poller instead of N competing ones)."""
    global _JSTREAM
    if _JSTREAM is not None:
        return _JSTREAM
    b = get_bridge()
    if b is None:
        return None
    from joint_stream import DEGRADED_HZ, JointStream

    def read():
        obs = b.get_observation()
        return {name: _joint_value(val) for name, val in (obs.joint_positions or {}).items()}

    if callable(getattr(b, "stream_joints", None)):
        # Push primary + poll fallback: a cell may carry a STUB stream_joints (the seed
        # template's TODO), so the stream probes BEHAVIOR — no samples within probe_s →
        # falls back to observation polling and says so in the edge log (never silent).
        js = JointStream(read, subscribe=b.stream_joints, hz=DEGRADED_HZ, lock=_bridge_lock,
                         name="joint-stream-push")
        print("[edge] joint stream: push mode (stream_joints) with observation-poll fallback")
    else:
        js = JointStream(read, hz=DEGRADED_HZ, lock=_bridge_lock, name="joint-stream-degraded")
        print("[edge] joint stream: degraded observation polling (cell has no stream_joints)")
    js.subscribe(_feed_calib_recorder)
    _JSTREAM = js.start()
    # The MEASURED-joints tap for the motion stack's tracking watchdog: lock-free (reads the
    # stream's cached latest), name-keyed — how "the arm is NOT actually moving" gets caught
    # DURING execution instead of after 134 silent seconds.
    try:
        b.latest_joints = lambda: (_JSTREAM.latest[1] if _JSTREAM and _JSTREAM.latest else {})
    except Exception:  # noqa: BLE001 — watchdog degrades to post-move proximity only
        pass
    return _JSTREAM


# ── the motion stack's LIFECYCLE (the Studio header's ⚡ chip) ────────────────
# The commission-proven MotionStack is expensive to bring alive (planner build + cuRobo
# warmup, tens of seconds) so it is NEVER built implicitly — the operator starts it from the
# header, and consumers (drive-to-start) refuse until it's alive.
_MOTION_WARM = {"state": "off", "error": None}   # off | warming | error (alive = stack != None)


def h_motion_status(_q):
    """The motion stack lifecycle for the header chip: alive | warming | error | off.
    WARMING WINS over the stack object existing — `motion_stack()` returns in seconds but the
    planner prewarm runs for minutes after; reporting alive early was the lying chip."""
    if _MOTION_WARM["state"] == "warming":
        return {"ok": True, "state": "warming", "groups": [], "error": None}
    if _MOTION_STACK is not None:
        try:
            groups = list(_MOTION_STACK.group_names)
        except Exception:  # noqa: BLE001
            groups = []
        return {"ok": True, "state": "alive", "groups": groups, "error": None}
    return {"ok": True, "state": _MOTION_WARM["state"], "groups": [], "error": _MOTION_WARM["error"]}


def _warm_motion_stack():
    """The warm-up BODY (shared by the operator ⚡ and the boot auto-warm; caller sets
    _MOTION_WARM to "warming" first). LOCK-FREE: the bridge lock is held only for the brief
    frame grab — never across GPU work — so the operator can jog/calibrate/watch the mirror
    seconds after boot while the full warm proceeds in the background.

    WHY this is safe now (it was NOT in round 3): cuRobo captures CUDA graphs in PyTorch's
    default `capture_error_mode="global"`, which forbids CUDA calls in EVERY thread during a
    capture — that's what killed the ZED grab and poisoned the process. `capture_mode.install()`
    (run at every cuRobo entry point) switches capture to "thread_local": only the capturing
    thread is restricted, camera grabs in other threads are legal. The documented residual —
    a cross-thread call can still INVALIDATE the capture itself — fails SAFE: the warmup step
    errors, `_recover_if_capture_poisoned` resets the solver graphs, the next plan recaptures.

    The stack's own bridge is the `_LockedBridge` proxy, so its seed reads serialize per-call
    with operator traffic; concurrent gotos can't double-build (MotionStack._build_lock) — they
    WAIT for the planner build, exactly as they used to wait for the ⚡ click. Warm order:
    planner build + IK/trajopt warmup → ALIVE → PRM roadmap → perception WARM-ONLY build
    (mapper/segmenter/ESDF kernels paid; the planner keeps planning against the modeled world —
    see MotionStack.prewarm_perception). Post-ALIVE failures are stamped, never fatal."""
    try:
        from motion_engine.mtrace import span as _mspan, stamp as _mstamp
        with _mspan("warmup: MotionStack.from_cell (artifact loads)"):
            st = motion_stack()
        with _mspan("warmup: prewarm (planner build + IK/trajopt warmup — the fast path, "
                    "bridge lock NOT held)"):
            warm = st.prewarm()                       # THE robot planner, built up front
        _MOTION_WARM.update(state="off", error=None)   # ALIVE (derived from the stack itself)
        print(f"[edge] motion stack ALIVE · prewarm {warm.get('seconds')}s · groups {warm.get('groups')}"
              " · deep-warm (PRM roadmap + perception) continuing in the background")
    except Exception as e:  # noqa: BLE001
        _MOTION_WARM.update(state="error", error=f"{type(e).__name__}: {e}")
        print(f"[edge] motion stack warmup FAILED: {type(e).__name__}: {e}")
        return

    # ── stage 2: the stack is ALIVE; pre-pay the rest in the background, lock-free.
    try:
        if hasattr(st, "finish_warmup"):
            st.finish_warmup()                        # PRM roadmap (thread_local capture)
    except Exception as e:  # noqa: BLE001
        _mstamp("warmup: PRM roadmap warm failed (non-fatal — roadmap stays lazy)",
                error=f"{type(e).__name__}: {e}")
    try:
        with _bridge_lock:                            # ONLY the grab holds the lock (~2 s)
            b = get_bridge()
            frames, reasons = (_live_frames(st, b) if b is not None else ([], ["no bridge"]))
        if frames:
            with _mspan("warmup: perception pre-build (WARM-ONLY — kernels/mapper paid, "
                        "the modeled world stays the planner's world)", cameras=len(frames)):
                if hasattr(st, "prewarm_perception"):
                    st.prewarm_perception(frames)
                else:
                    st.update_world_live(frames)
        else:
            _mstamp("warmup: perception pre-build skipped (live world will build lazily)",
                    reasons="; ".join(reasons) or "no frames")
    except Exception as e:  # noqa: BLE001 — perception pre-build is opportunistic
        _mstamp("warmup: perception pre-build failed (non-fatal — lazy build remains)",
                error=f"{type(e).__name__}: {e}")
    print("[edge] motion deep-warm complete (PRM roadmap + perception kernels)")


def h_motion_warmup(_q):
    """Bring the COMMISSIONED motion stack alive on a background thread (build + warmup holds
    the bridge lock for tens of seconds; the header chip shows 'warming' meanwhile)."""
    if _MOTION_STACK is not None:
        return {"ok": True, "state": "alive"}
    if _MOTION_WARM["state"] == "warming":
        return {"ok": True, "state": "warming"}
    _MOTION_WARM.update(state="warming", error=None)
    threading.Thread(target=_warm_motion_stack, daemon=True, name="motion-warmup").start()
    return {"ok": True, "state": "warming"}


def _autowarm_on_boot():
    """AUTO-prewarm at edge boot, in the background: when the cell is already COMMISSIONED,
    start the exact warm-up the ⚡ chip triggers without waiting for an operator. WHY: cuRobo's
    design amortizes a large fixed cost (runtime kernel compile + CUDA-graph capture + warmup
    plans) over a resident process, but our workflow restarts the edge after every authored-cell
    fix — so the fixed cost kept landing on the first MOVEMENT request, minutes of dead air.
    Paying it at boot (contention-free: no operator is mid-flow yet) makes the common case
    "already warm". Motion itself stays exactly as gated (commission gate + alive check) — only
    the passive GPU build becomes implicit. Kill-switch: REMOROO_EDGE_AUTOWARM=0.

    Waits for the BRIDGE first: the planner cfg seeds its cspace default from live joints, and a
    bridgeless build would bake zeros (the fly-home hazard `_seed_positions` guards against)."""
    if os.environ.get("REMOROO_EDGE_AUTOWARM", "1").lower() in ("0", "false", "off"):
        print("[edge] auto-warmup disabled (REMOROO_EDGE_AUTOWARM=0)")
        return

    def _wait_and_warm():
        st_file = CELL_DIR / "setup_state.json"
        try:
            gates = (json.loads(st_file.read_text(encoding="utf-8")).get("gates") or {}) if st_file.exists() else {}
        except Exception:  # noqa: BLE001
            gates = {}
        if gates.get("commission") != "done":
            return                       # not commissioned — warm-up stays operator-triggered (⚡)
        deadline = time.time() + 180.0
        while time.time() < deadline:
            if get_bridge() is not None:
                break
            time.sleep(3.0)
        else:
            print("[edge] auto-warmup: bridge never came up within 3 min — skipping (⚡ still works)")
            return
        if _MOTION_STACK is not None or _MOTION_WARM["state"] == "warming":
            return                       # an operator beat us to it
        _MOTION_WARM.update(state="warming", error=None)
        print("[edge] auto-warmup: commissioned cell + bridge up — warming the motion stack in the background")
        _warm_motion_stack()

    threading.Thread(target=_wait_and_warm, daemon=True, name="motion-autowarm").start()


def _planned_motion_gate():
    """The trust line for AUTOMATIC planned motion (shared by drive-to-start + planned replay):
    the commission gate must be DONE (setup_state.json — server-side enforcement; the UI gates
    too) and the commission-proven stack must be ALIVE (the header ⚡). Returns an error string
    or None."""
    import json as _json
    st_file = CELL_DIR / "setup_state.json"
    try:
        gates = (_json.loads(st_file.read_text(encoding="utf-8")).get("gates") or {}) if st_file.exists() else {}
    except Exception:  # noqa: BLE001
        gates = {}
    if gates.get("commission") != "done":
        return ("commission gate not done — automatic planned motion is only allowed once the "
                "motion stack was PROVEN live; use the tape replay or jog manually")
    if _MOTION_STACK is None:
        return "motion stack not running — start it from the header (⚡ Motion), then retry"
    return None


def _replay_step_planned() -> dict:
    """One PLANNED replay step: fetch the next capture mark from the session, drive it with the
    commissioned stack (one smooth streamed whole-body trajectory — not the tape's ramped
    micro-hops), then capture. The session owns collection state; the edge owns motion — the
    same boundary as drive-to-start."""
    err = _planned_motion_gate()
    if err:
        return {"ok": False, "error": err}
    svc = _calib_service()
    s = svc.session
    if s is None:
        return {"ok": False, "error": "no calibration selected — select the step first"}
    g = s.replay_next_goal()
    if g.get("error"):
        return {"ok": False, "error": g["error"]}
    if g.get("done"):
        return {"ok": True, "done": True, "collected": g.get("collected"), "heldout": g.get("heldout")}
    goal = {n: float(v) for n, v in zip(g["joint_names"], g["joints"])}
    t0 = time.time()
    with _bridge_lock:
        res = _MOTION_STACK.goto_joints(goal, execute=True)
    if not res.ok:
        # PLAN refused (never moved) → the world changed under a recorded pose: SKIP the mark
        # and calibrate from the rest. EXECUTION failure (the arm was moving) → stop the run.
        if res.executed or res.aborted:
            return {"ok": False, "error": f"mark {g['mark']}/{g['marks']}: {res.message}"}
        sk = s.replay_skip_mark(res.message)
        if sk.get("error"):
            return {"ok": False, "error": sk["error"]}
        print(f"[replay-planned] mark {g['mark']}/{g['marks']} SKIPPED (unplannable in the "
              f"current world): {res.message}", flush=True)
        return {"ok": True, "skipped": True, **sk}
    cap = s.replay_capture_mark()
    if cap.get("error"):
        return {"ok": False, "error": cap["error"]}
    st = cap.get("settle") or {}
    settle_txt = (f"settled {st.get('waited_s')}s/{st.get('drift_px')}px" if st.get("settled")
                  else f"UNSETTLED after {st.get('waited_s')}s (drift {st.get('drift_px')}px)")
    print(f"[replay-planned] mark {g['mark']}/{g['marks']} · goto {time.time() - t0:.1f}s · "
          f"{settle_txt} · capture {'ok' if cap.get('accepted') else 'MISSED'}", flush=True)
    return {"ok": True, **cap}


def _active_step() -> dict:
    """One ACTIVE calibration step: the session picks the next-best-view pose (posegen's
    predicted-σ ranking, board-size-aware framing), the edge drives it with the commissioned
    stack, then the session captures + re-solves. Loops from the Studio until the covariance
    converges — the closed loop beyond the recorded marks. Same boundary as planned replay:
    the engine never moves; motion is the edge's fusion, behind the commission gate."""
    err = _planned_motion_gate()
    if err:
        return {"ok": False, "error": err}
    svc = _calib_service()
    s = svc.session
    if s is None:
        return {"ok": False, "error": "no calibration selected — select the step first"}
    g = s.active_next()
    if g.get("error"):
        return {"ok": False, "error": g["error"]}
    if g.get("done"):
        print(f"[active] done · reason={g.get('reason')} · added {g.get('added')} poses"
              + (f" · posegen culls {g['posegen']}" if g.get("posegen") else ""), flush=True)
        return {"ok": True, **g}
    goal = {n: float(v) for n, v in zip(g["joint_names"], g["joints"])}
    t0 = time.time()
    with _bridge_lock:
        res = _MOTION_STACK.goto_joints(goal, execute=True)
    if not res.ok:
        # PLAN refused (never moved) → drop this suggestion, active_next picks another (the
        # session ends the loop after 3 refusals in a row). EXECUTION failure → stop the run.
        if res.executed or res.aborted:
            return {"ok": False, "error": f"pose {g['added'] + 1}/{g['max']}: {res.message}"}
        fr = s.active_goto_failed(res.message)
        if fr.get("error"):
            return {"ok": False, "error": fr["error"]}
        print(f"[active] suggestion unplannable ({fr.get('fails', '?')}/3 in a row) — "
              f"{'ending: workspace too constrained' if fr.get('done') else 'trying another'}: "
              f"{res.message}", flush=True)
        return {"ok": True, **fr}
    cap = s.active_capture()
    if cap.get("error"):
        return {"ok": False, "error": cap["error"]}
    st = cap.get("settle") or {}
    settle_txt = (f"settled {st.get('waited_s')}s/{st.get('drift_px')}px" if st.get("settled")
                  else f"UNSETTLED after {st.get('waited_s')}s (drift {st.get('drift_px')}px)")
    obs = cap.get("observability") or {}
    worst_t = max((v for k, v in obs.items() if k.startswith("t")), default=None)
    print(f"[active] pose {cap.get('added')}/{cap.get('max')} · goto {time.time() - t0:.1f}s · "
          f"{settle_txt} · capture {'ok' if cap.get('accepted') else 'MISSED'} · "
          f"worst trans σ {worst_t if worst_t is None else round(worst_t, 2)}mm", flush=True)
    return {"ok": True, **cap}


def _replay_goto_start() -> dict:
    """Drive the arm to the selected step's routine START with the COMMISSION-PROVEN motion
    stack (collision-free joint-space plan through the commissioned executor) — the automatic
    'jog it there'. Every guard refuses LOUDLY: (1) the commission gate must be DONE
    (setup_state.json — the trust line: automatic planned motion only after the stack was
    proven live; the UI gates too, this is the server-side enforcement), (2) the stack must be
    ALIVE (the header ⚡ chip), (3) a saved routine must exist, and (4) the GROUP is resolved
    from the routine's JOINT SET (data over labels — any morphology), cross-checked against the
    stack's groups. The tape replay itself stays planner-free (move_direct) after this."""
    import json as _json
    import numpy as _np
    from calib_engine.routine import HOP_TOL_RAD

    t0 = time.time()

    def _lap(tag, **kv):
        kvs = (" · " + " ".join(f"{k}={v}" for k, v in kv.items())) if kv else ""
        print(f"[goto] +{time.time() - t0:6.2f}s {tag}{kvs}", flush=True)

    _lap("verb entry")
    svc = _calib_service()
    s = svc.session
    if s is None:
        return {"ok": False, "error": "no calibration selected — select the step first"}
    st_file = CELL_DIR / "setup_state.json"
    try:
        gates = (_json.loads(st_file.read_text(encoding="utf-8")).get("gates") or {}) if st_file.exists() else {}
    except Exception:  # noqa: BLE001
        gates = {}
    _lap("gates read", commission=gates.get("commission"))
    if gates.get("commission") != "done":
        return {"ok": False, "error": "commission gate not done — automatic planned motion is only "
                "allowed once the motion stack was PROVEN live; jog the arm to the start manually"}
    if _MOTION_STACK is None:
        return {"ok": False, "error": "motion stack not running — start it from the header (⚡ Motion), then retry"}
    if not s.bridge.estop_ok():
        return {"ok": False, "error": "E-stop not OK"}
    sp = s.routine_start_pose(svc.calib_dir or ".")
    _lap("routine start pose", ok=sp.get("ok"), joints=len(sp.get("joints") or []),
         at_start=sp.get("at_start"))
    if not sp.get("ok"):
        return {"ok": False, "error": sp.get("error", "no recorded routine")}
    if sp.get("at_start"):
        return {"ok": True, "message": "already at the start pose", "at_start": True,
                "deltas_rad": sp.get("deltas_rad")}
    jnames = sp.get("joint_names") or []
    if not jnames:
        return {"ok": False, "error": "routine carries no joint names (older recording) — "
                "re-record this calibration once; new recordings capture the whole body"}
    # WHOLE-BODY goal: every recorded joint goes to its recorded start. A commissioned rig is
    # ONE body — no group resolution, no held-at-seed limbs; the routine says where EVERYTHING
    # belongs, and the foreign-motion guard refuses any plan that moves an un-goaled joint.
    goal = {n: float(v) for n, v in zip(jnames, sp["joints"])}
    _lap("goal built — calling goto_joints (bridge lock)", named=len(goal))
    with _bridge_lock:
        res = _MOTION_STACK.goto_joints(goal, execute=True)
    _lap("goto_joints returned", ok=res.ok, executed=getattr(res, "executed", None),
         message=res.message)
    out = {"ok": bool(res.ok), "message": res.message}
    if not res.ok and not (res.executed or res.aborted):
        # PLAN refused, never moved — the recorded START may now sit inside a new obstacle.
        # Flag it so the Studio can still run the PLANNED replay (its marks are absolute
        # goals; only the TAPE needs start proximity, and that path refuses on its own).
        out["plan_refused"] = True
    try:
        traj = getattr(res, "trajectory", None)
        if traj is not None:
            import numpy as _np2
            pos = _np2.asarray(traj.positions, float)
            span = _np2.max(_np2.abs(pos - pos[0]), axis=0)
            movers = {n: round(float(v), 3) for n, v in zip(traj.joint_names, span) if v > 0.02}
            out["moved_joints"] = movers
            print(f"[edge] drive-to-start planned motion: {movers}")
    except Exception:  # noqa: BLE001 — diagnostics only
        pass
    try:
        chain_names = list(getattr(s.bridge, "joint_names", []) or [])
        start_map = dict(zip(jnames, sp["joints"]))
        if chain_names and all(n in start_map for n in chain_names):
            w0 = _np.asarray([start_map[n] for n in chain_names], float)
        else:
            w0 = _np.asarray(sp["joints"], float)
        d = _np.abs(_np.asarray(s.bridge.read_joints(), float).reshape(-1) - w0)
        out["deltas_rad"] = d.round(4).tolist()
        out["at_start"] = bool(d.max() <= HOP_TOL_RAD)
        _lap("post-move proximity", at_start=out["at_start"], worst=round(float(d.max()), 3))
    except Exception as e:  # noqa: BLE001 — proximity is advisory; replay_start re-gates it
        _lap("post-move proximity FAILED", error=f"{type(e).__name__}: {e}")
    _lap("done", ok=out.get("ok"))
    return out


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
    try:
        _joint_stream()   # the independent joint stream feeds the routine recorder (+ the mirror)
    except Exception as e:  # noqa: BLE001 — telemetry down must never block calibration itself
        print(f"[edge] joint stream unavailable (recorder will be sparse): {type(e).__name__}: {e}")
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
        items = pipeline.resolve_items(psteps, urdf_path, arm_flanges=_arm_flange_map(cy, urdf_path))
        target_specs = {tid: {"type": t.type, "params": t.params} for tid, t in tspecs.items()}
    else:
        from calib_engine.session import build_plan
        items = build_plan(urdf_path)
        spec = _target_spec_from_cell(cal)
        for it in items:
            it.target_id = "default"
        target_specs = {"default": spec} if spec else {}
    links = [l.get("name") for l in ET.parse(urdf_path).getroot().findall("link")]
    from calib_engine.service import saved_calib_flags
    # `saved` from the PERSISTED calibrations — the edge answers `pipeline` itself (this
    # lightweight pre-camera path), so the flags must be stamped HERE too, same function as
    # the service, or saved steps stay locked in the Studio (the "redo everything" complaint).
    items_json = saved_calib_flags(str(CELL_DIR / "calibration"), [_planitem_json(p) for p in items])
    return {"type": "pipeline", "items": items_json,
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


# --------------------------------------------------------------------------- #
# Task protocol: /edge/task/<verb> -> task_engine TaskService (mirrors calib).  #
# Off-robot verbs answer immediately; bridge-touching verbs (BRIDGE_VERBS)      #
# serialize under the same _bridge_lock as calibration (G14 discipline).        #
# --------------------------------------------------------------------------- #
_task_service = None


def _task_handle(verb: str, body: dict) -> dict:
    global _task_service
    try:
        from task_engine.service import BRIDGE_VERBS, TaskService
    except Exception as e:  # noqa: BLE001 - stated, never a dead handler
        return {"error": f"task_engine unavailable: {type(e).__name__}: {e}"}
    if _task_service is None:
        # stack_provider hands the LIVE MotionStack to build_env(shared) — the authored
        # task package composes its GenericEnv from shared["stack"]/shared["bridge"],
        # never by reaching into this module.
        _task_service = TaskService(stack_provider=lambda: motion_stack())
    if _task_service._bridge is None:
        # The cell Bridge comes up mid-session (primitives.py is authored at G2), so bind
        # lazily on every call until it exists — same discipline as the live mirror.
        try:
            _task_service._bridge = get_bridge()
        except Exception:  # noqa: BLE001 - bridge not ready is a normal early state
            pass
    if verb in BRIDGE_VERBS:
        with _bridge_lock:
            return _task_service.handle(verb, body)
    return _task_service.handle(verb, body)


_SNAP_CACHE: dict = {}   # viewfinder coalescing: verb -> {"t": stamp, "data": last-good payload}


def _calib_handle(verb: str, body: dict) -> dict:
    # The pure-URDF reads (`pipeline`/`plan`) touch no bridge, so they don't need the lock;
    # everything else may move/capture/read the shared device, so it serialises with the live
    # SSE poll (G14). RLock so a handler that internally reads joints doesn't self-deadlock.
    if verb in ("snapshot", "b2b_snapshot"):
        # VIEWFINDER FAST PATH — never queue a snapshot behind real work. The panel polls at
        # 1 Hz; each snapshot costs >1 s on the Orin (grabs + detection + JPEG), so blocking
        # on _bridge_lock made arrivals outpace service and the lock queue grew WITHOUT BOUND
        # — every button (Save included) then waited minutes behind stacked snapshots. A
        # fresh-enough cache answers instantly; when the bridge is busy (a solve, a capture)
        # the STALE frame is returned marked, and the feed catches up when the bridge frees.
        c = _SNAP_CACHE.get(verb)
        now = time.time()
        if c is not None and now - c["t"] < 0.7:
            return c["data"]
        if not _bridge_lock.acquire(blocking=False):
            if c is not None:
                return {**c["data"], "stale": True}
            return {"error": "camera busy — the feed resumes when the current operation finishes"}
        try:
            out = _calib_handle_locked(verb, body)
        finally:
            _bridge_lock.release()
        if isinstance(out, dict) and "error" not in out:
            _SNAP_CACHE[verb] = {"t": time.time(), "data": out}
        return out
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
        if verb == "replay_step_planned":
            return _replay_step_planned()
        if verb == "active_step":
            return _active_step()
        if verb == "active_start":
            gate_err = _planned_motion_gate()
            if gate_err:
                return {"ok": False, "error": gate_err}
            return _calib_service().handle(verb, body)
        if verb == "replay_start_planned":
            gate_err = _planned_motion_gate()
            if gate_err:
                return {"ok": False, "error": gate_err}
            return _calib_service().handle(verb, body)
        if verb == "replay_goto_start":
            # drive-to-start via the COMMISSION-PROVEN motion stack (edge-level: the engine
            # never imports motion_engine — gate fusion is the edge's job).
            return _replay_goto_start()
        return _calib_service().handle(verb, body)
    except Exception as e:  # noqa: BLE001
        return {"error": f"{type(e).__name__}: {e}"}


# --------------------------------------------------------------------------- #
# The AUTHORED kinematic config (the morphology-agnostic model the Studio + the #
# engine share) + static obstacles. The config is `cell.yaml: groups` — authored #
# at the model gate, validated against the URDF, served live. No arms.yaml.      #
# --------------------------------------------------------------------------- #
def h_arms(_q):
    """Serve the AUTHORED kinematic config (groups) for the Studio: `cell.yaml.groups` (or `arms[]`
    back-compat), VALIDATED against the URDF. No derivation, no `arms.yaml` — computed on demand from
    the single source of truth, so it cannot drift. `errors` flags any agent mistake to surface."""
    from calib_engine import urdf_io
    rm = CELL_DIR / "robot_model"
    urdf = rm / "robot.urdf"
    cy = load_cell_yaml()
    if not urdf.exists():
        return {"base_link": "", "groups": [], "reason": "no robot.urdf yet — model the cell first"}
    try:
        cfg = urdf_io.robot_config(cy, str(urdf))
        errors = urdf_io.validate_robot_config(cfg, str(urdf))
        base = (cfg["groups"][0].get("base_link") if cfg["groups"] else "") or ""
        return {"base_link": base, "groups": cfg["groups"], "cameras": cfg["cameras"],
                "source": ("authored" if cy.get("groups") else "arms_promoted" if cy.get("arms") else "empty"),
                "errors": errors}
    except Exception as e:  # noqa: BLE001
        return {"error": f"{type(e).__name__}: {e}", "groups": []}


def h_set_arms(_q, body):
    """Persist the operator's confirmed group labels (the verify-by-motion result) into
    `cell.yaml.groups` — the source of truth. Body: `{labels: {group: {side: 'left', …}}}` (or legacy
    `{sides: {group: 'left'}}`). Materialises the authored groups into cell.yaml on confirm, so the
    config is deliberate + confirmed, never re-derived."""
    import yaml  # type: ignore
    from calib_engine import urdf_io
    urdf = CELL_DIR / "robot_model" / "robot.urdf"
    cy = load_cell_yaml()
    cfg = urdf_io.robot_config(cy, str(urdf)) if urdf.exists() else {"groups": list(cy.get("groups") or [])}
    groups = cfg["groups"]
    labels = (body or {}).get("labels") or {}
    sides = (body or {}).get("sides") or {}
    for g in groups:
        nm = g.get("name")
        if isinstance(labels.get(nm), dict):
            g.setdefault("tags", {}).update(labels[nm])
        if nm in sides:
            g.setdefault("tags", {})["side"] = str(sides[nm])
    cy["groups"] = groups
    (CELL_DIR / "cell.yaml").write_text(yaml.safe_dump(cy, sort_keys=False), encoding="utf-8")
    return {"ok": True, "groups": groups}


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
    reset_curobo_cache()   # the SEPARATE feasibility checker (_curobo_world) bakes obstacles in
    # HOT-SWAP the warm motion planner's world (cuRobo update_world) instead of a rebuild+re-warm —
    # an obstacle edit must NOT throw away the costly cuRobo warmup. Fall back to a rebuild only if the
    # in-place swap fails (e.g. the obstacle count overflowed the planner's collision cache).
    swap = {"hot_swapped": False}
    if _MOTION_STACK is not None:
        try:
            with _bridge_lock:
                r = _MOTION_STACK.reload_world()
            swap = {"hot_swapped": bool(r.get("ok")), "n_planners": r.get("n_planners")}
            if not r.get("ok"):
                reset_motion_stack()
        except Exception as e:  # noqa: BLE001
            reset_motion_stack()
            swap = {"hot_swapped": False, "error": f"{type(e).__name__}: {e}"}
    return {"ok": True, "obstacles": cy["obstacles"], **swap}


def _find_collision_spheres(obj) -> dict:
    """Find the `collision_spheres` mapping (link -> [sphere,...]) at ANY depth — the agent's file
    may nest it under robot_cfg.kinematics, at the top level, or elsewhere."""
    if isinstance(obj, dict):
        cs = obj.get("collision_spheres")
        if isinstance(cs, dict) and cs:
            return cs
        for v in obj.values():
            found = _find_collision_spheres(v)
            if found:
                return found
    return {}


def h_spheres(_q):
    """The REAL cuRobo collision spheres for the Studio's collision view — parsed HERE on the edge
    (which owns yaml/numpy/cuRobo), NOT in the stdlib-only studio_server proxy (it has no PyYAML, so
    `/project/spheres` returned nothing and the view fell back to the mesh approximation). Robust to
    nesting, to `{center,radius}` AND `[x,y,z,r]` sphere forms, and to numpy-tagged dumps."""
    import yaml  # type: ignore
    f = CELL_DIR / "robot_model" / "collision_spheres.yml"
    if not f.exists():
        return {"spheres": []}
    try:
        text = f.read_text(encoding="utf-8")
        try:
            data = yaml.safe_load(text) or {}
        except yaml.YAMLError:
            data = yaml.unsafe_load(text) or {}        # local agent-written file: recover numpy tags
        cs = _find_collision_spheres(data)
        out, skipped = [], 0
        for link, arr in (cs or {}).items():
            for s in (arr or []):
                try:
                    if isinstance(s, dict):
                        c, r = s.get("center"), s.get("radius")
                    elif isinstance(s, (list, tuple)) and len(s) >= 4:
                        c, r = s[:3], s[3]
                    else:
                        c = r = None
                    if c is not None and r is not None and len(c) >= 3:
                        out.append({"link": str(link), "center": [float(c[0]), float(c[1]), float(c[2])], "radius": float(r)})
                    else:
                        skipped += 1
                except (TypeError, ValueError):
                    skipped += 1
        resp = {"spheres": out, "n": len(out)}
        if not out:
            resp["error"] = (f"no parseable spheres (found_links={len(cs)}, skipped={skipped}); expected "
                             "link -> [{center:[x,y,z], radius:r}] or [x,y,z,r]")
        return resp
    except Exception as e:  # noqa: BLE001
        return {"spheres": [], "error": f"{type(e).__name__}: {e}"}


# route table: path -> (kind, handler). kind in {"json","json_body","sse"}
def sse_realtime(_q):
    """The REALTIME gate — LAUNCH the cell's STANDALONE `realtime.py` as its OWN PROCESS
    (`python3 -u realtime.py --emit-json`) and stream the JSON events it prints to stdout. We do NOT
    import it into the edge: process isolation is LOAD-BEARING. realtime.py grabs a CUDA-native camera
    (ZED) AND runs cuRobo — in a SEPARATE process it gets its OWN clean CUDA context, so the camera and
    cuRobo can't corrupt each other's CUDA-graph state (the cudaErrorStreamCaptureUnsupported /
    illegal-address cascade you get running it inside the busy edge process). This is ALSO exactly how the
    customer runs it: `python3 remoroo_cell/realtime.py`. The ZED is single-open, so we RELEASE the edge's
    bridge first (the child owns the devices) and reconnect lazily after; the child polls the physical
    E-stop, and the Studio E-stop (/edge/estop) also terminates the child."""
    global _bridge, _realtime_active, _realtime_proc
    import subprocess
    import sys as _sys
    import json as _json
    script = CELL_DIR / "realtime.py"
    if not script.exists():
        yield {"done": True, "result": {"error": "realtime.py not authored yet — the agent writes it; "
                                                  "run it standalone with `python3 remoroo_cell/realtime.py`"}}
        return
    # Yield the devices to the child: stop touching the bridge, then close it so the child can open the ZED.
    _realtime_active = True
    with _bridge_lock:
        if _bridge is not None:
            try:
                _bridge.close()
            except Exception:  # noqa: BLE001
                pass
            _bridge = None
    yield {"type": "status", "message": "launching `python3 realtime.py` (isolated process)…"}
    proc = subprocess.Popen([_sys.executable, "-u", str(script), "--emit-json"],
                            cwd=str(CELL_DIR.parent), stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1)
    _realtime_proc = proc
    _last_text = ""        # last human/stderr line — the ACTUAL error if the child dies (argparse usage, traceback)
    _saw_voxels = False    # did any event actually carry the world? (chips-but-no-3D is a real failure mode)
    try:
        for line in proc.stdout:                       # stream the child's stdout as it prints
            line = line.rstrip()
            if not line:
                continue
            try:
                ev = _json.loads(line)                 # a JSON event → forwarded (voxels render in the 3D view)
                if isinstance(ev, dict) and ev.get("voxels"):
                    _saw_voxels = True
                yield ev
            except Exception:  # noqa: BLE001
                _last_text = line[:400]
                yield {"type": "status", "message": line[:400]}   # a human print / a traceback line
        proc.wait(timeout=10)
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:  # noqa: BLE001
                proc.kill()
        _realtime_proc = None
        _realtime_active = False                       # the edge may reconnect its own bridge again
    code = proc.returncode or 0
    if code == 0 and _saw_voxels:
        msg = "realtime.py finished"
    elif code == 0:
        msg = "realtime.py exited 0 but NEVER streamed any `voxels` — the 3D world stayed blank (the loop " \
              "isn't emitting the world, or no frame synced). Check the event `voxels`/`mask` fields."
    else:
        msg = f"realtime.py exited with code {code}"
        if _last_text:
            msg += f" — {_last_text}"
        if code == 2:                                  # argparse sys.exit(2): the CLI was rejected, not the perception
            msg += " · code 2 = bad command line (argparse): realtime.py's main() must accept `--emit-json` exactly"
    yield {"done": True, "result": {"ok": code == 0 and _saw_voxels, "message": msg}}


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
    "/edge/spheres": ("json", h_spheres),
    "/live/joints": ("sse", sse_live_joints),
    "/edge/record": ("sse", sse_record),
    "/edge/plan": ("sse", sse_plan),
    "/edge/motion/commission": ("sse", sse_commission),
    "/edge/realtime/run": ("sse", sse_realtime),   # the realtime gate (replaces the deleted demo gate)
    "/edge/motion/plan_move": ("sse", sse_plan_move),
    "/edge/motion/status": ("json", h_motion_status),
    "/edge/motion/warmup": ("json", h_motion_warmup),
    "/edge/motion/tcp_pose": ("json", h_tcp_pose),
    "/edge/motion/suggest_targets": ("json", h_suggest_targets),
    "/edge/motion/diagnose": ("json", h_diagnose_motion),
    "/edge/motion/validate_config": ("json", h_validate_config),
    "/edge/motion/verify_start": ("json", h_verify_start),
    "/edge/motion/world": ("json", h_motion_world),
    "/edge/motion/esdf": ("json", h_motion_esdf),
    "/edge/motion/esdf_live": ("sse", sse_esdf_live),
    "/edge/motion/debug_world": ("json", h_debug_world),
    "/edge/motion/debug_dump": ("json", h_debug_dump),
    "/edge/motion/diagnose_plan": ("json", h_diagnose_plan),
    "/edge/calib/bake": ("json", h_calib_bake),
    "/edge/motion/world_alignment": ("json", h_world_alignment),
    "/edge/motion/world_collision_at_seed": ("json", h_world_collision_at_seed),
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

    def handle_one_request(self):
        # A client (the studio proxy / browser) that disconnects mid-response makes the
        # handler's wfile.write raise BrokenPipeError/ConnectionReset — a NORMAL disconnect
        # (a cancelled poll, a closed tab, the proxy timing out), not an edge error. Swallow it
        # so ThreadingHTTPServer doesn't dump a traceback (now teed to the operator's shell as
        # [edge] noise) and, worse, bury a REAL authored-code traceback the agent needs to see.
        try:
            super().handle_one_request()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            self.close_connection = True

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
        # Task protocol: /edge/task/<verb> -> the stateful TaskService.
        if path.startswith("/edge/task/"):
            return self._serve_json(_task_handle(path[len("/edge/task/"):], self._read_body()))
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
    _autowarm_on_boot()      # commissioned cell → warm the motion stack in the background NOW
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
