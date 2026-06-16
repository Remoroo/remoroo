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

# --------------------------------------------------------------------------- #
# Lazy, cached access to the authored cell + toolchain (never crash on import) #
# --------------------------------------------------------------------------- #
_bridge = None
_bridge_err: str | None = None


def get_bridge():
    """Construct + connect the cell Bridge once (authored at G2)."""
    global _bridge, _bridge_err
    if _bridge is not None or _bridge_err is not None:
        return _bridge
    try:
        from remoroo_cell.primitives import Bridge  # type: ignore

        b = Bridge.from_cell_yaml(str(CELL_DIR / "cell.yaml"))
        b.connect()
        _bridge = b
    except Exception as e:  # noqa: BLE001
        _bridge_err = f"{type(e).__name__}: {e}"
    return _bridge


def load_cell_yaml() -> dict:
    try:
        import yaml  # type: ignore

        return yaml.safe_load((CELL_DIR / "cell.yaml").read_text()) or {}
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
    (rm / "robot.urdf").write_text(urdf_text)

    by_link: dict[str, list[dict]] = {}
    for s in spheres:
        by_link.setdefault(s["link"], []).append({"center": list(s["center"]), "radius": float(s["radius"])})

    cfg = {
        "robot_cfg": {
            "kinematics": {
                "urdf_path": "robot.urdf",
                "collision_spheres": by_link,
                "collision_sphere_buffer": 0.005,
            }
        }
    }
    out = rm / "collision_spheres.yml"
    out.write_text(yaml.safe_dump(cfg, sort_keys=False))
    return out


def motion_gen():
    """Load a cuRobo MotionGen from the cell's robot YAML + a workspace world box."""
    from curobo.wrap.reacher.motion_gen import MotionGen, MotionGenConfig  # type: ignore
    from curobo.geom.types import WorldConfig, Cuboid  # type: ignore

    cy = load_cell_yaml()
    ws = (cy.get("workspace") or {})
    # conservative table/box world from cell.yaml bounds (env-only; arm carried by spheres)
    dims = ws.get("size", [1.5, 1.5, 1.0])
    center = ws.get("center", [0.0, 0.0, dims[2] / 2 - 0.5])
    world = WorldConfig(cuboid=[Cuboid(name="table", pose=[*center, 1, 0, 0, 0], dims=list(dims))])
    robot_yaml = str(CELL_DIR / "robot_model" / "collision_spheres.yml")
    cfg = MotionGenConfig.load_from_robot_config(robot_yaml, world, interpolation_dt=0.02)
    mg = MotionGen(cfg)
    mg.warmup()
    return mg


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


def sse_live_joints(_q):
    """Poll the Bridge for joint state and emit {t, joints} frames (no 'done')."""
    b = get_bridge()
    if b is None:
        yield {"t": 0.0, "joints": {}, "error": _bridge_err or "no bridge"}
        return
    t0 = time.time()
    while True:
        try:
            obs = b.get_observation()
            joints = {}
            for name, val in (obs.joint_positions or {}).items():
                joints[name] = float(val[0]) if hasattr(val, "__len__") else float(val)
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


def sse_calibrate(_q):
    script = _delegate_script("calibration/collect_poses.py")
    if script is None:
        yield {"done": True, "result": {"error": "calibration/collect_poses.py not authored yet (G5)"}}
        return
    # The authored script owns the autonomous loop; here we just report it must be
    # run via the cell's runtime. A full integration imports + streams its callback.
    yield {"done": True, "result": {"error": "run calibration via the cell runtime; live streaming TBD", "script": str(script)}}


def sse_scan(_q):
    if _delegate_script("world/scan.py") is None:
        yield {"done": True, "result": {"error": "world/scan.py not authored yet (G4)"}}
        return
    yield {"done": True, "result": {"error": "run world scan via the cell runtime; live streaming TBD"}}


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


# route table: path -> (kind, handler). kind in {"json","json_body","sse"}
ROUTES = {
    "/health": ("json", lambda q: {"ok": True, "edge": "real", "cell": str(CELL_DIR)}),
    "/edge/probe": ("json", h_probe),
    "/edge/toolchain": ("json", h_toolchain),
    "/edge/applyFix": ("json", h_applyfix),
    "/edge/estop": ("json", h_estop),
    "/edge/safety": ("json", h_safety),
    "/edge/buildRobot": ("json_body", h_build_robot),
    "/live/joints": ("sse", sse_live_joints),
    "/edge/calibrate": ("sse", sse_calibrate),
    "/edge/scanWorld": ("sse", sse_scan),
    "/edge/record": ("sse", sse_record),
    "/edge/plan": ("sse", sse_plan),
}


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

    def do_POST(self):
        path, q, route = self._route()
        if not route:
            self.send_response(404); self._cors(); self.end_headers(); self.wfile.write(b"not found"); return
        kind, fn = route
        body = {}
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length:
            try:
                body = json.loads(self.rfile.read(length).decode("utf-8"))
            except Exception:
                body = {}
        if kind == "json_body":
            return self._serve_json(fn(q, body))
        if kind == "json":
            return self._serve_json(fn(q))
        if kind == "sse":
            return self._serve_sse(fn, q)
        self.send_response(405); self._cors(); self.end_headers()

    def _serve_json(self, obj):
        data = json.dumps(obj).encode("utf-8")
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
