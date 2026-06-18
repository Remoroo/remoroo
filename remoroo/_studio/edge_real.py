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
_bridge_last_try: float = 0.0


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
    """Adapts the cell's primitives Bridge + camera + ChArUco detector to calib_engine's
    BridgeProtocol. The flange pose is reported as chain.fk(joints) (the URDF model FK),
    so the bundle's FK-correction has a chain to refine; joints are read in the chain's
    joint order. Joint-move probes the common cell API names (the agent authors one)."""

    def __init__(self, cell_bridge, chain, joint_names, board_params, dict_name,
                 K=None, dist=None, sdk_T_lr=None):
        self.b = cell_bridge
        self.chain = chain
        self.joint_names = joint_names
        self.board_params = board_params
        self.dict_name = dict_name
        self.K = K
        self.dist = dist                 # optional radial-tangential coeffs (7.2)
        self._sdk_T_lr = sdk_T_lr        # optional 4x4 left->right stereo baseline (F9)

    def read_joints(self):
        import numpy as np
        jp = (self.b.get_observation().joint_positions) or {}
        out = []
        for n in self.joint_names:
            if n not in jp:
                raise RuntimeError(f"joint {n!r} not in observation.joint_positions")
            v = jp[n]
            out.append(float(v[0]) if hasattr(v, "__len__") else float(v))
        return np.asarray(out, float)

    def read_pose(self):
        return self.chain.fk(self.read_joints())

    def move_to_joints(self, joints):
        q = [float(x) for x in joints]
        for name in ("move_to_joints", "move_joints", "goto_joints", "set_joint_positions", "move_j"):
            fn = getattr(self.b, name, None)
            if callable(fn):
                fn(q)
                return
        raise RuntimeError("cell Bridge has no joint-move method (expected one of "
                           "move_to_joints/move_joints/goto_joints/set_joint_positions/move_j)")

    def estop_ok(self):
        return True

    def _detect(self, img):
        import numpy as np
        from calib_engine.detect import detect_charuco
        ids, uv = detect_charuco(np.asarray(img), dict_name=self.dict_name, **self.board_params)
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
        obs = self.b.get_observation()
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
        """Optional cuRobo collision/in-envelope filter (7.7): if the cell Bridge exposes
        one, suggested poses are filtered BEFORE the operator accepts; else accept all."""
        for name in ("is_joint_pose_feasible", "feasible", "plan_feasible"):
            fn = getattr(self.b, name, None)
            if callable(fn):
                try:
                    return bool(fn([float(x) for x in joints]))
                except Exception:  # noqa: BLE001
                    return True
        return True

    def capture_image(self):
        """The raw RGB frame from the cell camera (for the live-cam inset + overlay)."""
        obs = self.b.get_observation()
        img = next((getattr(obs, a, None) for a in ("rgb", "color", "image", "rgb_image", "left")
                    if getattr(obs, a, None) is not None), None)
        if img is None:
            raise RuntimeError("observation has no image (expected obs.rgb/color/image)")
        return img


def _calib_config():
    cy = load_cell_yaml()
    cal = (cy.get("calibration") or {})
    bp = (cal.get("board") or {})
    board_params = {
        "squares_x": int(bp.get("squares_x", 7)), "squares_y": int(bp.get("squares_y", 5)),
        "square_len": float(bp.get("square_len", 0.03)), "marker_len": float(bp.get("marker_len", 0.022)),
    }
    return (board_params, str(bp.get("dict", "DICT_5X5_1000")), cal.get("K"),
            cal.get("wh", [1280, 720]), cal.get("dist"), cal.get("T_left_right"),
            float(cal.get("accept_heldout_px", 1.5)), float(cal.get("accept_tip_mm", 3.0)))


def _as_K(kcfg):
    import numpy as np
    if isinstance(kcfg, dict):
        return np.array([[kcfg["fx"], 0, kcfg["cx"]], [0, kcfg["fy"], kcfg["cy"]], [0, 0, 1.0]], float)
    return np.asarray(kcfg, float).reshape(3, 3)


def _intrinsics_from_bridge(b):
    """Read FACTORY intrinsics live off the cell camera's observation (the SDK already
    has them — ZED: calibration_parameters.left_cam; RealSense: the stream profile). This
    is why calibration must NOT demand a hand-typed cell.yaml K: the camera publishes a
    trustworthy K every frame. Returns (K 3x3, (w,h)) or (None, None) if absent."""
    import numpy as np
    try:
        intr = getattr(b.get_observation(), "intrinsics", None)
    except Exception:  # noqa: BLE001 — no camera up yet; caller falls back / errors clearly
        return None, None
    if isinstance(intr, dict) and all(k in intr for k in ("fx", "fy", "cx", "cy")):
        K = np.array([[intr["fx"], 0.0, intr["cx"]], [0.0, intr["fy"], intr["cy"]], [0.0, 0.0, 1.0]], float)
        wh = (int(intr.get("width", 1280)), int(intr.get("height", 720)))
        return K, wh
    return None, None


_CALIB = None


def _calib_service():
    global _CALIB
    if _CALIB is not None:
        return _CALIB
    from calib_engine import urdf_io
    from calib_engine.detect import charuco_board_points
    from calib_engine.service import CalibService
    from calib_engine.types import BoardModel

    b = get_bridge()
    if b is None:
        raise RuntimeError(_bridge_err or "no bridge connected")
    import numpy as np
    board_params, dict_name, kcfg, wh, distcfg, tlr, acc_px, acc_mm = _calib_config()
    dist = np.asarray(distcfg, float) if distcfg is not None else None
    sdk_T_lr = np.asarray(tlr, float).reshape(4, 4) if tlr is not None else None
    # Intrinsics: prefer an explicit cell.yaml override (a cam with no factory K), else read
    # the camera's factory K live off the SDK. Only error if the camera reports nothing AND
    # there is no override — the agent never has to hand-author a K for a ZED/RealSense.
    if kcfg is not None:
        K = _as_K(kcfg)
        wh = tuple(wh)
    else:
        K, wh_sdk = _intrinsics_from_bridge(b)
        if wh_sdk is not None:
            wh = wh_sdk
    if K is None:
        raise RuntimeError(
            "no camera intrinsics available — the cell camera's get_observation().intrinsics "
            "is empty and cell.yaml calibration.K is unset. Have primitives.py report factory "
            "intrinsics {fx,fy,cx,cy,width,height} from the camera SDK (ZED: "
            "get_camera_information().camera_configuration.calibration_parameters.left_cam), or "
            "set calibration.K in cell.yaml as a fallback."
        )
    pts = charuco_board_points(dict_name=dict_name, **board_params)
    board = BoardModel(points=pts, rows=board_params["squares_y"], cols=board_params["squares_x"],
                       square_m=board_params["square_len"])
    urdf_path = str(CELL_DIR / "robot_model" / "robot.urdf")

    def chain_provider(flange):
        return urdf_io.chain_from_urdf(urdf_path, flange)[0]

    def bridge_factory(item):
        # A world-fixed camera with a handheld board has no moving chain — the static path
        # only needs capture()/capture_image(), never joints/move. Build a chainless bridge.
        if item.kind == "eye_to_hand" and getattr(item, "board_source", "handheld") != "arm":
            from calib_engine.geometry import Chain
            return RealBridge(b, Chain([], []), [], board_params, dict_name, K=K, dist=dist, sdk_T_lr=sdk_T_lr)
        chain, names, _ = urdf_io.chain_from_urdf(urdf_path, item.flange_link)
        return RealBridge(b, chain, names, board_params, dict_name, K=K, dist=dist, sdk_T_lr=sdk_T_lr)

    _CALIB = CalibService(urdf_path, board, K, bridge_factory, chain_provider, wh=tuple(wh),
                          calib_dir=str(CELL_DIR / "calibration"),
                          accept_heldout_px=acc_px, accept_tip_mm=acc_mm)
    return _CALIB


def _calib_set_board(body: dict) -> dict:
    """Persist the printed board to cell.yaml calibration.board and apply it. If a service
    is already live (a camera was selected), update its board + detector IN PLACE so the
    in-progress session is preserved (nulling the cache would drop it → "no calibration
    selected"); otherwise the next select reads the fresh cell.yaml."""
    global _CALIB
    import yaml  # type: ignore
    b = body or {}
    board = {
        "dict": str(b.get("dict", "DICT_5X5_1000")),
        "squares_x": int(b["squares_x"]), "squares_y": int(b["squares_y"]),
        "square_len": float(b["square_len"]), "marker_len": float(b["marker_len"]),
    }
    cy = load_cell_yaml()
    cal = dict(cy.get("calibration") or {})
    cal["board"] = board
    cy["calibration"] = cal
    (CELL_DIR / "cell.yaml").write_text(yaml.safe_dump(cy, sort_keys=False))

    if _CALIB is not None:
        from calib_engine.detect import charuco_board_points
        from calib_engine.types import BoardModel
        dict_name = board["dict"]
        bp = {k: board[k] for k in ("squares_x", "squares_y", "square_len", "marker_len")}
        new_board = BoardModel(points=charuco_board_points(dict_name=dict_name, **bp),
                               rows=bp["squares_y"], cols=bp["squares_x"], square_m=bp["square_len"])
        _CALIB.board = new_board
        if _CALIB.session is not None:
            _CALIB.session.board = new_board
            rb = _CALIB.session.bridge
            if hasattr(rb, "board_params"):
                rb.board_params = bp
            if hasattr(rb, "dict_name"):
                rb.dict_name = dict_name
    return {"type": "set_board", "board": board}


def _calib_snapshot() -> dict:
    """Current camera frame (JPEG, base64) + factory intrinsics + the ChArUco detection
    overlay (corner pixels) for the live-cam inset. Needs cv2 + a connected camera; any
    failure returns {error} and the UI falls back to a placeholder."""
    import base64
    import numpy as np
    import cv2  # type: ignore
    from calib_engine.detect import detect_charuco

    b = get_bridge()
    if b is None:
        return {"error": _bridge_err or "no bridge connected"}
    board_params, dict_name, kcfg, wh, *_ = _calib_config()
    rb = RealBridge(b, None, [], board_params, dict_name)
    img = np.asarray(rb.capture_image())
    if img.ndim == 2:
        img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)
    bgr = cv2.cvtColor(img, cv2.COLOR_RGB2BGR) if img.shape[-1] == 3 else img
    ok, buf = cv2.imencode(".jpg", bgr, [int(cv2.IMWRITE_JPEG_QUALITY), 70])
    if not ok:
        return {"error": "jpeg encode failed"}
    h, w = img.shape[:2]
    ids, uv = detect_charuco(img, dict_name=dict_name, **board_params)
    K, _ = _intrinsics_from_bridge(b)
    return {
        "type": "snapshot", "jpeg_b64": base64.b64encode(buf.tobytes()).decode("ascii"),
        "w": int(w), "h": int(h), "seen": bool(len(ids) >= 6), "n_corners": int(len(ids)),
        "corners": np.asarray(uv, float).round(1).tolist(),
        "intrinsics": None if K is None else {"fx": K[0, 0], "fy": K[1, 1], "cx": K[0, 2], "cy": K[1, 2]},
    }


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


def _calib_handle(verb: str, body: dict) -> dict:
    try:
        # `plan` is a pure URDF read — it must NOT need the bridge / intrinsics / cv2, so
        # the operator sees the rig's cameras even before the camera is up. Heavier deps
        # (bridge, K, board, cv2) are only built on `select`.
        if verb == "plan":
            import xml.etree.ElementTree as ET
            from calib_engine.service import _planitem_json
            from calib_engine.session import build_plan
            urdf_path = str(CELL_DIR / "robot_model" / "robot.urdf")
            if not os.path.exists(urdf_path):
                return {"error": f"no URDF at {urdf_path} — build the rig first"}
            items = [_planitem_json(p) for p in build_plan(urdf_path)]
            links = [l.get("name") for l in ET.parse(urdf_path).getroot().findall("link")]
            # Return the ACTUAL configured board from cell.yaml (if any) so the form shows
            # what the operator/agent really set — NOT a fabricated default. `board` is null
            # when nothing is configured yet (then the UI offers a clearly-labelled example).
            bp = ((load_cell_yaml().get("calibration") or {}).get("board") or {})
            board = {
                "dict": bp.get("dict"), "squares_x": bp.get("squares_x"),
                "squares_y": bp.get("squares_y"), "square_len": bp.get("square_len"),
                "marker_len": bp.get("marker_len"),
            } if bp else None
            return {"type": "plan", "items": items, "links": links, "urdf": urdf_path, "board": board}
        if verb == "set_board":
            # The ONE thing not in the URDF — the printed board — set from the Studio form.
            # Persist it to cell.yaml calibration.board and drop the cached service so the
            # next `select` rebuilds with the new params (and the new detector).
            return _calib_set_board(body)
        if verb == "snapshot":
            # The live-cam inset: the current frame (JPEG) + factory intrinsics + the board
            # detection overlay. Edge-only (needs the image + cv2); degrades to {error} so the
            # UI shows a placeholder off-robot.
            return _calib_snapshot()
        if verb == "frame_image":
            return _calib_frame_image(int((body or {}).get("index", -1)))
        return _calib_service().handle(verb, body)
    except Exception as e:  # noqa: BLE001
        return {"error": f"{type(e).__name__}: {e}"}


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
