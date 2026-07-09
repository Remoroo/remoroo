"""VLA policy-server lifecycle — LingBot-VLA runs in its OWN venv, never ours.

The task engine talks to the policy server over HTTP (:8791 by default); nothing in the
cuRobo/edge interpreter imports the model. What was missing is the LIFECYCLE: who starts
the server, in which venv, with which checkpoint. This module answers that with the same
pidfile-managed pattern as `edge_service.py` (the proven `remoroo edge` discipline), driven
by ONE declaration the rig owner writes once:

  <project>/.remoroo/vla.yaml
      runtime: lingbot
      python: /opt/lingbot/venv/bin/python      # the OTHER venv's interpreter
      workdir: /opt/lingbot/lingbot-vla-v2      # repo checkout (deploy/ module lives here)
      # checkpoint: <path>                      # optional — defaults to what
      #                                         # `remoroo models install` produced
      # port: 8791                              # optional
      # extra_args: [--use_compile]             # optional

Nothing is hardcoded: the interpreter path IS the venv selection, and REMOROO_VLA_*
environment variables override any key for scripts/CI. Runtime state lives beside the
edge's under `.remoroo/`: vla.pid, vla.log, vla.state.json.

Unlike the edge, a live-but-silent process is LEFT ALONE by `start`: a 6B model takes
minutes to load, and "not answering yet" usually means "still loading", not "broken".
Only `restart`/`stop` kill it.
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import time
from pathlib import Path
from typing import Callable, List, Optional

Echo = Callable[[str], None]

VLA_PORT_DEFAULT = "8791"
VLA_MODULE_DEFAULT = "deploy.lingbot_vla_v2_policy"   # LingBot's own server entrypoint
_READY_TIMEOUT_S = 20.0


# --------------------------------------------------------------------------- #
# Paths                                                                        #
# --------------------------------------------------------------------------- #
def _state_dir(project_dir: Path) -> Path:
    d = Path(project_dir) / ".remoroo"
    d.mkdir(parents=True, exist_ok=True)
    return d


def config_path(project_dir: Path) -> Path:
    return Path(project_dir) / ".remoroo" / "vla.yaml"


def _pidfile(project_dir: Path) -> Path:
    return _state_dir(project_dir) / "vla.pid"


def _statefile(project_dir: Path) -> Path:
    return _state_dir(project_dir) / "vla.state.json"


def log_path(project_dir: Path) -> Path:
    return _state_dir(project_dir) / "vla.log"


def weights_root(project_dir: Path) -> Path:
    """Where `remoroo models install` lands snapshots (loaders.py owns the layout)."""
    return Path(project_dir) / ".remoroo" / "task" / "weights"


# --------------------------------------------------------------------------- #
# Config: .remoroo/vla.yaml + REMOROO_VLA_* overrides                          #
# --------------------------------------------------------------------------- #
def configured(project_dir: Path) -> bool:
    return config_path(project_dir).exists() or bool(os.environ.get("REMOROO_VLA_PYTHON"))


def _installed_snapshots(project_dir: Path) -> List[Path]:
    """Snapshot dirs `remoroo models install` verified (it stamps REVISION on success).
    Discovery instead of a hardcoded name: the pin lives in ONE place (the runtime),
    and whatever install produced is what we serve."""
    root = weights_root(project_dir)
    if not root.is_dir():
        return []
    return sorted(p for p in root.iterdir() if p.is_dir() and (p / "REVISION").exists())


def load_config(project_dir: Path) -> Optional[dict]:
    """The resolved server config, or None when this rig declares no VLA.
    Precedence per key: REMOROO_VLA_* env > vla.yaml > default."""
    project_dir = Path(project_dir)
    p = config_path(project_dir)
    raw: dict = {}
    if p.exists():
        import yaml

        raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    if not p.exists() and not os.environ.get("REMOROO_VLA_PYTHON"):
        return None

    def pick(env_key: str, yaml_key: str, default=None):
        return os.environ.get(env_key) or raw.get(yaml_key) or default

    checkpoint = pick("REMOROO_VLA_CHECKPOINT", "checkpoint")
    if not checkpoint:
        snaps = _installed_snapshots(project_dir)
        checkpoint = str(snaps[-1]) if snaps else None
    extra = raw.get("extra_args") or []
    if isinstance(extra, str):
        extra = extra.split()
    return {
        "runtime": pick("REMOROO_VLA_RUNTIME", "runtime", "lingbot"),
        "python": pick("REMOROO_VLA_PYTHON", "python"),
        "workdir": pick("REMOROO_VLA_WORKDIR", "workdir"),
        "checkpoint": checkpoint,
        "port": str(pick("REMOROO_VLA_PORT", "port", VLA_PORT_DEFAULT)),
        "module": pick("REMOROO_VLA_MODULE", "module", VLA_MODULE_DEFAULT),
        "extra_args": [str(a) for a in extra],
        # env vars the SERVER process needs (QWEN3VL_PATH for the base model, CUDA/cuDNN
        # workarounds, …) — merged over the inherited environment at spawn.
        "env": {str(k): str(v) for k, v in (raw.get("env") or {}).items()},
    }


def server_cmd(cfg: dict) -> List[str]:
    return [cfg["python"], "-m", cfg["module"],
            "--model_path", str(cfg["checkpoint"]),
            "--port", str(cfg["port"]), *cfg["extra_args"]]


def _validate(cfg: dict) -> Optional[str]:
    """One human sentence per missing prerequisite, or None when startable."""
    py = cfg.get("python")
    if not py:
        return ("no `python:` in .remoroo/vla.yaml (point it at the LingBot venv's "
                "interpreter, e.g. /opt/lingbot/venv/bin/python)")
    if not (Path(py).exists() or shutil.which(py)):
        return f"python {py!r} not found — is the LingBot venv where vla.yaml says?"
    wd = cfg.get("workdir")
    if not wd or not Path(wd).is_dir():
        return ("`workdir:` must be the LingBot repo checkout (its deploy/ module runs "
                f"the server); got {wd!r}")
    ck = cfg.get("checkpoint")
    if not ck or not Path(ck).exists():
        return ("no checkpoint found — run `remoroo models install` (pulls the pinned "
                "snapshot) or set `checkpoint:` in .remoroo/vla.yaml")
    return None


# --------------------------------------------------------------------------- #
# Liveness                                                                     #
# --------------------------------------------------------------------------- #
def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    try:  # zombies answer kill(0); treat them as dead (same reasoning as edge_service)
        out = subprocess.run(["ps", "-o", "state=", "-p", str(pid)],
                             capture_output=True, text=True, timeout=5).stdout.strip()
        if out and out[:1].upper() == "Z":
            return False
    except Exception:
        pass
    return True


def _read_pid(project_dir: Path) -> int:
    try:
        return int(_pidfile(project_dir).read_text(encoding="utf-8").strip() or "0")
    except Exception:
        return 0


def _listening(port: str, timeout: float = 1.0) -> bool:
    """TCP-level health: the policy server has no guaranteed /health route, but a bound
    port that accepts a connection means the model finished loading and serves."""
    try:
        socket.create_connection(("127.0.0.1", int(port)), timeout=timeout).close()
        return True
    except OSError:
        return False


def is_running(project_dir: Path) -> tuple[bool, int]:
    pid = _read_pid(Path(project_dir))
    return (_pid_alive(pid), pid)


def tail(project_dir: Path, n: int = 40) -> list[str]:
    try:
        lines = [ln for ln in log_path(project_dir).read_text(
            encoding="utf-8", errors="replace").splitlines() if ln.strip()]
        return lines if n <= 0 else lines[-n:]
    except Exception:
        return []


def read_log(project_dir: Path, max_bytes: int = 400_000) -> str:
    try:
        data = log_path(project_dir).read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""
    if len(data) <= max_bytes:
        return data
    return data[:max_bytes] + (
        f"\n\n[… {len(data) - max_bytes} more chars truncated — full log at .remoroo/vla.log …]")


def status(project_dir: Path) -> dict:
    project_dir = Path(project_dir)
    cfg = load_config(project_dir)
    alive, pid = is_running(project_dir)
    port = (cfg or {}).get("port") or VLA_PORT_DEFAULT
    listening = _listening(port) if alive else False
    st = {}
    try:
        st = json.loads(_statefile(project_dir).read_text(encoding="utf-8")) or {}
    except Exception:
        pass
    return {
        "configured": cfg is not None,
        "running": alive,
        "listening": listening,     # serving = process up AND port answering
        "loading": alive and not listening,
        "pid": pid if alive else None,
        "port": port,
        "url": f"http://127.0.0.1:{port}",
        "runtime": (cfg or {}).get("runtime"),
        "python": (cfg or {}).get("python"),
        "workdir": (cfg or {}).get("workdir"),
        "checkpoint": (cfg or {}).get("checkpoint"),
        "started_at": st.get("started_at"),
        "config": str(config_path(project_dir)),
        "log": str(log_path(project_dir)),
        "last_log": tail(project_dir, 12),
    }


# --------------------------------------------------------------------------- #
# Start / stop / restart                                                       #
# --------------------------------------------------------------------------- #
def start(project_dir: Path, echo: Echo = print, *, wait: bool = True) -> dict:
    project_dir = Path(project_dir).resolve()
    cfg = load_config(project_dir)
    if cfg is None:
        echo("  no VLA declared on this rig (write .remoroo/vla.yaml to enable one).")
        return {"ok": False, "error": "not_configured"}

    alive, pid = is_running(project_dir)
    if alive:
        if _listening(cfg["port"]):
            echo(f"  vla server already serving (pid {pid}) on :{cfg['port']}.")
            return {"ok": True, "already_running": True, "pid": pid, "port": cfg["port"]}
        # Alive but silent = probably still loading the 6B checkpoint. Never replace it
        # on `start` — that would loop the multi-minute load forever.
        echo(f"  vla server is up (pid {pid}) but not serving yet — likely still loading "
             f"weights. Follow {log_path(project_dir)}; `remoroo vla restart` to force.")
        return {"ok": True, "loading": True, "pid": pid, "port": cfg["port"]}

    problem = _validate(cfg)
    if problem:
        echo(f"  ❌ can't start the vla server: {problem}")
        return {"ok": False, "error": problem}

    cmd = server_cmd(cfg)
    env = dict(os.environ)
    env.update({"PYTHONUNBUFFERED": "1", "PYTHONUTF8": "1"})
    env.update(cfg.get("env") or {})       # the declaration's server env (QWEN3VL_PATH…)
    lp = log_path(project_dir)
    try:
        log_fh = open(lp, "w", encoding="utf-8")   # fresh log per process
    except Exception:
        log_fh = None
    try:
        proc = subprocess.Popen(
            cmd, cwd=cfg["workdir"], env=env,
            stdout=log_fh or subprocess.DEVNULL, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL, start_new_session=True)
    except Exception as e:  # noqa: BLE001
        echo(f"  ❌ failed to start the vla server: {type(e).__name__}: {e}")
        return {"ok": False, "error": str(e)}
    finally:
        if log_fh is not None:
            try:
                log_fh.close()
            except Exception:
                pass

    _pidfile(project_dir).write_text(str(proc.pid), encoding="utf-8")
    try:
        _statefile(project_dir).write_text(json.dumps({
            "pid": proc.pid, "cmd": cmd, "port": cfg["port"],
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }, indent=2), encoding="utf-8")
    except Exception:
        pass
    echo(f"  vla server starting (pid {proc.pid}) on :{cfg['port']} — {cfg['runtime']}")
    echo(f"  vla log → {lp}")

    if not wait:
        return {"ok": True, "pid": proc.pid, "port": cfg["port"], "listening": None}

    deadline = time.time() + _READY_TIMEOUT_S
    while time.time() < deadline:
        if proc.poll() is not None:
            time.sleep(0.3)
            echo(f"  ❌ vla server EXITED immediately (code {proc.poll()}).")
            for ln in tail(project_dir, 20):
                echo(f"     │ {ln}")
            echo("     Common causes: wrong venv python, checkpoint path, or CUDA OOM.")
            return {"ok": False, "error": f"exited:{proc.poll()}", "pid": proc.pid}
        if _listening(cfg["port"]):
            echo(f"  ✅ vla server serving on :{cfg['port']}")
            return {"ok": True, "pid": proc.pid, "port": cfg["port"], "listening": True}
        time.sleep(0.5)
    echo(f"  ⏳ vla server is loading weights (a 6B model takes minutes on first start) — "
         f"it will come up on :{cfg['port']}; follow {lp}.")
    return {"ok": True, "pid": proc.pid, "port": cfg["port"], "listening": False}


def stop(project_dir: Path, echo: Echo = print) -> dict:
    """SIGTERM→SIGKILL the server's process group. No e-stop step: the policy server
    never owns the robot — motion always goes through the edge's GuardedExecutor."""
    project_dir = Path(project_dir).resolve()
    alive, pid = is_running(project_dir)
    if not alive:
        echo("  vla server is not running.")
        _pidfile(project_dir).unlink(missing_ok=True)
        return {"ok": True, "was_running": False}
    echo(f"  stopping vla server (pid {pid})…")
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if not _pid_alive(pid):
            break
        try:
            os.killpg(os.getpgid(pid), sig)
        except Exception:
            try:
                os.kill(pid, sig)
            except Exception:
                pass
        for _ in range(20):
            if not _pid_alive(pid):
                break
            time.sleep(0.1)
    _pidfile(project_dir).unlink(missing_ok=True)
    echo("  vla server stopped.")
    return {"ok": True, "was_running": True, "pid": pid}


def restart(project_dir: Path, echo: Echo = print) -> dict:
    echo("  restarting vla server…")
    stop(project_dir, echo)
    return start(project_dir, echo)


def ensure_started(project_dir: Path, echo: Echo = print) -> dict:
    """`remoroo task`'s opportunistic hook: unconfigured rigs skip silently; configured
    ones get a non-blocking start (weights warm while the agent grounds the task).
    Failures are stated and never block the run — the geometric path doesn't need this."""
    if not configured(project_dir):
        return {"skipped": "not_configured"}
    return start(project_dir, echo, wait=False)
