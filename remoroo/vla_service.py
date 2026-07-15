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
_READY_TIMEOUT_S = 20.0

# Runtime registry — ALL vendor knowledge lives here (and only here). Adding a VLA =
# one row; a customer's private runtime needs no code change at all: vla.yaml can
# override any field (probe_import / module / repo_marker / weights_glob).
RUNTIMES: dict = {
    "lingbot": {
        "probe_import": "lingbotvla",              # importable => this python serves it
        "module": "deploy.lingbot_vla_v2_policy",  # the server entrypoint (cwd=repo)
        "repo_marker": "deploy/lingbot_vla_v2_policy.py",
        "weights_glob": "*vla*",
        "qwen_glob": "Qwen3-VL*",                  # companion weights (QWEN3VL_PATH)
        # NO default extra args: the vendor's own defaults win (its --use_compile
        # takes a VALUE — passing it bare crashed argparse on the first live boot).
        # Rigs override per hardware, e.g. extra_args: ["--use_compile", "False"].
        "extra_args": [],
    },
    "openpi": {
        "probe_import": "openpi",
        "module": "openpi.serving.http_policy_server",
        "repo_marker": "src/openpi/__init__.py",
        "weights_glob": "*pi0*",
        "qwen_glob": "",
        "extra_args": [],
    },
}
DEFAULT_RUNTIME = "lingbot"


def runtime_descriptor(name: str = "", overrides: dict = None) -> dict:
    """The active runtime's descriptor: registry row + vla.yaml overrides on top."""
    d = dict(RUNTIMES.get(name or DEFAULT_RUNTIME, RUNTIMES[DEFAULT_RUNTIME]))
    for k, v in (overrides or {}).items():
        if k in d and v:
            d[k] = v
    return d


VLA_MODULE_DEFAULT = RUNTIMES[DEFAULT_RUNTIME]["module"]   # back-compat name


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
        "module": pick("REMOROO_VLA_MODULE", "module",
                       runtime_descriptor(str(raw.get("runtime", "")),
                                          raw)["module"]),
        "qwen_path": pick("REMOROO_VLA_QWEN", "qwen_path"),
        # a shell file SOURCED before spawn (systemd EnvironmentFile= equivalent) —
        # for env files that COMPUTE their exports (the copy-into-env: trick can't)
        "env_file": pick("REMOROO_VLA_ENV_FILE", "env_file"),
        "extra_args": [str(a) for a in extra],
        # env vars the SERVER process needs (QWEN3VL_PATH for the base model, CUDA/cuDNN
        # workarounds, …) — merged over the inherited environment at spawn.
        "env": {str(k): str(v) for k, v in (raw.get("env") or {}).items()},
    }


def _sourced_env(env_file: str) -> dict:
    """Source the file in bash and return ONLY what it changed or added (two-run
    diff), so we apply exactly the file's effect — computed exports included."""
    import shlex

    def env_of(cmd: str) -> dict:
        r = subprocess.run(["bash", "-c", cmd], capture_output=True, timeout=30)
        out = {}
        for chunk in r.stdout.split(b"\0"):
            if b"=" in chunk:
                k, v = chunk.decode(errors="replace").split("=", 1)
                out[k] = v
        return out

    base = env_of("env -0")
    sourced = env_of(f"set -a; source {shlex.quote(env_file)} >/dev/null 2>&1; env -0")
    return {k: v for k, v in sourced.items() if base.get(k) != v}


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
    ef = cfg.get("env_file")
    if ef and not Path(ef).exists():
        return f"env_file {ef!r} does not exist — fix the path in .remoroo/vla.yaml"
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


def _listening(port: str, timeout: float = 2.0) -> bool:
    """A REAL WebSocket visit: proper upgrade handshake, then a clean close frame
    (masked, code 1000) — the server sees a normal connect/disconnect and logs
    NOTHING. (v1 bare-connect made it log EOF handshake errors; v2 plain HTTP made
    it log InvalidUpgrade. A probe must be indistinguishable from a citizen.)
    A non-websocket server answering the upgrade with any HTTP response also counts
    as alive."""
    import base64

    try:
        with socket.create_connection(("127.0.0.1", int(port)),
                                      timeout=timeout) as s:
            s.settimeout(timeout)
            key = base64.b64encode(os.urandom(16)).decode()
            s.sendall((f"GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
                       "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                       f"Sec-WebSocket-Key: {key}\r\n"
                       "Sec-WebSocket-Version: 13\r\n\r\n").encode())
            try:
                resp = s.recv(1024)
            except OSError:
                return True                      # connected but slow: alive
            if not resp:
                return False
            if resp.startswith(b"HTTP/1.1 101"):
                # handshake accepted: leave like a citizen (masked close, code 1000)
                mask = os.urandom(4)
                payload = bytes(b ^ mask[i % 4]
                                for i, b in enumerate(b"\x03\xe8"))
                s.sendall(b"\x88\x82" + mask + payload)
                try:
                    s.recv(64)                   # the server's close reply
                except OSError:
                    pass
            return True                          # any HTTP answer = something serves
    except OSError:
        return False


# Known-noise a websockets server logs when probed by OLDER remoroo versions or
# portscanners — filtered from the DISPLAYED tail only (the log keeps everything).
_NOISE_MARKERS = ("connection closed while reading HTTP request line",
                  "did not receive a valid HTTP request",
                  "opening handshake failed",
                  "websockets.exceptions.InvalidMessage",
                  "websockets.exceptions.InvalidUpgrade",
                  "invalid Connection header",
                  ") = self.process_request(request)",
                  "raise InvalidUpgrade(",
                  "site-packages/websockets/",
                  "request = yield from Request.parse",
                  "await connection.handshake(",
                  "raise self.protocol.handshake_exc",
                  'raise EOFError("connection closed')


def _display_tail(lines: list) -> list:
    """Drop probe-noise blocks from what status SHOWS; keep real content."""
    return [ln for ln in lines if not any(m in ln for m in _NOISE_MARKERS)]


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
        "last_log": _display_tail(tail(project_dir, 40))[-12:],
    }


# --------------------------------------------------------------------------- #
# Start / stop / restart                                                       #
# --------------------------------------------------------------------------- #
def _mem_available_bytes() -> int:
    """Linux MemAvailable in bytes; 0 when unknowable (never blocks blind)."""
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except Exception:                              # noqa: BLE001
        pass
    return 0


def _preflight_memory(cfg: dict, headroom: float = 1.3) -> str:
    """LOADING a checkpoint needs its size on disk times headroom of AVAILABLE RAM
    (torch stages tensors while placing them; unified-memory boards share this with
    the GPU). Twice on 2026-07-14 a load beside a warm edge died at Action Expert
    init — once as a logged OOM kill, once as a silent native abort. Empty string =
    enough room; otherwise the stated shortfall. Never blocks non-Linux or unsized
    checkpoints."""
    ckpt = cfg.get("checkpoint") or ""
    try:
        total = sum(f.stat().st_size for f in Path(ckpt).rglob("*") if f.is_file())
        have = _mem_available_bytes()
        if not total or not have:
            return ""
    except Exception:                              # noqa: BLE001 - never block blind
        return ""
    need = int(total * headroom)
    if have < need:
        return (f"checkpoint is {total / 1e9:.1f} GB; loading needs about "
                f"{need / 1e9:.1f} GB available, this machine has {have / 1e9:.1f} GB")
    return ""


def _preflight_import(cfg: dict, env: dict, timeout_s: float = 60.0) -> str:
    """Import the runtime package in the DECLARED python with the FULLY RESOLVED env
    (env_file sourced + explicit env). Empty string = healthy; otherwise the import
    error's last line — which is exactly what the server's crash log would have said."""
    desc = RUNTIMES.get(cfg.get("runtime", ""), {})
    pkg = desc.get("probe_import")
    py = cfg.get("python")
    if not pkg or not py:
        return ""                                   # nothing declared to verify
    try:
        r = subprocess.run([py, "-c", f"import {pkg}"], env=env,
                           capture_output=True, text=True, timeout=timeout_s)
    except subprocess.TimeoutExpired:
        return f"import {pkg} did not finish in {timeout_s:.0f}s"
    except Exception as e:  # noqa: BLE001
        return f"{type(e).__name__}: {e}"
    if r.returncode == 0:
        return ""
    tail = [ln for ln in (r.stderr or "").strip().splitlines() if ln.strip()]
    return tail[-1] if tail else f"import {pkg} failed (exit {r.returncode})"


def start(project_dir: Path, echo: Echo = print, *, wait: bool = True,
          preflight: bool = True) -> dict:
    project_dir = Path(project_dir).resolve()
    if not configured(project_dir):
        ensure_declared(project_dir, echo)         # zero-flag: discover + write
    cfg = load_config(project_dir)
    if cfg is None:
        echo("  no VLA on this rig and none discoverable — `remoroo vla init` "
             "auto-discovers once the LingBot repo+venv exist near the project.")
        return {"ok": False, "error": "not_configured"}

    alive, pid = is_running(project_dir)
    strays = [s for s in _server_pids(cfg) if s != pid]
    if strays:
        echo(f"  ❌ another {cfg.get('module', 'vla server')} instance is ALREADY "
             f"running (pid {strays}) outside this declaration — a lost pidfile, a "
             "second directory's declaration, or an unkillable corpse. ONE instance "
             "per board: `remoroo vla stop` sweeps it; if it will not die, reboot. "
             "Never stack a second 12GB load.")
        return {"ok": False, "error": "instance_already_running", "strays": strays}
    port_busy = False
    if not alive:
        # a just-killed server's socket can LINGER a few seconds — the instant check
        # misread it as a foreign server (rig 2026-07-15, twice; the "phantom" on
        # :8791 was always our own dying socket). Grace, then decide.
        for _ in range(6):
            port_busy = _listening(int(cfg.get("port", 8791)))
            if not port_busy:
                break
            time.sleep(1.0)
    if not alive and port_busy:
        echo(f"  ❌ something ALREADY serves on :{cfg.get('port', 8791)} but it is not "
             "THIS project's declared server. You are almost certainly in the wrong "
             "directory — the real declaration lives in the project where you ran "
             "`remoroo task`/`remoroo setup` (its .remoroo/vla.yaml). cd there and "
             "retry; never start a duplicate onto a busy port.")
        return {"ok": False, "error": "port_busy_foreign_server"}
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
    if cfg.get("env_file"):
        env.update(_sourced_env(cfg["env_file"]))   # sourced first ...
    env.update(cfg.get("env") or {})       # ... explicit env: still wins (QWEN3VL_PATH…)

    # PREFLIGHT with the EXACT env the server would get: a wrong/missing env_file
    # surfaces here as the import's own words in seconds, instead of a crash minutes
    # into weight loading (the libcudnn.so.9 class — bit the live run of 2026-07-13
    # after the server had served for days). Declaration verification, not venv repair.
    # EXPLICIT starts only: the import costs 30-90s of torch on embedded boards, which
    # stalled `remoroo task` startup (2026-07-14) — the opportunistic hook skips it.
    problem = _preflight_memory(cfg)
    if problem:
        echo(f"  ❌ not enough memory to LOAD the vla: {problem}")
        echo("     On a unified-memory board a concurrent load thrashes or aborts "
             "NATIVELY (silent death at Action Expert init — 2026-07-14 twice). "
             "Free the RAM first:  remoroo edge stop  →  remoroo vla start  →  "
             "wait for serving  →  remoroo edge start.")
        return {"ok": False, "error": f"memory: {problem}"}
    problem = _preflight_import(cfg, env) if preflight else ""
    if problem:
        echo(f"  ❌ vla preflight failed: {problem}")
        echo("     the server would crash the same way — fix `env_file`/`env` in "
             ".remoroo/vla.yaml (the CUDA env the runtime's own venv needs), then "
             "`remoroo vla start` again.")
        return {"ok": False, "error": f"preflight: {problem}"}
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
            tail_txt = " ".join(tail(project_dir, 20))
            if "cannot open shared object file" in tail_txt and not cfg.get("env_file"):
                echo("     A shared library the venv's torch needs isn't visible. If "
                     "your working invocation sources an env file (cuda_env.sh etc.), "
                     "declare it:  env_file: /path/to/that.sh  in .remoroo/vla.yaml")
            else:
                echo("     Common causes: wrong venv python, checkpoint path, or CUDA OOM.")
            return {"ok": False, "error": f"exited:{proc.poll()}", "pid": proc.pid}
        if _listening(cfg["port"]) and proc.poll() is None:
            echo(f"  ✅ vla server serving on :{cfg['port']}")
            return {"ok": True, "pid": proc.pid, "port": cfg["port"], "listening": True}
        time.sleep(0.5)
    echo(f"  ⏳ vla server is loading weights (a 6B model takes minutes on first start) — "
         f"it will come up on :{cfg['port']}; follow {lp}.")
    return {"ok": True, "pid": proc.pid, "port": cfg["port"], "listening": False}


def _server_pids(cfg: Optional[dict]) -> List[int]:
    """EVERY live process running this runtime's server module — found by cmdline,
    never by pidfile (pidfiles get lost to rm -rf, crashes, and split-brain dirs;
    2026-07-14: an orphaned loader stacked under a fresh start and OOM'd the board).
    Includes D-state corpses the kernel can't kill: those hold GPU memory and MUST
    block a new instance."""
    module = (cfg or {}).get("module") or "websocket_policy_server"
    pids: List[int] = []
    try:
        r = subprocess.run(["pgrep", "-f", module], capture_output=True, text=True,
                           timeout=5)
        pids = [int(x) for x in r.stdout.split() if x.strip().isdigit()
                and int(x) != os.getpid()]
    except Exception:                              # noqa: BLE001 - census, not a wall
        pass
    return pids


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
    survivors = _sweep_orphans(project_dir, echo)
    if survivors:
        return {"ok": False, "was_running": True, "pid": pid, "survivors": survivors,
                "error": "unkillable server process(es) remain"}
    echo("  vla server stopped.")
    return {"ok": True, "was_running": True, "pid": pid}


def _sweep_orphans(project_dir: Path, echo: Echo = print) -> List[int]:
    """Kill every OTHER process running the server module (lost pidfiles, split-brain
    declarations) and return the ones that will not die — D-state corpses wedged in
    the GPU driver. Survivors hold GPU memory: only a reboot frees them."""
    cfg = load_config(project_dir)
    strays = _server_pids(cfg)
    for spid in strays:
        for sig in (signal.SIGTERM, signal.SIGKILL):
            if not _pid_alive(spid):
                break
            try:
                os.kill(spid, sig)
            except Exception:                      # noqa: BLE001
                pass
            for _ in range(20):
                if not _pid_alive(spid):
                    break
                time.sleep(0.1)
    survivors = [spid for spid in strays if _pid_alive(spid)]
    if survivors:
        echo(f"  ❌ server process(es) {survivors} will NOT die — uninterruptible in "
             "the GPU driver, still holding GPU memory. No new instance can start "
             "over them; REBOOT the board to reclaim.")
    return survivors


def restart(project_dir: Path, echo: Echo = print) -> dict:
    echo("  restarting vla server…")
    out = stop(project_dir, echo)
    if out.get("survivors"):
        return {"ok": False, "error": "unkillable_survivors",
                "survivors": out["survivors"],
                "note": "restart ABORTED — starting over a corpse stacks GPU memory "
                        "(the 2026-07-14 OOM); reboot the board first"}
    return start(project_dir, echo)


def ensure_started(project_dir: Path, echo: Echo = print) -> dict:
    """`remoroo task`'s opportunistic hook: unconfigured rigs skip silently; configured
    ones get a non-blocking start (weights warm while the agent grounds the task).
    Failures are stated and never block the run — the geometric path doesn't need this."""
    if not configured(project_dir) and not ensure_declared(project_dir, echo):
        return {"skipped": "not_configured"}
    return start(project_dir, echo, wait=False, preflight=False)


# --------------------------------------------------------------------------- #
# Discovery — GROUND TRUTH, not path guesses. Order:                           #
#   1. interpreters already in play (the ACTIVE venv, the edge's locked        #
#      python, PATH) asked directly: can you `import lingbotvla`? The          #
#      same-venv install (VLA in the curobo/edge venv) is a first-class        #
#      answer, not an accident.                                                #
#   2. a bounded filesystem search for the INSTALLED PACKAGE                   #
#      (site-packages/lingbotvla) — wherever it is — walking up to its         #
#      interpreter; plus the repo (deploy/lingbot_vla_v2_policy.py).           #
#   3. the repo derived FROM the package (editable installs live in the        #
#      checkout), so one probe usually answers both questions.                 #
# The wizard asks a human only for what remains, and VERIFIES every answer     #
# by probing before writing anything.                                          #
# --------------------------------------------------------------------------- #
SEARCH_ROOTS = ["/home", "/opt", "/srv", "/data", "/usr/local"]


def _probe_src(desc: dict) -> str:
    pkg = desc["probe_import"]
    marker = desc["repo_marker"]
    return (f"import {pkg}, pathlib; "
            f"p = pathlib.Path({pkg}.__file__).resolve(); "
            "r = next((str(s) for s in p.parents "
            f"if (s / {marker!r}).exists()), ''); "
            "print(r)")


def _python_candidates_from_answer(ans: str) -> List[str]:
    """A human points anywhere inside a venv (the venv root, lib/python3.10/,
    site-packages, bin/, or the interpreter itself) — resolve to interpreter
    candidates instead of demanding the exact binary path."""
    s = ans.strip().rstrip("/")
    if not s:
        return []
    p = Path(s).expanduser()
    out: List[str] = []
    if p.is_file():
        out.append(str(p))
    for base in [p, *list(p.parents)[:4]]:
        for name in ("python", "python3"):
            cand = base / "bin" / name
            if cand.exists():
                out.append(str(cand))
        if base.name == "bin":
            for name in ("python", "python3"):
                if (base / name).exists():
                    out.append(str(base / name))
    seen, uniq = set(), []
    for c in out:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq


def probe_python(py: str, desc: dict, timeout_s: float = 90.0) -> Optional[str]:
    """Ask an interpreter directly: can you import the runtime's package, and where is
    the repo it came from? Returns the repo path ('' when the package isn't an
    editable checkout) or None when the import fails. Ground truth, never a guess.
    ISOLATED (-I) and run from a NEUTRAL cwd: on 2026-07-14 a probe launched from
    inside the vendor repo imported the SOURCE TREE via cwd and blessed a venv with
    none of the deps installed (einops/transformers crash loop). An install that only
    works via PYTHONPATH must be declared explicitly (python: in vla.yaml)."""
    import tempfile
    try:
        r = subprocess.run([py, "-I", "-c", _probe_src(desc)], capture_output=True,
                           text=True, timeout=timeout_s, cwd=tempfile.gettempdir())
    except Exception:                              # noqa: BLE001
        return None
    return r.stdout.strip() if r.returncode == 0 else None


def _candidate_pythons(project_dir: Path) -> List[str]:
    """Interpreters already in play, most-likely first. Includes the ACTIVE venv the
    operator is standing in and the edge's locked interpreter — the same-venv install
    is fully supported."""
    cands: List[str] = []
    for env_var in ("REMOROO_VLA_PYTHON", "VIRTUAL_ENV", "CONDA_PREFIX"):
        v = os.environ.get(env_var, "")
        if v:
            cands.append(v if v.endswith("python") else str(Path(v) / "bin" / "python"))
    try:
        edge_cfg = json.loads((Path(project_dir) / ".remoroo" / "edge.json"
                               ).read_text(encoding="utf-8"))
        if edge_cfg.get("interpreter"):
            cands.append(edge_cfg["interpreter"])
    except Exception:                              # noqa: BLE001
        pass
    for name in ("python3", "python"):
        w = shutil.which(name)
        if w:
            cands.append(w)
    seen, out = set(), []
    for c in cands:
        rc = str(Path(c))
        if rc not in seen and Path(rc).exists():
            seen.add(rc)
            out.append(rc)
    return out


def _system_search(desc: dict, roots: Optional[List[str]] = None,
                   timeout_s: float = 60.0) -> Dict[str, List[str]]:
    """Bounded find for the installed package and the repo, wherever they are.
    Package hits become interpreters (site-packages -> the venv's bin/python)."""
    roots = [r for r in (roots or SEARCH_ROOTS) if Path(r).is_dir()]
    if not roots:
        return {"pythons": [], "repos": []}
    pkg = desc["probe_import"]
    marker = desc["repo_marker"]
    expr = ("find " + " ".join(roots) + " -maxdepth 9 "
            f"\\( -path '*/site-packages/{pkg}/__init__.py' "
            f"-o -path '*/{marker}' \\) "
            "-not -path '*/.git/*' 2>/dev/null | head -40")
    try:
        # no `timeout` binary dependency (absent on stock macOS): subprocess enforces it
        r = subprocess.run(["bash", "-c", expr], capture_output=True, text=True,
                           timeout=timeout_s)
        hits = [ln for ln in r.stdout.splitlines() if ln.strip()]
    except subprocess.TimeoutExpired as e:         # keep what the scan DID find
        raw = e.stdout or b""
        raw = raw.decode(errors="replace") if isinstance(raw, bytes) else raw
        hits = [ln for ln in raw.splitlines() if ln.strip()]
    except Exception:                              # noqa: BLE001
        hits = []
    pythons, repos = [], []
    for h in hits:
        hp = Path(h)
        if hp.name == "__init__.py":               # …/site-packages/<pkg>/__init__.py
            for up in hp.parents:
                py = up / "bin" / "python"
                if py.exists():
                    pythons.append(str(py))
                    break
        else:                                       # …/repo/deploy/lingbot_vla_v2_policy.py
            repos.append(str(hp.parent.parent))
    return {"pythons": pythons, "repos": repos}


def discover(project_dir: Path, *, runtime: str = "",
             roots: Optional[List[str]] = None,
             echo: Echo = lambda m: None) -> Optional[dict]:
    """Ground-truth discovery. Returns a vla.yaml-shaped dict, a PARTIAL dict with
    "missing" listed (the wizard fills it), or None when nothing was found at all."""
    project_dir = Path(project_dir).resolve()
    desc = runtime_descriptor(runtime)
    python = None
    repo = None
    for py in _candidate_pythons(project_dir):
        got = probe_python(py, desc)
        if got is not None:
            python, repo = py, (got or None)
            echo(f"  vla: {py} imports {desc['probe_import']}"
                 + (f" (repo {got})" if got else ""))
            break
    if python is None:
        found = _system_search(desc, roots)
        for py in found["pythons"]:
            got = probe_python(py, desc)
            if got is not None:
                python, repo = py, (got or None)
                echo(f"  vla: found installed package -> {py}")
                break
        if repo is None and found["repos"]:
            repo = found["repos"][0]
    if repo is None:                                # repo not derivable from the package
        found = _system_search(desc, roots) if python is not None else {"repos": []}
        if found.get("repos"):
            repo = found["repos"][0]
    if python is None and repo is None:
        return None
    cfg: dict = {"runtime": runtime or DEFAULT_RUNTIME,
                 "port": int(VLA_PORT_DEFAULT),
                 "extra_args": list(desc["extra_args"])}
    missing: List[str] = []
    if python:
        cfg["python"] = python
    else:
        missing.append("python")
    if repo:
        cfg["workdir"] = repo
        weights = Path(repo) / "weights"
        if weights.is_dir():
            ckpts = sorted(weights.glob(desc["weights_glob"]))
            if ckpts:
                cfg["checkpoint"] = str(ckpts[0])
            qwen = (sorted(weights.glob(desc["qwen_glob"]))
                    if desc.get("qwen_glob") else [])
            if qwen:
                cfg["qwen_path"] = str(qwen[0])
                cfg["env"] = {"QWEN3VL_PATH": str(qwen[0])}
    else:
        missing.append("workdir")
    if missing:
        cfg["missing"] = missing
    return cfg


def wizard(project_dir: Path, *, ask: Callable[[str, str], str],
           echo: Echo = print, runtime: str = "",
           roots: Optional[List[str]] = None) -> Optional[dict]:
    """Derive everything derivable; ask a human ONLY for what remains; VERIFY every
    answer by probing before it is accepted. Returns the final config (not written)."""
    desc = runtime_descriptor(runtime)
    cfg = discover(project_dir, runtime=runtime, roots=roots, echo=echo) or {
        "runtime": runtime or DEFAULT_RUNTIME, "port": int(VLA_PORT_DEFAULT),
        "extra_args": list(desc["extra_args"]), "missing": ["python", "workdir"]}
    missing = cfg.pop("missing", [])
    while "python" in missing:
        ans = ask(f"Where is the VLA's python? Any of these work: the venv dir, its "
                  "bin/python, or lib/pythonX.Y — I'll resolve it. (the curobo/edge "
                  "venv is fine if you installed the VLA there)", "").strip()
        if not ans:
            return None
        cands = _python_candidates_from_answer(ans)
        if not cands:
            echo(f"  ✗ no python interpreter found under {ans!r} — I looked for "
                 "bin/python and bin/python3 in it and its parents")
            continue
        accepted = None
        for cand in cands:
            got = probe_python(cand, desc)
            if got is not None:
                accepted, repo_from_probe = cand, got
                break
        if accepted is None:
            echo(f"  ✗ tried {', '.join(cands)} — none can import "
                 f"{desc['probe_import']}. Check with: "
                 f"{cands[0]} -c 'import {desc['probe_import']}'")
            continue
        echo(f"  ✓ {accepted} imports {desc['probe_import']}")
        cfg["python"] = accepted
        if repo_from_probe and "workdir" in missing:
            cfg["workdir"] = repo_from_probe
            missing.remove("workdir")
        missing.remove("python")
    ef = ask("Does the server need a shell env file sourced first (e.g. a "
             "cuda_env.sh)? Path, or Enter to skip", "").strip()
    if ef:
        if Path(ef).expanduser().exists():
            cfg["env_file"] = str(Path(ef).expanduser())
        else:
            echo(f"  ✗ {ef} does not exist — skipping (add env_file: to vla.yaml later)")
    while "workdir" in missing:
        ans = ask(f"Where is the runtime checkout (the dir containing "
                  f"{desc['repo_marker']})?", "").strip()
        if not ans:
            return None
        if (Path(ans) / desc["repo_marker"]).exists():
            cfg["workdir"] = str(Path(ans).resolve())
            missing.remove("workdir")
        else:
            echo(f"  ✗ {ans} has no {desc['repo_marker']} — not accepting it")
    return cfg


def ensure_declared(project_dir: Path, echo: Echo = print) -> bool:
    """No vla.yaml -> full ground-truth discovery; write only a COMPLETE config
    (partial ones wait for `remoroo vla init`'s questions). True when declared."""
    if configured(project_dir):
        return True
    cfg = discover(project_dir, echo=echo)
    if cfg is None or cfg.get("missing"):
        return False
    import yaml

    p = config_path(project_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    echo(f"  vla auto-discovered: python={cfg['python']} repo={cfg['workdir']} "
         f"(wrote {p})")
    return True
