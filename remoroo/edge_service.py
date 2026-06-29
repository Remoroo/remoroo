"""Edge lifecycle — the ONE managed-service home for `edge_real.py`.

The edge runs the agent's AUTHORED cell code (remoroo_cell/primitives.py, scan.py,
collect_poses.py, recorder.py) plus cuRobo, on the robot computer. It used to be a
fire-and-forget child of the Studio launcher that the agent had no handle to — so when
the agent FIXED its authored code, the running edge kept executing the STALE cached
modules (`from remoroo_cell.primitives import Bridge` is cached in sys.modules, and the
edge caches `_bridge`/`_MOTION_STACK`/cuRobo), and there was no way to restart it short
of re-running `remoroo setup`.

This module makes the edge a first-class, pidfile-managed service so the SAME lifecycle
serves everyone:

  * the agent restarts it after fixing authored code (`remoroo edge restart`, run through
    its ordinary execute_command tool) so a FRESH process loads the fix — the only reliable
    reset (no fragile importlib.reload / in-handler os.execv);
  * the operator restarts it from the Studio;
  * the `remoroo setup` launcher starts it (locking the right robotics interpreter) and
    follows its log to the shell.

State lives under `<project>/.remoroo/`:
  * edge.pid    — the detached edge process id
  * edge.json   — locked launch config (interpreter, port, cell dir, started_at); REUSED on
                  restart so the agent's restart can't pick a numpy-less python even without
                  CONDA_PREFIX/VIRTUAL_ENV in its environment
  * edge.log    — the edge's full stdout+stderr (the agent reads this to debug its own code)
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Callable, Optional

Echo = Callable[[str], None]

EDGE_PORT_DEFAULT = "7779"
_READY_TIMEOUT_S = 12.0


# --------------------------------------------------------------------------- #
# Paths + small IO helpers                                                     #
# --------------------------------------------------------------------------- #
def _state_dir(project_dir: Path) -> Path:
    d = Path(project_dir) / ".remoroo"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _pidfile(project_dir: Path) -> Path:
    return _state_dir(project_dir) / "edge.pid"


def _configfile(project_dir: Path) -> Path:
    return _state_dir(project_dir) / "edge.json"


def log_path(project_dir: Path) -> Path:
    return _state_dir(project_dir) / "edge.log"


def _read_json(p: Path) -> dict:
    try:
        return json.loads(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _write_json(p: Path, data: dict) -> None:
    try:
        p.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        pass


def tail(project_dir: Path, n: int = 40) -> list[str]:
    try:
        lines = log_path(project_dir).read_text(encoding="utf-8", errors="replace").splitlines()
        return [ln for ln in lines if ln.strip()][-n:]
    except Exception:
        return []


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
    # os.kill(pid, 0) succeeds for a ZOMBIE (defunct, not yet reaped by its parent) too. A killed
    # edge whose parent process is still alive lingers as a zombie until reaped — treat that as dead
    # so a stopped/restarted edge is never reported "running". (POSIX `ps` state; 'Z' = zombie.)
    try:
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


def _health_ok(port: str, timeout: float = 1.0) -> bool:
    from urllib.request import urlopen

    try:
        with urlopen(f"http://127.0.0.1:{port}/health", timeout=timeout) as r:
            return getattr(r, "status", 200) == 200
    except Exception:
        return False


def is_running(project_dir: Path) -> tuple[bool, int]:
    """(alive, pid). Alive = the pidfile pid is a live process (NOT pid-reuse-proof, but the
    pidfile is ours; /health in `status` confirms it's really the edge)."""
    pid = _read_pid(Path(project_dir))
    return (_pid_alive(pid), pid)


def status(project_dir: Path) -> dict:
    project_dir = Path(project_dir)
    cfg = _read_json(_configfile(project_dir))
    port = str(cfg.get("port") or os.environ.get("EDGE_PORT") or EDGE_PORT_DEFAULT)
    alive, pid = is_running(project_dir)
    healthy = _health_ok(port) if alive else False
    return {
        "running": alive,
        "healthy": healthy,
        "pid": pid if alive else None,
        "port": port,
        "url": f"http://127.0.0.1:{port}",
        "interpreter": cfg.get("interpreter"),
        "cell": cfg.get("cell"),
        "started_at": cfg.get("started_at"),
        "log": str(log_path(project_dir)),
        "last_log": tail(project_dir, 12),
    }


# --------------------------------------------------------------------------- #
# Interpreter + edge-script resolution (lazy import to avoid an import cycle)  #
# --------------------------------------------------------------------------- #
def _edge_script() -> Optional[Path]:
    from .studio_launch import find_studio

    s = find_studio()
    return s.edge_py if s else None


def _resolve_interpreter(project_dir: Path, echo: Echo, relock: bool = False) -> tuple[str, bool]:
    """Pick the interpreter to run the edge and LOCK it in edge.json. Priority:
    REMOROO_EDGE_PYTHON → the locked interpreter from a prior start (so the agent's restart
    reuses the robotics python even with no CONDA_PREFIX in its env) → fresh detection.
    Returns (python, numpy_ok)."""
    from .studio_launch import _edge_python  # detection logic lives with the launcher

    explicit = os.environ.get("REMOROO_EDGE_PYTHON")
    if explicit:
        return explicit, _numpy_ok(explicit)

    cfg = _read_json(_configfile(project_dir))
    locked = cfg.get("interpreter")
    if locked and not relock and Path(locked).exists():
        return locked, _numpy_ok(locked)

    return _edge_python(echo)


def _numpy_ok(py: str) -> bool:
    try:
        return subprocess.run([py, "-c", "import numpy"], capture_output=True, timeout=20).returncode == 0
    except Exception:
        return False


# --------------------------------------------------------------------------- #
# Start / stop / restart                                                       #
# --------------------------------------------------------------------------- #
def start(project_dir: Path, echo: Echo = print, *, port: Optional[str] = None,
          cell_dir: Optional[Path] = None, wait: bool = True, relock: bool = False) -> dict:
    """Start the edge as a DETACHED daemon (survives the launching process — the agent's
    `remoroo edge start` returns but the edge keeps serving), logging to .remoroo/edge.log,
    and write the pidfile + locked config. Idempotent: a healthy edge is left running."""
    project_dir = Path(project_dir).resolve()
    cfg = _read_json(_configfile(project_dir))
    port = str(port or cfg.get("port") or os.environ.get("EDGE_PORT") or EDGE_PORT_DEFAULT)
    cell = str(cell_dir or cfg.get("cell") or (project_dir / "remoroo_cell"))

    alive, pid = is_running(project_dir)
    if alive and _health_ok(port):
        echo(f"  edge already running (pid {pid}) on :{port} — leaving it up. (use `remoroo edge restart` to cycle it)")
        return {"ok": True, "already_running": True, "pid": pid, "port": port,
                "url": f"http://127.0.0.1:{port}"}
    if alive:  # process exists but not answering — replace it cleanly
        _terminate(pid, echo, port, estop_first=False)

    script = _edge_script()
    if script is None:
        echo("  ❌ couldn't locate edge_real.py (studio not found). Reinstall remoroo or set REMOROO_STUDIO_DIR.")
        return {"ok": False, "error": "edge_script_not_found"}

    py, numpy_ok = _resolve_interpreter(project_dir, echo, relock=relock)
    if not py:
        echo("  ❌ no python interpreter found to run the edge.")
        return {"ok": False, "error": "no_interpreter"}

    env = dict(os.environ)
    env.update({"EDGE_PORT": port, "REMOROO_CELL": cell,
                "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"})

    lp = log_path(project_dir)
    try:
        log_fh = open(lp, "w", encoding="utf-8")  # fresh log per process: the agent reasons about THIS run
    except Exception:
        log_fh = None

    # Detached: own session/pgid so it outlives the launching `remoroo edge`/`remoroo setup`
    # process and is shut down only via stop() (controlled), not the parent's Ctrl-C.
    try:
        proc = subprocess.Popen(
            [py, str(script)], env=env,
            stdout=log_fh or subprocess.DEVNULL, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL, start_new_session=True,
        )
    except Exception as e:  # noqa: BLE001
        echo(f"  ❌ failed to start the edge: {type(e).__name__}: {e}")
        return {"ok": False, "error": str(e)}
    finally:
        if log_fh is not None:
            try:
                log_fh.close()  # the child holds its own dup'd fd; parent's copy not needed
            except Exception:
                pass

    _pidfile(project_dir).write_text(str(proc.pid), encoding="utf-8")
    _write_json(_configfile(project_dir), {
        "interpreter": py, "port": port, "cell": cell,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "pid": proc.pid,
    })
    echo(f"  edge starting (pid {proc.pid}) on :{port} using {py}")
    echo(f"  edge log → {lp}")
    if not numpy_ok:
        echo("  ⚠ that python has NO numpy — calibration/world/cuRobo will fail with import errors.")
        echo("    Set REMOROO_EDGE_PYTHON=/path/to/your/robotics/python and `remoroo edge restart`.")

    if not wait:
        return {"ok": True, "pid": proc.pid, "port": port, "url": f"http://127.0.0.1:{port}", "healthy": None}

    healthy = _await_ready(proc, project_dir, port, echo)
    return {"ok": healthy is not False, "pid": proc.pid, "port": port,
            "url": f"http://127.0.0.1:{port}", "healthy": healthy}


def _await_ready(proc: subprocess.Popen, project_dir: Path, port: str, echo: Echo) -> Optional[bool]:
    """Poll /health; announce success, or LOUDLY report a dead edge with its log tail + the
    most common fix. Returns True/False/None (unknown — still booting)."""
    deadline = time.time() + _READY_TIMEOUT_S
    while time.time() < deadline:
        if proc.poll() is not None:
            time.sleep(0.3)  # let the final lines flush to the log
            _report_death(proc.poll(), project_dir, echo)
            return False
        if _health_ok(port):
            echo(f"  ✅ edge is up on :{port} (live robot mirror + cuRobo gates enabled)")
            return True
        time.sleep(0.5)
    if proc.poll() is None:
        echo(f"  ⚠ edge on :{port} hasn't answered /health yet — if the live dot stays orange, "
             f"check {log_path(project_dir)}.")
        return None
    _report_death(proc.poll(), project_dir, echo)
    return False


def _report_death(code: Optional[int], project_dir: Path, echo: Echo) -> None:
    echo("")
    echo(f"  ❌ The edge (edge_real.py) EXITED immediately (code {code}). The Studio's live robot")
    echo("     mirror + the calibration/world/cuRobo gates will show 'disconnected' (orange).")
    t = tail(project_dir, 25)
    if t:
        echo(f"     ── last lines of {log_path(project_dir)} ──")
        for ln in t:
            echo(f"     │ {ln}")
        echo("     ──────────────────────────────────────────────")
    echo("     Most common cause: the edge python can't `import numpy`/`curobo` or a robot SDK.")
    echo("     Fix: REMOROO_EDGE_PYTHON=/path/to/your/robotics/python, then `remoroo edge restart`.")


def _estop(port: str) -> None:
    """Best-effort halt motion BEFORE we kill the edge, so a restart never yanks the process
    out from under an in-flight supervised trajectory."""
    from urllib.request import urlopen

    try:
        urlopen(f"http://127.0.0.1:{port}/edge/estop", timeout=2.0).read()
    except Exception:
        pass


def _terminate(pid: int, echo: Echo, port: str, estop_first: bool = True) -> None:
    if pid <= 0 or not _pid_alive(pid):
        return
    if estop_first:
        _estop(port)
        time.sleep(0.4)  # give the bridge a beat to halt motion before the process dies
    # Kill the whole session/group (the edge + any children it spawned).
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
        for _ in range(20):  # up to ~2s for SIGTERM to take
            if not _pid_alive(pid):
                break
            time.sleep(0.1)


def stop(project_dir: Path, echo: Echo = print, *, estop_first: bool = True) -> dict:
    """Stop the edge (e-stop first, then SIGTERM→SIGKILL its process group) and clear the
    pidfile. A deliberate stop STAYS stopped — nothing auto-respawns it."""
    project_dir = Path(project_dir).resolve()
    cfg = _read_json(_configfile(project_dir))
    port = str(cfg.get("port") or os.environ.get("EDGE_PORT") or EDGE_PORT_DEFAULT)
    alive, pid = is_running(project_dir)
    if not alive:
        echo("  edge is not running.")
        _pidfile(project_dir).unlink(missing_ok=True)
        return {"ok": True, "was_running": False}
    echo(f"  stopping edge (pid {pid})…")
    _terminate(pid, echo, port, estop_first=estop_first)
    _pidfile(project_dir).unlink(missing_ok=True)
    echo("  edge stopped.")
    return {"ok": True, "was_running": True, "pid": pid}


def restart(project_dir: Path, echo: Echo = print) -> dict:
    """Stop + start, REUSING the locked config (interpreter/port/cell). This is what the agent
    runs after fixing its authored cell code so a FRESH process loads the fix."""
    echo("  restarting edge…")
    stop(project_dir, echo)
    return start(project_dir, echo)


# --------------------------------------------------------------------------- #
# Shell follower (the launcher's live [edge] view, decoupled from the process) #
# --------------------------------------------------------------------------- #
def follow_log_to_shell(project_dir: Path, echo: Echo, stop_event: Optional[threading.Event] = None) -> threading.Thread:
    """Tail .remoroo/edge.log and echo each new line as `[edge] …` to the shell. Decoupled from
    the edge PROCESS (which is detached), so it survives restarts and re-reads from the top when
    the log is truncated by a fresh start."""
    lp = log_path(project_dir)

    def _follow() -> None:
        pos = 0
        while stop_event is None or not stop_event.is_set():
            try:
                if not lp.exists():
                    time.sleep(0.3)
                    continue
                size = lp.stat().st_size
                if size < pos:           # truncated by a fresh `start` → re-read from the top
                    pos = 0
                with open(lp, "r", encoding="utf-8", errors="replace") as f:
                    f.seek(pos)
                    chunk = f.read()
                    pos = f.tell()
                if chunk:
                    for line in chunk.splitlines():
                        echo(f"[edge] {line}")
                else:
                    time.sleep(0.3)
            except Exception:
                time.sleep(0.5)

    t = threading.Thread(target=_follow, daemon=True)
    t.start()
    return t
