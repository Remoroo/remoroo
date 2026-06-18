"""Launch Remoroo Studio (the visual setup surface) from the CLI.

`remoroo setup` (studio mode) drives the REAL `@robot_setup` agent run and shows
it entirely in the browser — no terminal, no TUI. It:

  1. creates the run on the control plane and starts the LOCAL worker (the same
     battle-tested machinery the TUI/headless paths use — `prepare_local_worker_context`
     + `_headless_step_loop`), so the agent executes the gate tools on the robot;
  2. serves the prebuilt Studio over the LAN and reverse-proxies the run API
     (`/runs/{id}/stream`, `/awaiting`, `/answer`, ...) to the control plane so the
     browser streams the agent's live events + answers ask_human;
  3. optionally spawns the real edge (`edge_real.py`) for live joints + cuRobo.

The Studio ships prebuilt (minified ``dist/`` + Python servers) as CLI package
data under ``remoroo/_studio/`` and is served by Python — the robot needs only
Python at runtime (Node is build-time only).
"""
from __future__ import annotations

import os
import secrets
import shutil
import socket
import subprocess
import sys
import threading
from pathlib import Path
from typing import Callable, NamedTuple, Optional

Echo = Callable[[str], None]


class Studio(NamedTuple):
    dist: Path
    server_py: Path
    edge_py: Path
    build_dir: Optional[Path]  # set only when running from the repo (needs `npm run build`)


def _bundled() -> Optional[Studio]:
    base = Path(__file__).resolve().parent / "_studio"
    if (base / "studio_server.py").exists():
        return Studio(base / "dist", base / "studio_server.py", base / "edge_real.py", None)
    return None


def _repo() -> Optional[Studio]:
    cands: list[Path] = []
    env = os.environ.get("REMOROO_STUDIO_DIR")
    if env:
        cands.append(Path(env).expanduser())
    for start in (Path(__file__).resolve(), Path.cwd().resolve()):
        for b in [start, *start.parents]:
            cands.append(b / "remoroo_studio")
    seen: set[Path] = set()
    for c in cands:
        if c in seen:
            continue
        seen.add(c)
        if (c / "server" / "studio_server.py").exists():
            return Studio(c / "dist", c / "server" / "studio_server.py", c / "server" / "edge_real.py", c)
    return None


def _pkg_version() -> str:
    """The installed remoroo version — surfaced in the Studio so the operator can
    confirm the robot is running the build they deployed (the deploy→robot gap)."""
    try:
        from importlib.metadata import version
        return version("remoroo")
    except Exception:
        return ""


def _stable_token(project_dir: Path) -> str:
    """A STABLE per-project Studio token, persisted outside the repo.

    The token was random per launch (`secrets.token_hex`), so every `remoroo
    setup` restart minted a new one and silently invalidated the remote operator's
    open browser tab/bookmark — the SPA still loaded (static is unauthenticated)
    but every /project write 401'd ("couldn't save the model"). A stable token
    keeps the operator's URL working across restarts.
    """
    import hashlib

    key = hashlib.sha1(str(Path(project_dir).resolve()).encode()).hexdigest()[:16]
    cache = Path.home() / ".cache" / "remoroo" / "studio_tokens"
    try:
        cache.mkdir(parents=True, exist_ok=True)
        f = cache / key
        if f.exists():
            t = f.read_text().strip()
            if t:
                return t
        t = secrets.token_hex(8)
        f.write_text(t)
        return t
    except Exception:
        return secrets.token_hex(8)  # never block launch on token persistence


def find_studio() -> Optional[Studio]:
    return _bundled() or _repo()


def lan_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "localhost"


def ensure_built(studio: Studio, echo: Echo) -> bool:
    if (studio.dist / "index.html").exists():
        return True
    if studio.build_dir is None:
        echo("The bundled studio is missing dist/ — reinstall remoroo.")
        return False
    npm = shutil.which("npm")
    if not npm:
        echo("Studio isn't prebuilt and npm/Node isn't installed (dev). Install Node 22+ or reinstall a prebuilt remoroo.")
        return False
    if not (studio.build_dir / "node_modules").exists():
        echo("Installing studio dependencies (one-time)…")
        if subprocess.run([npm, "install"], cwd=str(studio.build_dir)).returncode != 0:
            return False
    echo("Building the studio (one-time)…")
    return subprocess.run([npm, "run", "build"], cwd=str(studio.build_dir)).returncode == 0


def _print_qr(url: str) -> None:
    try:
        import qrcode  # optional

        qr = qrcode.QRCode(border=1)
        qr.add_data(url)
        qr.make(fit=True)
        qr.print_ascii(invert=True)
    except Exception:
        pass


def _python() -> Optional[str]:
    return sys.executable or shutil.which("python3") or shutil.which("python")


def _edge_python(echo: Echo) -> tuple[str, bool]:
    """Pick the interpreter to run the REAL edge (edge_real.py + the agent's
    primitives.py / collect_poses.py / cuRobo). This is NOT the CLI's own
    interpreter: `remoroo` is usually a uv-tool isolated venv WITHOUT numpy/cuRobo/
    the robot SDKs, so the edge fails there ('No module named numpy').

    We use the env `remoroo setup` was LAUNCHED FROM, automatically — captured by
    CONDA_PREFIX / VIRTUAL_ENV (these survive even though `remoroo` is a uv-tool).
    Priority: REMOROO_EDGE_PYTHON (explicit) → the first candidate that can
    `import numpy` (the activated robotics env, normally) → the launched-from env
    even if numpy isn't found (with a warning). Returns (python, numpy_ok)."""
    import subprocess as sp

    def has_numpy(p: str) -> bool:
        try:
            return sp.run([p, "-c", "import numpy"], capture_output=True, timeout=20).returncode == 0
        except Exception:
            return False

    explicit = os.environ.get("REMOROO_EDGE_PYTHON")
    if explicit:
        return explicit, has_numpy(explicit)

    conda = os.environ.get("CONDA_PREFIX")
    venv = os.environ.get("VIRTUAL_ENV")
    # Ordered: the activated env (what you launched from) first, then PATH, then the
    # CLI's own interpreter as a last resort.
    candidates = [
        (str(Path(conda) / "bin" / "python") if conda else None),
        (str(Path(venv) / "bin" / "python") if venv else None),
        shutil.which("python3"),
        shutil.which("python"),
        sys.executable,
    ]
    ordered = [p for p in dict.fromkeys(candidates) if p]
    for p in ordered:
        if has_numpy(p):
            return p, True
    # None have numpy → prefer the launched-from env (not the uv-tool python) + warn.
    for p in ordered:
        if p != sys.executable:
            return p, False
    return (sys.executable or "python3"), False


def _start_edge(studio: Studio, project_dir: Path, echo: Echo) -> tuple[Optional[subprocess.Popen], str]:
    py, numpy_ok = _edge_python(echo)
    if not py:
        echo("python not found — can't launch the real edge; the gates needing it will show 'edge not connected'.")
        return None, ""
    eport = os.environ.get("EDGE_PORT", "7779")
    env = dict(os.environ)
    # Force UTF-8 for the edge subprocess: a robot with an ascii/C locale otherwise makes
    # Path.read_text()/open() default to ascii, which crashes on any non-ascii byte in
    # cell.yaml / pipeline.yaml / the agent's code (e.g. a "—" in a comment → 0xe2).
    env.update({"EDGE_PORT": eport, "REMOROO_CELL": str(project_dir / "remoroo_cell"),
                "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"})
    proc = subprocess.Popen([py, str(studio.edge_py)], env=env)
    echo(f"Started the real edge (edge_real.py) on :{eport} using {py}")
    if not numpy_ok:
        echo("  ⚠ that python has NO numpy — calibration/world/cuRobo will fail with import errors.")
        echo("    Set REMOROO_EDGE_PYTHON=/path/to/your/robotics/python (the env with numpy + cuRobo + the robot SDKs) and re-run.")
    return proc, f"http://127.0.0.1:{eport}"


def _spawn_studio(studio: Studio, env: dict, echo: Echo) -> Optional[subprocess.Popen]:
    py = _python()
    if not py:
        echo("No Python interpreter found to run the studio server.")
        return None
    return subprocess.Popen([py, str(studio.server_py)], cwd=str(studio.server_py.parent), env=env)


def _banner(echo: Echo, url: str, port: int, token: str, project_dir: Path, edge_url: str, brain_url: str, run_id: str) -> None:
    echo("")
    echo("  ┌─ Remoroo Studio ─────────────────────────────────────────────")
    echo("  │  Open this on your laptop/tablet (same network):")
    echo(f"  │    {url}")
    echo(f"  │  (local: http://localhost:{port}/?token={token})")
    echo(f"  │  Project + remoroo_cell/ under: {project_dir}")
    echo(f"  │  edge: {edge_url or 'not connected'} · run: {run_id or '(editor only)'}")
    echo("  │  Press Ctrl+C here to stop.")
    echo("  └──────────────────────────────────────────────────────────────")
    _print_qr(url)


def _serve_loop(studio_proc: subprocess.Popen, others: list[Optional[subprocess.Popen]], on_stop, echo: Echo) -> bool:
    try:
        studio_proc.wait()
        return True
    except KeyboardInterrupt:
        echo("\nStopping…")
        return True
    finally:
        if on_stop:
            on_stop()
        for p in [studio_proc, *others]:
            if p is None:
                continue
            p.terminate()
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()


def serve_studio(project_dir: Path, port: int = 7777, token: Optional[str] = None, echo: Echo = print,
                 edge_url: str = "", brain_url: str = "", spawn_edge: bool = False) -> bool:
    """Editor-only / dev serve (no agent run). Used when there's no CP session."""
    studio = find_studio()
    if studio is None:
        echo("Couldn't find Remoroo Studio (not bundled and no repo). Reinstall remoroo or set REMOROO_STUDIO_DIR.")
        return False
    if not ensure_built(studio, echo):
        return False
    project_dir = Path(project_dir).resolve()
    project_dir.mkdir(parents=True, exist_ok=True)
    token = token or _stable_token(project_dir)
    edge_proc = None
    if spawn_edge and not edge_url:
        edge_proc, edge_url = _start_edge(studio, project_dir, echo)
    env = dict(os.environ)
    env.update({"PORT": str(port), "TOKEN": token, "PROJECT": str(project_dir), "DIST": str(studio.dist),
                "STUDIO_VERSION": _pkg_version(), "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"})
    if edge_url:
        env["EDGE_URL"] = edge_url
    if brain_url:
        env["BRAIN_URL"] = brain_url
    proc = _spawn_studio(studio, env, echo)
    if proc is None:
        return False
    url = f"http://{lan_ip()}:{port}/?token={token}"
    _banner(echo, url, port, token, project_dir, edge_url, brain_url, "")
    return _serve_loop(proc, [edge_proc], None, echo)


def launch_setup_studio(cfg, echo: Echo = print, *, spawn_edge: bool = False, edge_url: str = "",
                        agent_url: str = "", port: int = 7777) -> bool:
    """Create the real @robot_setup run + local worker, serve the Studio, and
    proxy the run API to the control plane so the browser is the entire UX."""
    from .run_local import (
        prepare_local_worker_context,
        finalize_local_worker_session,
        _headless_step_loop,
        RunPrepareError,
    )
    from .engine.local_worker import WorkerService

    studio = find_studio()
    if studio is None:
        echo("Couldn't find Remoroo Studio. Reinstall remoroo or set REMOROO_STUDIO_DIR.")
        return False
    if not ensure_built(studio, echo):
        return False

    project_dir = Path(cfg.repo_path).resolve()
    project_dir.mkdir(parents=True, exist_ok=True)

    # 1. create the run on the control plane + prepare the local worker context
    echo("Starting the @robot_setup run on the control plane…")
    try:
        ctx = prepare_local_worker_context(
            repo_path=cfg.repo_path, goal=cfg.goal, metrics=cfg.metrics_list, brain_url=cfg.brain_url,
            engine=cfg.engine, verbose=cfg.verbose, cache_env=cfg.cache_env, in_place=cfg.in_place,
            agentic=cfg.agentic, engine_version=cfg.engine_version, model=cfg.model,
            resume_run_id=cfg.resume_run_id, max_wall_time_s=cfg.max_wall_time_s,
            allow_overage=cfg.allow_overage, interactive=getattr(cfg, "interactive", True),
            operator_note=getattr(cfg, "operator_note", ""),
        )
    except RunPrepareError as exc:
        echo(f"❌ Could not start the run: {exc.message}")
        return False

    token = _stable_token(project_dir)
    edge_proc = None
    if spawn_edge and not edge_url:
        edge_proc, edge_url = _start_edge(studio, project_dir, echo)

    # 2. serve the studio, proxying the run API to the CP (api_url) with the session key
    env = dict(os.environ)
    env.update({
        "PORT": str(port), "TOKEN": token, "PROJECT": str(project_dir), "DIST": str(studio.dist),
        "STUDIO_VERSION": _pkg_version(),
        "BRAIN_URL": (agent_url or ctx.api_url or cfg.brain_url or "").rstrip("/"),
        "RUN_ID": ctx.remote_run_id,
        "SESSION_KEY": ctx.session_key or "",
    })
    if edge_url:
        env["EDGE_URL"] = edge_url
    studio_proc = _spawn_studio(studio, env, echo)
    if studio_proc is None:
        ctx.stop_heartbeat.set()
        return False

    url = f"http://{lan_ip()}:{port}/?token={token}"
    _banner(echo, url, port, token, project_dir, edge_url, env["BRAIN_URL"], ctx.remote_run_id)
    echo("  The agent is now driving setup — watch + answer it in the browser.")

    # 3. run the worker loop in the background (executes the agent's gate tools here)
    def _worker_loop():
        try:
            worker = WorkerService(
                repo_root=str(ctx.repo_path), artifact_dir=str(ctx.run_output_dir),
                original_repo_root=ctx.original_repo_path, run_id=ctx.remote_run_id,
                engine=ctx.engine, persistence_dir=str(ctx.run_output_dir),
                cache_env=ctx.cache_env, in_place=ctx.in_place,
            )
            outcome, _ = _headless_step_loop(
                run_id=ctx.remote_run_id,
                poll_fn=lambda timeout, run_id: ctx.server.get_next_step(timeout=timeout, run_id=run_id),
                handle_fn=worker.handle_request,
                submit_fn=ctx.server.submit_result,
            )
            finalize_local_worker_session(ctx, {
                "final_result": outcome.final_result, "outcome": outcome.outcome,
                "success": outcome.success, "partial_success": outcome.partial_success,
                "_cleanup_worker": worker,
            })
            echo(f"\n✅ Setup run finished: {outcome.outcome}. Studio still serving for review — Ctrl+C to stop.")
        except Exception as e:  # noqa: BLE001
            echo(f"\n⚠ Worker loop error: {e}")
        finally:
            ctx.stop_heartbeat.set()

    worker_thread = threading.Thread(target=_worker_loop, daemon=True)
    worker_thread.start()

    return _serve_loop(studio_proc, [edge_proc], on_stop=lambda: ctx.stop_heartbeat.set(), echo=echo)
