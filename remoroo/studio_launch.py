"""Launch Remoroo Studio (the visual setup surface) from the CLI.

`remoroo setup` serves the Studio from the robot computer over the LAN and prints
the URL/QR the operator opens on their laptop/tablet (the robot computer usually
has no display). The Studio ships **prebuilt** (minified ``dist/``) as CLI package
data under ``remoroo/_studio/`` and is served by a **Python** server, so the robot
machine needs only Python — no Node at runtime (Node is build-time only, in our CI).

Resolution order:
  1. bundled:  ``remoroo/_studio/`` (studio_server.py + dist/ + edge_real.py)
  2. repo dev: ``$REMOROO_STUDIO_DIR`` or a ``remoroo_studio/`` walking up (built with npm)
"""
from __future__ import annotations

import os
import secrets
import shutil
import socket
import subprocess
import sys
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
        echo("Studio isn't prebuilt and npm/Node isn't installed. Install Node 22+ to build it (dev), or reinstall a prebuilt remoroo.")
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


def serve_studio(
    project_dir: Path,
    port: int = 7777,
    token: Optional[str] = None,
    echo: Echo = print,
    edge_url: str = "",
    brain_url: str = "",
    spawn_edge: bool = False,
) -> bool:
    """Find + (build if dev) + serve the Studio with the Python server, print the
    LAN URL/QR, and block until interrupted. With `spawn_edge`/`edge_url` the gates
    run on the REAL edge; `brain_url` points the agent dock at the brain."""
    studio = find_studio()
    if studio is None:
        echo("Couldn't find Remoroo Studio (not bundled with this install and no repo). Reinstall remoroo, or set REMOROO_STUDIO_DIR.")
        return False
    if not ensure_built(studio, echo):
        return False
    py = _python()
    if not py:
        echo("No Python interpreter found to run the studio server.")
        return False

    project_dir = Path(project_dir).resolve()
    project_dir.mkdir(parents=True, exist_ok=True)
    token = token or secrets.token_hex(8)
    url = f"http://{lan_ip()}:{port}/?token={token}"

    edge_proc = None
    if spawn_edge and not edge_url:
        eport = os.environ.get("EDGE_PORT", "7779")
        eenv = dict(os.environ)
        eenv.update({"EDGE_PORT": eport, "REMOROO_CELL": str(project_dir / "remoroo_cell")})
        edge_proc = subprocess.Popen([py, str(studio.edge_py)], env=eenv)
        edge_url = f"http://127.0.0.1:{eport}"
        echo(f"Started the real edge (edge_real.py) on :{eport} — drives primitives.py + cuRobo.")

    env = dict(os.environ)
    env.update({"PORT": str(port), "TOKEN": token, "PROJECT": str(project_dir), "DIST": str(studio.dist)})
    if edge_url:
        env["EDGE_URL"] = edge_url
    if brain_url:
        env["BRAIN_URL"] = brain_url

    echo("")
    echo("  ┌─ Remoroo Studio ─────────────────────────────────────────────")
    echo("  │  Open this on your laptop/tablet (same network):")
    echo(f"  │    {url}")
    echo(f"  │  (local: http://localhost:{port}/?token={token})")
    echo(f"  │  Project + remoroo_cell/ will be written under: {project_dir}")
    echo(f"  │  edge: {edge_url or 'sim (client-side)'} · agent: {brain_url or 'sim'}")
    echo("  │  Press Ctrl+C here to stop serving.")
    echo("  └──────────────────────────────────────────────────────────────")
    _print_qr(url)

    proc = subprocess.Popen([py, str(studio.server_py)], cwd=str(studio.server_py.parent), env=env)
    try:
        proc.wait()
        return True
    except KeyboardInterrupt:
        echo("\nStopping the studio…")
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        return True
    finally:
        if edge_proc is not None:
            edge_proc.terminate()
            try:
                edge_proc.wait(timeout=5)
            except Exception:
                edge_proc.kill()
