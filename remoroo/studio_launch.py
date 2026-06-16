"""Launch Remoroo Studio (the visual setup surface) from the CLI.

`remoroo setup` serves the Studio from the robot computer over the LAN and prints
the URL/QR the operator opens on their laptop/tablet (the robot computer usually
has no display). This module only does what the brain can't: locate the studio,
ensure it's built, spawn the Node server, and report the address.

The Studio is the standalone app at ``remoroo_studio/`` (dev) or a prebuilt copy
pointed to by ``REMOROO_STUDIO_DIR``. The server writes the cell project +
``remoroo_cell/`` bundle under the chosen project dir (the operator's repo).
"""
from __future__ import annotations

import os
import secrets
import shutil
import socket
import subprocess
from pathlib import Path
from typing import Callable, Optional

Echo = Callable[[str], None]


def find_studio_dir() -> Optional[Path]:
    """Locate the studio: $REMOROO_STUDIO_DIR, else walk up from this file / cwd."""
    env = os.environ.get("REMOROO_STUDIO_DIR")
    if env:
        p = Path(env).expanduser()
        if (p / "server" / "studio_server.mjs").exists():
            return p
    seen: set[Path] = set()
    for start in (Path(__file__).resolve(), Path.cwd().resolve()):
        for base in [start, *start.parents]:
            cand = base / "remoroo_studio"
            if cand in seen:
                continue
            seen.add(cand)
            if (cand / "server" / "studio_server.mjs").exists():
                return cand
    return None


def lan_ip() -> str:
    """Best-effort primary LAN IPv4 (so another device can reach the studio)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "localhost"


def ensure_built(studio_dir: Path, echo: Echo = print) -> bool:
    """Make sure ``dist/`` exists; build it once if needed (npm install + build)."""
    if (studio_dir / "dist" / "index.html").exists():
        return True
    npm = shutil.which("npm")
    if not npm:
        echo(
            "npm/Node not found — can't build the studio. Install Node 22+, or set "
            "REMOROO_STUDIO_DIR to a prebuilt studio (one containing dist/)."
        )
        return False
    if not (studio_dir / "node_modules").exists():
        echo("Installing studio dependencies (one-time: npm install)…")
        if subprocess.run([npm, "install"], cwd=str(studio_dir)).returncode != 0:
            echo("npm install failed.")
            return False
    echo("Building the studio (one-time: npm run build)…")
    if subprocess.run([npm, "run", "build"], cwd=str(studio_dir)).returncode != 0:
        echo("Studio build failed.")
        return False
    return True


def _print_qr(url: str) -> None:
    try:
        import qrcode  # optional dependency

        qr = qrcode.QRCode(border=1)
        qr.add_data(url)
        qr.make(fit=True)
        qr.print_ascii(invert=True)
    except Exception:
        pass  # QR is a nicety; the URL is what matters


def _spawn_edge(studio_dir: Path, project_dir: Path, echo: Echo) -> tuple[Optional[subprocess.Popen], str]:
    """Spawn the REAL edge service (server/edge_real.py) for this cell on a local
    port; returns (process, edge_url). Falls back to ("", "") if Python is missing."""
    py = shutil.which("python3") or shutil.which("python")
    if not py:
        echo("python3 not found — can't launch the real edge; using the built-in sim.")
        return None, ""
    eport = os.environ.get("EDGE_PORT", "7779")
    env = dict(os.environ)
    env.update({"EDGE_PORT": eport, "REMOROO_CELL": str(project_dir / "remoroo_cell")})
    proc = subprocess.Popen([py, str(studio_dir / "server" / "edge_real.py")], env=env)
    echo(f"Started the real edge (edge_real.py) on :{eport} (drives primitives.py + cuRobo).")
    return proc, f"http://127.0.0.1:{eport}"


def serve_studio(
    project_dir: Path,
    port: int = 7777,
    token: Optional[str] = None,
    echo: Echo = print,
    edge_url: str = "",
    brain_url: str = "",
    spawn_edge: bool = False,
) -> bool:
    """Find + build + serve the Studio, print the LAN URL/QR, and block until
    interrupted. With `spawn_edge` (or an explicit `edge_url`) the gates run on the
    REAL edge; `brain_url` points the agent dock at the brain. Returns True on a
    clean shutdown, False if it couldn't start."""
    studio_dir = find_studio_dir()
    if studio_dir is None:
        echo(
            "Couldn't find Remoroo Studio. Run from the remoroo repo, or set "
            "REMOROO_STUDIO_DIR to the studio directory."
        )
        return False
    if not ensure_built(studio_dir, echo):
        return False

    node = shutil.which("node")
    if not node:
        echo("node not found on PATH — install Node 22+.")
        return False

    project_dir = Path(project_dir).resolve()
    project_dir.mkdir(parents=True, exist_ok=True)
    token = token or secrets.token_hex(8)
    url = f"http://{lan_ip()}:{port}/?token={token}"

    edge_proc = None
    if spawn_edge and not edge_url:
        edge_proc, edge_url = _spawn_edge(studio_dir, project_dir, echo)

    env = dict(os.environ)
    env.update({"PORT": str(port), "TOKEN": token, "PROJECT": str(project_dir)})
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
    echo(f"  │  edge: {edge_url or 'sim (built-in)'} · agent: {brain_url or 'sim'}")
    echo("  │  Press Ctrl+C here to stop serving.")
    echo("  └──────────────────────────────────────────────────────────────")
    _print_qr(url)

    proc = subprocess.Popen([node, "server/studio_server.mjs"], cwd=str(studio_dir), env=env)
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
