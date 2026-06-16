#!/usr/bin/env python3
"""Remoroo Studio — local server (Python; bundled with the CLI).

Serves the prebuilt SPA (dist/) over the LAN with a per-launch token, does the
project IO (cell.json + the remoroo_cell/ export + git commit), and reverse-
proxies the data planes to the REAL edge/brain when configured. The simulators
are entirely CLIENT-SIDE (the SPA's DispatchEdge falls back to SimEdge when
/health reports edge:"sim"), so this server needs no sim code and the robot
computer needs only Python — no Node at runtime.

    DIST=/path/to/dist PROJECT=/path/to/repo PORT=7777 TOKEN=… \
    [EDGE_URL=http://127.0.0.1:7779] [BRAIN_URL=https://…] python studio_server.py
"""
from __future__ import annotations

import io
import json
import os
import shutil
import socket
import subprocess
import tempfile
import zipfile
from http.client import HTTPConnection, HTTPSConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, urlparse, parse_qs

HERE = Path(__file__).resolve().parent
DIST = Path(os.environ.get("DIST", HERE.parent / "dist")).resolve()
PROJECT = Path(os.environ.get("PROJECT", HERE.parent / "project")).resolve()
PORT = int(os.environ.get("PORT", "7777"))
TOKEN = os.environ.get("TOKEN", os.urandom(8).hex())
EDGE_URL = os.environ.get("EDGE_URL", "").rstrip("/")
BRAIN_URL = os.environ.get("BRAIN_URL", "").rstrip("/")  # the control plane (run API + agent)
RUN_ID = os.environ.get("RUN_ID", "")  # the @robot_setup run the browser attaches to
SESSION_KEY = os.environ.get("SESSION_KEY", "")  # auth for the CP run API (browser never sees it)

MIME = {
    ".html": "text/html", ".js": "text/javascript", ".mjs": "text/javascript",
    ".css": "text/css", ".json": "application/json", ".svg": "image/svg+xml",
    ".png": "image/png", ".jpg": "image/jpeg", ".glb": "model/gltf-binary",
    ".stl": "model/stl", ".urdf": "application/xml", ".wasm": "application/wasm",
    ".ico": "image/x-icon", ".map": "application/json",
}


def lan_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "localhost"


def token_ok(query: dict, headers) -> bool:
    q = (query.get("token") or [""])[0]
    auth = (headers.get("Authorization") or "").replace("Bearer ", "")
    return q == TOKEN or auth == TOKEN


def git_commit(repo: Path, rel: str) -> bool:
    try:
        subprocess.run(["git", "-C", str(repo), "rev-parse", "--is-inside-work-tree"], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(repo), "add", rel], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(repo), "commit", "-m", "remoroo setup: cell bundle"], check=True, capture_output=True)
        return True
    except Exception:
        return False


class Handler(BaseHTTPRequestHandler):
    server_version = "RemorooStudio/1.0"

    def log_message(self, *a):
        pass

    # ---- helpers ----
    def _json(self, obj, code=200):
        data = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _proxy(self, base: str, auth: str = ""):
        u = urlsplit(base)
        cls = HTTPSConnection if u.scheme == "https" else HTTPConnection
        conn = cls(u.hostname, u.port or (443 if u.scheme == "https" else 80), timeout=600)
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length) if length else None
        headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "connection", "content-length")}
        if auth:  # inject the CP session key; the browser never holds it
            headers["Authorization"] = f"Bearer {auth}"
        if body is not None:
            headers["Content-Length"] = str(len(body))
        try:
            conn.request(self.command, self.path, body=body, headers=headers)
            resp = conn.getresponse()
        except Exception as e:
            self._json({"error": "upstream unreachable", "detail": str(e)}, 502)
            return
        self.send_response(resp.status)
        for k, v in resp.getheaders():
            if k.lower() in ("transfer-encoding", "connection", "content-length"):
                continue
            self.send_header(k, v)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()  # close-delimited (HTTP/1.0) — streams SSE fine
        try:
            while True:
                chunk = resp.read(2048)
                if not chunk:
                    break
                self.wfile.write(chunk)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            conn.close()

    def _static(self, path: str):
        rel = "index.html" if path == "/" else path.lstrip("/")
        f = (DIST / rel)
        if not f.exists() or f.is_dir():
            f = DIST / "index.html"  # SPA fallback
        if not f.exists():
            self.send_response(404); self.end_headers(); self.wfile.write(b"Not built. Run the studio build.")
            return
        data = f.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", MIME.get(f.suffix, "application/octet-stream"))
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    # ---- project IO ----
    def _project(self, path: str, query: dict):
        if not token_ok(query, self.headers):
            self._json({"error": "token required"}, 401)
            return
        cell_path = PROJECT / "cell.studio.json"
        if path == "/project/cell" and self.command == "GET":
            if not cell_path.exists():
                self._json({"error": "no project"}, 404)
                return
            self._json(json.loads(cell_path.read_text()))
            return
        if path == "/project/cell" and self.command == "PUT":
            PROJECT.mkdir(parents=True, exist_ok=True)
            length = int(self.headers.get("Content-Length", "0") or 0)
            cell_path.write_bytes(self.rfile.read(length))
            self._json({"ok": True, "path": str(cell_path)})
            return
        # cell.yaml — the canonical RobotModel inputs the operator authors at G0.
        # Stored verbatim (the SPA sends YAML text) so the edge/agent read it as-is.
        yaml_path = PROJECT / "remoroo_cell" / "cell.yaml"
        if path == "/project/cellyaml" and self.command == "GET":
            if not yaml_path.exists():
                self._json({"error": "no cell.yaml"}, 404)
                return
            text = yaml_path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/yaml")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(text)))
            self.end_headers()
            self.wfile.write(text)
            return
        if path == "/project/cellyaml" and self.command == "PUT":
            yaml_path.parent.mkdir(parents=True, exist_ok=True)
            length = int(self.headers.get("Content-Length", "0") or 0)
            yaml_path.write_bytes(self.rfile.read(length))
            self._json({"ok": True, "path": str(yaml_path)})
            return
        # Gate completion = real artifact presence under remoroo_cell/ (the setup
        # output contract). The SPA polls this to drive the G0–G9 rail — no sim.
        if path == "/project/artifacts" and self.command == "GET":
            cell = PROJECT / "remoroo_cell"
            has = lambda *rels: any((cell / r).exists() for r in rels)
            self._json({
                "detect": has("cell.yaml"),
                "toolchain": has("requirements.lock"),
                "bridge": has("primitives.py"),
                "calibrate": has("calibration/report.md", "calibration/hand_eye.yaml"),
                "spheres": has("robot_model/collision_spheres.yml"),
                "model": has("robot_model/robot.urdf"),
                "world": has("world/scene.json", "world/collision.obj", "world/collision.ply"),
                "capture": has("capture/schema.json", "capture/sample_episode"),
                "taskspec": has("task_spec.md"),
                "signoff": has("setup_report.md"),
            })
            return
        if path == "/project/export" and self.command == "POST":
            length = int(self.headers.get("Content-Length", "0") or 0)
            raw = self.rfile.read(length)
            dest = PROJECT / "remoroo_cell"
            tmp = Path(tempfile.mkdtemp(prefix="remoroo_cell.", dir=str(PROJECT)))
            try:
                with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                    zf.extractall(tmp)
                if dest.exists():
                    shutil.rmtree(dest)
                tmp.rename(dest)
            finally:
                if tmp.exists():
                    shutil.rmtree(tmp, ignore_errors=True)
            committed = git_commit(PROJECT, "remoroo_cell") if parse_qs(urlparse(self.path).query).get("commit") == ["1"] else False
            self._json({"ok": True, "path": str(dest), "committed": committed})
            return
        self._json({"error": "not found"}, 404)

    # ---- routing ----
    def _route(self):
        u = urlparse(self.path)
        return u.path, parse_qs(u.query)

    def do_GET(self):
        p, q = self._route()
        if p == "/health":
            self._json({"ok": True, "edge": "real" if EDGE_URL else "none", "brain": bool(BRAIN_URL), "run_id": RUN_ID})
        elif p == "/studio/session":
            self._json({"run_id": RUN_ID, "edge": "real" if EDGE_URL else "none", "brain": bool(BRAIN_URL)})
        elif p == "/live/joints" or p.startswith("/edge/"):
            self._proxy(EDGE_URL) if EDGE_URL else self._json({"error": "edge not connected"}, 501)
        elif p.startswith("/runs/") or p.startswith("/agent/"):
            self._proxy(BRAIN_URL, SESSION_KEY) if BRAIN_URL else self._json({"error": "no control plane"}, 501)
        elif p.startswith("/project/"):
            self._project(p, q)
        else:
            self._static(p)

    def do_POST(self):
        p, q = self._route()
        if p.startswith("/runs/") or p.startswith("/agent/"):
            self._proxy(BRAIN_URL, SESSION_KEY) if BRAIN_URL else self._json({"error": "no control plane"}, 501)
        elif p.startswith("/edge/"):
            self._proxy(EDGE_URL) if EDGE_URL else self._json({"error": "edge not connected"}, 501)
        elif p.startswith("/project/"):
            self._project(p, q)
        else:
            self._json({"error": "not found"}, 404)

    def do_PUT(self):
        p, q = self._route()
        if p.startswith("/project/"):
            self._project(p, q)
        else:
            self._json({"error": "not found"}, 404)


def main():
    PROJECT.mkdir(parents=True, exist_ok=True)
    url = f"http://{lan_ip()}:{PORT}/?token={TOKEN}"
    print(f"[remoroo-studio] {url}")
    print(f"[remoroo-studio] serving {DIST} · project {PROJECT}")
    print(f"[remoroo-studio] edge: {EDGE_URL or 'sim (client-side)'} · brain: {BRAIN_URL or 'sim agent'}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
