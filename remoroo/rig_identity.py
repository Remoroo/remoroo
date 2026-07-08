"""Rig identity — the client half of the continuity chain (ADC/pricing/
robotics_pricing_model.md section 2a).

Leg 1: the hardware fingerprint, best-effort component ids gathered from the host and the
authored cell (camera/controller serials from remoroo_cell/cell.yaml, host machine id, GPU
uuid). The CP ROLLS the stored set forward on every match, so gradual replacement never
breaks identity.

Leg 2: the rig token, minted once into <repo>/.remoroo/rig_token (0600, and .remoroo/ is
gitignored — `git clone` never carries it). Only its sha256 ever lives server-side.

Leg 3 lives on the CP: when neither leg matches, activation returns a QUESTION and a human
answers it in the Studio/CLI. Nothing in this module can create an invoice.
"""
from __future__ import annotations

import json
import re
import secrets
import subprocess
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

TOKEN_FILE = "rig_token"
SERIAL_FILE = "rig_serial"


def _read_cmd(args: List[str]) -> str:
    try:
        return subprocess.run(args, capture_output=True, text=True,
                              timeout=10).stdout.strip()
    except Exception:                                   # noqa: BLE001 - best-effort probe
        return ""


def _host_machine_id() -> str:
    for p in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
        try:
            v = Path(p).read_text().strip()
            if v:
                return v
        except Exception:                               # noqa: BLE001
            continue
    out = _read_cmd(["ioreg", "-rd1", "-c", "IOPlatformExpertDevice"])   # macOS
    m = re.search(r'"IOPlatformUUID"\s*=\s*"([^"]+)"', out)
    if m:
        return m.group(1)
    return f"mac-{uuid.getnode():012x}"                 # last resort: primary MAC


def _gpu_uuids() -> List[str]:
    out = _read_cmd(["nvidia-smi", "--query-gpu=uuid", "--format=csv,noheader"])
    return [ln.strip() for ln in out.splitlines() if ln.strip()]


def _cell_serials(repo: Path) -> Dict[str, List[str]]:
    """Camera/controller/arm serials from the authored cell, parsed tolerantly (the CLI
    does not depend on yaml; serial-shaped fields are enough for fingerprinting)."""
    out: Dict[str, List[str]] = {"camera": [], "controller": []}
    cell = repo / "remoroo_cell" / "cell.yaml"
    try:
        text = cell.read_text(encoding="utf-8")
    except Exception:                                   # noqa: BLE001
        return out
    for m in re.finditer(r"(camera_serial|zed_serial|cam_serial)\s*:\s*['\"]?"
                         r"([A-Za-z0-9_-]+)", text):
        out["camera"].append(m.group(2))
    for m in re.finditer(r"(serial|controller_id|arm_serial)\s*:\s*['\"]?"
                         r"([A-Za-z0-9_-]+)", text):
        out["controller"].append(m.group(2))
    out["camera"] = sorted(set(out["camera"]))
    out["controller"] = sorted(set(out["controller"] ) - set(out["camera"]))
    return out


def compute_fingerprint(repo: Path) -> Dict[str, Any]:
    cell = _cell_serials(repo)
    return {"components": {
        "camera": cell["camera"],
        "controller": cell["controller"],
        "host": [_host_machine_id()],
        "gpu": _gpu_uuids(),
    }}


def _ensure_gitignored(repo: Path) -> None:
    gi = repo / ".gitignore"
    try:
        text = gi.read_text(encoding="utf-8") if gi.exists() else ""
        if ".remoroo/" not in text and ".remoroo" not in text.split():
            gi.write_text(text + ("" if text.endswith("\n") or not text else "\n")
                          + ".remoroo/\n", encoding="utf-8")
    except Exception:                                   # noqa: BLE001 - never block on this
        pass


def rig_token(repo: Path) -> str:
    """Read-or-mint the host-local rig token. Minting is idempotent per host+repo."""
    d = repo / ".remoroo"
    d.mkdir(parents=True, exist_ok=True)
    _ensure_gitignored(repo)
    f = d / TOKEN_FILE
    if f.exists():
        tok = f.read_text().strip()
        if tok:
            return tok
    tok = secrets.token_hex(32)
    f.write_text(tok + "\n", encoding="utf-8")
    try:
        f.chmod(0o600)
    except Exception:                                   # noqa: BLE001
        pass
    return tok


def saved_serial(repo: Path) -> Optional[str]:
    f = repo / ".remoroo" / SERIAL_FILE
    try:
        s = f.read_text().strip()
        return s or None
    except Exception:                                   # noqa: BLE001
        return None


def save_serial(repo: Path, serial: str) -> None:
    d = repo / ".remoroo"
    d.mkdir(parents=True, exist_ok=True)
    (d / SERIAL_FILE).write_text(serial + "\n", encoding="utf-8")


def post_activate(base_url: str, token: str, payload: Dict[str, Any],
                  timeout: float = 30.0) -> Dict[str, Any]:
    r = requests.post(f"{base_url.rstrip('/')}/rigs/activate", json=payload,
                      headers={"Authorization": f"Bearer {token}"}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def post_heartbeat(base_url: str, token: str, serial: str,
                   payload: Dict[str, Any], timeout: float = 15.0) -> Dict[str, Any]:
    r = requests.post(f"{base_url.rstrip('/')}/rigs/{serial}/heartbeat", json=payload,
                      headers={"Authorization": f"Bearer {token}"}, timeout=timeout)
    r.raise_for_status()
    return r.json()
