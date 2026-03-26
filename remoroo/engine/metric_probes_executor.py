"""Execute agent-declared metric probes after a supervised job completes."""
from __future__ import annotations

import json
import re
import subprocess
from typing import Any, Dict, List, Optional, Tuple

_MAX_STDOUT_SCAN_BYTES = 2 * 1024 * 1024
_MAX_PROBE_SHELL_TIMEOUT_S = 60.0
_MAX_PROBES = 16


def _coerce_value(raw: str) -> Any:
    s = raw.strip()
    low = s.lower()
    if low == "true":
        return True
    if low == "false":
        return False
    try:
        if "." in s or "e" in low or "E" in s:
            return float(s)
        return int(s)
    except ValueError:
        return s


def _get_json_at_pointer(data: Any, pointer: str) -> Any:
    """Very small JSON pointer: /a/b/c or empty = root."""
    p = (pointer or "").strip()
    if not p or p == "/":
        return data
    parts = [x for x in p.split("/") if x]
    cur = data
    for part in parts:
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def execute_metric_probes(
    probes: Optional[List[Dict[str, Any]]],
    *,
    stdout_text: str,
    stderr_text: str,
    cwd: str,
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Run probes; return (snapshot, errors_by_name)."""
    snapshot: Dict[str, Any] = {}
    errors: Dict[str, str] = {}
    if not probes:
        return snapshot, errors

    combined = (stdout_text or "") + "\n" + (stderr_text or "")
    if len(combined) > _MAX_STDOUT_SCAN_BYTES:
        combined = combined[-_MAX_STDOUT_SCAN_BYTES:]

    for i, probe in enumerate(probes[:_MAX_PROBES]):
        if not isinstance(probe, dict):
            continue
        name = probe.get("name")
        ptype = probe.get("type")
        if not name or not ptype:
            errors[str(name or f"probe_{i}")] = "missing name or type"
            continue
        name = str(name)

        try:
            if ptype == "regex_on_job_stdout":
                pattern = probe.get("pattern") or ""
                flags = re.MULTILINE
                if str(probe.get("flags", "")).lower() == "i":
                    flags |= re.IGNORECASE
                m = re.search(pattern, combined, flags)
                if not m:
                    snapshot[name] = None
                    errors[name] = "regex no match"
                else:
                    g = m.group(1) if m.lastindex else m.group(0)
                    snapshot[name] = _coerce_value(g) if g is not None else None

            elif ptype == "read_json_path":
                path = probe.get("path") or ""
                if not path:
                    snapshot[name] = None
                    errors[name] = "missing path"
                    continue
                import os

                fp = path if os.path.isabs(path) else os.path.join(cwd, path)
                if not os.path.isfile(fp):
                    snapshot[name] = None
                    errors[name] = f"file not found: {path}"
                    continue
                with open(fp, "r", encoding="utf-8", errors="replace") as f:
                    data = json.load(f)
                ptr = probe.get("json_pointer") or probe.get("key")
                if isinstance(ptr, str) and ptr and not ptr.startswith("/"):
                    val = data.get(ptr) if isinstance(data, dict) else None
                else:
                    val = _get_json_at_pointer(data, str(ptr or "/"))
                if val is None:
                    snapshot[name] = None
                    errors[name] = "json pointer miss"
                elif isinstance(val, (int, float, bool)):
                    snapshot[name] = val
                else:
                    snapshot[name] = val

            elif ptype == "shell":
                cmd = probe.get("command") or ""
                if not cmd.strip():
                    snapshot[name] = None
                    errors[name] = "empty shell command"
                    continue
                r = subprocess.run(
                    ["/bin/sh", "-c", cmd],
                    cwd=cwd,
                    capture_output=True,
                    text=True,
                    timeout=_MAX_PROBE_SHELL_TIMEOUT_S,
                )
                out = (r.stdout or "").strip()
                if r.returncode != 0:
                    snapshot[name] = None
                    errors[name] = f"exit {r.returncode}: {(r.stderr or '')[:200]}"
                elif not out:
                    snapshot[name] = None
                    errors[name] = "empty output"
                else:
                    snapshot[name] = _coerce_value(out.splitlines()[-1])

            else:
                errors[name] = f"unknown type: {ptype}"
                snapshot[name] = None
        except subprocess.TimeoutExpired:
            snapshot[name] = None
            errors[name] = "probe timeout"
        except Exception as e:
            snapshot[name] = None
            errors[name] = str(e)[:200]

    return snapshot, errors
