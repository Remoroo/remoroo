from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Callable

from ...execution.configs import DEFAULT_DENY_PATHS, DEFAULT_EXCLUDED_DIRS
from .executor import run_commands


_FLAG_RE = re.compile(r"--[a-zA-Z0-9][a-zA-Z0-9_-]*")


def _write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


def _safe_read_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except Exception:
        return ""


def _extract_flags(text: str) -> List[str]:
    return list(dict.fromkeys(_FLAG_RE.findall(text or "")))


def _rewrite_python_launcher(cmd: str, *, venv_python: Optional[str]) -> str:
    """
    Rewrite `python` / `python3` at the start of a command to venv_python when provided.
    This keeps discovery robust under venv usage, without affecting non-Python commands.
    """
    if not venv_python:
        return cmd
    s = (cmd or "").lstrip()
    if s.startswith("python3 "):
        return f"{venv_python} {s[len('python3 '):]}"
    if s == "python3":
        return f"{venv_python}"
    if s.startswith("python "):
        return f"{venv_python} {s[len('python '):]}"
    if s == "python":
        return f"{venv_python}"
    return cmd


def _normalize_stage_name(flag: str) -> str:
    # --train -> train, --evaluate -> evaluate
    f = flag.strip().lstrip("-")
    f = re.sub(r"[^a-zA-Z0-9_]+", "_", f).strip("_").lower()
    return f or "stage"


def _infer_required_mode_flags(stderr: str) -> List[str]:
    """
    Best-effort parse of argparse-style usage errors, returning required mode flags.
    Generic patterns:
    - "one of the arguments --a --b is required"
    - "the following arguments are required: --a, --b"
    """
    s = stderr or ""
    if "required" not in s.lower():
        return []

    # Prefer "one of the arguments ..." since it implies mutual exclusivity.
    m = re.search(r"one of the arguments\s+(.+?)\s+is required", s, flags=re.IGNORECASE | re.DOTALL)
    if m:
        flags = _extract_flags(m.group(1))
        return flags

    m = re.search(r"the following arguments are required:\s+(.+)$", s, flags=re.IGNORECASE | re.DOTALL)
    if m:
        flags = _extract_flags(m.group(1))
        return flags

    # Fallback: grab any flags mentioned in stderr when it looks like a usage error.
    if "usage:" in s.lower() or "error:" in s.lower():
        return _extract_flags(s)

    return []


def _should_require_eval_stage(metric_names: List[str]) -> bool:
    toks = " ".join([m.lower() for m in metric_names if isinstance(m, str)])
    return any(k in toks for k in ["accuracy", "f1", "precision", "recall", "auc", "bleu", "rouge", "wer", "cer"])


def _maybe_add_common_args(cmd: str, *, help_text: str) -> str:
    """
    Add generic stable args when the CLI supports them:
    - --workdir <path>  (ensures multi-stage commands share artifacts)
    - --seed <int>      (improves determinism)
    """
    flags = set(_extract_flags(help_text))
    out = cmd
    if "--workdir" in flags and "--workdir" not in cmd:
        out = f"{out} --workdir workdir"
    if "--seed" in flags and "--seed" not in cmd:
        out = f"{out} --seed 7"
    return out


@dataclass
class CommandDiscoveryResult:
    success: bool
    command_plan: Dict[str, List[str]]
    candidates: List[Dict[str, Any]]
    chosen_entry_cmd: Optional[str] = None
    reason: str = ""


def discover_command_plan(
    *,
    repo_root: str,
    artifact_dir: str,
    metric_names: List[str],
    initial_commands: List[str],
    venv_python: Optional[str] = None,
    timeout_s: float = 8.0,
    runner_factory: Optional[Callable] = None,
    output_callback: Optional[Callable] = None,
) -> CommandDiscoveryResult:
    """
    Discover a *valid* command plan by probing likely entry commands.

    This is intentionally language-agnostic:
    - it probes `--help` / `-h`
    - it rejects invalid invocations (usage errors)
    - it can expand mutually-exclusive required flags into multiple stages
    """
    repo_root = os.path.abspath(repo_root)
    os.makedirs(os.path.join(repo_root, "artifacts"), exist_ok=True)

    candidates: List[Dict[str, Any]] = []
    base_cmds = [c for c in (initial_commands or []) if isinstance(c, str) and c.strip()]
    if not base_cmds:
        # Minimal fallback: try python main.py if present.
        if os.path.exists(os.path.join(repo_root, "main.py")):
            base_cmds = ["python main.py"]

    # Deduplicate while preserving order
    seen = set()
    base_cmds = [c for c in base_cmds if not (c in seen or seen.add(c))]

    for base in base_cmds[:6]:
        base = _rewrite_python_launcher(base, venv_python=venv_python)

        # Probe help first.
        help_cmds = [f"{base} --help", f"{base} -h"]
        help_outcomes = run_commands(
            repo_root=repo_root,
            commands=help_cmds,
            timeout_s=float(timeout_s),
            stage_name="COMMAND_DISCOVERY_HELP",
            env={},
            runner_factory=runner_factory,
            output_callback=output_callback,
        )
        # run_commands returns a list of dict outcomes
        for oc in help_outcomes:
            candidates.append({"kind": "help_probe", **oc})

        # Use the first help output that looks like usage/help text.
        help_text = ""
        for oc in help_outcomes:
            out = (oc.get("stdout") or "") + "\n" + (oc.get("stderr") or "")
            if "usage:" in out.lower() or "--" in out:
                help_text = out
                break

        # Validate base execution (without flags) is runnable; if not, attempt to expand.
        exec_outcomes = run_commands(
            repo_root=repo_root,
            commands=[base],
            timeout_s=float(timeout_s),
            stage_name="COMMAND_DISCOVERY_RUN",
            env={},
            runner_factory=runner_factory,
            output_callback=output_callback,
        )
        for oc in exec_outcomes:
            candidates.append({"kind": "run_probe", **oc})

        oc0 = exec_outcomes[0] if exec_outcomes else {}
        exit_code = oc0.get("exit_code")
        stderr = str(oc0.get("stderr") or "")

        # If the base command runs, accept it as a single-stage plan.
        # Treat timeout as success for long-running/interactive apps (like Pygame)
        # provided they didn't crash immediately (duration check implied by timeout)
        is_success = exit_code == 0
        if not is_success and oc0.get("timed_out"):
            is_success = True
            # Optional: Add a note or handle differently if needed
        
        if is_success:
            cmd = _maybe_add_common_args(base, help_text=help_text)
            plan = {"Stage_1": [cmd]}
            return CommandDiscoveryResult(
                success=True,
                command_plan=plan,
                candidates=candidates,
                chosen_entry_cmd=base,
                reason="Base entry command succeeded (or timed out safely).",
            )

        # If it looks like a usage error, try to infer required mode flags and expand.
        mode_flags = _infer_required_mode_flags(stderr)
        if mode_flags:
            expanded: List[Tuple[str, str]] = []
            for f in mode_flags:
                cmd = _maybe_add_common_args(f"{base} {f}", help_text=help_text)
                expanded.append((_normalize_stage_name(f), cmd))

            # If metrics imply evaluation, try to order stages so eval-like runs happen.
            require_eval = _should_require_eval_stage(metric_names)
            if require_eval:
                def is_eval(name: str) -> bool:
                    return any(k in name for k in ["eval", "evaluate", "test", "valid"])

                expanded.sort(key=lambda kv: (0 if is_eval(kv[0]) else 1, kv[0]))

            # Create stages in order.
            plan: Dict[str, List[str]] = {}
            for i, (stage, cmd) in enumerate(expanded, start=1):
                key = f"Stage_{i}"
                plan[key] = [cmd]

            return CommandDiscoveryResult(
                success=True,
                command_plan=plan,
                candidates=candidates,
                chosen_entry_cmd=base,
                reason=f"Expanded required mode flags from usage error: {mode_flags}",
            )

    return CommandDiscoveryResult(
        success=False,
        command_plan={},
        candidates=candidates,
        chosen_entry_cmd=None,
        reason="No viable commands discovered from initial candidates.",
    )


def persist_command_plan(
    *,
    repo_root: str,
    artifact_dir: str,
    result: CommandDiscoveryResult,
) -> None:
    """
    Persist the discovery output to both repo artifacts and run artifacts for debugging.
    """
    payload = {
        "version": 1,
        "success": bool(result.success),
        "reason": result.reason,
        "chosen_entry_cmd": result.chosen_entry_cmd,
        "command_plan": result.command_plan,
        "candidates": result.candidates,
    }
    _write_json(os.path.join(repo_root, "artifacts", "command_plan.json"), payload)
    _write_json(os.path.join(artifact_dir, "command_plan.json"), payload)
