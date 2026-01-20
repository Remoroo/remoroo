"""
Instrumentation target selection (generic, evidence-based).

This module helps the engine choose *where* to instrument so that metrics can be
observed reliably in large repositories.

Design goals (Remoroo Constitution-aligned):
- Evidence-based: provide reasons + provenance, not guesses.
- Context as a product surface: produce a compact candidates artifact for LLMs.
- Generic: no benchmark-specific rules.
"""

from __future__ import annotations

import os
import re
import json
from .file_access_tracker import FileAccessTracker
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .configs import DEFAULT_DENY_PATHS, DEFAULT_EXCLUDED_DIRS
from ..engine.core.repo_indexer import RepoIndexer


_PY_MOD_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*$")


@dataclass(frozen=True)
class Candidate:
    file_path: str
    module: str
    depth: int
    score: float
    reasons: List[str]


def _safe_read_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return None


def _norm_token(s: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", (s or "").strip().lower()).strip("_")


def _is_denied_path(rel_posix_path: str, *, deny_paths: List[str]) -> bool:
    p = (rel_posix_path or "").replace("\\", "/")
    for pref in deny_paths or []:
        if not isinstance(pref, str) or not pref:
            continue
        pref_norm = pref.replace("\\", "/")
        if p.startswith(pref_norm):
            return True
    return False


def _has_excluded_dir_component(rel_posix_path: str, *, excluded_dirs: set) -> bool:
    p = (rel_posix_path or "").replace("\\", "/")
    parts = [seg for seg in p.split("/") if seg]
    return any(seg in excluded_dirs for seg in parts)


def _command_entrypoints_from_commands(repo_root: str, commands: List[str]) -> List[str]:
    """
    Resolve python entrypoint file paths from commands.

    Supported patterns:
    - python script.py
    - python -m package_or_module
    """
    entrypoints: List[str] = []
    for cmd in commands or []:
        if not isinstance(cmd, str):
            continue
        cmd_s = cmd.strip()
        if not cmd_s:
            continue

        # python foo/bar.py
        m = re.search(r"\bpython(?:3)?\s+([^\s]+\.py)\b", cmd_s)
        if m:
            rel = m.group(1)
            abs_path = os.path.join(repo_root, rel)
            if os.path.exists(abs_path) and os.path.isfile(abs_path):
                entrypoints.append(rel)
            continue

        # python -m pkg.mod
        m = re.search(r"\bpython(?:3)?\s+-m\s+([^\s]+)\b", cmd_s)
        if m:
            mod = m.group(1).strip()
            if not _PY_MOD_RE.match(mod):
                continue
            mod_path = mod.replace(".", os.sep)
            candidates = [
                os.path.join(repo_root, f"{mod_path}.py"),
                os.path.join(repo_root, mod_path, "__main__.py"),
            ]
            for abs_path in candidates:
                if os.path.exists(abs_path) and os.path.isfile(abs_path):
                    rel = os.path.relpath(abs_path, repo_root)
                    entrypoints.append(rel)
                    break
            continue

    # De-dupe while preserving order
    seen = set()
    out: List[str] = []
    for p in entrypoints:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def select_instrumentation_targets(
    *,
    repo_root: str,
    commands: List[str],
    metric_names: List[str],
    max_depth: int = 3,
    max_candidates: int = 20,
    select_top_n: int = 3,
) -> Dict[str, Any]:
    """
    Build an evidence-rich candidate list and choose a small set of files to instrument.

    Returns a JSON-serializable dict for persistence and LLM context.
    """
    metric_tokens = [_norm_token(m) for m in (metric_names or []) if isinstance(m, str) and m.strip()]
    metric_tokens = [t for t in metric_tokens if t]

    # Lightweight, generic token affinities:
    # - Prefer "core" / "runner" / "eval" / "model" style modules for metrics capture.
    # - Deprioritize pure I/O modules (dataset/output), which often don't compute metrics and are easy to break.
    #
    # This is intentionally small and generic (not benchmark-specific).
    POS_TOKENS = {
        "runner",
        "run",
        "eval",
        "evaluate",
        "metric",
        "metrics",
        "score",
        "model",
        "train",
        "predict",
        "inference",
        "benchmark",
        "bench",
        "core",
    }
    NEG_TOKENS = {
        "io",
        "dataset",
        "datasets",
        "output",
        "outputs",
        "data",
    }

    from .repo_manager import get_index_path
    index_path = get_index_path(repo_root)
    index = _safe_read_json(index_path) or {}
    files: List[Dict[str, Any]] = index.get("files", []) or []
    symbols: List[Dict[str, Any]] = index.get("symbols", []) or []

    # Harden against stale/over-inclusive indexes: never consider denied/venv/vendor paths
    # as instrumentation candidates (even if they exist in a previously-built index).
    deny_paths = DEFAULT_DENY_PATHS
    excluded_dirs = DEFAULT_EXCLUDED_DIRS

    def _ok_file_entry(f: Dict[str, Any]) -> bool:
        fp = f.get("path")
        if not isinstance(fp, str) or not fp:
            return False
        fp = fp.replace("\\", "/")
        if _is_denied_path(fp, deny_paths=deny_paths):
            return False
        if _has_excluded_dir_component(fp, excluded_dirs=excluded_dirs):
            return False
        return True

    files = [f for f in files if isinstance(f, dict) and _ok_file_entry(f)]
    by_path: Dict[str, Dict[str, Any]] = {f.get("path"): f for f in files if isinstance(f, dict) and f.get("path")}
    by_module: Dict[str, Dict[str, Any]] = {f.get("module"): f for f in files if isinstance(f, dict) and f.get("module")}

    symbols_by_file: Dict[str, List[Dict[str, Any]]] = {}
    for sym in symbols:
        if not isinstance(sym, dict):
            continue
        fp = sym.get("file_path")
        if not isinstance(fp, str) or not fp:
            continue
        fp = fp.replace("\\", "/")
        if _is_denied_path(fp, deny_paths=deny_paths) or _has_excluded_dir_component(fp, excluded_dirs=excluded_dirs):
            continue
        symbols_by_file.setdefault(fp, []).append(sym)

    entry_files = _command_entrypoints_from_commands(repo_root, commands)

    # If we can't resolve entrypoints from commands, fall back to repo index entrypoints.
    if not entry_files:
        for ep in (index.get("entrypoints", []) or []):
            if isinstance(ep, dict) and isinstance(ep.get("file_path"), str):
                entry_files.append(ep["file_path"])

    entry_files = [p.replace("\\", "/") for p in entry_files if isinstance(p, str)]
    entry_files = [p for p in entry_files if p in by_path]

    # BFS on module imports (best-effort, generic).
    reachable: Dict[str, int] = {}  # file_path -> depth
    frontier: List[Tuple[str, int]] = [(p, 0) for p in entry_files]
    while frontier:
        fp, depth = frontier.pop(0)
        if fp in reachable and reachable[fp] <= depth:
            continue
        reachable[fp] = depth
        if depth >= max_depth:
            continue
        file_info = by_path.get(fp) or {}
        for imp in file_info.get("imports", []) or []:
            if not isinstance(imp, str) or not imp:
                continue
            # Only follow imports that are in-repo modules.
            target = by_module.get(imp)
            if target and isinstance(target.get("path"), str):
                frontier.append((target["path"], depth + 1))

    # Build candidates with reasons (no file content reads here).
    candidates: List[Candidate] = []
    for fp, depth in reachable.items():
        file_info = by_path.get(fp) or {}
        module = str(file_info.get("module") or "")
        imports = set([i for i in (file_info.get("imports", []) or []) if isinstance(i, str)])
        syms = symbols_by_file.get(fp, [])
        sym_names = [str(s.get("name") or "") for s in syms if isinstance(s, dict)]

        reasons: List[str] = []
        score = 0.0

        if fp in entry_files:
            # Being the invoked entrypoint is useful as a *seed*, but often it is a thin
            # wrapper that merely calls into the real implementation. Don't let this
            # dominate the ranking.
            score += 0.75
            reasons.append("directly referenced by executed command (entrypoint seed)")

        # Reachability weight (closer to entrypoint is better).
        reach_w = 2.0 * (1.0 / (1 + max(0, depth)))
        score += reach_w
        reasons.append(f"reachable from entrypoint via imports (depth={depth})")

        # "Meat" signal: files that orchestrate multiple in-repo modules are usually
        # better instrumentation targets than thin wrappers.
        in_repo_imports = 0
        for imp in imports:
            if imp in by_module:
                in_repo_imports += 1
        if in_repo_imports:
            score += min(2.0, 0.4 * float(in_repo_imports))
            reasons.append(f"imports {in_repo_imports} in-repo module(s) (likely orchestration/meat)")

        # "Meat" signal: presence of callable/class symbols suggests real computation.
        callable_kinds = {"function", "method", "class"}
        callable_syms = [s for s in syms if isinstance(s, dict) and s.get("kind") in callable_kinds]
        callable_count = len(callable_syms)
        if callable_count:
            score += min(2.5, 0.6 + 0.35 * float(min(callable_count, 6)))
            reasons.append(f"defines {callable_count} callable/class symbol(s) (likely computation site)")
        else:
            score -= 0.5
            reasons.append("defines no callables/classes (likely thin wrapper)")

        # Penalize thin package entrypoints. In many repos, `__main__.py` is just a
        # delegator; prefer instrumenting the deeper module that computes metrics.
        if fp.replace("\\", "/").endswith("/__main__.py") and callable_count <= 1:
            score -= 2.25
            reasons.append("thin __main__.py penalty (prefer deeper modules for observability)")

        # Metric affinity: match metric names to symbol names and file/module identifiers.
        affinity_hits = 0
        haystack = " ".join([fp, module] + sym_names).lower()
        norm_haystack = _norm_token(haystack)
        for tok in metric_tokens:
            if tok and tok in norm_haystack:
                affinity_hits += 1
        if metric_tokens:
            affinity = affinity_hits / max(1, len(metric_tokens))
            if affinity > 0:
                score += 2.0 * affinity
                reasons.append(f"mentions metric-like tokens ({affinity_hits}/{len(metric_tokens)}) in symbols/module/path")

        # Generic path/symbol token affinity (helps avoid instrumenting I/O plumbing by default).
        tok_hay = set([t for t in _norm_token(fp).split("_") if t] + [t for t in _norm_token(module).split("_") if t])
        pos_hits = len([t for t in tok_hay if t in POS_TOKENS])
        neg_hits = len([t for t in tok_hay if t in NEG_TOKENS])
        if pos_hits:
            score += min(1.8, 0.5 + 0.35 * float(min(pos_hits, 4)))
            reasons.append(f"contains implementation-ish tokens ({pos_hits} hit(s))")
        if neg_hits:
            score -= min(2.0, 0.6 + 0.45 * float(min(neg_hits, 4)))
            reasons.append(f"contains I/O-ish tokens ({neg_hits} hit(s), deprioritized)")

        # I/O affinity: hints that this file reads/writes data/artifacts (generic via imports).
        io_imports = {"json", "csv", "pathlib", "pickle", "sqlite3"}
        io_hits = len([i for i in imports if i.split(".")[0] in io_imports])
        if io_hits:
            score += min(1.5, 0.5 + 0.25 * io_hits)
            reasons.append(f"imports common I/O stdlib modules ({io_hits} hit(s))")

        # De-risk: avoid tests.
        if "/test" in fp.replace("\\", "/") or fp.startswith("tests/") or fp.endswith("_test.py"):
            score -= 2.0
            reasons.append("looks like a test file (deprioritized)")

        candidates.append(Candidate(file_path=fp, module=module, depth=depth, score=score, reasons=reasons))

    candidates_sorted = sorted(candidates, key=lambda c: (-c.score, c.depth, c.file_path))
    top = candidates_sorted[: max(1, max_candidates)]
    selected = top[: max(1, select_top_n)]

    # If we found non-entry reachable candidates, prefer them over the thin entrypoint.
    # (Thin entrypoints often cannot expose metric signals; "meat" is usually one hop away.)
    non_entry = [c for c in selected if c.file_path not in entry_files]
    if non_entry:
        selected = non_entry + [c for c in selected if c.file_path in entry_files]
        selected = selected[: max(1, select_top_n)]

    # Provide snippet *requests* (line ranges) for context hydration.
    snippet_requests: List[Dict[str, Any]] = []
    for c in selected:
        file_info = by_path.get(c.file_path) or {}
        # Prefer __main__ block if known from indexer.
        eps = file_info.get("entrypoints", []) or []
        for ep in eps:
            if not isinstance(ep, dict):
                continue
            if ep.get("kind") == "__main__":
                snippet_requests.append({
                    "file_path": c.file_path,
                    "line_start": ep.get("line_start"),
                    "line_end": ep.get("line_end"),
                    "reason": "__main__ block (entrypoint evidence)"
                })
                break

        # Add up to 2 top-level functions as additional context (best-effort).
        added = 0
        for sym in sorted(symbols_by_file.get(c.file_path, []), key=lambda s: int(s.get("span", {}).get("line_start", 10**9))):
            if not isinstance(sym, dict) or sym.get("kind") not in {"function", "method"}:
                continue
            span = sym.get("span") or {}
            ls = span.get("line_start")
            le = span.get("line_end")
            if not isinstance(ls, int) or not isinstance(le, int) or le <= ls:
                continue
            # Keep snippets bounded.
            if (le - ls) > 200:
                continue
            snippet_requests.append({
                "file_path": c.file_path,
                "line_start": ls,
                "line_end": le,
                "reason": f"callable candidate: {sym.get('qualified_name') or sym.get('name')}"
            })
            added += 1
            if added >= 2:
                break

    return {
        "version": 1,
        "inputs": {
            "commands": commands,
            "metric_names": metric_names,
            "max_depth": max_depth,
            "max_candidates": max_candidates,
            "select_top_n": select_top_n,
        },
        "index": {
            "index_path": index_path,
            "file_count": len(files),
            "symbol_count": len(symbols),
            "resolved_entry_files": entry_files,
        },
        "reachable": [
            {"file_path": fp, "depth": depth, "module": (by_path.get(fp) or {}).get("module")}
            for fp, depth in sorted(reachable.items(), key=lambda kv: (kv[1], kv[0]))
        ],
        "candidates": [
            {
                "file_path": c.file_path,
                "module": c.module,
                "depth": c.depth,
                "score": round(float(c.score), 4),
                "reasons": c.reasons,
            }
            for c in top
        ],
        "selected_files": [c.file_path for c in selected],
        "snippet_requests": snippet_requests[:25],
    }

