from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

"""
Reasoning Context Selector (Stage 2)

This module is responsible for selecting the "Context Pack" for the Planner.
Unlike instrumentation_targets.py (which selects files to *inject code into*),
this module selects files that the Planner needs to *read* to understand the codebase,
the bug, and the verification strategy.

Priorities:
1. Root Cause Candidates (Entrypoints, Stack Traces)
2. Verification Candidates (Tests, Consumers)
3. Dependency Candidates (Imported Modules)
"""

@dataclass
class ContextCandidate:
    file_path: str
    score: float = 0.0
    reasons: List[str] = field(default_factory=list)
    depth: int = 999

# Stage 0: System-Critical Configs
CONFIG_FILES = {"setup.py", "pyproject.toml", "requirements.txt", "tox.ini", "Makefile", "dockerfile"}

def select_reasoning_context(
    *,
    repo_root: str,
    repo_index_summary: Dict[str, Any],
    commands: List[str],
    failing_test_paths: Optional[List[str]] = None,
    stack_trace_files: Optional[List[str]] = None,
    max_files: int = 10,
) -> Dict[str, Any]:
    """
    Selects files for the initial reasoning context.
    
    Args:
        repo_root: Absolute path to repo root.
        repo_index_summary: Summary from RepoIndexer (files, entrypoints, graph).
        commands: Commands being executed (to derive entrypoints).
        failing_test_paths: Explicit list of failing tests (if known).
        stack_trace_files: Files appearing in stack traces (if known).
        max_files: Budget for number of files to hydrate.
    
    Returns:
        Dict with 'selected_files' (List[str]) and 'candidates' (List[Dict]).
    """
    files = repo_index_summary.get("counts", {}).get("file_path_list", []) # RepoIndex summary usually lacks full list, we might need to rely on walking or passed-in list if available.
    # Logic fallback: if repo_index_summary doesn't have file list, we scan or assume caller passes it.
    # Actually, RepoIndexer usually builds a full index. Let's assume we can get it or walk.
    
    # If summary lacks file list, we do a quick walk to get candidates.
    all_files = _list_python_files(repo_root)
    
    candidates: Dict[str, ContextCandidate] = {
        f: ContextCandidate(file_path=f) for f in all_files
    }
    
    # 1. Score Entrypoints (Roots)
    entry_files = _derive_entrypoints(commands)
    for ef in entry_files:
        if ef in candidates:
            candidates[ef].score += 10.0
            candidates[ef].reasons.append("entrypoint_command")
            candidates[ef].depth = 0
            
    # 2. Score Failing Tests (Explicit Signal)
    if failing_test_paths:
        for tf in failing_test_paths:
            if tf in candidates:
                candidates[tf].score += 20.0  # Highest priority
                candidates[tf].reasons.append("reported_failing_test")
                candidates[tf].depth = 0

    # 3. Score Stack Trace Files (Explicit Signal)
    if stack_trace_files:
        for sf in stack_trace_files:
            if sf in candidates:
                candidates[sf].score += 15.0
                candidates[sf].reasons.append("stack_trace_frame")
                candidates[sf].depth = min(candidates[sf].depth, 1)

    # 4. Score Imports & Callers (Architecture Graph)
    # Use the 'graph' from repo_index_summary to find both neighbors and callers.
    graph = repo_index_summary.get("graph", {})
    if graph:
        # Forward: files imported by roots
        for root_file in entry_files | set(failing_test_paths or []) | set(stack_trace_files or []):
            neighbors = graph.get(root_file, {}).get("imports", [])
            for n in neighbors:
                if n in candidates:
                    candidates[n].score += 3.0
                    candidates[n].reasons.append(f"dependency_of_{os.path.basename(root_file)}")
                    candidates[n].depth = min(candidates[n].depth, 1)

        # Stage 0: Reverse Mapping (Callers)
        # Find who depends on the root files
        for root_file in entry_files:
            # Simple reverse lookup in graph
            for fp, info in graph.items():
                if root_file in info.get("imports", []):
                    if fp in candidates:
                        candidates[fp].score += 4.0
                        candidates[fp].reasons.append(f"caller_of_{os.path.basename(root_file)}")
                        candidates[fp].depth = min(candidates[fp].depth, 1)

    # 4.1 Proactive Config Inclusion
    for fp in all_files:
        if os.path.basename(fp).lower() in CONFIG_FILES:
            if fp in candidates:
                candidates[fp].score += 15.0
                candidates[fp].reasons.append("system_config_architect_priority")
                candidates[fp].depth = 0
    
    # 5. Test File Affinity
    # Unlike instrumentation (which penalizes tests), we BONUS tests because we want to run them.
    for fp, cand in candidates.items():
        if _is_test_file(fp):
            cand.score += 2.0
            cand.reasons.append("looks_like_test")
        
        # Heuristic: if a file is named `test_X.py` and `X.py` is an entrypoint, boost it.
        # Simple basename matching.
        basename = os.path.basename(fp)
        for ef in entry_files:
            ef_base = os.path.basename(ef)
            if ef_base.endswith(".py"):
                target_base = ef_base[:-3]
                if f"test_{target_base}.py" == basename or f"{target_base}_test.py" == basename:
                    cand.score += 5.0
                    cand.reasons.append(f"test_for_entrypoint_{ef_base}")

    # Sort and Select
    sorted_candidates = sorted(
        candidates.values(), 
        key=lambda c: (-c.score, c.depth, c.file_path)
    )
    
    selected = [c.file_path for c in sorted_candidates if c.score > 0][:max_files]
    
    # Fallback: if nothing selected, take top logic files (depth 0-1)
    if not selected:
        selected = [c.file_path for c in sorted_candidates][:max(1, max_files)]

    return {
        "selected_files": selected,
        "candidates": [
            {
                "file_path": c.file_path,
                "score": c.score,
                "reasons": c.reasons
            }
            for c in sorted_candidates[:20]
        ]
    }

def _list_python_files(root: str) -> List[str]:
    files = []
    for r, d, f in os.walk(root):
        for file in f:
            if file.endswith(".py"):
                path = os.path.relpath(os.path.join(r, file), root)
                if not path.startswith(".") and not "venv" in path:
                    files.append(path)
    return files

def _derive_entrypoints(commands: List[str]) -> Set[str]:
    entry_files = set()
    for cmd in commands:
        parts = cmd.split()
        for p in parts:
            if p.endswith(".py"):
                entry_files.add(p)
    return entry_files

def _is_test_file(path: str) -> bool:
    return "test" in path.lower()
