from __future__ import annotations

import json
import os
import time
import glob
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Set

from datetime import datetime

from jsonschema import validate as jsonschema_validate

from ..engine.core.applier import apply_patchproposal
from .configs import DEFAULT_DENY_PATHS, DEFAULT_EXCLUDED_DIRS
from ..engine.core.executor import run_commands
from .file_access_tracker import FileAccessTracker
from .instrumentation_targets import select_instrumentation_targets
from .metrics_utils import unwrap_metrics_dict
from ..engine.core.repo_indexer import RepoIndexer
from ..engine.schemas import (
    BASELINE_METRICS_ARTIFACT_SCHEMA,
    CURRENT_METRICS_ARTIFACT_SCHEMA,
    MERGED_METRICS_ARTIFACT_SCHEMA,
)


def _safe_mkdir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass


def _read_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_json(path: str, obj: Any) -> None:
    _safe_mkdir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


def _read_file_lines(abs_path: str) -> List[str]:
    with open(abs_path, "r", encoding="utf-8", errors="replace") as f:
        return f.read().splitlines()


def _merge_metrics(base: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """Merge new metrics into base, overwriting duplicates."""
    merged = dict(base)
    merged.update(new)
    return merged


def _collect_and_merge_partial_artifacts(artifact_dir: str) -> Dict[str, Any]:
    """
    Collect all partial_{timestamp}_{uuid}_{name}.json files, merge them,
    and return the consolidated metrics dictionary.
    """
    metrics: Dict[str, Any] = {}
    pattern = os.path.join(artifact_dir, "partial_*.json")
    
    # Sort by timestamp (part of filename) to ensure consistent replay order
    # Filename format: partial_{timestamp}_{uuid}_{name}.json
    partial_files = sorted(glob.glob(pattern))
    
    for pf in partial_files:
        data = _read_json(pf)
        if not isinstance(data, dict):
            continue
        
        # Schema 1.0: {metric_name, value, unit, ...}
        m_name = data.get("metric_name")
        m_value = data.get("value")
        
        if m_name and isinstance(m_name, str):
            # Last write wins semantics
            metrics[m_name] = m_value
            
    return metrics


def _read_file_text(abs_path: str) -> str:
    with open(abs_path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def _is_repo_empty(repo_root: str) -> bool:
    """
    Detect an "empty repo" early.

    Empty is defined as: no code files / no entrypoints. For v1 we implement this as:
    - no Python files (*.py) outside excluded/denied paths.

    This guard exists to avoid indexing/hydrating vendor code (e.g., .remoroo_venvs/)
    and to skip the LLM entirely for empty repositories.
    """
    repo_root = os.path.abspath(repo_root)
    deny_paths = [p.replace("\\", "/") for p in (DEFAULT_DENY_PATHS or []) if isinstance(p, str)]
    excluded_dirs = set(DEFAULT_EXCLUDED_DIRS or set())

    for root, dirs, files in os.walk(repo_root):
        # Prune excluded dirs by basename and deny-path prefixes.
        kept_dirs: List[str] = []
        for d in list(dirs):
            if d in excluded_dirs:
                continue
            abs_d = os.path.join(root, d)
            rel_d = os.path.relpath(abs_d, repo_root).replace(os.sep, "/") + "/"
            if any(rel_d.startswith(pref) for pref in deny_paths):
                continue
            kept_dirs.append(d)
        dirs[:] = kept_dirs

        for fn in files:
            if not fn.endswith(".py"):
                continue
            abs_f = os.path.join(root, fn)
            rel_f = os.path.relpath(abs_f, repo_root).replace(os.sep, "/")
            if any(rel_f.startswith(pref) for pref in deny_paths):
                continue
            # Found a real code file.
            return False

    return True


def _extract_snippet(repo_root: str, file_path: str, line_start: Optional[int], line_end: Optional[int]) -> str:
    abs_path = os.path.join(repo_root, file_path)
    try:
        lines = _read_file_lines(abs_path)
    except Exception:
        return ""
    if not isinstance(line_start, int) or not isinstance(line_end, int) or line_end <= 0:
        # fallback: return first 200 lines
        return "\n".join(lines[:200])
    s = max(1, line_start)
    e = max(s, line_end)
    # convert to 0-based slice
    return "\n".join(lines[s - 1 : e])


def _summarize_repo_index(repo_index: Dict[str, Any]) -> Dict[str, Any]:
    files = repo_index.get("files", []) or []
    entrypoints = repo_index.get("entrypoints", []) or []
    graph = repo_index.get("graph", {}) or {}
    return {
        "schema_version": repo_index.get("schema_version"),
        "repo": repo_index.get("repo", {}),
        "build": {
            "created_at": (repo_index.get("build") or {}).get("created_at"),
            "languages": (repo_index.get("build") or {}).get("languages"),
        },
        "counts": {
            "file_count": len(files),
            "symbol_count": len((repo_index.get("symbols") or [])),
            "entrypoint_count": len(entrypoints),
        },
        "entrypoints": entrypoints[:50],
        "graph": {
            "module_deps_count": len((graph.get("module_deps") or [])),
        },
    }


def _compute_baseline_requirements(experiment_contract: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Returns (baseline_required, baseline_required_metric_names).
    Baseline is required if any required comparison references baseline.
    """
    comparisons = experiment_contract.get("comparisons") or []
    required_metrics: List[str] = []
    baseline_required = False
    for comp in comparisons:
        if not isinstance(comp, dict):
            continue
        if comp.get("required") is not True:
            continue
        left = comp.get("left") or {}
        right = comp.get("right") or {}
        left_src = left.get("source")
        right_src = right.get("source")
        if left_src == "baseline" or right_src == "baseline":
            baseline_required = True
            # collect baseline-referenced metric names (if present)
            for side in (left, right):
                if isinstance(side, dict) and side.get("source") == "baseline":
                    m = side.get("metric")
                    if isinstance(m, str) and m and m not in required_metrics:
                        required_metrics.append(m)
    return baseline_required, required_metrics


def _flatten_command_plan(command_plan: Any) -> List[str]:
    cmds: List[str] = []
    if not isinstance(command_plan, dict):
        return cmds
    for stage, arr in command_plan.items():
        if stage == "diagnostics_on_failure":
            continue
        if isinstance(arr, list):
            cmds.extend([c for c in arr if isinstance(c, str) and c.strip()])
    return cmds


@dataclass
class InstrumentationResult:
    success: bool
    instrumentation_plan: Optional[Dict[str, Any]] = None
    instrumentation_manifest: Optional[Dict[str, Any]] = None
    baseline_metrics: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    attempts: int = 0


class InstrumentationPipeline:
    """
    Generic instrumentation pipeline:
    - Index repo
    - Select targets
    - Ask LLM to produce instrumentation plan (patches + baseline capture commands)
    - Apply patches
    - Run baseline capture to produce artifacts/baseline_metrics.json
    """

    def __init__(
        self,
        *,
        repo_root: str,
        artifact_dir: str,
        planner_callback: Callable,
        enable_metric_source_heuristics: bool = False,
        max_attempts: int = 2,
        metrics_phase_env_var: str = "REMOROO_METRICS_PHASE",
        venv_python: Optional[str] = None,
    ):
        self.repo_root = repo_root
        self.artifact_dir = artifact_dir
        self.planner_callback = planner_callback
        self.enable_metric_source_heuristics = bool(enable_metric_source_heuristics)
        self.max_attempts = max(1, int(max_attempts))
        self.metrics_phase_env_var = metrics_phase_env_var or "REMOROO_METRICS_PHASE"
        self.venv_python = venv_python

    def _inject_monitor(self) -> None:
        """Inject the monitor.py helper into the repo."""
        try:
            # Locate our internal monitor.py
            # Expected location: ../runtime/monitor.py relative to this file (remoroo_offline/execution/...)
            current_dir = os.path.dirname(os.path.abspath(__file__))
            monitor_src = os.path.join(current_dir, "..", "runtime", "monitor.py")
            
            if not os.path.exists(monitor_src):
                # Fallback: maybe we're in a flat layout or installed differently
                # Try relative to package root if structure is different
                monitor_src = os.path.join(current_dir, "runtime", "monitor.py")
            
            if os.path.exists(monitor_src):
                monitor_content = _read_file_text(monitor_src)
                target_path = os.path.join(self.repo_root, "remoroo_monitor.py")
                
                # Check if file exists and differs to avoid unnecessary writes
                needs_write = True
                if os.path.exists(target_path):
                    if _read_file_text(target_path) == monitor_content:
                        needs_write = False
                
                if needs_write:
                    with open(target_path, "w", encoding="utf-8") as f:
                        f.write(monitor_content)
        except Exception:
            # Non-fatal: if injection fails, LLM might still struggle but we proceed
            pass


    def run(
        self,
        *,
        original_goal: str,
        original_metric: str,
        experiment_contract: Dict[str, Any],
        models: Dict[str, str],
    ) -> InstrumentationResult:
        _safe_mkdir(os.path.join(self.repo_root, "artifacts"))

        metric_specs = experiment_contract.get("metric_specs") or []
        metric_names = [m.get("name") for m in metric_specs if isinstance(m, dict) and m.get("name")]

        baseline_required, baseline_required_metrics = _compute_baseline_requirements(experiment_contract)

        # Empty repo shortcut: do NOT index, do NOT select targets, do NOT call the LLM.
        if _is_repo_empty(self.repo_root):
            now = datetime.utcnow().isoformat() + "Z"

            # Deterministic null metrics (schema-safe). Prefer explicit keys so the engine
            # can reason about missing-vs-null deterministically.
            metrics: Dict[str, Any] = {}
            metrics_with_units: Dict[str, Any] = {}
            for spec in metric_specs:
                if not isinstance(spec, dict):
                    continue
                name = spec.get("name")
                if not isinstance(name, str) or not name:
                    continue
                unit = spec.get("unit")
                unit_str = str(unit) if isinstance(unit, str) else ""
                metrics[name] = None
                metrics_with_units[name] = {
                    "value": None,
                    "unit": unit_str,
                    "source": "empty_repo_default",
                }

            baseline_obj = {
                "version": 1,
                "phase": "baseline",
                "metrics": dict(metrics),
                "metrics_with_units": dict(metrics_with_units),
                "source": "empty_repo_default",
                "created_at": now,
            }
            current_obj = {
                "version": 1,
                "phase": "current",
                "metrics": dict(metrics),
                "metrics_with_units": dict(metrics_with_units),
                "source": "empty_repo_default",
                "created_at": now,
            }
            merged_obj = {
                "metrics": dict(metrics),
                "baseline_metrics": dict(metrics),
                "source": "empty_repo_default",
                "extracted_at": now,
                "phase": "current",
            }

            # Optional manifest (useful for downstream guardrails even in empty repos).
            manifest = {
                "version": 1,
                "protected_files": [],
                "sentinel_markers": [],
                "metrics_phase_env_var": self.metrics_phase_env_var,
                "artifact_paths": {
                    "baseline": "artifacts/baseline_metrics.json",
                    "current": "artifacts/current_metrics.json",
                    "merged": "artifacts/metrics.json",
                },
                "notes": "Empty repo shortcut: emitted null metrics without indexing or LLM instrumentation.",
            }

            # Repo artifacts (canonical for engine).
            _write_json(os.path.join(self.repo_root, "artifacts", "baseline_metrics.json"), baseline_obj)
            _write_json(os.path.join(self.repo_root, "artifacts", "current_metrics.json"), current_obj)
            _write_json(os.path.join(self.repo_root, "artifacts", "metrics.json"), merged_obj)
            _write_json(os.path.join(self.repo_root, "artifacts", "instrumentation_manifest.json"), manifest)

            # Run artifact snapshots (debug/audit).
            _write_json(os.path.join(self.artifact_dir, "baseline_metrics.json"), baseline_obj)
            _write_json(os.path.join(self.artifact_dir, "instrumentation_current_merged_metrics.json"), merged_obj)
            _write_json(os.path.join(self.artifact_dir, "instrumentation_manifest.json"), manifest)

            return InstrumentationResult(
                success=True,
                instrumentation_plan=None,
                instrumentation_manifest=manifest,
                baseline_metrics=dict(metrics),
                attempts=0,
            )

        # Build/ensure repo index (written into repo_root/.remoroo/)
        repo_index = RepoIndexer(self.repo_root).index(force=False)
        repo_index_summary = _summarize_repo_index(repo_index)

        command_plan = experiment_contract.get("command_plan") or {}
        commands_flat: List[str] = _flatten_command_plan(command_plan)

        # Select instrumentation targets based on command plan + metrics

        targets = select_instrumentation_targets(
            repo_root=self.repo_root,
            commands=commands_flat,
            metric_names=[m for m in metric_names if isinstance(m, str)],
            select_top_n=5,
        )

        # Promote top-K files and fully hydrate them (instrumentation input files).
        promoted_files = (targets.get("selected_files") or [])[:5]
        instrumentation_files_state: Dict[str, Any] = {}
        file_access_tracker = FileAccessTracker()
        for fp in promoted_files:
            if not isinstance(fp, str) or not fp:
                continue
            abs_path = os.path.join(self.repo_root, fp)
            if not os.path.exists(abs_path) or not os.path.isfile(abs_path):
                # If file doesn't exist, it cannot be an instrumentation input file.
                continue
            try:
                instrumentation_files_state[fp] = {
                    "exists": True,
                    "content": _read_file_text(abs_path),
                    "issues": [],
                    "syntax_errors": [],
                }
                file_access_tracker.mark_full(fp)
            except Exception:
                continue

        # Hydrate snippets (supplemental context only; NOT the instrumentation input surface).
        hydrated_snippets: List[Dict[str, Any]] = []
        for req in (targets.get("snippet_requests") or [])[:12]:
            fp = req.get("file_path")
            if not isinstance(fp, str) or not fp:
                continue
            hydrated_snippets.append(
                {
                    "file_path": fp,
                    "reason": req.get("reason", ""),
                    "content": _extract_snippet(self.repo_root, fp, req.get("line_start"), req.get("line_end")),
                }
            )

        # Optional heuristic suggestions (suggest-only). v1 keeps this minimal and generic.
        suggested_metric_sources: List[str] = []
        if self.enable_metric_source_heuristics:
            # Suggest-only: do not enforce. Keep generic tokens based on metric names/units.
            for spec in metric_specs:
                if not isinstance(spec, dict):
                    continue
                name = (spec.get("name") or "").strip()
                unit = (spec.get("unit") or "").strip().lower()
                if not name:
                    continue
                # Only generic hints; not domain-specific.
                if unit in ("s", "sec", "second", "seconds", "ms", "millisecond", "milliseconds"):
                    suggested_metric_sources.append(f"wrapper_runtime:{name}")
                if unit in ("b", "byte", "bytes", "kb", "mb", "gb", "kilobytes", "megabytes", "gigabytes"):
                    suggested_metric_sources.append(f"wrapper_memory:{name}")

        # Retry loop (bounded)
        last_error: Optional[str] = None
        for attempt in range(1, self.max_attempts + 1):
            plan = self.planner_callback(
                repo_root=self.repo_root,
                original_goal=original_goal,
                original_metric=original_metric,
                experiment_contract=experiment_contract,
                repo_index_summary=repo_index_summary,
                instrumentation_targets=targets,
                instrumentation_files_state=instrumentation_files_state,
                hydrated_snippets=hydrated_snippets,
                baseline_required=baseline_required,
                suggested_metric_sources=suggested_metric_sources,
                metrics_phase_env_var=self.metrics_phase_env_var,
                models=models,
                attempt=attempt
            )

            # Persist plan for debugging
            _write_json(os.path.join(self.artifact_dir, f"instrumentation_plan_attempt{attempt}.json"), plan)

            # Inject monitor helper
            self._inject_monitor()

            patch = plan.get("patch_proposal") or {}
            try:
                apply_patchproposal(self.repo_root, patch, file_access_tracker=file_access_tracker)
            except Exception as e:
                last_error = f"apply_patchproposal failed: {type(e).__name__}: {e}"
                _write_json(os.path.join(self.artifact_dir, f"instrumentation_apply_error_attempt{attempt}.json"), {"error": last_error})
                continue

            # Persist instrumentation manifest into repo_root/artifacts/ so Engine+Patcher can see it
            manifest = plan.get("instrumentation_manifest") or {}
            try:
                _write_json(os.path.join(self.repo_root, "artifacts", "instrumentation_manifest.json"), manifest)
            except Exception:
                # Non-fatal, but we still want it in artifact_dir
                pass
            _write_json(os.path.join(self.artifact_dir, "instrumentation_manifest.json"), manifest)

            # Baseline capture
            baseline_capture = plan.get("baseline_capture") or {}
            # Hard rule: baseline capture must run the SAME commands the engine will run.
            # However, the Instrumentation LLM is the expert here. If it provides a baseline_capture command list,
            # that represents the "Instrumented Plan" and should supercede the initial guess.
            llm_commands = baseline_capture.get("commands") or []
            if llm_commands:
                # LLM provided specific commands - prioritize them
                cmds = [c for c in llm_commands if isinstance(c, str) and c.strip()]
                
                # Update the ExperimentContract to match this new plan
                # This ensures the Engine adopts the separate instrumentation strategy (e.g. 2 commands)
                if isinstance(experiment_contract, dict):
                    # Create a standard stage name for instrumentation
                    experiment_contract["command_plan"] = {
                        "metrics_capture": cmds
                    }
                    if hasattr(self, "_log"):  # Optional logging if available
                         self._log(f"  📝 Updated ExperimentContract.command_plan to match instrumentation: {len(cmds)} commands")
            else:
                # Fallback to initial plan if LLM didn't specify commands (unlikely given schema)
                cmds = commands_flat[:] 
                
            timeout_s = min(baseline_capture.get("timeout_s") * 2 , 3600)
            
            env_list = baseline_capture.get("env") or []
            env: Dict[str, str] = {}
            if isinstance(env_list, list):
                for kv in env_list:
                    if isinstance(kv, dict) and kv.get("name") and kv.get("value") is not None:
                        env[str(kv["name"])] = str(kv["value"])

            # Prepare environment with baseline phase
            baseline_env = dict(env)
            baseline_env[self.metrics_phase_env_var] = "baseline"
            
            # CRITICAL: Inject REMOROO_ARTIFACTS_DIR so instrumented code can find it without guessing
            repo_artifacts_dir = os.path.join(self.repo_root, "artifacts")
            baseline_env["REMOROO_ARTIFACTS_DIR"] = repo_artifacts_dir
            
            # Execute BASELINE capture
            final_cmds = []
            if self.venv_python:
                # Rewrite calls to use the venv python if available
                venv_bin_dir = os.path.dirname(self.venv_python)
                current_path = baseline_env.get("PATH", os.environ.get("PATH", ""))
                baseline_env["PATH"] = f"{venv_bin_dir}:{current_path}"
                
                for cmd in cmds:
                    # Naively replace 'python' with venv_python
                    # A robust solution needs full shlex parsing, but this works for standard 'python script.py'
                    if cmd.strip().startswith("python "):
                        final_cmds.append(cmd.replace("python ", self.venv_python + " ", 1))
                    elif cmd.strip().startswith("python3 "):
                        final_cmds.append(cmd.replace("python3 ", self.venv_python + " ", 1))
                    else:
                        final_cmds.append(cmd)
            else:
                final_cmds = cmds

            baseline_outcomes = run_commands(
                repo_root=self.repo_root,
                commands=final_cmds,
                timeout_s=float(timeout_s) if isinstance(timeout_s, (int, float)) else None,
                stage_name="INSTRUMENTATION_BASELINE",
                env=baseline_env,
            )
            _write_json(os.path.join(self.artifact_dir, f"instrumentation_baseline_outcomes_attempt{attempt}.json"), baseline_outcomes)

            # PHASE 1 MERGE: Collect partial artifacts from baseline run
            repo_artifacts_dir = os.path.join(self.repo_root, "artifacts")
            partial_metrics = _collect_and_merge_partial_artifacts(repo_artifacts_dir)
            
            # If we found partial metrics, merge them into the main baseline_metrics.json
            # This handles the case where the LLM used our injected monitor.
            if partial_metrics:
                baseline_path = os.path.join(self.repo_root, "artifacts", "baseline_metrics.json")
                existing_baseline = _read_json(baseline_path) or {}
                
                # Ensure structure
                if "metrics" not in existing_baseline:
                    existing_baseline["metrics"] = {}
                
                # Update metrics
                existing_baseline["metrics"].update(partial_metrics)
                
                # Write back canonical artifact
                _write_json(baseline_path, existing_baseline)


            # Validate artifacts existence + schema (best-effort)
            baseline_path = os.path.join(self.repo_root, "artifacts", "baseline_metrics.json")
            merged_path = os.path.join(self.repo_root, "artifacts", "metrics.json")
            baseline_obj = _read_json(baseline_path)
            merged_obj = _read_json(merged_path)

            if baseline_obj:
                try:
                    jsonschema_validate(baseline_obj, BASELINE_METRICS_ARTIFACT_SCHEMA)
                except Exception:
                    # best-effort: allow older/partial formats
                    pass
            if merged_obj:
                try:
                    jsonschema_validate(merged_obj, MERGED_METRICS_ARTIFACT_SCHEMA)
                except Exception:
                    pass

            baseline_metrics_dict = None
            if isinstance(baseline_obj, dict):
                baseline_metrics_dict = unwrap_metrics_dict(baseline_obj)

            # Required gate: if baseline required, ensure required baseline metrics exist
            if baseline_required:
                print(f"DEBUG: baseline_required=True, baseline_path={baseline_path}")
                print(f"DEBUG: baseline_obj={json.dumps(baseline_obj, default=str)}")
                print(f"DEBUG: baseline_metrics_dict={json.dumps(baseline_metrics_dict, default=str)}")
                
                missing = []
                for m in baseline_required_metrics:
                    if not baseline_metrics_dict or m not in baseline_metrics_dict:
                        missing.append(m)
                if missing:
                    print(f"DEBUG: MISSING metrics: {missing}")
                    last_error = f"Baseline required metrics missing after instrumentation: {missing}"
                    _write_json(
                        os.path.join(self.artifact_dir, f"instrumentation_missing_baseline_attempt{attempt}.json"),
                        {"missing": missing, "baseline_path": baseline_path, "merged_path": merged_path},
                    )
                    continue

            # Preflight CURRENT capture: ensure the *same* commands update current metrics artifacts.
            # This prevents entering Engine.run with stale metrics (common failure mode).
            current_path = os.path.join(self.repo_root, "artifacts", "current_metrics.json")
            pre_mtime_metrics = None
            pre_mtime_current = None
            try:
                if os.path.exists(merged_path):
                    pre_mtime_metrics = os.path.getmtime(merged_path)
            except Exception:
                pre_mtime_metrics = None
            try:
                if os.path.exists(current_path):
                    pre_mtime_current = os.path.getmtime(current_path)
            except Exception:
                pre_mtime_current = None

            current_env = dict(env)
            current_env[self.metrics_phase_env_var] = "current"
            
            # CRITICAL: Inject REMOROO_ARTIFACTS_DIR so instrumented code can find it without guessing
            # Note: repo_artifacts_dir is calculated above (approx line 434)
            current_env["REMOROO_ARTIFACTS_DIR"] = repo_artifacts_dir
            
            # Use same finalized commands (with venv python if applicable)
            current_outcomes = run_commands(
                repo_root=self.repo_root,
                commands=final_cmds,
                timeout_s=float(timeout_s) if isinstance(timeout_s, (int, float)) else None,
                stage_name="INSTRUMENTATION_CURRENT",
                env=current_env,
            )
            _write_json(os.path.join(self.artifact_dir, f"instrumentation_current_outcomes_attempt{attempt}.json"), current_outcomes)

            # PHASE 2 MERGE: Collect partial artifacts from current run
            partial_metrics_current = _collect_and_merge_partial_artifacts(repo_artifacts_dir)
            
            # If we found partial metrics, update current_metrics.json and metrics.json
            if partial_metrics_current:
                current_path = os.path.join(self.repo_root, "artifacts", "current_metrics.json")
                merged_path = os.path.join(self.repo_root, "artifacts", "metrics.json")
                
                # Update current
                existing_current = _read_json(current_path) or {}
                if "metrics" not in existing_current:
                    existing_current["metrics"] = {}
                existing_current["metrics"].update(partial_metrics_current)
                _write_json(current_path, existing_current)
                
                # Update merged
                existing_merged = _read_json(merged_path) or {}
                if "metrics" not in existing_merged:
                    existing_merged["metrics"] = {}
                existing_merged["metrics"].update(partial_metrics_current)
                _write_json(merged_path, existing_merged)


            post_mtime_metrics = None
            post_mtime_current = None
            try:
                if os.path.exists(merged_path):
                    post_mtime_metrics = os.path.getmtime(merged_path)
            except Exception:
                post_mtime_metrics = None
            try:
                if os.path.exists(current_path):
                    post_mtime_current = os.path.getmtime(current_path)
            except Exception:
                post_mtime_current = None

            # Read current metrics from repo artifacts (prefer merged metrics.json, fallback to current_metrics.json).
            current_obj = _read_json(merged_path) or _read_json(current_path)
            current_metrics_dict = unwrap_metrics_dict(current_obj) if isinstance(current_obj, dict) else {}

            required_current = [
                s.get("name")
                for s in metric_specs
                if isinstance(s, dict) and s.get("required") and isinstance(s.get("name"), str)
            ]
            missing_current = [m for m in required_current if m and m not in (current_metrics_dict or {})]

            metrics_stale = False
            if post_mtime_metrics is not None and pre_mtime_metrics is not None and post_mtime_metrics <= pre_mtime_metrics:
                metrics_stale = True
            if post_mtime_current is not None and pre_mtime_current is not None and post_mtime_current <= pre_mtime_current:
                metrics_stale = True

            if missing_current or metrics_stale:
                last_error = (
                    f"Instrumentation preflight failed: current metrics stale={metrics_stale}, "
                    f"missing_current={missing_current}"
                )
                _write_json(
                    os.path.join(self.artifact_dir, f"instrumentation_preflight_current_failed_attempt{attempt}.json"),
                    {
                        "stale": metrics_stale,
                        "missing_current": missing_current,
                        "required_current": required_current,
                        "merged_path": merged_path,
                        "current_path": current_path,
                        "pre_mtime_metrics": pre_mtime_metrics,
                        "post_mtime_metrics": post_mtime_metrics,
                        "pre_mtime_current": pre_mtime_current,
                        "post_mtime_current": post_mtime_current,
                    },
                )
                continue

            # Snapshot baseline artifacts into run artifact_dir
            if baseline_obj:
                _write_json(os.path.join(self.artifact_dir, "baseline_metrics.json"), baseline_obj)
            if merged_obj:
                # NOTE: This is a BASELINE snapshot of repo_root/artifacts/metrics.json taken during instrumentation.
                # Do not name it "metrics.json" in the run folder (ambiguous with per-turn current metrics).
                _write_json(os.path.join(self.artifact_dir, "instrumentation_baseline_merged_metrics.json"), merged_obj)
            # Snapshot current artifacts too (debug/audit)
            if current_obj:
                _write_json(os.path.join(self.artifact_dir, "instrumentation_current_merged_metrics.json"), current_obj)

            return InstrumentationResult(
                success=True,
                instrumentation_plan=plan,
                instrumentation_manifest=manifest if isinstance(manifest, dict) else None,
                baseline_metrics=baseline_metrics_dict,
                attempts=attempt,
            )

        return InstrumentationResult(success=False, error=last_error, attempts=self.max_attempts)

