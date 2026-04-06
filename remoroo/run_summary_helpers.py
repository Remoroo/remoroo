"""Headless helpers for run outcome summary, metrics table, and patch apply."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from .run_local import LocalRunResult


def clean_metrics_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    clean: Dict[str, Any] = {}
    blacklist = ["created_at", "source", "version", "phase"]
    if "metrics" in d and isinstance(d["metrics"], dict):
        for k, v in d["metrics"].items():
            if isinstance(v, (int, float)):
                clean[k] = v
    if "metrics_with_units" in d and isinstance(d["metrics_with_units"], dict):
        for k, v in d["metrics_with_units"].items():
            if isinstance(v, dict) and "value" in v:
                val = v["value"]
                if isinstance(val, (int, float)):
                    clean[k] = val
    for k, v in d.items():
        if k in blacklist or k in ["metrics", "metrics_with_units", "baseline_metrics", "target_files"]:
            continue
        if isinstance(v, (int, float)) and k not in clean:
            clean[k] = v
    return clean


def load_metrics_comparison_rows(run_root: Path) -> List[Tuple[str, str, str, str]]:
    """Return rows (metric, baseline, final, progress_markup) for display."""
    metrics_file = run_root / "metrics.json"
    baseline_file = run_root / "baseline_metrics.json"
    if not metrics_file.exists():
        return []
    try:
        with open(metrics_file, "r") as f:
            final_metrics_raw = json.load(f)
        baseline_metrics_raw: Dict[str, Any] = {}
        if baseline_file.exists():
            with open(baseline_file, "r") as bf:
                baseline_metrics_raw = json.load(bf)
        final_metrics = clean_metrics_dict(final_metrics_raw)
        baseline_metrics = clean_metrics_dict(baseline_metrics_raw)
        rows: List[Tuple[str, str, str, str]] = []
        for m_name, final_val in final_metrics.items():
            base_val = baseline_metrics.get(m_name, "N/A")
            progress = ""
            try:
                f_v = float(final_val)
                b_v = float(base_val)
                diff = f_v - b_v
                sym = "-" if diff < 0 else "+"
                progress = f"{sym}{abs(diff):.4f}"
            except Exception:
                pass
            rows.append((str(m_name), str(base_val), str(final_val), progress))
        return rows
    except Exception:
        return []


def rich_progress_cell(baseline_str: str, final_str: str) -> str:
    """Colored Rich markup for delta column (lower is better → green)."""
    try:
        f_v = float(final_str)
        b_v = float(baseline_str)
        diff = f_v - b_v
        color = "green" if diff < 0 else "red"
        return f"[{color}]{diff:+.4f}[/{color}]"
    except Exception:
        return ""


def outcome_rich_border_color(result: "LocalRunResult") -> str:
    """Rich console color name for panel border (matches legacy display_local_run_result)."""
    cat = outcome_style_category(result)
    return {
        "success": "bright_green",
        "partial": "bright_yellow",
        "interrupted": "bright_black",
        "detached": "cyan",
        "error": "red",
        "warning": "bright_yellow",
    }.get(cat, "bright_yellow")


def artifact_paths(run_root: Path) -> Dict[str, Optional[Path]]:
    report = run_root / "final_report.md"
    diagram = run_root / "system_diagram.md"
    patch = run_root / "final_patch.diff"
    return {
        "report": report if report.exists() else None,
        "diagram": diagram if diagram.exists() else None,
        "patch": patch if patch.exists() else None,
    }


def apply_patch_to_repo(repo_path: Path, patch_path: Path) -> Tuple[bool, str]:
    try:
        is_git = (repo_path / ".git").exists()
        if is_git:
            subprocess.run(["git", "apply", str(patch_path)], cwd=repo_path, check=True)
        else:
            subprocess.run(["patch", "-p1", "-i", str(patch_path)], cwd=repo_path, check=True)
        return True, ""
    except Exception as e:
        return False, str(e)


def should_prompt_patch(
    result: "LocalRunResult",
    patch_exists: bool,
    *,
    no_patch: bool,
    yes: bool,
) -> bool:
    if not patch_exists:
        return False
    if not (result.success or getattr(result, "partial_success", False)):
        return False
    if no_patch:
        return False
    if yes:
        return False
    return True


def exit_code_for_local_result(result: "LocalRunResult") -> int:
    if result.success:
        return 0
    if getattr(result, "partial_success", False):
        return 2
    return 1


def outcome_style_category(result: "LocalRunResult") -> str:
    """Map to Rich/Textual style name bucket."""
    if result.success:
        return "success"
    if getattr(result, "partial_success", False):
        return "partial"
    if result.outcome == "INTERRUPTED":
        return "interrupted"
    if result.outcome == "DETACHED":
        return "detached"
    if result.outcome == "PREPARE_FAILED":
        return "error"
    if (
        "ERROR" in result.outcome
        or "CRASH" in result.outcome
        or result.outcome in ("FAIL", "FAILED", "ABORT")
    ):
        return "error"
    return "warning"
