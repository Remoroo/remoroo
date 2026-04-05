"""Launch configuration and screen-skip rules for the unified Remoroo TUI."""
from __future__ import annotations

import sys
from dataclasses import dataclass, field, replace
from enum import Enum
from pathlib import Path
from typing import List, Optional, Tuple


class WizardStep(str, Enum):
    GOAL = "goal"
    METRICS = "metrics"
    CONFIRM_NEW = "confirm_new"
    CONFIRM_ATTACH = "confirm_attach"


@dataclass(frozen=True)
class LaunchConfig:
    """Frozen input from Typer + resolved paths; drives which screens appear."""

    mode: str  # "new" | "attach"
    repo_path: Path
    out_dir: Path
    brain_url: str
    engine: str
    verbose: bool
    cache_env: bool
    in_place: bool
    agentic: bool
    engine_version: str
    max_wall_time_s: int
    allow_overage: bool
    yes: bool
    no_patch: bool
    pick_model: bool
    goal: str = ""
    metrics_list: List[str] = field(default_factory=list)
    model: Optional[str] = None
    resume_run_id: Optional[str] = None
    run_id_display: str = ""
    attach_status: str = ""
    attach_goal_preview: str = ""
    # True if user passed --metrics on CLI (even empty); skips metrics wizard when list empty.
    metrics_option_provided: bool = False

    def is_resume(self) -> bool:
        return bool(self.resume_run_id)


def parse_metrics_option(metrics: Optional[str]) -> List[str]:
    if not metrics:
        return []
    return [m.strip() for m in metrics.split(",") if m.strip()]


def build_launch_config_for_local_run(
    *,
    mode: str,
    repo_path: Path,
    out_dir: Path,
    brain_url: str,
    engine: str,
    verbose: bool,
    cache_env: bool,
    in_place: bool,
    agentic: bool,
    v2: bool,
    max_wall_time_s: int,
    allow_overage: bool,
    yes: bool,
    no_patch: bool,
    pick_model: bool,
    goal: Optional[str],
    metrics: Optional[str],
    model: Optional[str],
    resume: Optional[str],
    run_id_display: str,
    attach_status: str = "",
    attach_goal_preview: str = "",
    metrics_option_provided: bool = False,
) -> LaunchConfig:
    """Mirror cli.py branching for goal/metrics/resume."""
    g = (goal or "").strip()
    ml = parse_metrics_option(metrics)
    if mode == "attach" or resume:
        rid = (resume or "").strip() or run_id_display
        return LaunchConfig(
            mode="attach",
            repo_path=repo_path,
            out_dir=out_dir,
            brain_url=brain_url,
            engine=engine,
            verbose=verbose,
            cache_env=cache_env,
            in_place=in_place,
            agentic=agentic,
            engine_version="v2" if v2 else "v1",
            max_wall_time_s=max_wall_time_s,
            allow_overage=allow_overage,
            yes=yes,
            no_patch=no_patch,
            pick_model=False,
            goal=g,
            metrics_list=ml,
            model=None,
            resume_run_id=rid,
            run_id_display=rid,
            attach_status=attach_status,
            attach_goal_preview=attach_goal_preview,
            metrics_option_provided=metrics_option_provided,
        )
    return LaunchConfig(
        mode="new",
        repo_path=repo_path,
        out_dir=out_dir,
        brain_url=brain_url,
        engine=engine,
        verbose=verbose,
        cache_env=cache_env,
        in_place=in_place,
        agentic=agentic,
        engine_version="v2" if v2 else "v1",
        max_wall_time_s=max_wall_time_s,
        allow_overage=allow_overage,
        yes=yes,
        no_patch=no_patch,
        pick_model=pick_model,
        goal=g,
        metrics_list=ml,
        model=model,
        resume_run_id=None,
        run_id_display=run_id_display,
        metrics_option_provided=metrics_option_provided,
    )


def wizard_steps_needed(cfg: LaunchConfig) -> Tuple[WizardStep, ...]:
    """Ordered wizard substeps before model / server prepare."""
    if cfg.mode == "attach":
        if cfg.yes:
            return ()
        return (WizardStep.CONFIRM_ATTACH,)
    steps: List[WizardStep] = []
    if not cfg.goal.strip():
        steps.append(WizardStep.GOAL)
    need_metrics = not cfg.metrics_list and not cfg.metrics_option_provided
    if need_metrics:
        steps.append(WizardStep.METRICS)
    if not cfg.yes:
        steps.append(WizardStep.CONFIRM_NEW)
    return tuple(steps)


def skip_model_screen(cfg: LaunchConfig) -> bool:
    if cfg.mode == "attach":
        return True
    if not cfg.pick_model:
        return True
    if cfg.model is not None and str(cfg.model).strip() != "":
        return True
    return False


def with_updated_goal_metrics(
    cfg: LaunchConfig,
    goal: str,
    metrics_list: List[str],
) -> LaunchConfig:
    return replace(cfg, goal=goal.strip(), metrics_list=list(metrics_list))


def with_model(cfg: LaunchConfig, model: Optional[str]) -> LaunchConfig:
    return replace(cfg, model=model)


def unified_tui_requires_tty() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def exit_code_for_result(success: bool, partial_success: bool) -> int:
    if success:
        return 0
    if partial_success:
        return 2
    return 1
