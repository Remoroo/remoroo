"""``remoroo setup`` — stage the robotics seeds and launch ``@robot_setup``.

Thin launcher, exactly like ``remoroo init``. It owns **no** agent
instructions: the goal prompt, the phase gates, and the definition of done
live in ONE place — the ``@robot_setup`` alias in
``remoroo_brain/prompts/goal_aliases.py`` — and are resolved server-side. This
command only does the two things the brain cannot do itself:

  1. stage the bundled ``robotics`` seed catalog onto the operator's disk
     (the CLI is the side with filesystem access), and
  2. start an interactive, operator-supervised local run with ``@robot_setup``.

For the canonical, always-current description of what setup does, run
``remoroo aliases`` (served from the brain) — not this file.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer


def setup(
    repo: Path = typer.Option(
        Path("."),
        "--repo",
        help="Repository to set up for robot data collection. Defaults to the current directory.",
    ),
    yes: bool = typer.Option(
        False, "-y", "--yes", help="Skip confirmation prompts."
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the output directory."
    ),
    brain_url: Optional[str] = typer.Option(
        None, "--brain-url", help="URL of the control plane."
    ),
    engine: Optional[str] = typer.Option(
        None, "--engine", help="Execution engine ('docker' or 'venv')."
    ),
    model: Optional[str] = typer.Option(
        None, "--model", help="LLM model id (e.g. anthropic/claude-opus-4.8)."
    ),
    pick_model: bool = typer.Option(
        True,
        "--pick-model/--no-pick-model",
        help="Show the model picker before launching the TUI.",
    ),
    budget_hours: float = typer.Option(
        4.0,
        "--budget",
        help=(
            "Time budget in hours (default 4.0). Setup is interactive and "
            "resumable — re-run to continue where it left off."
        ),
    ),
    headless: bool = typer.Option(
        False,
        "--headless",
        help="Skip the Rich TUI (CI / non-tty environments).",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", help="Verbose logging."
    ),
):
    """Stage robotics seeds and launch the interactive ``@robot_setup`` run.

    Thin launcher (mirrors ``remoroo init``): the agent's instructions, phase
    gates, and definition of done are owned by the ``@robot_setup`` alias in
    the brain and resolved server-side — run ``remoroo aliases`` for the
    canonical description.

    Safety: the run is interactive and operator-supervised — every motion
    pauses for your go-ahead, so keep a hand on the E-stop.

    Equivalent to:
        remoroo run --local --goal "@robot_setup"
    """
    from .auth import ensure_logged_in
    from .configs import get_api_url, get_default_engine
    from .engine.utils.doctor import ensure_ready, resolve_execution_engine
    from .ids import new_run_id
    from .paths import resolve_repo_path, resolve_out_dir
    from .tui_launch_config import LaunchConfig, exit_code_for_result
    from .tui_unified_app import (
        echo_session_finished_line,
        run_unified_local_session,
    )

    typer.secho(
        "🤖  remoroo setup → @robot_setup (interactive, operator-supervised). "
        "Keep a hand on the E-stop; the agent pauses for your go-ahead before "
        "any motion.",
        fg=typer.colors.YELLOW,
    )
    typer.echo(
        "    Staging the bundled robotics seed catalog locally, then handing "
        "off to the agent.\n"
    )

    ensure_ready()

    if brain_url is None:
        brain_url = get_api_url()

    engine_opt = engine
    engine_resolved = (engine or get_default_engine()).lower()
    if engine_resolved not in ("docker", "venv"):
        typer.secho(
            f"❌ Invalid engine '{engine_resolved}'. Choose 'docker' or 'venv'.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)
    engine_resolved = resolve_execution_engine(
        engine_resolved,
        explicit_docker=(engine_opt is not None and engine_resolved == "docker"),
    )

    ensure_logged_in()

    repo_path = resolve_repo_path(repo)
    out_dir = resolve_out_dir(out, repo_path)
    run_id = new_run_id()
    max_wall_time_s = int(budget_hours * 3600)

    # Make sure ``.remoroo_init/`` is gitignored BEFORE we stage anything
    # there, so a stray ``git add .`` can't commit the staged catalog. The
    # cell integration the agent authors (``remoroo_cell/``) is deliberately
    # NOT ignored — it is the deliverable and gets committed.
    from .engine.core.workspace import (
        GITIGNORE_BLOCK_HEADER,
        missing_gitignore_entries,
    )

    gitignore_path = repo_path / ".gitignore"
    existing = (
        gitignore_path.read_text(encoding="utf-8", errors="replace")
        if gitignore_path.exists()
        else ""
    )
    to_add = missing_gitignore_entries(existing)
    if to_add:
        block = (
            ("\n" if existing and not existing.endswith("\n") else "")
            + GITIGNORE_BLOCK_HEADER
            + "\n"
            + "\n".join(to_add)
            + "\n"
        )
        try:
            with open(gitignore_path, "a", encoding="utf-8") as fh:
                fh.write(block)
        except OSError as exc:
            typer.secho(
                f"⚠ Could not update .gitignore ({exc}); staged seeds may "
                "not be ignored by git.",
                fg=typer.colors.YELLOW,
            )

    # Stage the bundled robotics seed catalog INTO THE OPERATOR'S REPO before
    # the run is submitted. The CLI is the side with filesystem access; the
    # brain is never asked to write files in the operator's workspace. Aligns
    # with what ``@robot_setup`` declares as
    # ``cli_seed_categories=("robotics",)``.
    from .seed_staging import stage_seed_categories, SEED_STAGING_ROOT

    stage_result = stage_seed_categories(repo_path, ("robotics",))
    typer.secho(
        f"📚 Staged {len(stage_result.staged)} robotics seeds into "
        f"{SEED_STAGING_ROOT}/robotics/  "
        f"(skipped {len(stage_result.skipped)}, "
        f"failed {len(stage_result.failed)})",
        fg=typer.colors.GREEN if not stage_result.failed else typer.colors.YELLOW,
    )
    if stage_result.failed:
        for f in stage_result.failed:
            typer.secho(f"   ⚠ {f}", fg=typer.colors.YELLOW)
    if stage_result.requested == 0:
        typer.secho(
            "❌ No robotics seeds were bundled with this CLI build. Reinstall "
            "remoroo or contact support.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)

    cfg = LaunchConfig(
        mode="new",
        repo_path=repo_path,
        out_dir=out_dir,
        brain_url=brain_url,
        engine=engine_resolved,
        verbose=verbose,
        cache_env=True,
        in_place=True,
        agentic=True,
        engine_version="v2",
        max_wall_time_s=max_wall_time_s,
        allow_overage=False,
        yes=yes,
        no_patch=False,
        pick_model=pick_model,
        goal="@robot_setup",
        metrics_list=[],
        model=model,
        resume_run_id=None,
        run_id_display=run_id,
        attach_status="",
        attach_goal_preview="",
        # Skip the metrics wizard pane: the alias's ``default_metrics`` is
        # filled in server-side by ``resolve_goal`` (the bar = autonomous
        # safe motion + gates green), so the operator has nothing meaningful
        # to type here.
        metrics_option_provided=True,
        interactive=True,
    )

    try:
        if headless:
            from .run_local import run_local_worker_headless

            lr = run_local_worker_headless(cfg)
            code = exit_code_for_result(lr.success, lr.partial_success)
            echo_session_finished_line(lr, code)
            raise typer.Exit(code=code)

        lr, code = run_unified_local_session(cfg)
        echo_session_finished_line(lr, code)
        raise typer.Exit(code=code)
    except typer.Exit:
        raise
    except Exception as exc:
        typer.secho(f"Robot setup failed: {exc}", fg=typer.colors.RED)
        if verbose:
            import traceback

            traceback.print_exc()
        raise typer.Exit(code=1)
