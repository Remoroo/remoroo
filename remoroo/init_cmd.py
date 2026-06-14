"""``remoroo init`` — bootstrap a program.md for the current repo.

Thin wrapper that does what ``remoroo run --local --goal
@bootstrap_program_md`` does, but with an obvious name. The brain
diagnoses the codebase, picks the closest example from its seed
catalog, asks gap-filling questions, and commits ``program.md``. No
experiment loop is started.

Why a dedicated subcommand:
    ``remoroo run --goal @bootstrap_program_md`` works but leaks an
    internal alias name into the customer-facing flow. ``init`` matches
    the convention every developer already knows from ``npm init`` /
    ``git init`` / ``cargo init`` / ``terraform init`` — "set this
    directory up for the tool to manage." The goal alias is still
    resolved by the brain; ``init`` is just the friendly handle.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer


def init(
    repo: Path = typer.Option(
        Path("."),
        "--repo",
        help="Repository to bootstrap. Defaults to the current directory.",
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
        0.5,
        "--budget",
        help=(
            "Time budget in hours for the bootstrap run. Defaults to 0.5 "
            "(30 min) — bootstrap is short, no experiment loop runs."
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
    """Bootstrap a program.md for THIS repo (no experiment loop).

    Runs an interactive setup loop where the agent inspects your repo,
    picks the closest example from a built-in catalog (LM pretraining,
    vision, tabular, RL, robotics, ASR, TTS), and asks you a handful of
    gap-filling questions. Result: a single commit adding
    ``program.md`` to your repo, ready for ``remoroo run``.

    Equivalent to:
        remoroo run --local --goal "@bootstrap_program_md"
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
        "🌱  Bootstrap mode: I'll author program.md by reading your repo "
        "and asking a few questions.",
        fg=typer.colors.CYAN,
    )
    typer.echo(
        "    Staging the bundled seed catalog locally so the agent can "
        "pick the closest match.\n"
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
    # there, so a stray ``git add .`` after init can't accidentally
    # commit the staged catalog. ``prepare_local_worker_context`` will
    # also touch .gitignore later, but that runs after POST /runs — too
    # late for this directory.
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

    # Stage the bundled seed catalog INTO THE OPERATOR'S REPO before the
    # run is submitted. The CLI is the side with filesystem access; the
    # brain is never asked to write files in the operator's workspace.
    # Aligns with what `@bootstrap_program_md` declares as
    # ``cli_seed_categories=("program_md",)``.
    from .seed_staging import stage_seed_categories, SEED_STAGING_ROOT

    stage_result = stage_seed_categories(repo_path, ("program_md",))
    typer.secho(
        f"📚 Staged {len(stage_result.staged)} seeds into "
        f"{SEED_STAGING_ROOT}/program_md/  "
        f"(skipped {len(stage_result.skipped)}, "
        f"failed {len(stage_result.failed)})",
        fg=typer.colors.GREEN if not stage_result.failed else typer.colors.YELLOW,
    )
    if stage_result.failed:
        for f in stage_result.failed:
            typer.secho(f"   ⚠ {f}", fg=typer.colors.YELLOW)
    if stage_result.requested == 0:
        typer.secho(
            "❌ No seeds were bundled with this CLI build. Reinstall "
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
        goal="@bootstrap_program_md",
        metrics_list=[],
        model=model,
        resume_run_id=None,
        run_id_display=run_id,
        attach_status="",
        attach_goal_preview="",
        # Skip the metrics wizard pane: the alias's ``default_metrics``
        # is filled in server-side by ``resolve_goal``, so the operator
        # has nothing meaningful to type here. (For bootstrap, "success"
        # = "program.md committed" — not a research metric the operator
        # picks.) Setting this to True tells ``wizard_steps_needed`` to
        # skip ``WizardStep.METRICS`` even though metrics_list is empty.
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
        typer.secho(f"Bootstrap failed: {exc}", fg=typer.colors.RED)
        if verbose:
            import traceback

            traceback.print_exc()
        raise typer.Exit(code=1)
