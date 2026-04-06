import typer
from pathlib import Path
from typing import Optional

from .auth import ensure_logged_in
from .prompts import prompt_goal, prompt_metrics
from .ids import new_run_id
from .paths import resolve_repo_path, resolve_out_dir


from .worker_cmd import worker
app = typer.Typer(no_args_is_help=True)
app.command(name="worker")(worker)

@app.command()
def login():
    """Lock in your API Key."""
    from .auth import _client
    _client.login()

@app.command()
def logout():
    """Remove stored credentials."""
    from .auth import _client
    _client.logout()

@app.command()
def whoami():
    """Show current authentication status."""
    from .auth import _client
    _client.whoami()


@app.command("list")
def list_runs_cmd(
    limit: int = typer.Option(50, "--limit", "-n", help="Max runs to fetch (server caps at 100)."),
    attachable: bool = typer.Option(
        False,
        "--attachable",
        "-a",
        help="Only PENDING / RUNNING / PAUSED (typical attach targets).",
    ),
    status: Optional[str] = typer.Option(
        None,
        "--status",
        "-s",
        help="Filter by one status (e.g. RUNNING). Ignored if --attachable is set.",
    ),
    json_out: bool = typer.Option(False, "--json", help="Print raw JSON."),
    brain_url: Optional[str] = typer.Option(None, "--brain-url", help="Override API base URL."),
):
    """List your runs and statuses (attach with `remoroo attach --id <run_id>` or `remoroo run --local --resume`)."""
    from .list_sessions import list_sessions

    list_sessions(
        limit=limit,
        attachable=attachable,
        status=status,
        json_out=json_out,
        brain_url=brain_url,
    )


@app.command("abort")
def abort_run_cmd(
    run_id: str = typer.Argument(..., metavar="RUN_ID", help="Run id to stop on the control plane."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation."),
    brain_url: Optional[str] = typer.Option(None, "--brain-url", help="API base URL."),
):
    """Abort a run on the server (sets status FAILED). Frees attach/resume and monthly quota counting for infra aborts.

    Note: A cloud ``brain_runner`` process may keep working until that process exits or is restarted;
    this endpoint updates the database and clears pause/detach flags—it does not SIGKILL the worker VM process.
    """
    import requests

    from .list_sessions import _api_and_headers

    ensure_logged_in()
    rid = run_id.strip()
    if not rid:
        typer.secho("Run id is empty.", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    if not yes:
        if not typer.confirm(f"Abort run {rid} on the server?", default=True):
            raise typer.Exit(code=0)

    api, headers = _api_and_headers(brain_url)
    try:
        r = requests.post(f"{api}/runs/{rid}/abort", headers=headers, timeout=30.0)
        if r.status_code == 404:
            typer.secho(f"Run not found: {rid}", fg=typer.colors.RED)
            raise typer.Exit(code=1)
        if r.status_code in (401, 403):
            typer.secho("Authentication failed.", fg=typer.colors.RED)
            raise typer.Exit(code=1)
        if r.status_code == 400:
            detail = ""
            try:
                detail = r.json().get("detail", "")
            except Exception:
                detail = r.text or ""
            typer.secho(
                f"Cannot abort (run may already be finished): {detail or r.status_code}",
                fg=typer.colors.YELLOW,
            )
            raise typer.Exit(code=1)
        r.raise_for_status()
    except typer.Exit:
        raise
    except requests.RequestException as e:
        typer.secho(f"Abort request failed: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    typer.secho(f"Aborted run {rid} on the server.", fg=typer.colors.GREEN)
    typer.echo(
        "If a brain worker is still busy on AWS, restart that worker or wait for the current handle_run to finish."
    )


@app.command()
def attach(
    run_id: str = typer.Option(..., "--id", "-i", metavar="RUN_ID", help="Run id from `remoroo list`."),
    repo: Optional[Path] = typer.Option(
        None,
        "--repo",
        exists=True,
        file_okay=False,
        dir_okay=True,
        help="Local checkout; if omitted, uses server repo_path when it exists on disk.",
    ),
    out: Optional[Path] = typer.Option(None, "--out", help="Output base directory."),
    brain_url: Optional[str] = typer.Option(None, "--brain-url", help="API base URL."),
    engine: Optional[str] = typer.Option(None, "--engine", help="docker or venv (default: config)."),
    cache_env: bool = typer.Option(True, "--cache-env/--no-cache-env"),
    in_place: bool = typer.Option(True, "--in-place/--no-in-place"),
    agentic: bool = typer.Option(True, "--agentic/--no-agentic"),
    v2: bool = typer.Option(True, "--v2/--v1"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip attach and patch confirmation prompts."),
    no_patch: bool = typer.Option(False, "--no-patch", help="Never apply patch after run."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
):
    """Attach local worker to an existing run (goal/metrics loaded from server; no model picker)."""
    from .attach_session import attach_to_run

    attach_to_run(
        run_id=run_id.strip(),
        repo=repo,
        out=out,
        engine=engine,
        in_place=in_place,
        cache_env=cache_env,
        agentic=agentic,
        v2=v2,
        yes=yes,
        no_patch=no_patch,
        verbose=verbose,
        brain_url=brain_url,
    )


@app.callback()
def main():
    """
    Remoroo CLI
    """
    pass

@app.command()
def run(
    local: bool = typer.Option(False, "--local", help="Run execution on this machine (default)."),
    remote: bool = typer.Option(
        False,
        "--remote",
        help="Hosted Cloud execution (Enterprise). Not available in this CLI release; shows how to contact sales.",
    ),
    repo: Path = typer.Option(Path("."), "--repo", exists=True, file_okay=False, dir_okay=True),
    out: Path = typer.Option(None, "--out", help="Base directory for run outputs."),
    yes: bool = typer.Option(False, "--yes", help="Skip confirmation."),
    verbose: bool = typer.Option(False, "--verbose", help="Verbose output."),
    goal: str = typer.Option(None, "--goal", help="Goal of the run."),
    metrics: str = typer.Option(None, "--metrics", help="Comma-separated metrics."),
    brain_url: str = typer.Option(None, "--brain-url", help="URL of the Brain Server."),
    no_patch: bool = typer.Option(False, "--no-patch", help="Do not ask to apply patch (auto-deny)."),
    engine: str = typer.Option(None, "--engine", help="Execution engine (docker or venv). Defaults to 'docker'."),
    cache_env: bool = typer.Option(True, "--cache-env", help="Cache Docker environment (skip already-installed packages, commit changes)."),
    in_place: bool = typer.Option(True, "--in-place", help="Edit the repo directly instead of creating a temporary working copy."),
    agentic: bool = typer.Option(True, "--agentic", help="Use Conductor-driven agentic loop instead of the legacy pipeline."),
    v2: bool = typer.Option(True, "--v2/--v1", help="Use v2 agent loop (default) or legacy v1."),
    model: Optional[str] = typer.Option(None, "--model", help="v2 LLM model id (e.g. anthropic/claude-sonnet-4.5)."),
    pick_model: bool = typer.Option(True, "--pick-model", help="Full-screen picker: Haiku / Sonnet / Opus before the run TUI."),
    budget_hours: float = typer.Option(10.0, "--budget", help="Time budget in hours (default 10). Sets wall-clock and cost caps."),
    allow_overage: bool = typer.Option(False, "--allow-overage", help="Allow run to exceed credit balance (billed as overage)."),
    resume: Optional[str] = typer.Option(
        None,
        "--resume",
        metavar="RUN_ID",
        help="Attach worker to an existing run instead of creating a new one.",
    ),
):
    from .configs import get_api_url, get_default_engine
    from .engine.utils.doctor import ensure_ready
    
    # Pre-flight checks
    ensure_ready()

    if brain_url is None:
        brain_url = get_api_url()
    
    if engine is None:
        engine = get_default_engine()
    
    # Validation
    if engine not in ["docker", "venv"]:
        typer.secho(f"❌ Invalid engine '{engine}'. Choose 'docker' or 'venv'.", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    # --local wins if both flags are passed.
    if local:
        remote = False

    if remote:
        typer.secho(
            "☁️  Hosted remote runs are not available in this CLI release.",
            fg=typer.colors.YELLOW,
        )
        typer.echo("")
        typer.echo(
            "Remoroo Cloud execution is available to Enterprise customers. "
            "Visit https://remoroo.com to contact us and learn about hosted runs."
        )
        typer.echo("")
        typer.echo("Run on your machine (default):")
        typer.echo("  remoroo run")
        typer.echo("Or explicitly:")
        typer.echo("  remoroo run --local")
        raise typer.Exit(code=2)

    ensure_logged_in()

    repo_path = resolve_repo_path(repo)
    out_dir = resolve_out_dir(out, repo_path)

    run_id = resume if resume else new_run_id()
    max_wall_time_s = int(budget_hours * 3600)
    gl = (goal or "").strip() if goal else ""
    ml = [m.strip() for m in metrics.split(",") if m.strip()] if metrics else []
    metrics_option_provided = metrics is not None

    from .tui_launch_config import LaunchConfig
    from .tui_unified_app import echo_session_finished_line, run_unified_local_session

    try:
        cfg = LaunchConfig(
            mode="attach" if resume else "new",
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
            pick_model=pick_model and not resume,
            goal=gl,
            metrics_list=ml,
            model=model,
            resume_run_id=resume,
            run_id_display=run_id,
            attach_status="",
            attach_goal_preview="",
            metrics_option_provided=metrics_option_provided,
        )
        lr, code = run_unified_local_session(cfg)
        echo_session_finished_line(lr, code)
        raise typer.Exit(code=code)
    except typer.Exit:
        raise
    except Exception as e:
        typer.secho(f"Run failed with error: {e}", fg=typer.colors.RED)
        if verbose:
            import traceback
            traceback.print_exc()
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
