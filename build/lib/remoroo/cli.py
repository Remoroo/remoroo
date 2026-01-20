import typer
from pathlib import Path

from .auth import ensure_logged_in
from .prompts import prompt_goal, prompt_metrics, confirm_run
from .ids import new_run_id
from .paths import resolve_repo_path, resolve_out_dir


from .run_local import run_local_worker

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

@app.callback()
def main():
    """
    Remoroo CLI
    """
    pass

@app.command()
def run(
    local: bool = typer.Option(False, "--local", help="Run execution on this machine (Free/Offline)."),
    remote: bool = typer.Option(True, "--remote", help="Run execution on hosted Cloud Engine (Commercial)."),
    repo: Path = typer.Option(Path("."), "--repo", exists=True, file_okay=False, dir_okay=True),
    out: Path = typer.Option(None, "--out", help="Base directory for run outputs."),
    yes: bool = typer.Option(False, "--yes", help="Skip confirmation."),
    verbose: bool = typer.Option(False, "--verbose", help="Verbose output."),
    goal: str = typer.Option(None, "--goal", help="Goal of the run."),
    metrics: str = typer.Option(None, "--metrics", help="Comma-separated metrics."),
    brain_url: str = typer.Option("http://localhost:8000", "--brain-url", help="URL of the Brain Server."),
):
    # Logic: Default is Remote. 
    # If user explicitly says --local, then remote=False. 
    # Because 'remote' defaults to True, we check if local is True.
    if local:
        remote = False

    if remote:
        from .run_remote import run_remote_experiment
        # typer.secho("Remote execution is not available yet.", fg=typer.colors.YELLOW)
        # raise typer.Exit(code=2)
        # Prepare arguments
        if not goal:
            goal = prompt_goal()
        
        metrics_list = []
        if metrics:
            metrics_list = [m.strip() for m in metrics.split(",") if m.strip()]
        if not metrics_list:
            metrics_list = prompt_metrics()

        try:
             res = run_remote_experiment(
                run_id=new_run_id(),
                repo_path=resolve_repo_path(repo),
                out_dir=resolve_out_dir(out, resolve_repo_path(repo)),
                goal=goal,
                metrics=metrics_list,
             )
             typer.echo(f"Run outcome: {res.outcome}")
             raise typer.Exit(code=0)
        except Exception as e:
             typer.echo(f"Error: {e}")
             raise typer.Exit(code=1)

    ensure_logged_in()

    repo_path = resolve_repo_path(repo)
    out_dir = resolve_out_dir(out, repo_path)

    if not goal:
        goal = prompt_goal()
    
    metrics_list = []
    if metrics:
        metrics_list = [m.strip() for m in metrics.split(",") if m.strip()]
    
    if not metrics_list:
        metrics_list = prompt_metrics()

    run_id = new_run_id()

    if not yes:
        if not confirm_run(repo_path, goal, metrics_list, mode="local"):
            raise typer.Exit(code=0)

    typer.secho(f"\nStarting run {run_id}...", fg=typer.colors.BLUE)
    
    try:
        result = run_local_worker(
            run_id=run_id,
            repo_path=repo_path,
            out_dir=out_dir,
            goal=goal,
            metrics=metrics_list,
            brain_url=brain_url,
            verbose=verbose,
        )

        if result.success:
            typer.secho("Run completed successfully.", fg=typer.colors.GREEN)
        else:
            typer.secho(f"Run finished with outcome: {result.outcome}", fg=typer.colors.YELLOW)
            
        typer.echo(f"Run ID: {result.run_id}")
        typer.echo(f"Results saved to: {result.run_root}")
        
    except Exception as e:
        typer.secho(f"Run failed with error: {e}", fg=typer.colors.RED)
        if verbose:
            import traceback
            traceback.print_exc()
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
