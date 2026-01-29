import typer
from pathlib import Path

from .auth import ensure_logged_in
from .prompts import prompt_goal, prompt_metrics, confirm_run
from .ids import new_run_id
from .paths import resolve_repo_path, resolve_out_dir
import re
import sys

def robust_confirm(text: str, default: bool = True) -> bool:
    """A more resilient confirmation prompt that filters out ANSI escape noise."""
    prompt = f"🚀 {text} [{'Y/n' if default else 'y/N'}]: "
    while True:
        try:
            # Clear input buffer if possible to remove stale noise
            try:
                import termios
                if sys.stdin.isatty():
                    termios.tcflush(sys.stdin, termios.TCIFLUSH)
            except Exception:
                pass

            sys.stdout.write(prompt)
            sys.stdout.flush()
            
            line = sys.stdin.readline()
            if not line:
                return default
            
            # Filter out ANSI escape sequences (like DSR responses ^[[25;1R)
            # and other control characters
            clean_line = re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', line).strip().lower()
            
            if not clean_line:
                return default
            if clean_line in ('y', 'yes'):
                return True
            if clean_line in ('n', 'no'):
                return False
            
            sys.stdout.write(f"Error: invalid input\n")
        except EOFError:
            return default
        except Exception:
            return default


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

@app.command()
def whoami():
    """Show current authentication status."""
    from .auth import _client
    _client.whoami()


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
    brain_url: str = typer.Option(None, "--brain-url", help="URL of the Brain Server."),
    engine: str = typer.Option(None, "--engine", help="Execution engine (docker or venv). Defaults to 'docker'."),
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
            engine=engine,
            verbose=verbose,
        )

        from rich.console import Console
        from rich.table import Table
        from rich.panel import Panel
        from rich.style import Style
        console = Console()

        if result.success:
            outcome_color = "green"
        elif getattr(result, 'partial_success', False):
            outcome_color = "yellow"
        elif result.outcome == "INTERRUPTED":
            outcome_color = "bright_black"
        elif "ERROR" in result.outcome or "CRASH" in result.outcome or result.outcome == "FAIL" or result.outcome == "FAILED":
            outcome_color = "red"
        else:
            outcome_color = "yellow"

        console.print("")
        console.print(Panel(
            f"[bold {outcome_color}]{result.outcome}[/bold {outcome_color}]\n"
            f"Run ID: [white]{result.run_id}[/white]\n"
            f"Artifacts: [cyan]{result.run_root}[/cyan]",
            title="[bold]Run Summary[/bold]",
            border_style=outcome_color
        ))

        # Show Metrics if available
        metrics_file = result.run_root / "metrics.json"
        baseline_file = result.run_root / "baseline_metrics.json"
        
        if metrics_file.exists():
            import json
            def clean_metrics_dict(d):
                clean = {}
                blacklist = ["created_at", "source", "version", "phase"]
                if "metrics" in d and isinstance(d["metrics"], dict):
                    for k, v in d["metrics"].items():
                        if isinstance(v, (int, float)): clean[k] = v
                if "metrics_with_units" in d and isinstance(d["metrics_with_units"], dict):
                    for k, v in d["metrics_with_units"].items():
                        if isinstance(v, dict) and "value" in v:
                            val = v["value"]
                            if isinstance(val, (int, float)): clean[k] = val
                for k, v in d.items():
                    if k in blacklist or k in ["metrics", "metrics_with_units", "baseline_metrics", "target_files"]: continue
                    if isinstance(v, (int, float)):
                        if k not in clean: clean[k] = v
                return clean

            try:
                with open(metrics_file, 'r') as f:
                    final_metrics_raw = json.load(f)
                with open(baseline_file, 'r') if baseline_file.exists() else None as bf:
                    baseline_metrics_raw = json.load(bf) if bf else {}
                
                final_metrics = clean_metrics_dict(final_metrics_raw)
                baseline_metrics = clean_metrics_dict(baseline_metrics_raw)
                '''

                if final_metrics:
                    #console.print("\n📊 [bold]Metric Comparison:[/bold]")
                    for m_name, final_val in final_metrics.items():
                        base_val = baseline_metrics.get(m_name, "N/A")
                        try:
                            f_v = float(final_val)
                            b_v = float(base_val)
                            diff = f_v - b_v
                            color = "green" if diff < 0 else "red" # Assuming lower is better for optimization
                            console.print(f"   {m_name}: [magenta]{base_val}[/magenta] (baseline) [bold]--->[/bold] [{color}]{final_val}[/{color}] (current)")
                        except:
                            console.print(f"   {m_name}: {base_val} (baseline) ---> {final_val} (current)")
                '''
                table = Table(title="\n📈 Detailed Performance", box=None)
                table.add_column("Metric", style="cyan")
                table.add_column("Baseline", justify="right", style="magenta")
                table.add_column("Final", justify="right", style="green")
                table.add_column("Progress", justify="right")

                for m_name, final_val in final_metrics.items():
                    base_val = baseline_metrics.get(m_name, "N/A")
                    progress = ""
                    try:
                        f_v = float(final_val)
                        b_v = float(base_val)
                        diff = f_v - b_v
                        color = "green" if diff < 0 else "red"
                        progress = f"[{color}]{diff:+.4f}[/{color}]"
                    except:
                        pass
                    table.add_row(m_name, str(base_val), str(final_val), progress)
                
                console.print(table)
            except Exception as e:
                if verbose:
                    console.print(f"[dim]Note: Could not parse metrics: {e}[/dim]")

        # Clickable Links
        report_path = result.run_root / "final_report.md"
        patch_path = result.run_root / "final_patch.diff"
        
        console.print("")
        if report_path.exists():
            console.print(f"📄 [bold]Report:[/bold] [link=file://{report_path.absolute()}]{report_path.name}[/link]")
        if patch_path.exists():
            console.print(f"🩹 [bold]Clean Patch:[/bold] [link=file://{patch_path.absolute()}]{patch_path.name}[/link]")
        
        # Apply Patch Prompt
        # v18: Show prompt for SUCCESS or PARTIAL_SUCCESS (anything with a patch)
        if (result.success or getattr(result, 'partial_success', False) or result.outcome == "COMPLETED") and patch_path.exists():
            console.print("")
            should_apply = yes # Initialize should_apply based on --yes flag
            if not yes:
                # Only ask for confirmation if --yes was not provided
                should_apply = typer.confirm("Would you like to apply the generated patch to your local repository?", default=True)
            
            if should_apply:
                try:
                    import subprocess
                    # Use 'git apply' if in git repo, or 'patch'
                    is_git = (repo_path / ".git").exists()
                    if is_git:
                         subprocess.run(["git", "apply", str(patch_path)], cwd=repo_path, check=True)
                    else:
                         subprocess.run(["patch", "-p1", "-i", str(patch_path)], cwd=repo_path, check=True)
                    
                    console.print("[bold green]✅ Patch applied successfully![/bold green]")
                except Exception as e:
                    console.print(f"[bold red]❌ Failed to apply patch:[/bold red] {e}")


        # Exit Codes
        if result.success:
            raise typer.Exit(code=0)
        elif result.outcome in ["PARTIAL_SUCCESS", "COMPLETED"]:
            raise typer.Exit(code=2)
        else:
            raise typer.Exit(code=1)
            
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
