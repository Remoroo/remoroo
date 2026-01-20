from pathlib import Path
import typer

def prompt_goal() -> str:
    while True:
        goal = typer.prompt("What is the goal you want to achieve?")
        goal = (goal or "").strip()
        if goal:
            return goal
        typer.secho("Goal is required. Please enter a goal.", fg=typer.colors.RED)

def prompt_metrics() -> list[str]:
    typer.echo("What metric(s) should improve?")
    typer.echo("(Enter one per line, empty line to finish)")
    metrics: list[str] = []
    while True:
        m = typer.prompt(">", default="", show_default=False)
        m = (m or "").strip()
        if not m:
            if metrics:
                return metrics
            typer.secho("At least one metric is required.", fg=typer.colors.RED)
            continue
        metrics.append(m)

def confirm_run(repo_path: Path, goal: str, metrics: list[str], mode: str) -> bool:
    typer.echo("")
    typer.echo(f"Repository: {repo_path}")
    typer.echo(f"Goal: {goal}")
    typer.echo(f"Metrics: {', '.join(metrics)}")
    typer.echo(f"Mode: {mode}")
    typer.echo("")
    return bool(typer.confirm("Proceed?", default=True))
