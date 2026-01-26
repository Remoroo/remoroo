from pathlib import Path
import typer

def prompt_goal() -> str:
    from prompt_toolkit import prompt
    from prompt_toolkit.formatted_text import HTML
    
    typer.echo("")
    typer.secho("🎯 What is the goal you want to achieve?", fg=typer.colors.CYAN, bold=True)
    
    while True:
        # User requested "regular enter should confirm", so we disable multiline
        goal = prompt(HTML("<b>> </b>"))
        goal = (goal or "").strip()
        if goal:
            return goal
        typer.secho("Goal is required. Please enter a goal.", fg=typer.colors.RED)

def prompt_metrics() -> list[str]:
    from prompt_toolkit import prompt
    from prompt_toolkit.formatted_text import HTML
    
    typer.echo("")
    typer.secho("📈 What metric(s) should improve?", fg=typer.colors.CYAN, bold=True)
    typer.echo("(Enter one per line, empty line to finish)")
    metrics: list[str] = []
    while True:
        m = prompt(HTML("<b>> </b>"))
        m = (m or "").strip()
        if not m:
            if metrics:
                return metrics
            typer.secho("At least one metric is required.", fg=typer.colors.RED)
            continue
        metrics.append(m)

def confirm_run(repo_path: Path, goal: str, metrics: list[str], mode: str) -> bool:
    from prompt_toolkit import prompt
    from prompt_toolkit.formatted_text import HTML
    
    typer.echo("")
    typer.echo(f"Repository: {repo_path}")
    typer.echo(f"Goal: {goal}")
    typer.echo(f"Metrics: {', '.join(metrics)}")
    typer.echo(f"Mode: {mode}")
    typer.echo("")
    
    while True:
        resp = prompt(HTML("<b>Proceed? [Y/n]: </b>")).strip().lower()
        if not resp or resp == 'y':
            return True
        if resp == 'n':
            return False
        typer.secho("Please enter 'y' or 'n'.", fg=typer.colors.RED)
