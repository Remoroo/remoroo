"""`remoroo edge {start|stop|restart|status|logs}` — manage the edge service.

The edge runs the agent's AUTHORED cell code on the robot. The remoroo AGENT drives these
through its ordinary command tool (so it can reset/restart the edge after fixing its code —
a fresh process loads the fix), and the operator can run them by hand. All four share the one
pidfile-managed lifecycle in `edge_service`.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from . import edge_service

edge_app = typer.Typer(no_args_is_help=True, help="Manage the edge service (the robot-side process that runs the cell's authored code + cuRobo).")


def _project(project: Optional[str]) -> Path:
    return Path(project).expanduser().resolve() if project else Path.cwd().resolve()


@edge_app.command("start")
def start(
    project: Optional[str] = typer.Option(None, "--project", "-p", help="Project dir (defaults to cwd)."),
    port: Optional[str] = typer.Option(None, "--port", help="Edge port (default 7779 / locked config)."),
):
    """Start the edge as a managed daemon (idempotent — a healthy edge is left running)."""
    res = edge_service.start(_project(project), echo=typer.echo, port=port)
    raise typer.Exit(code=0 if res.get("ok") else 1)


@edge_app.command("stop")
def stop(project: Optional[str] = typer.Option(None, "--project", "-p", help="Project dir (defaults to cwd).")):
    """Stop the edge (e-stops motion first, then terminates). Stays stopped."""
    edge_service.stop(_project(project), echo=typer.echo)


@edge_app.command("restart")
def restart(project: Optional[str] = typer.Option(None, "--project", "-p", help="Project dir (defaults to cwd).")):
    """Restart the edge — the way to pick up a fix to the cell's authored code (a fresh process
    drops the cached remoroo_cell.* modules + bridge + cuRobo state)."""
    res = edge_service.restart(_project(project), echo=typer.echo)
    raise typer.Exit(code=0 if res.get("ok") else 1)


@edge_app.command("status")
def status(project: Optional[str] = typer.Option(None, "--project", "-p", help="Project dir (defaults to cwd).")):
    """Show whether the edge is running + healthy, its pid/port/interpreter, and the log tail."""
    st = edge_service.status(_project(project))
    state = "running" if st["running"] else "stopped"
    health = "healthy" if st["healthy"] else ("no /health" if st["running"] else "—")
    typer.echo(f"edge: {state} ({health})  pid={st['pid']}  port={st['port']}  url={st['url']}")
    if st.get("interpreter"):
        typer.echo(f"  interpreter: {st['interpreter']}")
    typer.echo(f"  log: {st['log']}")
    for ln in st.get("last_log") or []:
        typer.echo(f"  | {ln}")


@edge_app.command("logs")
def logs(
    project: Optional[str] = typer.Option(None, "--project", "-p", help="Project dir (defaults to cwd)."),
    tail: int = typer.Option(80, "--tail", "-n", help="Number of trailing lines to show."),
):
    """Print the tail of the edge log (the full stdout+stderr of the edge — where authored-code
    tracebacks land)."""
    lines = edge_service.tail(_project(project), n=tail)
    if not lines:
        typer.echo("(edge log is empty or not present yet)")
        return
    for ln in lines:
        typer.echo(ln)
