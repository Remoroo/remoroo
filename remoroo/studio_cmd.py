"""``remoroo studio`` — open the Studio standalone, with NO agent run.

Use it to inspect a cell, view its artifacts (model, calibration, world, capture),
and keep editing the robot model — independent of a setup run. Handy for review,
fixing the model offline, or showing a finished cell. For the guided, agent-driven
build use ``remoroo setup``; this is the viewer/editor.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer


def studio(
    path: Optional[Path] = typer.Argument(
        None, help="Project directory that holds remoroo_cell/ (default: current directory)."
    ),
    port: int = typer.Option(7777, "--port", help="Port to serve the Studio on."),
    edge: bool = typer.Option(
        False, "--edge", help="Also start the edge so a connected robot/camera streams live."
    ),
    edge_url: str = typer.Option(
        "", "--edge-url", help="Use an existing edge URL instead of spawning one."
    ),
) -> None:
    """Open the Studio standalone (no agent run) to inspect/edit a cell."""
    from .studio_launch import serve_studio

    project_dir = Path(path).resolve() if path else Path.cwd()
    typer.secho(
        f"🎛  Remoroo Studio (viewer / editor) — project {project_dir}\n"
        "    No agent run: edit the model, inspect artifacts. Use `remoroo setup` "
        "for the guided build.",
        fg=typer.colors.CYAN,
    )
    ok = serve_studio(project_dir, port=port, edge_url=edge_url, spawn_edge=edge)
    raise typer.Exit(code=0 if ok else 1)
