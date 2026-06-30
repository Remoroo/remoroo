"""``remoroo studio`` — open the Studio standalone, with NO agent run.

Use it on an ALREADY-AUTHORED cell to let a human operator RE-RUN the operator
processes the agent set up — re-run the calibration pipeline, commission, realtime,
edit/re-export the model — independent of a setup run. By default it also starts the
edge (``edge_real.py``) so the Studio is connected to the live robot/camera + cuRobo
exactly like ``remoroo setup``; pass ``--no-edge`` for a no-hardware machine (pure
model viewer/editor). For the guided, agent-driven build use ``remoroo setup``.
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
        True,
        "--edge/--no-edge",
        help=(
            "Start the edge (edge_real.py) so the Studio is connected to the live "
            "robot/camera + cuRobo and the operator can RE-RUN calibration / commission "
            "/ realtime against the real rig. On by default (studio runs on the robot "
            "computer); --no-edge for a no-hardware machine (model viewer/editor only). "
            "The edge degrades gracefully and connects the bridge once primitives.py exists."
        ),
    ),
    edge_url: str = typer.Option(
        "", "--edge-url", help="Use an existing edge URL instead of spawning one."
    ),
) -> None:
    """Open the Studio standalone (no agent run) for an operator to re-run/inspect a cell."""
    from .studio_launch import serve_studio

    project_dir = Path(path).resolve() if path else Path.cwd()
    typer.secho(
        f"🎛  Remoroo Studio (operator cockpit) — project {project_dir}\n"
        "    No agent run: re-run calibration / commission / realtime and edit the model on "
        "the live rig.\n"
        f"    Edge: {'starting (connect to the live robot/camera)' if (edge and not edge_url) else (edge_url or 'off — pass --edge to connect a robot')}. "
        "Use `remoroo setup` for the guided build.",
        fg=typer.colors.CYAN,
    )
    ok = serve_studio(project_dir, port=port, edge_url=edge_url, spawn_edge=edge)
    raise typer.Exit(code=0 if ok else 1)
