"""``remoroo aliases`` — list goal aliases registered on the brain.

The brain owns the canonical alias registry (see
``remoroo_brain/prompts/goal_aliases.py``). This command fetches the public
summary from ``GET /goal_aliases`` and renders it for the operator.
"""
from __future__ import annotations

import typer

from .configs import get_api_url
from .goal_aliases import fetch_aliases


def aliases(
    brain_url: str = typer.Option(
        None, "--brain-url", help="Override the control-plane URL."
    ),
):
    """List ``--goal @<name>`` aliases registered on the brain."""
    from .auth import _client

    api_url = brain_url or get_api_url()
    token = None
    try:
        token = _client.get_token()
    except Exception:
        pass

    items = fetch_aliases(api_url, token=token)
    if not items:
        typer.secho(
            "No goal aliases reachable from this brain (or you are not "
            "authenticated). Pass --brain-url or run `remoroo login`.",
            fg=typer.colors.YELLOW,
        )
        raise typer.Exit(code=1)

    typer.secho("Available goal aliases:", fg=typer.colors.CYAN, bold=True)
    typer.echo("")
    for a in items:
        name = a.get("name", "?")
        desc = a.get("description", "") or ""
        interactive = a.get("interactive", False)
        seeds = a.get("seed_files", []) or []

        typer.secho(f"  @{name}", fg=typer.colors.GREEN, bold=True, nl=False)
        if interactive:
            typer.secho("  [interactive]", fg=typer.colors.MAGENTA)
        else:
            typer.echo("")
        if desc:
            for line in desc.split("\n"):
                typer.echo(f"      {line}")
        if seeds:
            paths = ", ".join(s.get("dest_path", "?") for s in seeds)
            typer.secho(f"      seeds → {paths}", fg=typer.colors.BLUE)
        typer.echo("")

    typer.echo("Use:  remoroo run --goal @<name>")
