"""CLI: list runs on the control plane for attach / status overview."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import List, Optional, Set, Tuple

import requests
import typer
from rich.console import Console
from rich.table import Table


ATTACHABLE_STATUSES: Set[str] = {"PENDING", "RUNNING", "PAUSED"}


def _api_and_headers(brain_url: Optional[str]) -> Tuple[str, dict]:
    from .configs import get_api_url

    api = (brain_url or get_api_url()).rstrip("/")
    session_key = os.getenv("REMOROO_API_KEY")
    if not session_key:
        from .auth import _client

        if _client.is_authenticated():
            session_key = _client.get_token()
    if not session_key:
        typer.secho(
            "No API key: set REMOROO_API_KEY or run `remoroo login`.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)
    return api, {"Authorization": f"Bearer {session_key}"}


def _fmt_ts(ts: float) -> str:
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except (TypeError, ValueError, OSError):
        return "—"


def _short(s: str, n: int) -> str:
    s = (s or "").replace("\n", " ").strip()
    if len(s) <= n:
        return s
    return s[: n - 1] + "…"


def list_sessions(
    *,
    limit: int = 50,
    attachable: bool = False,
    status: Optional[str] = None,
    json_out: bool = False,
    brain_url: Optional[str] = None,
) -> None:
    from .auth import ensure_logged_in

    ensure_logged_in()

    api, headers = _api_and_headers(brain_url)

    try:
        r = requests.get(
            f"{api}/runs",
            params={"limit": min(max(limit, 1), 100)},
            headers=headers,
            timeout=30.0,
        )
        if r.status_code == 401:
            typer.secho(
                "Authentication failed (401). Check REMOROO_API_KEY or `remoroo login`.",
                fg=typer.colors.RED,
            )
            raise typer.Exit(code=1)
        r.raise_for_status()
    except requests.RequestException as e:
        typer.secho(f"Request failed: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    body = r.json()
    runs: List[dict] = body.get("runs") or []
    if not isinstance(runs, list):
        runs = []

    if attachable:
        runs = [
            x
            for x in runs
            if isinstance(x, dict) and str(x.get("status", "")).upper() in ATTACHABLE_STATUSES
        ]
    elif status:
        st = status.strip().upper()
        runs = [
            x
            for x in runs
            if isinstance(x, dict) and str(x.get("status", "")).upper() == st
        ]

    if json_out:
        typer.echo(json.dumps({"runs": runs}, indent=2))
        return

    console = Console()
    if not runs:
        console.print("[dim]No runs match the filter.[/dim]")
        if attachable:
            try:
                bs = requests.get(f"{api}/billing/summary", headers=headers, timeout=10.0)
                if bs.status_code == 200:
                    summary = bs.json()
                    active_res = summary.get("active_reservations", 0)
                    if active_res > 0:
                        console.print(
                            f"[bold yellow]Warning:[/bold yellow] {active_res} active reservation(s) found "
                            "with no attachable runs. Releasing stale reservations..."
                        )
                        try:
                            fix = requests.post(
                                f"{api}/billing/release-stale",
                                headers=headers,
                                timeout=10.0,
                            )
                            if fix.status_code == 200:
                                data = fix.json()
                                released = data.get("released", 0)
                                remaining = data.get("active_reservations", 0)
                                if released > 0:
                                    console.print(
                                        f"[bold green]Fixed:[/bold green] released {released} stale reservation(s). "
                                        "You can start a new run now."
                                    )
                                elif remaining > 0:
                                    console.print(
                                        f"[bold yellow]Note:[/bold yellow] {remaining} reservation(s) still active "
                                        "(runs may still be in progress on another worker)."
                                    )
                        except Exception:
                            console.print(
                                "[dim]Could not auto-release. Contact support if the issue persists.[/dim]"
                            )
            except Exception:
                pass
        console.print(
            "[dim]Start one: [bold]remoroo run --local --goal \"…\"[/bold]  ·  "
            "Attach: [bold]remoroo run --local --resume RUN_ID[/bold][/dim]"
        )
        return

    table = Table(title="Remoroo runs", show_lines=False, header_style="bold cyan")
    table.add_column("run_id", style="yellow", no_wrap=True)
    table.add_column("status", style="green")
    table.add_column("updated", style="dim")
    table.add_column("goal", overflow="ellipsis", max_width=36)
    table.add_column("metrics", style="dim", overflow="ellipsis", max_width=28)
    table.add_column("repo", style="dim", overflow="ellipsis", max_width=32)

    for row in runs:
        if not isinstance(row, dict):
            continue
        rid = str(row.get("run_id", ""))
        st = str(row.get("status", ""))
        up = _fmt_ts(row.get("updated_at") or row.get("created_at") or 0)
        goal = _short(str(row.get("goal", "")), 200)
        met = _short(str(row.get("metrics", "")), 120)
        repo = _short(str(row.get("repo_path", "")), 200)
        table.add_row(rid, st, up, goal, met, repo)

    console.print(table)
    console.print(
        f"[dim]{len(runs)} run(s). Attach local worker:[/] "
        f"[bold]remoroo run --local --resume <run_id>[/bold] "
        f"[dim](use same --repo / --engine / --in-place as the original run)[/]"
    )
