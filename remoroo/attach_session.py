"""Attach local worker using only run id — goal/metrics/repo from control plane when possible."""
from __future__ import annotations

import os
from pathlib import Path
from typing import FrozenSet, Optional

import requests
import typer

from .paths import resolve_out_dir, resolve_repo_path

ATTACHABLE: FrozenSet[str] = frozenset({"PENDING", "RUNNING", "PAUSED"})


def _headers() -> tuple[str, dict]:
    from .configs import get_api_url

    api = get_api_url().rstrip("/")
    key = os.getenv("REMOROO_API_KEY")
    if not key:
        from .auth import _client

        if _client.is_authenticated():
            key = _client.get_token()
    if not key:
        typer.secho("Set REMOROO_API_KEY or run `remoroo login`.", fg=typer.colors.RED)
        raise typer.Exit(1)
    return api, {"Authorization": f"Bearer {key}"}


def attach_to_run(
    *,
    run_id: str,
    repo: Optional[Path],
    out: Optional[Path],
    engine: Optional[str],
    in_place: bool,
    cache_env: bool,
    agentic: bool,
    v2: bool,
    yes: bool,
    no_patch: bool,
    verbose: bool,
    brain_url: Optional[str],
) -> None:
    from .configs import get_default_engine
    from .engine.utils.doctor import ensure_ready
    from .auth import ensure_logged_in
    from .run_local import run_local_worker
    from .run_summary import display_local_run_result

    ensure_ready()
    ensure_logged_in()

    api, headers = _headers()
    if brain_url:
        api = brain_url.rstrip("/")

    try:
        r = requests.get(f"{api}/runs/{run_id}", headers=headers, timeout=30.0)
        if r.status_code == 404:
            typer.secho(f"Run not found: {run_id}", fg=typer.colors.RED)
            raise typer.Exit(1)
        if r.status_code in (401, 403):
            typer.secho("Authentication failed.", fg=typer.colors.RED)
            raise typer.Exit(1)
        r.raise_for_status()
    except requests.RequestException as e:
        typer.secho(f"Failed to fetch run: {e}", fg=typer.colors.RED)
        raise typer.Exit(1)

    body = r.json()
    info = body.get("run") or {}
    st = str(info.get("status", "")).upper()
    if st not in ATTACHABLE:
        typer.secho(
            f"Run {run_id} is not attachable (status={st}). "
            f"Use a run in {', '.join(sorted(ATTACHABLE))}.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)

    goal = str(info.get("goal") or "")
    mstr = str(info.get("metrics") or "")
    metrics_list = [x.strip() for x in mstr.split(",") if x.strip()]
    server_repo = str(info.get("repo_path") or "").strip()

    if repo is not None:
        repo_path = resolve_repo_path(repo)
    else:
        if not server_repo or server_repo.startswith(("http://", "https://")):
            typer.secho(
                "This run has no local repo_path (or it is a URL). "
                "Pass --repo /path/to/your/checkout",
                fg=typer.colors.RED,
            )
            raise typer.Exit(1)
        cand = Path(server_repo).expanduser()
        if not cand.is_absolute():
            cand = Path.cwd() / cand
        cand = cand.resolve()
        if not cand.is_dir():
            typer.secho(
                f"Server repo path is not a directory on this machine:\n  {cand}\n"
                f"Pass --repo to point at your local clone.",
                fg=typer.colors.RED,
            )
            raise typer.Exit(1)
        repo_path = cand

    out_dir = resolve_out_dir(out, repo_path)
    eng = engine or get_default_engine()
    if eng not in ("docker", "venv"):
        typer.secho(f"Invalid engine '{eng}'.", fg=typer.colors.RED)
        raise typer.Exit(1)

    if not yes:
        if not typer.confirm(f"Attach to run {run_id} ({st}) at {repo_path}?", default=True):
            raise typer.Exit(0)

    typer.secho(f"\nAttaching to {run_id}…", fg=typer.colors.BLUE)
    if goal:
        typer.secho(f"  Goal: {goal[:120]}{'…' if len(goal) > 120 else ''}", fg=typer.colors.CYAN)

    try:
        result = run_local_worker(
            run_id=run_id,
            repo_path=repo_path,
            out_dir=out_dir,
            goal=goal,
            metrics=metrics_list,
            brain_url=api,
            engine=eng,
            verbose=verbose,
            cache_env=cache_env,
            in_place=in_place,
            agentic=agentic,
            engine_version="v2" if v2 else "v1",
            model=None,
            resume_run_id=run_id,
        )
        display_local_run_result(
            result,
            repo_path,
            verbose=verbose,
            no_patch=no_patch,
            yes=yes,
        )
    except typer.Exit:
        raise
    except Exception as e:
        typer.secho(f"Attach failed: {e}", fg=typer.colors.RED)
        if verbose:
            import traceback

            traceback.print_exc()
        raise typer.Exit(1)
