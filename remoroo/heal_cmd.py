"""remoroo doctor / remoroo heal — the self-healing surface (doctrine 2026-07-18).

doctor answers "is this rig ready to run?" — edge up, motion health — and TRIAGES
failures to the only party that can fix each: HUMAN (physical reality: world model,
limits, cables — the agent is never woken), AGENT (`remoroo heal` launches the scoped
repair run), ENGINE (our bug; report it). heal's exit metric is the health verb
itself passing — engine-computed, the repair run cannot declare itself done.
"""
from __future__ import annotations

import json
import subprocess
import time
import urllib.request

import typer

EDGE = "http://127.0.0.1:7779"


def _verb(verb: str, body: dict | None = None, timeout: float = 30.0) -> dict:
    req = urllib.request.Request(f"{EDGE}/edge/task/{verb}",
                                 data=json.dumps(body or {}).encode(),
                                 headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def _motion_health(poll_s: float = 1800.0) -> dict:
    """Run the edge's motion_health job to completion. Healthy rigs answer in seconds;
    a failing arm pays the full ablation — many MINUTES of GPU planning — so the poll
    budget must outlast it (the 600s default once mislabeled a still-running check as
    an engine fault)."""
    r = _verb("motion_health", {"wait_s": 20}, timeout=40)
    job = r.get("job")
    t0 = time.time()
    while job and time.time() - t0 < poll_s:
        time.sleep(12)
        r = _verb("job_status", {"job": job})
        state = r.get("state")
        if state and state != "running":
            r = r.get("result") or r
            break
    else:
        if job:
            return {"error": f"health check still running after {int(poll_s)}s — "
                             f"poll job_status {{job: {job!r}}} yourself; do NOT re-issue"}
    return r


def _print_health(mh: dict) -> str:
    route = str(mh.get("route") or ("ok" if mh.get("ok") else "engine"))
    for arm, a in (mh.get("arms") or {}).items():
        color = typer.colors.GREEN if a.get("ok") else typer.colors.RED
        typer.secho(f"  {arm}: {'ok' if a.get('ok') else a.get('route','?').upper()} — "
                    f"{a.get('verdict','')}", fg=color)
    return route


def doctor() -> None:
    """Rig health with triage: who fixes what (human / remoroo heal / engine)."""
    try:
        h = json.loads(urllib.request.urlopen(f"{EDGE}/health", timeout=4).read())
        typer.secho(f"  edge: up · cell={h.get('cell', '?')}", fg=typer.colors.GREEN)
    except Exception:
        typer.secho("  edge: DOWN — start it: `remoroo edge start` (then re-run doctor)",
                    fg=typer.colors.RED)
        raise typer.Exit(1)
    typer.echo("  motion health (plan-only, no motion) ...")
    mh = _motion_health()
    if mh.get("error"):
        typer.secho(f"  motion health unavailable: {mh['error']}", fg=typer.colors.RED)
        raise typer.Exit(4)
    route = _print_health(mh)
    if mh.get("ok"):
        typer.secho("  RIG HEALTHY — runs may start.", fg=typer.colors.GREEN)
        return
    if route == "agent":
        typer.secho("  AGENT-FIXABLE → run: remoroo heal", fg=typer.colors.YELLOW)
        raise typer.Exit(2)
    if route == "human":
        typer.secho("  HUMAN NEEDED (physical: world model / limits / cabling). "
                    "The agent will NOT be woken for this.", fg=typer.colors.RED)
        raise typer.Exit(3)
    typer.secho("  ENGINE FAULT — report this output to the dev loop.", fg=typer.colors.RED)
    raise typer.Exit(4)


def heal(yes: bool = typer.Option(False, "--yes", help="Skip run confirmation.")) -> None:
    """Launch the scoped repair run for an AGENT-routed health failure; exit metric =
    motion_health passing for every arm (engine-computed)."""
    mh = _motion_health()
    if mh.get("ok"):
        typer.secho("already healthy — nothing to heal", fg=typer.colors.GREEN)
        return
    route = _print_health(mh)
    if route != "agent":
        typer.secho(f"route={route}: not agent-fixable — see doctor output above.",
                    fg=typer.colors.RED)
        raise typer.Exit(3)
    hl = mh.get("heal") or {}
    goal = "@robot_heal " + str(hl.get("goal") or mh.get("verdict") or "motion health failed")
    typer.secho("  launching heal run (exit metric: motion_health ok for every arm)",
                fg=typer.colors.YELLOW)
    cmd = ["remoroo", "run", "--goal", goal, "--headless", "--yes",
           "--metrics", str(hl.get("metric") or "motion_health ok for every arm")]
    rc = subprocess.run(cmd).returncode
    typer.echo(f"  heal run finished (rc={rc}) — re-checking health ...")
    mh2 = _motion_health()
    _print_health(mh2)
    if mh2.get("ok"):
        typer.secho("  HEALED — motion health green.", fg=typer.colors.GREEN)
    else:
        typer.secho("  still failing after the heal run — escalate to the dev loop.",
                    fg=typer.colors.RED)
        raise typer.Exit(1)
