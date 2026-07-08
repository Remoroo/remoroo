"""``remoroo task`` — the whole trigger UX (COMP-33): one command, one sentence.

    remoroo task "put the metal parts into the tray"

Launches the AUTONOMOUS ``@robot_task`` run (no operator gates; the Studio is a window,
never a gate). Re-running the same sentence resumes that task's evolution: the slug is
derived from the sentence and the artifacts live under ``remoroo_cell/task_<slug>/``.
Companions: ``remoroo task-status`` and ``remoroo task-report``.
"""
from __future__ import annotations

import hashlib
import json
import re
import urllib.request
from pathlib import Path
from typing import Optional

import typer

EDGE_PORT_DEFAULT = 7779


def slug_of(sentence: str) -> str:
    words = re.sub(r"[^a-z0-9 ]", "", sentence.lower()).split()
    stem = "_".join(words[:4]) or "task"
    return f"{stem}_{hashlib.sha1(sentence.encode()).hexdigest()[:6]}"


def _edge(verb: str, body: dict, port: int) -> dict:
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/edge/task/{verb}",
        data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode())
    except Exception as e:  # noqa: BLE001
        return {"error": f"edge unreachable on :{port} ({type(e).__name__}): "
                         "is `remoroo edge` running?"}


def _headers(client) -> dict:
    return {"Authorization": f"Bearer {client.get_token() or ''}"}


def _skill_map_path(repo_path: Path) -> Path:
    return repo_path / ".remoroo" / "task" / "skill_map.json"


def _load_skill_map(repo_path: Path) -> dict:
    try:
        return json.loads(_skill_map_path(repo_path).read_text())
    except Exception:                                   # noqa: BLE001
        return {}


def _save_skill_map(repo_path: Path, mapping: dict) -> None:
    p = _skill_map_path(repo_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(mapping, indent=1), encoding="utf-8")


def _declare_skill(client, base: str, name: str) -> dict:
    import requests

    r = requests.post(f"{base.rstrip('/')}/skills", json={"name": name},
                      headers=_headers(client), timeout=30)
    r.raise_for_status()
    return r.json()["skill"]


def _attach_skill(client, base: str, skill_id: str, slug: str) -> None:
    import requests

    requests.post(f"{base.rstrip('/')}/skills/{skill_id}/attach",
                  json={"task_slug": slug}, headers=_headers(client),
                  timeout=30).raise_for_status()


def _pick_skill(client, base: str, *, sentence: str, slug: str,
                interactive: bool) -> dict:
    """The no-typing path: existing skills come back as a menu (re-running a known task
    never asks — the slug->skill memory answers first, in the caller). Skills are free to
    declare and never metered; a paid skill bills once, on its cert."""
    import requests

    r = requests.get(f"{base.rstrip('/')}/skills", headers=_headers(client), timeout=30)
    r.raise_for_status()
    skills = r.json().get("skills", [])
    default_name = " ".join(re.sub(r"[^a-zA-Z0-9 ]", "", sentence).split()[:4]) or slug

    if not interactive:
        for s in skills:                                # exact sentence-name match wins
            if s["name"].lower() == default_name.lower():
                return s
        return _declare_skill(client, base, default_name)

    if skills:
        typer.echo("Which skill is this task part of?")
        for i, s in enumerate(skills, 1):
            typer.echo(f"  {i}. {s['name']}  [{s['status']}]")
        typer.echo("  n. new skill…")
        choice = typer.prompt("Skill (number, or n)", default="1").strip()
        if choice.lower() != "n":
            try:
                return skills[int(choice) - 1]
            except (ValueError, IndexError):
                typer.secho("No such entry; declaring a new skill.",
                            fg=typer.colors.YELLOW)
    name = typer.prompt("Name the new skill", default=default_name)
    return _declare_skill(client, base, name)


def _resolve_skill(client, base: str, repo_path: Path, *, sentence: str, slug: str,
                   flag_value: Optional[str]) -> Optional[str]:
    """Skill for this run: --skill flag > remembered slug->skill > picker menu.
    Any failure is stated and non-fatal — a task run never blocks on bookkeeping."""
    import sys

    mapping = _load_skill_map(repo_path)
    try:
        if flag_value:
            skill = _declare_skill(client, base, flag_value)
        elif slug in mapping:
            skill = mapping[slug]
        else:
            skill = _pick_skill(client, base, sentence=sentence, slug=slug,
                                interactive=sys.stdin.isatty())
        _attach_skill(client, base, skill["skill_id"], slug)
        mapping[slug] = {"skill_id": skill["skill_id"], "name": skill["name"]}
        _save_skill_map(repo_path, mapping)
        typer.secho(f"    skill: {skill['name']} ({skill['skill_id']})",
                    fg=typer.colors.YELLOW)
        return skill["skill_id"]
    except Exception as e:                              # noqa: BLE001 - stated
        typer.secho(f"    (skill tracking unavailable: {type(e).__name__}: {e} — "
                    "the run proceeds; skills reconcile server-side)",
                    fg=typer.colors.YELLOW)
        return None


def task(
    sentence: str = typer.Argument(..., help="The task, in one sentence."),
    repo: Path = typer.Option(Path("."), "--repo"),
    skill: Optional[str] = typer.Option(None, "-s", "--skill",
                                        help="The DECLARED skill this run belongs to. "
                                             "Omit it: known tasks reuse their skill "
                                             "automatically, new ones get a picker."),
    budget_trials: int = typer.Option(200, "--budget", help="Max real trials this run."),
    budget_hours: float = typer.Option(10.0, "--hours",
                                       help="Wall-clock budget in hours (our cost "
                                            "control; never billed)."),
    model: Optional[str] = typer.Option(None, "--model"),
    brain_url: Optional[str] = typer.Option(None, "--brain-url"),
    yes: bool = typer.Option(True, "-y/--confirm", help="No prompts; the run is autonomous."),
):
    """Run + learn a task autonomously on the set-up cell (Setup must be complete)."""
    from .auth import ensure_logged_in
    from .configs import get_api_url, get_default_engine
    from .engine.utils.doctor import ensure_ready, resolve_execution_engine
    from .ids import new_run_id
    from .paths import resolve_out_dir, resolve_repo_path
    from .tui_launch_config import LaunchConfig, exit_code_for_result
    from .run_local import run_local_worker_headless

    repo_path = resolve_repo_path(repo)
    if not (repo_path / "remoroo_cell").is_dir():
        typer.secho("No remoroo_cell/ here — run `remoroo setup` first.",
                    fg=typer.colors.RED)
        raise typer.Exit(code=2)
    client = ensure_logged_in()
    ensure_ready()
    if brain_url is None:
        brain_url = get_api_url()
    engine_resolved = resolve_execution_engine(get_default_engine().lower(),
                                               explicit_docker=False)
    slug = slug_of(sentence)
    run_id = new_run_id()

    base = brain_url or client.base_url
    skill_id = _resolve_skill(client, base, repo_path, sentence=sentence, slug=slug,
                              flag_value=skill)
    try:                                                # rig liveness, best-effort
        from . import rig_identity as rid
        serial = rid.saved_serial(repo_path)
        if serial:
            rid.post_heartbeat(base, client.get_token() or "", serial,
                               {"rig_token": rid.rig_token(repo_path),
                                "host": rid.compute_fingerprint(repo_path)
                                ["components"]["host"][0]})
    except Exception:                                   # noqa: BLE001 - never blocks a run
        pass
    typer.secho(f"🤖  remoroo task → @robot_task  (slug: {slug}, run: {run_id})",
                fg=typer.colors.CYAN)
    typer.secho("    Autonomous: no gates. Watch (optional): Studio task window / "
                "sibling feed. Morning report lands in .remoroo/task/reports/.",
                fg=typer.colors.YELLOW)
    # Autonomous heavyweight reasoning: same flagship default as `remoroo setup`.
    if not model:
        model = "anthropic/claude-opus-4.8"

    cfg = LaunchConfig(
        mode="new",
        repo_path=repo_path,
        out_dir=resolve_out_dir(None, repo_path),
        brain_url=brain_url,
        engine=engine_resolved,
        verbose=False,
        cache_env=True,
        in_place=True,
        agentic=True,
        engine_version="v2",
        max_wall_time_s=int(budget_hours * 3600),
        allow_overage=False,
        yes=yes,
        no_patch=False,
        pick_model=False,
        goal=f"@robot_task {sentence}",
        metrics_list=[],
        model=model,
        resume_run_id=None,
        run_id_display=run_id,
        attach_status="",
        attach_goal_preview="",
        metrics_option_provided=True,      # alias default_metrics fill in server-side
        interactive=False,                 # DEC-04: no gates, no ask_human
        operator_note=(f"task_slug={slug} budget_trials={budget_trials}"
                       + (f" skill_id={skill_id}" if skill_id else "")),
    )
    lr = run_local_worker_headless(cfg)
    raise typer.Exit(code=exit_code_for_result(lr.success, lr.partial_success))


def task_status(port: int = typer.Option(EDGE_PORT_DEFAULT, "--port")):
    """Cell-side task service status (supervisor, bank, reversibility)."""
    typer.echo(json.dumps(_edge("status", {}, port), indent=1))


def task_report(
    sentence_or_slug: str = typer.Argument(..., help="The task sentence or its slug."),
    port: int = typer.Option(EDGE_PORT_DEFAULT, "--port"),
    last_n: int = typer.Option(50, "--last"),
):
    """Print the latest results for a task from the cell's trial records."""
    slug = (sentence_or_slug if re.fullmatch(r"[a-z0-9_]+", sentence_or_slug)
            else slug_of(sentence_or_slug))
    md = Path(".remoroo/task/reports") / f"{slug}.md"
    if md.exists():
        typer.echo(md.read_text())
        return
    ids = _edge("trial_ids", {"task_slug": slug}, port)
    if "error" in ids:
        typer.secho(str(ids["error"]), fg=typer.colors.RED)
        raise typer.Exit(code=2)
    rows = []
    for tid in ids.get("trial_ids", [])[-last_n:]:
        rec = _edge("trial_get", {"task_slug": slug, "trial_id": tid}, port).get("record", {})
        v = rec.get("verdict") or {}
        rows.append(f"{tid}  {rec.get('outcome','?'):<20} score={v.get('score')} "
                    f"ok={v.get('ok')}")
    typer.echo(f"task {slug}: {len(rows)} trials")
    for r in rows:
        typer.echo("  " + r)
