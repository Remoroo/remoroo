"""``remoroo setup`` — stage the robotics seeds and launch ``@robot_setup``.

Thin launcher, exactly like ``remoroo init``. It owns **no** agent
instructions: the goal prompt, the phase gates, and the definition of done
live in ONE place — the ``@robot_setup`` alias in
``remoroo_brain/prompts/goal_aliases.py`` — and are resolved server-side. This
command only does the two things the brain cannot do itself:

  1. stage the bundled ``robotics`` seed catalog onto the operator's disk
     (the CLI is the side with filesystem access), and
  2. start an interactive, operator-supervised local run with ``@robot_setup``.

For the canonical, always-current description of what setup does, run
``remoroo aliases`` (served from the brain) — not this file.
"""
from __future__ import annotations

import json as _json
import time as _time
from pathlib import Path
from typing import Optional

import typer


# --- Conversation continuity ------------------------------------------------
# The v2 AgentLoop checkpoints its full message history to
# `<repo>/.remoroo/runs/<run_id>/checkpoint.json` and restores it on start when
# the goal+metric match (see remoroo_brain/v2/agent_loop.py
# _load_and_restore_checkpoint). `remoroo setup` always mints a NEW run_id, so
# nothing is ever restored — every run cold-starts. To "continue the
# conversation" we mint a fresh run_id but SEED its checkpoint from a prior
# setup run; the brain then rehydrates the whole conversation. This needs no
# brain change and sidesteps the "run already finished" resume rejection.

_SETUP_POINTER_REL = ".remoroo_init/last_setup_run.json"


def _setup_pointer_path(repo_path: Path) -> Path:
    return repo_path / _SETUP_POINTER_REL


def _checkpoint_path(repo_path: Path, run_id: str) -> Path:
    return repo_path / ".remoroo" / "runs" / run_id / "checkpoint.json"


def _read_last_setup_run(repo_path: Path) -> Optional[str]:
    p = _setup_pointer_path(repo_path)
    if not p.exists():
        return None
    try:
        return _json.loads(p.read_text(encoding="utf-8")).get("run_id") or None
    except Exception:
        return None


def _write_last_setup_run(repo_path: Path, run_id: str) -> None:
    p = _setup_pointer_path(repo_path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            _json.dumps(
                {
                    "run_id": run_id,
                    "updated_at": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
                }
            ),
            encoding="utf-8",
        )
    except OSError:
        pass


def seed_checkpoint_from_prior(
    repo_path: Path, prior_run_id: str, new_run_id: str
) -> Optional[int]:
    """Copy the prior setup run's checkpoint (+ run_state/trace) into the new
    run's namespace so the brain restores the full conversation. Returns the
    number of messages carried over, or None if there is no usable prior
    checkpoint (caller then starts fresh)."""
    src = _checkpoint_path(repo_path, prior_run_id)
    if not src.exists():
        return None
    try:
        raw = src.read_text(encoding="utf-8")
        data = _json.loads(raw)
    except Exception:
        return None
    history = data.get("history") or []
    if not history:
        return None
    dst = _checkpoint_path(repo_path, new_run_id)
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(raw, encoding="utf-8")
    except OSError:
        return None
    # Best-effort: carry sibling artifacts too (run_state lives inside the
    # checkpoint already, but copy standalone files if present).
    for sibling in ("run_state.json", "trace.jsonl"):
        s = src.parent / sibling
        if s.exists():
            try:
                (dst.parent / sibling).write_text(
                    s.read_text(encoding="utf-8"), encoding="utf-8"
                )
            except OSError:
                pass
    return len(history)


def setup(
    repo: Path = typer.Option(
        Path("."),
        "--repo",
        help="Repository to set up for robot data collection. Defaults to the current directory.",
    ),
    yes: bool = typer.Option(
        False, "-y", "--yes", help="Skip confirmation prompts."
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the output directory."
    ),
    brain_url: Optional[str] = typer.Option(
        None, "--brain-url", help="URL of the control plane."
    ),
    engine: Optional[str] = typer.Option(
        None, "--engine", help="Execution engine ('docker' or 'venv')."
    ),
    model: Optional[str] = typer.Option(
        None, "--model", help="LLM model id (e.g. anthropic/claude-opus-4.8)."
    ),
    pick_model: bool = typer.Option(
        True,
        "--pick-model/--no-pick-model",
        help="Show the model picker before launching the TUI.",
    ),
    budget_hours: float = typer.Option(
        12.0,
        "--budget",
        help=(
            "Time budget in hours (default 12.0). Setup is interactive and "
            "resumable — re-run to continue where it left off. Time spent "
            "blocked on operator gates (gate_checkpoint / ask_human) is "
            "excluded from this budget."
        ),
    ),
    headless: bool = typer.Option(
        False,
        "--headless",
        help="Skip the Rich TUI (CI / non-tty environments).",
    ),
    studio: bool = typer.Option(
        True,
        "--studio/--no-studio",
        help=(
            "Launch the visual Remoroo Studio (served over the LAN — open the "
            "printed URL on your laptop/tablet; the robot computer usually has no "
            "display). Use --no-studio for the agent-driven TUI flow."
        ),
    ),
    edge: bool = typer.Option(
        True,
        "--edge/--no-edge",
        help=(
            "Launch the REAL edge service (server/edge_real.py) for this cell so the "
            "studio's gates drive the live robot — primitives.py + cuRobo + the live "
            "camera (the calibration/world wizards stream from it). On by default "
            "(setup runs on the robot computer); --no-edge for a no-hardware machine. "
            "The edge degrades gracefully and connects the bridge once primitives.py "
            "is authored (G2)."
        ),
    ),
    edge_url: Optional[str] = typer.Option(
        None, "--edge-url", help="Point the studio at an already-running edge service (instead of --edge)."
    ),
    agent_url: Optional[str] = typer.Option(
        None, "--agent-url", help="Point the studio's agent dock at a brain endpoint (else the built-in sim agent)."
    ),
    note: Optional[str] = typer.Option(
        None,
        "--note",
        "-n",
        help=(
            "Free-text guidance for the agent at launch — use it to correct its "
            "assumptions or steer decisions (e.g. 'wrist cam is a RealSense D435, "
            "NOT a ZED; the left arm is the leader; keep speeds very slow'). "
            "Injected into the agent's prompt as authoritative operator guidance. "
            "Combine with --note-file."
        ),
    ),
    note_file: Optional[Path] = typer.Option(
        None,
        "--note-file",
        help=(
            "Path to a text/Markdown file with operator guidance; its contents are "
            "appended to --note. Handy for longer cell descriptions."
        ),
    ),
    cont: bool = typer.Option(
        False,
        "--continue",
        "-c",
        help=(
            "Continue the MOST RECENT setup conversation for this repo: reuse "
            "its checkpoint so the agent keeps all its context (what it learned "
            "about the cell, which gates passed) instead of cold-starting and "
            "re-reading everything. Use after an error or a stop."
        ),
    ),
    resume_run_id: Optional[str] = typer.Option(
        None,
        "--resume",
        metavar="RUN_ID",
        help="Continue a SPECIFIC prior setup run by id (implies --continue).",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", help="Verbose logging."
    ),
):
    """Stage robotics seeds and launch the interactive ``@robot_setup`` run.

    Thin launcher (mirrors ``remoroo init``): the agent's instructions, phase
    gates, and definition of done are owned by the ``@robot_setup`` alias in
    the brain and resolved server-side — run ``remoroo aliases`` for the
    canonical description.

    Safety: the run is interactive and operator-supervised — every motion
    pauses for your go-ahead, so keep a hand on the E-stop.

    Equivalent to:
        remoroo run --local --goal "@robot_setup"
    """
    # Studio mode (default) drives the REAL @robot_setup run and shows it in the
    # browser (no terminal). It needs the full session setup (auth, engine, run
    # creation, local worker) — so it dispatches below after cfg is built, NOT here.
    from .auth import ensure_logged_in
    from .configs import get_api_url, get_default_engine
    from .engine.utils.doctor import ensure_ready, resolve_execution_engine
    from .ids import new_run_id
    from .paths import resolve_repo_path, resolve_out_dir
    from .tui_launch_config import LaunchConfig, exit_code_for_result
    from .tui_unified_app import (
        echo_session_finished_line,
        run_unified_local_session,
    )

    typer.secho(
        "🤖  remoroo setup → @robot_setup (interactive, operator-supervised). "
        "Keep a hand on the E-stop; the agent pauses for your go-ahead before "
        "any motion.",
        fg=typer.colors.YELLOW,
    )
    typer.echo(
        "    Staging the bundled robotics seed catalog locally, then handing "
        "off to the agent.\n"
    )

    ensure_ready()

    if brain_url is None:
        brain_url = get_api_url()

    engine_opt = engine
    engine_resolved = (engine or get_default_engine()).lower()
    if engine_resolved not in ("docker", "venv"):
        typer.secho(
            f"❌ Invalid engine '{engine_resolved}'. Choose 'docker' or 'venv'.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)
    engine_resolved = resolve_execution_engine(
        engine_resolved,
        explicit_docker=(engine_opt is not None and engine_resolved == "docker"),
    )

    client = ensure_logged_in()

    repo_path = resolve_repo_path(repo)

    # Activation gate: setup opens the Studio on a live rig, and unactivated rigs
    # cannot open the Studio. Recognized rigs (token / rolling fingerprint — every
    # previously activated rig) pass silently; an unknown rig gets the
    # once-per-lifetime question here, never an invoice.
    from .robot_cmd import ensure_rig_activated
    if not ensure_rig_activated(repo_path, client, client.base_url):
        typer.secho("Setup needs an activated rig: run `remoroo robot activate` once "
                    "on this machine (existing rigs re-bind for free).",
                    fg=typer.colors.RED)
        raise typer.Exit(code=3)

    out_dir = resolve_out_dir(out, repo_path)
    run_id = new_run_id()
    max_wall_time_s = int(budget_hours * 3600)

    # Conversation continuity (--continue / --resume): seed THIS run's checkpoint
    # from a prior setup run so the agent resumes with full context instead of
    # cold-starting. A fresh run_id avoids the "run already finished" resume
    # rejection; the brain restores history from the seeded checkpoint.
    prior_run_id = resume_run_id or (_read_last_setup_run(repo_path) if cont else None)
    if (cont or resume_run_id) and not prior_run_id:
        typer.secho(
            "❌ --continue: no prior setup run recorded for this repo "
            f"({_setup_pointer_path(repo_path)} not found). Run `remoroo setup` "
            "once first, then `remoroo setup --continue`.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)
    if prior_run_id:
        carried = seed_checkpoint_from_prior(repo_path, prior_run_id, run_id)
        if carried is None:
            typer.secho(
                f"⚠ Could not continue from {prior_run_id}: no usable checkpoint "
                f"at {_checkpoint_path(repo_path, prior_run_id)}. Starting fresh.",
                fg=typer.colors.YELLOW,
            )
        else:
            typer.secho(
                f"↻ Continuing the setup conversation from {prior_run_id} "
                f"({carried} messages restored) — the agent keeps its context "
                "and won't re-read the repo from scratch.",
                fg=typer.colors.GREEN,
            )
    # Record THIS run as the latest setup run so a later --continue finds it
    # (chains forward: each continue seeds from the previous one).
    _write_last_setup_run(repo_path, run_id)

    # Resolve operator guidance (--note text + --note-file contents). This is
    # injected into the agent's prompt at launch as authoritative guidance, so
    # the operator can correct assumptions and steer decisions before any motion.
    note_parts = []
    if note and note.strip():
        note_parts.append(note.strip())
    if note_file is not None:
        try:
            note_parts.append(note_file.read_text(encoding="utf-8").strip())
        except OSError as exc:
            typer.secho(
                f"❌ Could not read --note-file {note_file}: {exc}",
                fg=typer.colors.RED,
            )
            raise typer.Exit(code=1)
    operator_note = "\n\n".join(p for p in note_parts if p)
    if operator_note:
        typer.secho(
            f"📝 Operator note attached ({len(operator_note)} chars) — the agent "
            "will treat it as authoritative guidance.",
            fg=typer.colors.GREEN,
        )

    # Make sure ``.remoroo_init/`` is gitignored BEFORE we stage anything
    # there, so a stray ``git add .`` can't commit the staged catalog. The
    # cell integration the agent authors (``remoroo_cell/``) is deliberately
    # NOT ignored — it is the deliverable and gets committed.
    from .engine.core.workspace import (
        GITIGNORE_BLOCK_HEADER,
        missing_gitignore_entries,
    )

    gitignore_path = repo_path / ".gitignore"
    existing = (
        gitignore_path.read_text(encoding="utf-8", errors="replace")
        if gitignore_path.exists()
        else ""
    )
    to_add = missing_gitignore_entries(existing)
    if to_add:
        block = (
            ("\n" if existing and not existing.endswith("\n") else "")
            + GITIGNORE_BLOCK_HEADER
            + "\n"
            + "\n".join(to_add)
            + "\n"
        )
        try:
            with open(gitignore_path, "a", encoding="utf-8") as fh:
                fh.write(block)
        except OSError as exc:
            typer.secho(
                f"⚠ Could not update .gitignore ({exc}); staged seeds may "
                "not be ignored by git.",
                fg=typer.colors.YELLOW,
            )

    # Seeds are NO LONGER staged to the operator's repo or shipped in the CLI.
    # The robotics methodology (IP) lives in the brain; the agent reads it at
    # runtime via the brain-served `read_seed` tool. Nothing to stage.

    # Robot-cell setup is heavyweight, supervised reasoning work — it wants the
    # FLAGSHIP model. The Studio path (the default) does not run the model picker,
    # so default to the picker's flagship (Opus 4.8) when the operator didn't pass
    # --model; otherwise studio setup would fall back to the server's default tier.
    # Effort is already pinned to "high" by the @robot_setup alias (server-side).
    if not model:
        # Flagship for this heavyweight supervised work. Kept as a plain literal
        # (mirrors model_picker.CHOICES[0]) so the Studio path never depends on the
        # Textual picker just to choose the default model.
        model = "anthropic/claude-opus-4.8"
        typer.secho("🧠 Model: Claude Opus 4.8 — flagship · effort: high (override with --model)",
                    fg=typer.colors.CYAN)

    cfg = LaunchConfig(
        mode="new",
        repo_path=repo_path,
        out_dir=out_dir,
        brain_url=brain_url,
        engine=engine_resolved,
        verbose=verbose,
        cache_env=True,
        in_place=True,
        agentic=True,
        engine_version="v2",
        max_wall_time_s=max_wall_time_s,
        allow_overage=False,
        yes=yes,
        no_patch=False,
        pick_model=pick_model,
        goal="@robot_setup",
        metrics_list=[],
        model=model,
        resume_run_id=None,
        run_id_display=run_id,
        attach_status="",
        attach_goal_preview="",
        # Skip the metrics wizard pane: the alias's ``default_metrics`` is
        # filled in server-side by ``resolve_goal`` (the bar = autonomous
        # safe motion + gates green), so the operator has nothing meaningful
        # to type here.
        metrics_option_provided=True,
        interactive=True,
        operator_note=operator_note,
    )

    try:
        if headless:
            from .run_local import run_local_worker_headless

            lr = run_local_worker_headless(cfg)
            code = exit_code_for_result(lr.success, lr.partial_success)
            echo_session_finished_line(lr, code)
            raise typer.Exit(code=code)

        if studio:
            # Default: the visual Studio drives the real @robot_setup run in the
            # browser (no terminal/TUI). Serves the SPA over the LAN, proxies the
            # run API to the control plane, and runs the local worker here.
            from .studio_launch import launch_setup_studio

            typer.secho(
                "🤖  remoroo setup → Remoroo Studio (visual, LAN-served). Open the printed URL on "
                "your laptop/tablet; the agent drives setup there. Keep a hand on the E-stop at motion steps.",
                fg=typer.colors.YELLOW,
            )
            ok = launch_setup_studio(cfg, echo=typer.echo, spawn_edge=edge, edge_url=edge_url or "", agent_url=agent_url or "")
            raise typer.Exit(code=0 if ok else 1)

        lr, code = run_unified_local_session(cfg)
        echo_session_finished_line(lr, code)
        raise typer.Exit(code=code)
    except typer.Exit:
        raise
    except Exception as exc:
        typer.secho(f"Robot setup failed: {exc}", fg=typer.colors.RED)
        if verbose:
            import traceback

            traceback.print_exc()
        raise typer.Exit(code=1)
