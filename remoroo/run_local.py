from __future__ import annotations

import json
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Optional, Protocol, Tuple

import typer

from .paths import resolve_canonical_repo_root

if TYPE_CHECKING:
    from .http_transport import HttpTransport


@dataclass
class LocalRunResult:
    """Result of a local CLI session.

    ``run_root`` is the per-run directory ``<repo>/.remoroo/runs/<run_id>`` and is only
    set after the worker context was prepared (POST/GET run succeeded). It must not be
    filled with ``LaunchConfig.out_dir`` or other fallbacks — those are not artifact roots.

    ``detail`` is an optional human-readable reason (e.g. prepare/health/auth errors).
    """

    run_root: Optional[Path]
    run_id: str
    success: bool
    outcome: str
    partial_success: bool = False
    detail: Optional[str] = None


class RunPrepareError(Exception):
    """User-visible prepare failure (auth, health, quota, etc.)."""

    def __init__(self, message: str, code: int = 1) -> None:
        self.message = message
        self.code = code
        super().__init__(message)


@dataclass
class LocalWorkerContext:
    api_url: str
    session_key: str
    remote_run_id: str
    run_output_dir: Path
    repo_path: Path
    original_repo_path: str
    server: Any
    stop_heartbeat: threading.Event
    heartbeat_thread: threading.Thread
    client_id: str
    budget_ui: Any
    engine: str
    cache_env: bool
    in_place: bool
    verbose: bool


def _budget_tui_from_run_json(requested_max_wall_time_s: int, run_data: dict):
    """Build TUI budget strip state from POST /runs JSON."""
    from .tui_run import BudgetTuiState

    warnings = list(run_data.get("warnings") or [])
    raw_eff = run_data.get("max_wall_time_s_effective")
    eff = int(raw_eff) if raw_eff is not None else int(requested_max_wall_time_s)
    tier = str(run_data.get("model_tier") or "haiku")
    mul = int(run_data.get("model_multiplier") or 1)
    ca = run_data.get("credits_available")
    cr = run_data.get("credits_reserved")
    aff = run_data.get("max_affordable_hours")
    oc = run_data.get("projected_overage_credits")
    ou = run_data.get("projected_overage_usd")
    return BudgetTuiState(
        requested_wall_time_s=float(requested_max_wall_time_s),
        effective_wall_time_s=float(eff),
        model_tier=tier,
        multiplier=mul,
        clamped="budget_clamped_to_balance" in warnings,
        overage="overage_projected" in warnings,
        projected_overage_credits=int(oc) if oc is not None else None,
        projected_overage_usd=float(ou) if ou is not None else None,
        credits_available=int(ca) if ca is not None else None,
        credits_reserved=int(cr) if cr is not None else None,
        affordable_h=aff if isinstance(aff, dict) else None,
    )


_PREPARE_DEBUG_LOG = Path(tempfile.gettempdir()) / "remoroo_cli_prepare_debug.log"


def _prepare_cli_debug(event: str, **fields: Any) -> None:
    """Append one JSON line to the system temp dir (best-effort; never raises)."""
    try:
        record = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "event": event}
        record.update(fields)
        line = json.dumps(record, sort_keys=True, default=str) + "\n"
        with open(_PREPARE_DEBUG_LOG, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


def _teardown_local_worker_processes(worker) -> None:
    """Stop bash/training children and docker/venv sandbox (best-effort)."""
    if worker is None:
        return
    try:
        worker.kill_all_background_jobs()
    except Exception:
        pass
    try:
        if getattr(worker, "sandbox", None):
            worker.sandbox.stop()
    except Exception:
        pass


def prepare_local_worker_context(
    *,
    repo_path: Path,
    goal: str,
    metrics: List[str],
    brain_url: str,
    engine: str,
    verbose: bool,
    cache_env: bool,
    in_place: bool,
    agentic: bool,
    engine_version: str,
    model: Optional[str],
    resume_run_id: Optional[str],
    max_wall_time_s: int,
    allow_overage: bool,
    interactive: bool = False,
    operator_note: str = "",
) -> LocalWorkerContext:
    """Health check, auth, POST /runs or GET resume, dirs, transport, heartbeat."""
    import os

    repo_path = resolve_canonical_repo_root(Path(repo_path))
    _prepare_cli_debug(
        "repo_path_resolved",
        log_file=str(_PREPARE_DEBUG_LOG),
        repo_path=str(repo_path.resolve()),
    )

    import requests
    from .http_transport import HttpTransport

    metrics_str = ", ".join(metrics)
    API_URL = brain_url

    try:
        resp = requests.get(f"{API_URL}/health", timeout=2.0)
        if resp.status_code != 200:
            raise RunPrepareError(
                f"Server at {API_URL} returned status code {resp.status_code}",
                code=1,
            )
    except RunPrepareError:
        raise
    except Exception as e:
        msg = f"Could not connect to Brain Server at {API_URL}."
        if verbose:
            msg += f" Error: {e}"
        raise RunPrepareError(msg, code=1) from e

    session_key = os.getenv("REMOROO_API_KEY")
    if not session_key:
        from .auth import _client

        if _client.is_authenticated():
            session_key = _client.get_token()
    if not session_key:
        session_key = "remote-worker-key"

    headers = {"Authorization": f"Bearer {session_key}"}
    budget_ui = None
    # Initialised up-front because the resume-run branch below doesn't
    # populate it — only the fresh-run path assigns `run_data = resp.json()`.
    # Without this, the `run_data.get("run_token")` read at the bottom of
    # this function raises `UnboundLocalError` on every `--resume` path
    # (e.g. the Try-Now executor on the GPU host).
    run_data = None
    try:
        if resume_run_id:
            resp = requests.get(
                f"{API_URL}/runs/{resume_run_id}",
                headers=headers,
                timeout=20.0,
            )
            if resp.status_code == 404:
                raise RunPrepareError(f"Run not found: {resume_run_id}", code=1)
            if resp.status_code in (401, 403):
                raise RunPrepareError(
                    "Authentication failed. If connecting to a remote server, set REMOROO_API_KEY.",
                    code=1,
                )
            resp.raise_for_status()
            body = resp.json()
            run_info = body.get("run") or {}
            st = str(run_info.get("status") or "")
            if st in ("SUCCESS", "FAILED", "PARTIAL_SUCCESS", "COMPLETED"):
                raise RunPrepareError(
                    f"Run {resume_run_id} is already finished (status={st}).",
                    code=1,
                )
            remote_run_id = resume_run_id
        else:
            form: dict = {
                "repo_path": str(repo_path),
                "goal": goal,
                "metrics": metrics_str,
                "agentic": "true" if agentic else "false",
                "engine_version": engine_version,
                "in_place": "true" if in_place else "false",
                "max_wall_time_s": str(max_wall_time_s),
                "allow_overage": "true" if allow_overage else "false",
                "interactive": "true" if interactive else "false",
            }
            if operator_note and operator_note.strip():
                form["operator_note"] = operator_note.strip()
            if model:
                form["model"] = model
            resp = requests.post(f"{API_URL}/runs", data=form, headers=headers)
            if resp.status_code == 402:
                detail = "Quota exceeded"
                try:
                    detail = resp.json().get("detail", detail)
                except Exception:
                    pass
                if "concurrent" in detail.lower() or "limit" in detail.lower():
                    try:
                        fix = requests.post(
                            f"{API_URL}/billing/release-stale",
                            headers=headers,
                            timeout=10.0,
                        )
                        if fix.status_code == 200 and fix.json().get("released", 0) > 0:
                            resp = requests.post(f"{API_URL}/runs", data=form, headers=headers)
                            if resp.status_code != 402:
                                pass  # fall through to normal handling below
                            else:
                                raise RunPrepareError(
                                    f"{detail}. Upgrade or manage your plan at https://remoroo.com/pricing",
                                    code=1,
                                )
                    except RunPrepareError:
                        raise
                    except Exception:
                        pass
                if resp.status_code == 402:
                    raise RunPrepareError(
                        f"{detail}. Upgrade or manage your plan at https://remoroo.com/pricing",
                        code=1,
                    )
            if resp.status_code in (401, 403):
                raise RunPrepareError(
                    "Authentication failed. If connecting to a remote server, set REMOROO_API_KEY.",
                    code=1,
                )
            if resp.status_code == 422:
                # Server-side validation, including unknown ``--goal @<name>``
                # aliases, lands here. Surface the brain's message verbatim
                # (e.g. "Unknown goal alias: @foo. Known aliases: ...").
                detail = "Invalid run parameters"
                try:
                    body = resp.json()
                    detail = body.get("detail", detail) or detail
                except Exception:
                    pass
                raise RunPrepareError(str(detail), code=2)
            resp.raise_for_status()
            run_data = resp.json()
            remote_run_id = run_data["run_id"]
            # [RESUME-DEBUG] What did POST /runs return? If a "fresh" setup gets back a run
            # with prior status/created_at (or a resumed/existing flag), the control plane is
            # reusing a run for this (repo_path, goal) → that's the resume at the source.
            try:
                print(f"[RESUME-DEBUG] POST /runs → run_id={remote_run_id} "
                      f"status={run_data.get('status')!r} created_at={run_data.get('created_at')!r} "
                      f"resumed={run_data.get('resumed')!r} existing={run_data.get('existing')!r} "
                      f"keys={sorted(run_data.keys())}", flush=True)
            except Exception:
                pass
            budget_ui = _budget_tui_from_run_json(max_wall_time_s, run_data)
    except RunPrepareError:
        raise
    except Exception as e:
        raise RunPrepareError(f"Failed to start or attach run on server: {e}", code=1) from e

    _prepare_cli_debug(
        "run_registered",
        log_file=str(_PREPARE_DEBUG_LOG),
        remote_run_id=remote_run_id,
        resume_run_id=resume_run_id or "",
    )

    def _abort_run_on_failure(reason: str):
        """Best-effort abort so the server releases the reservation."""
        try:
            requests.post(
                f"{API_URL}/runs/{remote_run_id}/abort",
                headers=headers,
                timeout=10.0,
            )
        except Exception:
            pass

    try:
        remoroo_dir = repo_path / ".remoroo"
        run_output_dir = remoroo_dir / "runs" / remote_run_id
        run_output_dir.mkdir(parents=True, exist_ok=True)

        gitignore_path = repo_path / ".gitignore"
        _prepare_cli_debug(
            "gitignore_phase_start",
            log_file=str(_PREPARE_DEBUG_LOG),
            gitignore_path=str(gitignore_path),
            gitignore_exists=gitignore_path.exists(),
            remote_run_id=remote_run_id,
            repo_path=str(repo_path.resolve()),
        )
        try:
            # Append a block of recommended ignores (``.remoroo/`` plus
            # universal Python/Node/OS bloat). Fully idempotent: only
            # entries not already present in the file are written, so
            # re-runs never duplicate lines, and users who have already
            # ignored some of them keep their version. Rationale and
            # the canonical list live in engine.core.workspace.
            from .engine.core.workspace import (
                GITIGNORE_BLOCK_HEADER,
                missing_gitignore_entries,
            )

            existing = ""
            if gitignore_path.exists():
                existing = gitignore_path.read_text(encoding="utf-8", errors="replace")

            to_add = missing_gitignore_entries(existing)

            if not to_add:
                _prepare_cli_debug(
                    "gitignore_skip_already_has_all",
                    log_file=str(_PREPARE_DEBUG_LOG),
                    gitignore_path=str(gitignore_path),
                    prior_bytes=len(existing.encode("utf-8")),
                )
            else:
                # Ensure we start on a fresh line so we don't merge
                # with the user's last (possibly newline-less) entry.
                sep = ""
                if existing and not existing.endswith("\n"):
                    sep = "\n"

                block = (
                    f"{sep}\n{GITIGNORE_BLOCK_HEADER}\n"
                    + "\n".join(to_add)
                    + "\n"
                )

                if existing:
                    with open(gitignore_path, "a", encoding="utf-8") as f:
                        f.write(block)
                    _prepare_cli_debug(
                        "gitignore_appended",
                        log_file=str(_PREPARE_DEBUG_LOG),
                        gitignore_path=str(gitignore_path),
                        prior_bytes=len(existing.encode("utf-8")),
                        entries_added=list(to_add),
                    )
                else:
                    gitignore_path.write_text(
                        block.lstrip("\n"), encoding="utf-8"
                    )
                    _prepare_cli_debug(
                        "gitignore_created",
                        log_file=str(_PREPARE_DEBUG_LOG),
                        gitignore_path=str(gitignore_path),
                        entries_added=list(to_add),
                    )
        except Exception as ex:
            _prepare_cli_debug(
                "gitignore_error",
                log_file=str(_PREPARE_DEBUG_LOG),
                gitignore_path=str(gitignore_path),
                error_type=type(ex).__name__,
                error=str(ex),
            )

        memory_path = remoroo_dir / "memory.json"
        old_memory_path = remoroo_dir / "local_memory.json"
        if not memory_path.exists() and old_memory_path.exists():
            try:
                old_memory_path.rename(memory_path)
            except Exception:
                pass
        if not memory_path.exists():
            try:
                memory_path.write_text(
                    '{"repo_url": "", "last_updated": "", "world_facts": [], "entity_summaries": {}, "experiences": [], "beliefs": []}'
                )
            except Exception:
                pass

        config_dir = Path.home() / ".config" / "remoroo"
        config_dir.mkdir(parents=True, exist_ok=True)
        client_id_file = config_dir / "client_id"
        if client_id_file.exists():
            client_id = client_id_file.read_text().strip()
        else:
            client_id = f"worker-{uuid.uuid4()}"
            client_id_file.write_text(client_id)

        run_token = run_data.get("run_token") if run_data else None
        server = HttpTransport(API_URL, client_id=client_id, run_token=run_token)
        server.session.headers.update({"Authorization": f"Bearer {session_key}"})

        stop_heartbeat = threading.Event()

        def heartbeat_loop() -> None:
            import time as _time

            while not stop_heartbeat.is_set():
                try:
                    hb_body = {
                        "run_id": remote_run_id,
                        "client_id": client_id,
                        "timestamp": _time.time(),
                    }
                    if run_token:
                        hb_body["run_token"] = run_token
                    r = requests.post(
                        f"{API_URL}/workers/heartbeat",
                        json=hb_body,
                        headers={"Authorization": f"Bearer {session_key}"},
                        timeout=5.0,
                    )
                    if r.status_code >= 400 and verbose:
                        typer.secho(
                            f"[dim]heartbeat HTTP {r.status_code}[/]",
                            fg=typer.colors.YELLOW,
                        )
                    _time.sleep(5)
                except Exception:
                    _time.sleep(5)

        heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        heartbeat_thread.start()

        return LocalWorkerContext(
            api_url=API_URL,
            session_key=session_key,
            remote_run_id=remote_run_id,
            run_output_dir=run_output_dir,
            repo_path=repo_path,
            original_repo_path=str(repo_path.absolute()),
            server=server,
            stop_heartbeat=stop_heartbeat,
            heartbeat_thread=heartbeat_thread,
            client_id=client_id,
            budget_ui=budget_ui,
            engine=engine,
            cache_env=cache_env,
            in_place=in_place,
            verbose=verbose,
        )
    except RunPrepareError:
        _abort_run_on_failure("prepare_failed")
        raise
    except Exception as e:
        _abort_run_on_failure("prepare_failed")
        raise RunPrepareError(f"Local setup failed after run created: {e}", code=1) from e


def finalize_local_worker_session(ctx: LocalWorkerContext, rb: dict) -> LocalRunResult:
    """Post-TUI finalize artifacts, cleanup, metrics files; returns LocalRunResult."""
    from rich.console import Console

    from .engine.local_worker import WorkerService, current_local_worker
    from .engine.protocol import ExecutionRequest

    run_output_dir = ctx.run_output_dir
    remote_run_id = ctx.remote_run_id
    original_repo_path = ctx.original_repo_path
    run_root_str = str(run_output_dir.resolve())
    engine = ctx.engine
    cache_env = ctx.cache_env
    in_place = ctx.in_place
    stop_heartbeat = ctx.stop_heartbeat

    final_result = rb.get("final_result")
    outcome = rb.get("outcome", "UNKNOWN")
    success = bool(rb.get("success", False))
    partial_success = bool(rb.get("partial_success", False))
    if outcome == "UNKNOWN" and final_result:
        typer.secho(
            f"Outcome UNKNOWN. Payload: {json.dumps(final_result)}",
            fg=typer.colors.RED,
        )

    console = Console()
    worker_service = rb.get("_cleanup_worker")
    if worker_service is None:
        worker_service = WorkerService(
            repo_root=original_repo_path,
            artifact_dir=run_root_str,
            original_repo_root=original_repo_path,
            run_id=remote_run_id,
            engine=engine,
            persistence_dir=run_root_str,
            output_callback=console.print,
            cache_env=cache_env,
            in_place=in_place,
        )

    if worker_service.is_ephemeral:
        console.print("\n[bold blue]📦 Finalizing artifacts...[/bold blue]")
        try:
            finalize_request = ExecutionRequest(
                type="finalize_artifacts",
                payload={},
                request_id=f"finalize-{remote_run_id}",
            )
            worker_service.handle_request(finalize_request)
        except Exception as e:
            console.print(f"   [yellow]⚠️  Could not finalize artifacts: {e}[/yellow]")
    else:
        console.print("\n[dim]ℹ️  Artifacts already finalized by Brain.[/dim]")

    console.print("[bold blue]🧹 Cleaning up temporary resources...[/bold blue]")
    try:
        stop_heartbeat.set()
        if outcome == "INTERRUPTED":
            _teardown_local_worker_processes(rb.get("_cleanup_worker"))
            _teardown_local_worker_processes(current_local_worker())

        if success and hasattr(worker_service, "sandbox") and worker_service.sandbox:
            try:
                worker_service.sandbox.commit(success=True)
            except Exception as e:
                console.print(f"   [yellow]⚠️  Docker commit failed: {e}[/yellow]")

        cleanup_request = ExecutionRequest(
            type="cleanup_working_copy",
            payload={},
            request_id=f"cleanup-{remote_run_id}",
        )
        cleanup_res = worker_service.handle_request(cleanup_request)
        if cleanup_res.success and cleanup_res.data.get("cleaned"):
            console.print("   [green]✅ Temporary working copy cleaned up[/green]")
        elif not cleanup_res.success:
            console.print(f"   [yellow]⚠️ Cleanup failed: {cleanup_res.error}[/yellow]")

        if hasattr(worker_service, "sandbox") and worker_service.sandbox:
            try:
                worker_service.sandbox.stop()
            except Exception:
                pass
    except Exception as e:
        console.print(f"   [yellow]⚠️  Cleanup warning: {e}[/yellow]")

    try:
        _final_metrics = {}
        _baseline_metrics = {}
        if final_result:
            if isinstance(final_result.get("metrics"), dict):
                _final_metrics = {
                    k: v
                    for k, v in final_result["metrics"].items()
                    if isinstance(v, (int, float))
                }
            if isinstance(final_result.get("baseline_metrics"), dict):
                _baseline_metrics = {
                    k: v
                    for k, v in final_result["baseline_metrics"].items()
                    if isinstance(v, (int, float))
                }
        if _final_metrics:
            with open(run_output_dir / "metrics.json", "w") as f:
                json.dump(_final_metrics, f, indent=2)
        if _baseline_metrics:
            with open(run_output_dir / "baseline_metrics.json", "w") as f:
                json.dump(_baseline_metrics, f, indent=2)
    except Exception as e:
        console.print(f"   [yellow]Could not save metrics to cache: {e}[/yellow]")

    return LocalRunResult(
        run_root=run_output_dir,
        run_id=remote_run_id,
        success=success,
        outcome=outcome,
        partial_success=partial_success,
    )



def run_local_worker(
    run_id: str,
    repo_path: Path,
    out_dir: Path,
    goal: str,
    metrics: list,
    brain_url: str = None,
    engine: str = "docker",
    verbose: bool = False,
    cache_env: bool = False,
    in_place: bool = False,
    agentic: bool = False,
    engine_version: str = "v2",
    model: Optional[str] = None,
    resume_run_id: Optional[str] = None,
    max_wall_time_s: int = 36000,
    allow_overage: bool = False,
    *,
    metrics_option_provided: bool = True,
    yes: bool = False,
    no_patch: bool = False,
    pick_model: bool = True,
    attach_status: str = "",
    attach_goal_preview: str = "",
) -> LocalRunResult:
    """Run unified TUI; raises ``typer.Exit`` with process exit code (does not return)."""
    from .configs import get_api_url
    from .tui_launch_config import LaunchConfig, unified_tui_requires_tty
    from .tui_unified_app import run_unified_local_session

    if brain_url is None:
        brain_url = get_api_url()
    if not unified_tui_requires_tty():
        typer.secho(
            "Remoroo local run requires an interactive terminal (TTY).",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)
    cfg = LaunchConfig(
        mode="attach" if resume_run_id else "new",
        repo_path=repo_path,
        out_dir=out_dir,
        brain_url=brain_url,
        engine=engine,
        verbose=verbose,
        cache_env=cache_env,
        in_place=in_place,
        agentic=agentic,
        engine_version=engine_version,
        max_wall_time_s=max_wall_time_s,
        allow_overage=allow_overage,
        yes=yes,
        no_patch=no_patch,
        pick_model=False if resume_run_id else pick_model,
        goal=(goal or "").strip(),
        metrics_list=list(metrics),
        model=None if resume_run_id else model,
        resume_run_id=resume_run_id,
        run_id_display=run_id,
        attach_status=attach_status,
        attach_goal_preview=attach_goal_preview,
        metrics_option_provided=metrics_option_provided,
    )
    _result, code = run_unified_local_session(cfg)
    raise typer.Exit(code=code)


# ── Headless executor (Try-Now Spot / CI) ───────────────────────────
#
# The TUI path above is battle-tested but assumes a real terminal. On a
# GPU Spot worker running Try Now there is no TTY, only systemd; we need
# the same polling loop without the Rich rendering. `run_local_worker_headless`
# below reuses `prepare_local_worker_context` and
# `finalize_local_worker_session` verbatim — the only new code is a silent
# poll/handle/submit loop and structured JSON-line logging to stderr so
# `journalctl -u remoroo-executor.service` gives operators a readable trace.
#
# The loop is factored into a pure, DI-tested helper (`_headless_step_loop`)
# so we can unit-test exit conditions and logging without needing a live
# control plane or WorkerService.


class _PollFn(Protocol):
    def __call__(self, timeout: float, run_id: str) -> Tuple[Any, Any, Any]: ...


class _HandleFn(Protocol):
    def __call__(self, step: Any) -> Any: ...


class _SubmitFn(Protocol):
    def __call__(self, result: Any) -> None: ...


# Exit strings the control plane can return (surfaced via
# `ExecutionRequest.type` on the synthetic `workflow_complete` step). We
# treat all three as terminal — the worker has no more work to do and
# must tear down.
_HEADLESS_TERMINAL_STEP_TYPES = frozenset(
    {"workflow_complete", "workflow_error", "run_complete"}
)


@dataclass
class HeadlessLoopStats:
    """Counters the headless loop exposes for tests + observability."""

    polls: int = 0
    steps_handled: int = 0
    submit_failures: int = 0
    poll_errors: int = 0
    last_step_type: str = ""


@dataclass
class HeadlessLoopOutcome:
    """Terminal payload extracted from the `workflow_complete`-class step
    that ended the loop. Translated to `finalize_local_worker_session`'s
    `rb` dict by the caller.
    """

    outcome: str
    success: bool
    partial_success: bool
    final_result: Any
    step_type: str


def _emit_headless_log(
    event: str,
    *,
    stream: Any = None,
    **fields: Any,
) -> None:
    """One JSON line on stderr per handled step (or error).

    Schema is deliberately tiny: operators grep with `journalctl | jq`
    and the event name is the filter key. We never raise from the logger.
    """
    try:
        record = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "component": "remoroo.headless",
            "event": event,
        }
        record.update(fields)
        line = json.dumps(record, sort_keys=True, default=str)
    except Exception:
        line = f'{{"ts": "?", "event": "{event}", "log_error": true}}'
    out = stream if stream is not None else sys.stderr
    try:
        out.write(line + "\n")
        out.flush()
    except Exception:
        pass


def _headless_step_loop(
    *,
    run_id: str,
    poll_fn: _PollFn,
    handle_fn: _HandleFn,
    submit_fn: _SubmitFn,
    poll_timeout: float = 1.0,
    max_iterations: Optional[int] = None,
    log_stream: Any = None,
    stats: Optional[HeadlessLoopStats] = None,
) -> Tuple[HeadlessLoopOutcome, HeadlessLoopStats]:
    """Poll → handle → submit until a terminal step appears.

    Pure w.r.t. the network and the `WorkerService`; callers inject real
    implementations (see `run_local_worker_headless`), tests inject
    fakes. `max_iterations` is a safety net for tests / stalled loops
    (None means "loop forever").

    Exits on:
      - a terminal `workflow_*` step (returns the outcome),
      - or `max_iterations` polls if provided without seeing one.

    Never raises; any exception inside `handle_fn` / `submit_fn` is
    counted and logged, and the loop continues. A failure in
    `poll_fn` yields `(None, None, None)` by contract of `HttpTransport`.
    """
    if stats is None:
        stats = HeadlessLoopStats()

    loop_iters = 0
    while True:
        if max_iterations is not None and loop_iters >= max_iterations:
            _emit_headless_log(
                "headless.loop_exceeded_max_iterations",
                stream=log_stream,
                run_id=run_id,
                max_iterations=max_iterations,
            )
            outcome = HeadlessLoopOutcome(
                outcome="UNKNOWN",
                success=False,
                partial_success=False,
                final_result=None,
                step_type="",
            )
            return outcome, stats
        loop_iters += 1

        try:
            step, _metrics, _baseline = poll_fn(poll_timeout, run_id)
        except Exception as exc:  # noqa: BLE001 — logger decides fate
            stats.poll_errors += 1
            _emit_headless_log(
                "headless.poll_error",
                stream=log_stream,
                run_id=run_id,
                error_type=type(exc).__name__,
                error=str(exc)[:500],
            )
            continue
        stats.polls += 1

        if step is None:
            continue

        step_type = getattr(step, "type", "") or ""
        stats.last_step_type = step_type

        try:
            result = handle_fn(step)
        except Exception as exc:  # noqa: BLE001
            _emit_headless_log(
                "headless.handle_error",
                stream=log_stream,
                run_id=run_id,
                step_type=step_type,
                error_type=type(exc).__name__,
                error=str(exc)[:500],
            )
            # handle_fn failing before emitting a result is a worker
            # fault — bail with UNKNOWN so the CP can release the
            # reservation. Operators see the journalctl error.
            outcome = HeadlessLoopOutcome(
                outcome="UNKNOWN",
                success=False,
                partial_success=False,
                final_result=None,
                step_type=step_type,
            )
            return outcome, stats

        stats.steps_handled += 1

        try:
            submit_fn(result)
        except Exception as exc:  # noqa: BLE001
            stats.submit_failures += 1
            _emit_headless_log(
                "headless.submit_error",
                stream=log_stream,
                run_id=run_id,
                step_type=step_type,
                error_type=type(exc).__name__,
                error=str(exc)[:500],
            )
            # HttpTransport.submit_result already retries; if the caller
            # raised, the result truly failed to land — the CP will
            # time us out and mark the run FAILED. Continue looping so
            # the next poll surfaces the terminal step.
            continue

        _emit_headless_log(
            "headless.step_handled",
            stream=log_stream,
            run_id=run_id,
            step_type=step_type,
        )

        if step_type in _HEADLESS_TERMINAL_STEP_TYPES:
            payload = getattr(step, "payload", None) or {}
            outcome = HeadlessLoopOutcome(
                outcome=str(payload.get("outcome", "UNKNOWN") or "UNKNOWN"),
                success=bool(payload.get("success", False)),
                partial_success=bool(payload.get("partial_success", False)),
                final_result=getattr(result, "data", None),
                step_type=step_type,
            )
            return outcome, stats


def run_local_worker_headless(cfg: "Any") -> LocalRunResult:
    """No-TUI `remoroo run --local` entry point for Try Now spot workers.

    Reuses the battle-tested `prepare_local_worker_context` and
    `finalize_local_worker_session` — the only new behaviour is:
      * no `unified_tui_requires_tty()` check (caller in cli.py handles it),
      * silent polling loop over the HTTP transport,
      * one JSON log line per handled step on stderr (journald-friendly).

    Exit code convention (interpreted by cli.py via
    `exit_code_for_result`): 0 on SUCCESS, 1 on FAILED/UNKNOWN/prepare
    errors, 2 on PARTIAL_SUCCESS.

    Signature mirrors `run_local_worker` so `cli.py` can dispatch to
    either by flag with no other changes.
    """
    from .engine.local_worker import WorkerService

    try:
        ctx = prepare_local_worker_context(
            repo_path=cfg.repo_path,
            goal=cfg.goal,
            metrics=cfg.metrics_list,
            brain_url=cfg.brain_url,
            engine=cfg.engine,
            verbose=cfg.verbose,
            cache_env=cfg.cache_env,
            in_place=cfg.in_place,
            agentic=cfg.agentic,
            engine_version=cfg.engine_version,
            model=cfg.model,
            resume_run_id=cfg.resume_run_id,
            max_wall_time_s=cfg.max_wall_time_s,
            allow_overage=cfg.allow_overage,
            interactive=getattr(cfg, "interactive", False),
            operator_note=getattr(cfg, "operator_note", ""),
        )
    except RunPrepareError as exc:
        _emit_headless_log(
            "headless.prepare_failed",
            error=exc.message,
            exit_code=exc.code,
        )
        return LocalRunResult(
            run_root=None,
            run_id=cfg.resume_run_id or "",
            success=False,
            outcome="PREPARE_FAILED",
            partial_success=False,
            detail=exc.message,
        )

    _emit_headless_log(
        "headless.prepare_ok",
        run_id=ctx.remote_run_id,
        repo=str(ctx.repo_path),
    )

    worker = WorkerService(
        repo_root=str(ctx.repo_path),
        artifact_dir=str(ctx.run_output_dir),
        original_repo_root=ctx.original_repo_path,
        run_id=ctx.remote_run_id,
        engine=ctx.engine,
        persistence_dir=str(ctx.run_output_dir),
        cache_env=ctx.cache_env,
        in_place=ctx.in_place,
    )

    try:
        outcome, _stats = _headless_step_loop(
            run_id=ctx.remote_run_id,
            poll_fn=lambda timeout, run_id: ctx.server.get_next_step(
                timeout=timeout, run_id=run_id
            ),
            handle_fn=worker.handle_request,
            submit_fn=ctx.server.submit_result,
        )
    finally:
        ctx.stop_heartbeat.set()

    rb = {
        "final_result": outcome.final_result,
        "outcome": outcome.outcome,
        "success": outcome.success,
        "partial_success": outcome.partial_success,
        "_cleanup_worker": worker,
    }
    lr = finalize_local_worker_session(ctx, rb)
    _emit_headless_log(
        "headless.finalized",
        run_id=lr.run_id,
        outcome=lr.outcome,
        success=lr.success,
        partial_success=lr.partial_success,
    )
    return lr
