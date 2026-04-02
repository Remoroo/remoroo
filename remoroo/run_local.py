from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
import typer
import json

@dataclass
class LocalRunResult:
    run_root: Path
    run_id: str
    success: bool
    outcome: str
    partial_success: bool = False


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


def run_local_worker(
    run_id: str,
    repo_path: Path,
    out_dir: Path,
    goal: str,
    metrics: list[str],
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
) -> LocalRunResult:
    from .configs import get_api_url
    if brain_url is None:
        brain_url = get_api_url()
    """
    Adapter that connects to the Remoroo Brain Server as a Worker.
    The Server must be running separately (remoroo server).
    """
    
    # Map input list[str] to string for Orchestrator if needed.
    metrics_str = ", ".join(metrics)
    
    # We need to construct artifact_dir based on out_dir and run_id
    artifact_dir = out_dir / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    
    # Use local engine components
    from .engine.local_worker import WorkerService
    from .engine.protocol import ExecutionRequest, ExecutionResult

    # STAGE 2 Retrofit: Pure Client
    import time
    import requests
    import threading
    from .http_transport import HttpTransport

    API_URL = brain_url
    
    # Check Server Health
    import requests
    try:
        resp = requests.get(f"{API_URL}/health", timeout=2.0)
        if resp.status_code != 200:
             typer.secho(f"❌ Server at {API_URL} returned status code {resp.status_code}", fg=typer.colors.RED)
             raise typer.Exit(code=1)
    except Exception as e:
         typer.secho(f"❌ Could not connect to Brain Server at {API_URL}.", fg=typer.colors.RED)
         typer.echo(f"   Check connectivity by running 'curl {API_URL}/health' in a terminal.")
         if verbose:
             typer.echo(f"   Error: {e}")
         raise typer.Exit(code=1)
    
    
    # Auth Key
    import os
    session_key = os.getenv("REMOROO_API_KEY")
    
    # Fallback to saved credentials from remoroo login
    if not session_key:
        from .auth import _client
        if _client.is_authenticated():
            session_key = _client.get_token()
    
    if not session_key:
         typer.echo("⚠️  No authentication found. Set REMOROO_API_KEY or run 'remoroo login'.")
         typer.echo("   Assuming server accepts unauthenticated requests or allow-list.")
         # Generate a dummy key just in case protocol requires non-empty string
         session_key = "remote-worker-key"

    # Verify Auth (Optional but good UX)
    try:
         auth_resp = requests.get(
             f"{API_URL}/user/me", 
             headers={"Authorization": f"Bearer {session_key}"},
             timeout=2.0
         )
         if auth_resp.status_code != 200:
            typer.secho(f"⚠️  Authentication failed (Status {auth_resp.status_code}). Check your credentials.", fg=typer.colors.YELLOW)
    except:
         pass
            
    # 3. Create run on server, or attach to an existing run (--resume)
    headers = {"Authorization": f"Bearer {session_key}"}
    budget_ui = None
    try:
        if resume_run_id:
            resp = requests.get(
                f"{API_URL}/runs/{resume_run_id}",
                headers=headers,
                timeout=20.0,
            )
            if resp.status_code == 404:
                typer.secho(f"❌ Run not found: {resume_run_id}", fg=typer.colors.RED)
                raise typer.Exit(code=1)
            if resp.status_code in (401, 403):
                typer.secho(
                    "\n❌ Authentication failed. If connecting to a remote server, set REMOROO_API_KEY.",
                    fg=typer.colors.RED,
                )
                raise typer.Exit(code=1)
            resp.raise_for_status()
            body = resp.json()
            run_info = body.get("run") or {}
            st = str(run_info.get("status") or "")
            if st in ("SUCCESS", "FAILED", "PARTIAL_SUCCESS", "COMPLETED"):
                typer.secho(
                    f"❌ Run {resume_run_id} is already finished (status={st}).",
                    fg=typer.colors.RED,
                )
                raise typer.Exit(code=1)
            remote_run_id = resume_run_id
        else:
            form: dict = {
                "repo_path": str(repo_path),
                "goal": goal,
                "metrics": metrics_str,
                "artifact_dir": str(artifact_dir),
                "agentic": "true" if agentic else "false",
                "engine_version": engine_version,
                "in_place": "true" if in_place else "false",
                "max_wall_time_s": str(max_wall_time_s),
                "allow_overage": "true" if allow_overage else "false",
            }
            if model:
                form["model"] = model
            resp = requests.post(f"{API_URL}/runs", data=form, headers=headers)

            if resp.status_code == 402:
                typer.secho(
                    "\n❌ Quota Exceeded. Please upgrade your plan at https://remoroo.com/pricing",
                    fg=typer.colors.RED,
                )
                raise typer.Exit(code=1)

            if resp.status_code in (401, 403):
                typer.secho(
                    "\n❌ Authentication failed. If connecting to a remote server, set REMOROO_API_KEY.",
                    fg=typer.colors.RED,
                )
                raise typer.Exit(code=1)

            resp.raise_for_status()
            run_data = resp.json()
            remote_run_id = run_data["run_id"]
            budget_ui = _budget_tui_from_run_json(max_wall_time_s, run_data)
    except typer.Exit:
        raise
    except Exception as e:
        typer.secho(f"❌ Failed to start or attach run on server: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    
    # 4. Start Log Streamer (Background) — started after display is created (see below)

    # 5. Initialize Proxy
    
    # Phase 3: Persistent Client ID
    config_dir = Path.home() / ".config" / "remoroo"
    config_dir.mkdir(parents=True, exist_ok=True)
    client_id_file = config_dir / "client_id"
    
    if client_id_file.exists():
        client_id = client_id_file.read_text().strip()
    else:
        import uuid
        client_id = f"worker-{uuid.uuid4()}"
        client_id_file.write_text(client_id)
        
    server = HttpTransport(API_URL, client_id=client_id)
    server.session.headers.update({"Authorization": f"Bearer {session_key}"}) # Authenticate Transport
    
    # Phase 2: Heartbeat Thread
    stop_heartbeat = threading.Event()
    def heartbeat_loop():
        # Wait for Initial Run creation before starting? 
        # We have remote_run_id from line 114.
        while not stop_heartbeat.is_set():
            try:
                import time
                r = requests.post(
                    f"{API_URL}/workers/heartbeat",
                    json={
                        "run_id": remote_run_id,
                        "client_id": client_id,
                        "timestamp": time.time()
                    },
                    headers={"Authorization": f"Bearer {session_key}"},
                    timeout=5.0
                )
                # Do not raise: transient 4xx/5xx should not tighten heartbeat spacing vs success.
                if r.status_code >= 400 and verbose:
                    typer.secho(f"[dim]heartbeat HTTP {r.status_code}[/]", fg=typer.colors.YELLOW)
                time.sleep(5)
            except Exception:
                time.sleep(5)
            
    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()
    
    # Phase 2.5: Bulletproof Isolation & Persistence
    # Create unique run output directory in the original repo
    remoroo_dir = repo_path / ".remoroo"
    run_output_base = remoroo_dir / "runs"
    run_output_dir = run_output_base / remote_run_id
    run_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Git Hygiene: Ensure .remoroo is ignored to prevent "patch soup"
    gitignore_path = repo_path / ".gitignore"
    try:
        if gitignore_path.exists():
            content = gitignore_path.read_text()
            if ".remoroo/" not in content:
                with open(gitignore_path, 'a') as f:
                    f.write("\n# Remoroo Metadata\n.remoroo/\n")
        else:
            gitignore_path.write_text("# Remoroo Metadata\n.remoroo/\n")
    except Exception:
        pass # Ignore gitignore failures

    # Initialize workspace memory file if it doesn't exist; migrate old name
    memory_path = remoroo_dir / "memory.json"
    old_memory_path = remoroo_dir / "local_memory.json"
    if not memory_path.exists() and old_memory_path.exists():
        try:
            old_memory_path.rename(memory_path)
        except Exception:
            pass
    if not memory_path.exists():
        try:
            memory_path.write_text('{"repo_url": "", "last_updated": "", "world_facts": [], "entity_summaries": {}, "experiences": [], "beliefs": []}')
        except Exception:
            pass

    from rich.console import Console
    from .engine.local_worker import current_local_worker
    from .tui_run import run_remoroo_tui_session

    original_repo_path = str(repo_path.absolute())

    final_result = None
    outcome = "UNKNOWN"
    success = False
    partial_success = False
    rb: dict = {}

    try:
        rb = run_remoroo_tui_session(
            server=server,
            api_url=API_URL,
            session_key=session_key,
            remote_run_id=remote_run_id,
            repo_path=original_repo_path,
            engine=engine,
            artifact_dir=str(artifact_dir),
            original_repo_path=original_repo_path,
            cache_env=cache_env,
            in_place=in_place,
            budget_ui=budget_ui,
        )
        final_result = rb.get("final_result")
        outcome = rb.get("outcome", "UNKNOWN")
        success = bool(rb.get("success", False))
        partial_success = bool(rb.get("partial_success", False))
        if outcome == "UNKNOWN" and final_result:
            typer.secho(f"Outcome UNKNOWN. Payload: {json.dumps(final_result)}", fg=typer.colors.RED)
    except KeyboardInterrupt:
        typer.echo("")
        typer.secho(
            "🛑 Interrupted — killing local jobs, stopping sandbox, aborting run…",
            fg=typer.colors.YELLOW,
            bold=True,
        )
        outcome = "INTERRUPTED"
        success = False
        stop_heartbeat.set()
        _teardown_local_worker_processes(rb.get("_cleanup_worker"))
        _teardown_local_worker_processes(current_local_worker())
        try:
            requests.post(
                f"{API_URL}/runs/{remote_run_id}/abort",
                headers={"Authorization": f"Bearer {session_key}"},
                timeout=12.0,
            )
        except Exception:
            pass
    except Exception as e:
        typer.secho(f"❌ Execution loop crashed: {e}", fg=typer.colors.RED)
        outcome = f"CRASH: {e}"
        success = False
        if verbose:
            import traceback
            traceback.print_exc()

    console = Console()
    worker_service = rb.get("_cleanup_worker")
    if worker_service is None:
        worker_service = WorkerService(
            repo_root=original_repo_path,
            artifact_dir=str(artifact_dir),
            original_repo_root=original_repo_path,
            run_id=remote_run_id,
            engine=engine,
            persistence_dir=str(artifact_dir),
            output_callback=console.print,
            cache_env=cache_env,
            in_place=in_place,
        )

    # v19: Fallback outcome detection from final_report.md
    # Removed as requested (root cause fixed in Brain).
    pass

    # 7. Finalize Artifacts (Worker generates local diff and delivers it)
    # v15: Only call manually if the Brain hasn't already triggered a cleanup
    if worker_service.is_ephemeral:
        console.print("\n[bold blue]📦 Finalizing artifacts...[/bold blue]")
        try:
            from .engine.protocol import ExecutionRequest
            finalize_request = ExecutionRequest(
                type="finalize_artifacts",
                payload={},
                request_id=f"finalize-{remote_run_id}"
            )
            worker_service.handle_request(finalize_request)
        except Exception as e:
            console.print(f"   [yellow]⚠️  Could not finalize artifacts: {e}[/yellow]")
    else:
        console.print("\n[dim]ℹ️  Artifacts already finalized by Brain.[/dim]")
    
    # Cleanup Phase: Ensure temporary resources are cleaned up
    console.print("[bold blue]🧹 Cleaning up temporary resources...[/bold blue]")
    try:
        # 1. Stop heartbeat
        stop_heartbeat.set()

        if outcome == "INTERRUPTED":
            _teardown_local_worker_processes(rb.get("_cleanup_worker"))
            _teardown_local_worker_processes(current_local_worker())

        # 2. Commit Docker environment if run was successful
        if success and hasattr(worker_service, 'sandbox') and worker_service.sandbox:
            try:
                worker_service.sandbox.commit(success=True)
            except Exception as e:
                console.print(f"   [yellow]⚠️  Docker commit failed: {e}[/yellow]")
        
        # 3. v14.1: HARDENED ARTIFACT SYNCHRONIZATION
        # Ensure we sync artifacts from the worker's active directory to the permanent CLI cache.
        if worker_service.artifact_dir:
             src_artifacts = Path(worker_service.artifact_dir)
             dst_artifacts = artifact_dir # This is the permanent path from line 36
             
             if src_artifacts.exists() and src_artifacts.resolve() != dst_artifacts.resolve():
                 console.print(f"   [green]💾 Synchronizing artifacts...[/green]")
                 try:
                     # Reclaim ownership first (important for Docker)
                     if hasattr(worker_service, '_reclaim_ownership'):
                         worker_service._reclaim_ownership(str(src_artifacts))
                     
                     # Sync files
                     import shutil
                     count = 0
                     for item in src_artifacts.iterdir():
                         s = item
                         d = dst_artifacts / item.name
                         if s.is_dir():
                             if d.exists(): shutil.rmtree(d)
                             shutil.copytree(s, d)
                         else:
                             shutil.copy2(s, d)
                         count += 1
                     if count > 0:
                         console.print(f"   [green]✅ Synchronized {count} artifacts to {dst_artifacts}[/green]")
                 except Exception as e:
                      console.print(f"   [red]❌ Failed to synchronize artifacts: {e}[/red]")
        
        # 4. Request cleanup of working copy via RPC (Handles both Mac and Linux)
        from .engine.protocol import ExecutionRequest
        cleanup_request = ExecutionRequest(
            type="cleanup_working_copy",
            payload={},
            request_id=f"cleanup-{remote_run_id}"
        )
        cleanup_res = worker_service.handle_request(cleanup_request)
        if cleanup_res.success and cleanup_res.data.get("cleaned"):
            console.print("   [green]✅ Temporary working copy cleaned up[/green]")
        elif not cleanup_res.success:
            console.print(f"   [yellow]⚠️ Cleanup failed: {cleanup_res.error}[/yellow]")
        
        # 4. Stop Docker sandbox (stopped by cleanup RPC above, but defensive here)
        if hasattr(worker_service, 'sandbox') and worker_service.sandbox:
            try:
                worker_service.sandbox.stop()
            except Exception:
                pass
    
    except Exception as e:
        console.print(f"   [yellow]⚠️  Cleanup warning: {e}[/yellow]")
    
    # 8. Save Metrics for CLI Summary (extract from workflow_complete payload)
    try:
        _final_metrics = {}
        _baseline_metrics = {}
        if final_result:
            if isinstance(final_result.get("metrics"), dict):
                _final_metrics = {k: v for k, v in final_result["metrics"].items() if isinstance(v, (int, float))}
            if isinstance(final_result.get("baseline_metrics"), dict):
                _baseline_metrics = {k: v for k, v in final_result["baseline_metrics"].items() if isinstance(v, (int, float))}
        if _final_metrics:
            with open(run_output_dir / "metrics.json", 'w') as f:
                json.dump(_final_metrics, f, indent=2)
        if _baseline_metrics:
            with open(run_output_dir / "baseline_metrics.json", 'w') as f:
                json.dump(_baseline_metrics, f, indent=2)
    except Exception as e:
        console.print(f"   [yellow]Could not save metrics to cache: {e}[/yellow]")

    return LocalRunResult(
        run_root=run_output_dir,
        run_id=remote_run_id,
        success=success,
        outcome=outcome,
        partial_success=partial_success
    )
    

