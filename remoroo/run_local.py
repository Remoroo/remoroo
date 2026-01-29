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

def run_local_worker(
    run_id: str,
    repo_path: Path,
    out_dir: Path,
    goal: str,
    metrics: list[str],
    brain_url: str = None,
    engine: str = "docker",
    verbose: bool = False,
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
    from .engine.protocol import ExecutionResult

    # STAGE 2 Retrofit: Pure Client
    import time
    import requests
    import threading
    import sseclient
    from .http_transport import HttpTransport

    API_URL = brain_url
    
    typer.echo(f"🔌 Connecting to Brain Server at {API_URL}...")
    
    # Check Server Health
    import requests
    try:
        resp = requests.get(f"{API_URL}/health", timeout=2.0)
        if resp.status_code != 200:
             typer.secho(f"❌ Server at {API_URL} returned status code {resp.status_code}", fg=typer.colors.RED)
             raise typer.Exit(code=1)
        typer.echo("✅ Server is reachable.")
    except Exception as e:
         typer.secho(f"❌ Could not connect to Brain Server at {API_URL}.", fg=typer.colors.RED)
         typer.echo(f"   Please ensure 'remoroo server' is running in another terminal.")
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
            typer.echo("🔐 Using saved credentials from 'remoroo login'")
    
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
            
    # 3. Start Run (Create Run on Server)
    try:
        headers = {}
        headers["Authorization"] = f"Bearer {session_key}"
        
        # Stage 6.5 Compat: Server expects Form Data now
        resp = requests.post(f"{API_URL}/runs", data={
            "repo_path": str(repo_path),
            "goal": goal,
            "metrics": metrics_str,
            "artifact_dir": str(artifact_dir) 
        }, headers=headers)
        
        if resp.status_code == 402:
             typer.secho("\n❌ Quota Exceeded. Please upgrade your plan at https://remoroo.com/pricing", fg=typer.colors.RED)
             raise typer.Exit(code=1)
             
        if resp.status_code in [401, 403]:
             typer.secho("\n❌ Authentication failed. If connecting to a remote server, set REMOROO_API_KEY.", fg=typer.colors.RED)
             raise typer.Exit(code=1)
             
        resp.raise_for_status()
    except Exception as e:
        typer.secho(f"❌ Failed to create run on server: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    run_data = resp.json()
    remote_run_id = run_data["run_id"]
    typer.echo(f"   Remote Run ID: {remote_run_id}")
    
    # 4. Start Log Streamer (Background)
    def stream_logs():
        try:
            # sseclient-py usage
            messages = sseclient.SSEClient(f"{API_URL}/runs/{remote_run_id}/stream")
            for msg in messages:
                if msg.event == "finish":
                    break
        except Exception:
            pass

    log_thread = threading.Thread(target=stream_logs, daemon=True)
    log_thread.start()

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
        
    typer.echo(f"🆔 Worker ID: {client_id}")

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
                requests.post(
                    f"{API_URL}/workers/heartbeat",
                    json={
                        "run_id": remote_run_id,
                        "client_id": client_id,
                        "timestamp": time.time()
                    },
                    headers={"Authorization": f"Bearer {session_key}"},
                    timeout=5.0
                )
            except Exception:
                pass # Silent fail
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

    from rich.console import Console, Group
    from rich.live import Live
    from rich.table import Table
    from rich.panel import Panel
    from rich.layout import Layout
    from rich.text import Text
    from rich.spinner import Spinner
    
    console = Console()

    # Initialize Execution Service (Does the work)
    # True original repo root preserved across context switches
    original_repo_path = str(repo_path.absolute())
    worker_service = WorkerService(
        repo_root=original_repo_path, 
        artifact_dir=str(artifact_dir), 
        original_repo_root=original_repo_path, 
        run_id=remote_run_id,
        engine=engine,
        # v16: Persistence Dir (CLI Cache) for real-time mirroring
        persistence_dir=str(artifact_dir),
        # Early instance might not have Live console yet, but we'll set it in the loop
        output_callback=console.print 
    )
    
    final_result = None
    outcome = "UNKNOWN"
    success = False
    

    # --- Dashboard Components ---
    scoreboard_data = {
        "baseline": {m: None for m in metrics}, 
        "current": {m: None for m in metrics}, 
        "status": "Initializing..."
    }

    # Create Persistent Layout to prevent flickering
    dashboard_layout = Layout()
    dashboard_layout.split_column(
        Layout(name="header", size=3),
        Layout(name="metrics", size=8),
        Layout(name="footer", size=3)
    )

    def update_dashboard(layout, data):
        # Header
        layout["header"].update(Panel(Text(f"🚀 Remoroo Run: {remote_run_id}", justify="center", style="bold magenta"), border_style="magenta"))
        
        # Metrics Table
        table = Table(box=None, expand=True)
        table.add_column("Metric", style="cyan")
        table.add_column("Baseline", justify="right", style="dim")
        table.add_column("Current", justify="right", style="bold green")
        table.add_column("Delta", justify="right")
        
        baseline = data.get("baseline") or {}
        current = data.get("current") or {}
        
        all_keys = sorted(set(list(baseline.keys()) + list(current.keys())))
        
        if not all_keys:
            table.add_row("[italic yellow]No metrics captured yet...[/italic yellow]", "", "", "")
        else:
            for k in all_keys:
                b_val = baseline.get(k)
                c_val = current.get(k)
                
                delta_str = ""
                if isinstance(b_val, (int, float)) and isinstance(c_val, (int, float)):
                    delta = c_val - b_val
                    color = "green" if delta >= 0 else "red"
                    # Some metrics are better if lower (like runtime)
                    if "time" in k.lower() or "runtime" in k.lower() or "cost" in k.lower():
                        color = "green" if delta <= 0 else "red"
                    delta_str = f"[{color}]{delta:+.2f}[/{color}]"
                
                table.add_row(
                    k, 
                    str(b_val) if b_val is not None else "-", 
                    str(c_val) if c_val is not None else "-",
                    delta_str
                )
            
        layout["metrics"].update(Panel(table, title="[bold]Scoreboard[/bold]", border_style="blue"))
        
        # Footer / Status
        status_msg = data.get("status", "")
        layout["footer"].update(Panel(Text(f" {status_msg}", justify="center", style="italic yellow"), border_style="dim"))
        
        return layout

    # Initialize layout content
    update_dashboard(dashboard_layout, scoreboard_data)

    typer.echo("")
    console.print(f"[bold blue]🧠 Brain connected to run [white]{remote_run_id}[/white]. Ready to solve![/bold blue]")
    typer.echo("")

    # Main Execution Loop (Pull Model)
    last_processed_id = None
    last_result = None
    
    try:
        with Live(dashboard_layout, console=console, refresh_per_second=10, vertical_overflow="visible") as live:
            # Connect Live console to worker for flicker-free logs
            worker_service.output_callback = live.console.print
            
            while True:
                # 1. Get next step
                step, current_metrics, baseline_metrics = server.get_next_step(timeout=10.0, run_id=remote_run_id)
                
                if baseline_metrics:
                    # Merge instead of overwrite
                    for k, v in baseline_metrics.items():
                         if k not in scoreboard_data["baseline"] or v is not None:
                             scoreboard_data["baseline"][k] = v
                
                # Sync with server metrics if available
                if current_metrics:
                    for k, v in current_metrics.items():
                         # PRIORITY: If we have a local value, only overwrite if server value is DIFFERENT 
                         # and not None. This prevents flickering back to "N/A" during sync delays.
                         if v is not None:
                             # If we have a local value, we only update if it's "new" info 
                             # (e.g. Brain calculated something the CLI didn't)
                             scoreboard_data["current"][k] = v
                
                # Debug info in status
                debug_info = f"B:{len(scoreboard_data['baseline'])} C:{len(scoreboard_data['current'])}"
                if not step:
                    if "Brain" not in scoreboard_data["status"]:
                         scoreboard_data["status"] = "🧠 Brain is analyzing results..."
                else:
                    scoreboard_data["status"] = f"🛠️ Executing {step.type}"
                
                # 2. Check for completion or timeout
                if step is None:
                    # Provide grounded status if possible
                    if "Running" in scoreboard_data["status"]:
                        pass # Keep current running status
                    elif "Applying" in scoreboard_data["status"]:
                        pass
                    else:
                        scoreboard_data["status"] = "🧠 Brain is analyzing results & planning next step..."
                    
                    update_dashboard(dashboard_layout, scoreboard_data)
                    time.sleep(0.5)
                    continue
                    
                # IDEMPOTENCY CHECK
                if step.request_id and step.request_id == last_processed_id:
                     if last_result:
                         live.console.print(f"[yellow]🔄 Resending cached result for {step.request_id}[/yellow]")
                         server.submit_result(last_result)
                         continue
                    
                # 3. Handle Special Control Steps
                if step.type == "workflow_complete":
                    final_result = step.payload or {}
                    # v18: Prioritize explicit outcome and success from Brain
                    outcome = final_result.get("outcome") or final_result.get("decision", "SUCCESS")
                    success = final_result.get("success", False)
                    partial_success = final_result.get("partial_success", False)
                    
                    scoreboard_data["status"] = "✅ Workflow Complete!"
                    update_dashboard(dashboard_layout, scoreboard_data)
                    break
                    
                if step.type == "workflow_error":
                    outcome = f"ERROR: {step.payload.get('error')}"
                    success = False
                    scoreboard_data["status"] = "❌ Workflow Error"
                    update_dashboard(dashboard_layout, scoreboard_data)
                    break
                
                # 4. Execute Step
                # Map active status to human readable
                if step.type == "execute_command":
                    cmd = step.payload.get("command_line", "command")
                    # Truncate if too long
                    if len(cmd) > 40: cmd = cmd[:37] + "..."
                    scoreboard_data["status"] = f"🏃 Running: {cmd}"
                elif step.type == "apply_patch":
                    scoreboard_data["status"] = "📝 Applying Fix..."
                elif step.type == "scan_repo":
                    scoreboard_data["status"] = "🔍 Scanning Codebase..." 
                else:
                    scoreboard_data["status"] = f"🛠️ Executing: {step.type}"
                
                update_dashboard(dashboard_layout, scoreboard_data)
                
                try:
                    live.console.print(f"\n[bold cyan]🛠️  Executing: {step.type}[/bold cyan]")
                    result = worker_service.handle_request(step)
                    
                    # --- INSTANT METRIC UPDATE (LOCAL) ---
                    # Helper to filter dirty metrics
                    def clean_metrics_dict(d):
                        clean = {}
                        blacklist = ["created_at", "source", "version", "phase"]
                        # 1. Primary Flattening of "metrics"
                        if "metrics" in d and isinstance(d["metrics"], dict):
                            for k, v in d["metrics"].items():
                                if isinstance(v, (int, float)): clean[k] = v
                        
                        # 2. Extract from "metrics_with_units"
                        if "metrics_with_units" in d and isinstance(d["metrics_with_units"], dict):
                            for k, v in d["metrics_with_units"].items():
                                if isinstance(v, dict) and "value" in v:
                                    val = v["value"]
                                    if isinstance(val, (int, float)): clean[k] = val
                        
                        # 3. Direct Key fallback
                        for k, v in d.items():
                            if k in blacklist or k == "metrics" or k == "metrics_with_units": continue
                            if isinstance(v, (int, float)):
                                if k not in clean: clean[k] = v
                        return clean

                    # Update scoreboard immediately with local result metrics
                    if result.metrics:
                        cleaned = clean_metrics_dict(result.metrics)
                        if cleaned:
                             # live.console.print(f"[green]📈 Local Metrics Captured: {cleaned}[/green]")
                             for k, v in cleaned.items():
                                  scoreboard_data["current"][k] = v
                    
                    # Check for baseline in specific artifact fields if available
                    if result.data.get("baseline_metrics"):
                        cleaned_base = clean_metrics_dict(result.data["baseline_metrics"])
                        for k, v in cleaned_base.items():
                             scoreboard_data["baseline"][k] = v
                    
                    update_dashboard(dashboard_layout, scoreboard_data)
                    # -------------------------------------

                    # Ensure request_id is preserved for the server
                    if not result.request_id:
                        result.request_id = step.request_id
                except Exception as e:
                    live.console.print_exception()
                    result = ExecutionResult(success=False, error=str(e), request_id=step.request_id)
                
                # 5. Handle Context Switching (Working Copy)
                if step.type == "create_working_copy" and result.success:
                     new_root = result.data.get("working_path")
                     if new_root:
                         # CLEANUP PREVIOUS EPHEMERAL WORKSPACE (if any)
                         try:
                             worker_service.handle_request(ExecutionRequest(type="cleanup_working_copy", payload={}))
                         except Exception:
                             pass

                         worker_service = WorkerService(
                             repo_root=new_root, 
                             artifact_dir=None, # v12: Use Ephemeral Artifacts (default) so 'ls' works
                             original_repo_root=original_repo_path, 
                             run_id=remote_run_id,
                             engine=engine, output_callback=live.console.print,
                             # v16: Persistence Dir (CLI Cache)
                             persistence_dir=str(artifact_dir)
                         )
                         live.console.print(f"[bold yellow]🔄 Switched execution context to:[/bold yellow] [dim]{new_root}[/dim]")

                # 6. Submit Result
                server.submit_result(result)
                
                # Update Cache
                last_processed_id = step.request_id
                last_result = result
                
                # Restart status for next poll
                scoreboard_data["status"] = "🧠 Brain: Analyzing results..."
                update_dashboard(dashboard_layout, scoreboard_data)

    except KeyboardInterrupt:
        typer.echo("")
        typer.secho("🛑 Experiment Paused by User.", fg=typer.colors.YELLOW, bold=True)
        outcome = "INTERRUPTED"
        success = False
        
        # Notify Server of Abort (Infrastructure failure)
        try:
            requests.post(
                f"{API_URL}/runs/{remote_run_id}/abort",
                headers={"Authorization": f"Bearer {session_key}"},
                timeout=2.0
            ) 
        except Exception:
            pass # Best effort
        
        # Fall through to cleanup
    except Exception as e:
        typer.secho(f"❌ Execution loop crashed: {e}", fg=typer.colors.RED)
        outcome = f"CRASH: {e}"
        success = False
        if verbose:
            import traceback
            traceback.print_exc()

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
    
    # 8. Save Metrics for CLI Summary
    try:
        if scoreboard_data["current"]:
            with open(artifact_dir / "metrics.json", 'w') as f:
                json.dump(scoreboard_data["current"], f, indent=2)
        if scoreboard_data["baseline"]:
            with open(artifact_dir / "baseline_metrics.json", 'w') as f:
                json.dump(scoreboard_data["baseline"], f, indent=2)
    except Exception as e:
        console.print(f"   [yellow]⚠️  Could not save metrics to cache: {e}[/yellow]")

    return LocalRunResult(
        run_root=artifact_dir,
        run_id=run_id,
        success=success,
        outcome=outcome,
        partial_success=partial_success
    )
    

