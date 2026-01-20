from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
import typer

@dataclass
class LocalRunResult:
    run_root: Path
    run_id: str
    success: bool
    outcome: str

def run_local_worker(
    run_id: str,
    repo_path: Path,
    out_dir: Path,
    goal: str,
    metrics: list[str],
    brain_url: str = None,
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
    if not session_key:
         typer.echo("⚠️  REMOROO_API_KEY not set. Assuming server accepts unauthenticated requests or allow-list.")
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
            typer.secho(f"⚠️  Authentication failed (Status {auth_resp.status_code}). check REMOROO_API_KEY.", fg=typer.colors.YELLOW)
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
            time.sleep(10)
            
    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()
    
    # Initialize Execution Service (Does the work)
    worker_service = WorkerService(repo_root=str(repo_path), artifact_dir=str(artifact_dir))
    
    final_result = None
    outcome = "UNKNOWN"
    success = False
    
    typer.echo("🧠 Brain connected. Waiting for commands...")

    # Main Execution Loop (Pull Model)
    last_processed_id = None
    last_result = None
    
    while True:
        # 1. Get next step
        step = server.get_next_step(timeout=10.0, run_id=remote_run_id)
        
        # 2. Check for completion or timeout
        if step is None:
            time.sleep(0.5)
            continue
            
        # IDEMPOTENCY CHECK
        if step.request_id and step.request_id == last_processed_id:
             if last_result:
                 typer.echo(f"🔄 Resending cached result for {step.request_id}")
                 server.submit_result(last_result)
                 continue
            
        # 3. Handle Special Control Steps
        if step.type == "workflow_complete":
            final_result = step.payload
            success = final_result.get("success", False) if final_result else True
            outcome = final_result.get("decision", "COMPLETED") if final_result else "COMPLETED"
            break
            
        if step.type == "workflow_error":
            outcome = f"ERROR: {step.payload.get('error')}"
            success = False
            break
            
        # 4. Execute Step
        try:
            result = worker_service.handle_request(step)
            # Ensure request_id is preserved for the server
            if not result.request_id:
                result.request_id = step.request_id
        except Exception as e:
            import traceback
            traceback.print_exc()
            result = ExecutionResult(success=False, error=str(e), request_id=step.request_id)

        # 5. Handle Context Switching (Working Copy)
        if step.type == "create_working_copy" and result.success:
             new_root = result.data.get("working_path")
             if new_root:
                 worker_service = WorkerService(repo_root=new_root, artifact_dir=str(artifact_dir))
                 typer.echo(f"🔄 Switched execution context to: {new_root}")

        # 6. Submit Result
        server.submit_result(result)
        
        # Update Cache
        last_processed_id = step.request_id
        last_result = result
        
    return LocalRunResult(
        run_root=artifact_dir,
        run_id=run_id,
        success=success,
        outcome=outcome
    )
    

