
import typer
import time
import sys
import os
import requests
from pathlib import Path

from .http_transport import HttpTransport
from .http_transport import HttpTransport
from .engine.local_worker import WorkerService
from .engine.protocol import ExecutionRequest, ExecutionResult

def worker(
    repo_path: Path = typer.Option(..., "--repo", help="Path to the repository to work on."),
    server_url: str = typer.Option(None, "--server", help="URL of the Brain Server."),
    poll_interval: float = typer.Option(1.0, "--interval", help="Polling interval in seconds."),
):
    from .configs import get_api_url
    from .engine.utils.doctor import ensure_ready
    
    # Pre-flight checks
    ensure_ready()

    if server_url is None:
        server_url = get_api_url()
    """
    Run a standalone Worker that polls the Brain for jobs.
    Simulates a Cloud Worker.
    """
    typer.secho(f"👷 Starting Remote Worker...", fg=typer.colors.BLUE)
    typer.echo(f"   Server: {server_url}")
    typer.echo(f"   Repo:   {repo_path}")

    # Ensure repo exists (in real cloud worker, we would clone it here)
    if not repo_path.exists():
        typer.secho(f"❌ Repo path does not exist: {repo_path}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    # Initialize Worker Service
    # We use a temp artifact dir for the worker
    artifact_dir = repo_path / "artifacts"
    worker_service = WorkerService(str(repo_path), str(artifact_dir))

    import socket
    import uuid
    
    # Generate Unique Identity
    hostname = socket.gethostname()
    short_id = str(uuid.uuid4())[:8]
    client_id = f"worker-{hostname}-{short_id}"

    # Polling Loop
    typer.echo(f"   🆔 Client ID: {client_id}")
    typer.echo("   Polling for jobs...")
    
    # Session for keep-alive
    session = requests.Session()
    
    # --- Background Heartbeat Thread ---
    active_run_id = None
    heartbeat_stop_event = threading.Event()

    def heartbeat_loop():
        while not heartbeat_stop_event.is_set():
            if active_run_id:
                try:
                    session.post(f"{server_url}/workers/heartbeat", json={
                        "run_id": active_run_id,
                        "client_id": client_id,
                        "timestamp": time.time()
                    }, timeout=5)
                except Exception:
                    pass
            time.sleep(5)

    hb_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    hb_thread.start()

    try:
        while True:
            try:
                # 1. Poll for work
                resp = session.post(f"{server_url}/workers/poll", json={
                    "capabilities": {"python": True, "bash": True},
                    "client_id": client_id,
                    "run_id": active_run_id
                }, timeout=5)
                
                if resp.status_code != 200:
                    time.sleep(poll_interval)
                    continue 
                
                data = resp.json()
                step_data = data.get("step")
                
                if not step_data:
                    # If server says finished, clear active run
                    if data.get("status") == "finished":
                        active_run_id = None
                    time.sleep(poll_interval)
                    continue
                
                # We got a request!
                request = ExecutionRequest(**step_data)
                
                # Update background heartbeat target
                active_run_id = request.run_id
                
                # 2. Execute Request
                typer.secho(f"\n📨 Received Job: {request.type}", fg=typer.colors.GREEN)
                
                # Execute using Worker Service
                result = worker_service.handle_request(request)
                
                # 3. Submit Result
                submit_resp = session.post(f"{server_url}/jobs/result", json={
                    "client_id": client_id,
                    "result": {
                        "request_id": request.request_id,
                        "success": result.success,
                        "data": result.data,
                        "error": result.error,
                        "metrics": result.metrics
                    }
                })
                submit_resp.raise_for_status()
                
                typer.echo("   ✅ Result submitted.")
                
                # Check if this was a terminal step
                if request.type in ["workflow_complete", "workflow_error", "run_complete"]:
                    active_run_id = None
                
            except requests.exceptions.ConnectionError:
                typer.secho("⚠️  Connection failed. Retrying...", fg=typer.colors.YELLOW)
                time.sleep(5)
            except Exception as e:
                typer.secho(f"⚠️  Error in worker loop: {e}", fg=typer.colors.RED)
                time.sleep(poll_interval)
                
    except KeyboardInterrupt:
        heartbeat_stop_event.set()
        typer.secho("\n🛑 Worker stopped.", fg=typer.colors.YELLOW)

