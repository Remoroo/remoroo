
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

    try:
        while True:
            try:
                # 1. Poll for work
                resp = session.post(f"{server_url}/workers/poll", json={
                    "capabilities": {"python": True, "bash": True},
                    "client_id": client_id
                }, timeout=5)
                
                if resp.status_code != 200:
                    # connection error or server down
                    pass 
                
                data = resp.json()
                
                # Check if we got a job? 
                # The current BrainServer implementation of /workers/poll:
                # It returns the NEXT request from the orchestrator queue.
                # If null/empty, it means no work.
                
                # Server returns {"step": {...} or None}
                step_data = data.get("step")
                
                if not step_data:
                    time.sleep(poll_interval)
                    continue
                
                # We got a request!
                request = ExecutionRequest(**step_data)
                
                # Special Case: workflow_complete / workflow_error handling?
                # The BrainServer puts these on the queue too?
                # Actually, BrainServer.get_next_step calls transport.get_next_request.
                # If it returns None, /workers/poll returns None.
                
                # 2. Execute Request
                typer.secho(f"\n📨 Received Job: {request.type}", fg=typer.colors.GREEN)
                
                # Execute using Worker Service
                result = worker_service.handle_request(request)
                
                # 3. Submit Result
                # We need to submit back to /jobs/result?
                # Wait, strictly speaking, /workers/poll usually just GETs.
                # But here we are using HTTP Transport concepts.
                
                # We can't use HttpTransport here because HttpTransport acts as a CLIENT to the Brain.
                # The Worker IS the client here too.
                # We need to POST the result back.
                
                # BrainServer expects result submission via... wait.
                # BrainServer wraps QueueTransport.
                # The /workers/poll endpoint pops from `transport.outbox`.
                # The /jobs/result endpoint puts into `transport.inbox`.
                
                # So we verify /jobs/result endpoint exists? 
                # Yes, in remoroo_offline/server.py.
                
                submit_resp = session.post(f"{server_url}/jobs/result", json={
                    "client_id": client_id,
                    "result": {
                        "request_id": request.request_id,
                        "success": result.success,
                        "data": result.data,
                        "error": result.error
                    }
                })
                submit_resp.raise_for_status()
                
                typer.echo("   ✅ Result submitted.")
                
            except requests.exceptions.ConnectionError:
                typer.secho("⚠️  Connection failed. Retrying...", fg=typer.colors.YELLOW)
                time.sleep(5)
            except Exception as e:
                typer.secho(f"⚠️  Error in worker loop: {e}", fg=typer.colors.RED)
                time.sleep(poll_interval)
                
    except KeyboardInterrupt:
        typer.secho("\n🛑 Worker stopped.", fg=typer.colors.YELLOW)

