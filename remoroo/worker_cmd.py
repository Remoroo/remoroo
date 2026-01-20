
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

    # Polling Loop
    typer.echo("   Polling for jobs...")
    
    # Session for keep-alive
    session = requests.Session()

    # Heartbeat State
    current_run_id = [None] # Use list for closure mutability
    stop_heartbeat = threading.Event()

    import threading
    
    def send_heartbeat(run_id_to_send):
        try:
            requests.post(
                f"{server_url}/workers/heartbeat",
                json={
                    "run_id": run_id_to_send,
                    "client_id": "simulated-cloud-worker-1",
                    "timestamp": time.time()
                },
                timeout=5
            )
        except Exception:
            pass # Silent fail

    def heartbeat_loop():
        while not stop_heartbeat.is_set():
            run_id = current_run_id[0]
            if run_id:
                send_heartbeat(run_id)
            time.sleep(10)

    hb_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    hb_thread.start()

    def process_request(request, run_id):
        try:
            # 2. Execute Request
            typer.secho(f"\n📨 Received Job: {request.type} (Run: {run_id})", fg=typer.colors.GREEN)
            
            # Execute using Worker Service
            # handle_request is now internally thread-safe with a lock
            result = worker_service.handle_request(request)
            
            # 3. Submit Result
            # Use a fresh session or the shared one (session is thread-safe in requests)
            submit_resp = session.post(f"{server_url}/jobs/result", json={
                "client_id": "simulated-cloud-worker-1",
                "result": {
                    "request_id": request.request_id,
                    "success": result.success,
                    "data": result.data,
                    "error": result.error
                }
            })
            submit_resp.raise_for_status()
            typer.echo(f"   ✅ Result for {request.type} submitted.")
        except Exception as e:
            typer.secho(f"⚠️  Error in job processing ({request.type}): {e}", fg=typer.colors.RED)

    try:
        while True:
            try:
                # 1. Poll for work
                payload = {
                    "capabilities": {"python": True, "bash": True},
                    "client_id": "simulated-cloud-worker-1"
                }
                
                resp = session.post(f"{server_url}/workers/poll", json=payload, timeout=5)
                
                if resp.status_code != 200:
                    time.sleep(poll_interval)
                    continue 
                
                data = resp.json()
                step_data = data.get("step")
                run_id = data.get("run_id")
                
                if not step_data:
                    time.sleep(poll_interval)
                    continue
                
                # Update heartbeat run_id
                if run_id != current_run_id[0]:
                    current_run_id[0] = run_id
                    # Force immediate heartbeat to prevent "Worker Disappeared" race condition
                    threading.Thread(target=send_heartbeat, args=(run_id,), daemon=True).start()
                
                # Got a step!
                request = ExecutionRequest(**step_data)
                
                # Start job in a background thread to avoid starvation
                threading.Thread(target=process_request, args=(request, run_id), daemon=True).start()
                
            except requests.exceptions.ConnectionError:
                typer.secho("⚠️  Connection failed. Retrying...", fg=typer.colors.YELLOW)
                time.sleep(5)
            except Exception as e:
                typer.secho(f"⚠️  Error in worker loop: {e}", fg=typer.colors.RED)
                time.sleep(poll_interval)
                
    except BaseException as e:
        import traceback
        crash_log = repo_path / "worker_crash.log"
        with open(crash_log, "w") as f:
            f.write(f"Worker crashed at {time.ctime()}\n")
            f.write(f"Exception: {e}\n")
            f.write(traceback.format_exc())
        
        typer.secho(f"\n🔥 Worker Crashed! Log saved to {crash_log}", fg=typer.colors.RED)
        stop_heartbeat.set()
        if not isinstance(e, KeyboardInterrupt):
            raise e

