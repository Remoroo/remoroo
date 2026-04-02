
from pathlib import Path
from dataclasses import dataclass
import typer
import requests
import json
import threading
import time

@dataclass
class RemoteRunResult:
    run_id: str
    success: bool
    outcome: str

def run_remote_experiment(
    run_id: str,
    repo_path: Path,
    out_dir: Path,
    goal: str,
    metrics: list[str],
    verbose: bool = False,
    model: str | None = None,
    max_wall_time_s: int = 36000,
    allow_overage: bool = False,
) -> RemoteRunResult:
    """
    Connects to Remoroo Brain (Cloud/Local) as a VIEWER.
    It initiates the run but does NOT execute any steps.
    It assumes the Brain will provision a Worker (Stage 6) 
    or that a Worker is already listening.
    """
    
    from .configs import get_api_url
    API_URL = get_api_url()
    metrics_str = ", ".join(metrics)
    
    typer.echo("☁️  Connecting to Remoroo Brain...")
    
    try:
        # Check if repo_path is a local directory
        repo_path_obj = Path(repo_path).resolve()
        use_upload = False
        packed_zip = None
        
        if repo_path_obj.is_dir():
            typer.echo(f"📦 Packing local repository: {repo_path_obj}")
            from .repo_packer import pack_repo
            packed_zip = pack_repo(repo_path_obj)
            typer.echo(f"   Packed to: {packed_zip} ({packed_zip.stat().st_size / 1024:.1f} KB)")
            use_upload = True
            
        # Prepare request
        data = {
            "goal": goal,
            "metrics": metrics_str,
            "artifact_dir": str(out_dir / run_id),
            "max_wall_time_s": str(max_wall_time_s),
            "allow_overage": "true" if allow_overage else "false",
        }
        if model:
            data["model"] = model
        
        # Read API Key
        import os
        api_key = os.environ.get("REMOROO_API_KEY")
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        
        if use_upload:
            # 1. Get Upload URL
            upload_req = {
                "filename": "repo.zip",
                "content_type": "application/zip"
            }
            res = requests.post(f"{API_URL}/runs/upload_url", json=upload_req, headers=headers)
            res.raise_for_status()
            upload_info = res.json()
            upload_url = upload_info["upload_data"]["url"]
            upload_fields = upload_info["upload_data"]["fields"]
            method = upload_info["upload_data"]["method"]
            key = upload_info["key"]
            
            typer.echo(f"   Uploading to storage...")
            
            # 2. Upload File
            with open(packed_zip, "rb") as f:
                if method == "PUT":
                    # Simple PUT (Local or S3-PUT)
                    requests.put(upload_url, data=f).raise_for_status()
                else:
                    # POST (S3 Presigned Post)
                    # For S3 POST, fields come first, then file
                    requests.post(upload_url, data=upload_fields, files={"file": f}).raise_for_status()
            
            # 3. Start Run with Key
            data["repo_path"] = str(repo_path_obj) # informational
            data["s3_key"] = key
            resp = requests.post(f"{API_URL}/runs", data=data, headers=headers)
            
        else:
            # URL mode (e.g. GitHub URL)
            data["repo_path"] = str(repo_path)
            resp = requests.post(f"{API_URL}/runs", data=data, headers=headers)
            
        resp.raise_for_status()
        
        # Cleanup zip if we created it
        if packed_zip:
            try:
                os.unlink(packed_zip)
            except:
                pass
                
    except Exception as e:
        typer.secho(f"❌ Failed to create run: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    run_data = resp.json()
    remote_run_id = run_data["run_id"]
    typer.echo(f"   Run ID: {remote_run_id}")

    # Surface budget warnings from CP
    for w in run_data.get("warnings") or []:
        if w == "budget_clamped_to_balance":
            affordable = run_data.get("max_affordable_hours", {})
            eff = run_data.get("max_wall_time_s_effective", "?")
            typer.secho(f"   ⚠️  Insufficient credits — budget clamped to {eff}s.", fg=typer.colors.YELLOW)
            if affordable:
                typer.echo(f"   You can afford: Haiku {affordable.get('haiku','?')}h / Sonnet {affordable.get('sonnet','?')}h / Opus {affordable.get('opus','?')}h")
            typer.echo("   Re-run with --allow-overage to use the full budget (overage charges apply).")
        elif w == "overage_projected":
            oc = run_data.get("projected_overage_credits", "?")
            ou = run_data.get("projected_overage_usd", "?")
            typer.secho(f"   ⚠️  Overage: ~{oc} credits (~${ou}) will be billed beyond your balance.", fg=typer.colors.YELLOW)

    typer.echo("   (Viewer Mode: Waiting for remote worker to execute...)")

    outcome = "UNKNOWN"
    success = False

    # Synchronous Log Stream (Blocking)
    # Synchronous Log Stream (Blocking)
    try:
        # requests stream
        with requests.get(f"{API_URL}/runs/{remote_run_id}/stream", stream=True) as response:
            for line in response.iter_lines():
                if line:
                    decoded_line = line.decode('utf-8')
                    if decoded_line.startswith("event: finish"):
                        # Next line is data: ...
                        pass
                    elif decoded_line.startswith("data: "):
                        data_str = decoded_line[6:]
                        try:
                            data = json.loads(data_str)
                            # If we saw event: finish before? SSE lines are tricky.
                            # Standard SSE:
                            # event: finish
                            # data: {...}
                            #
                            # We can just check data content or assume mixed logic.
                            # Simpler: check if "outcome" in data to detect finish?
                            # Or track current event type.
                            
                            outcome = data.get("outcome", "UNKNOWN")
                            if outcome != "UNKNOWN":
                                break
                            
                        except json.JSONDecodeError:
                            pass
    except KeyboardInterrupt:
        typer.echo("\n🛑 Disconnected viewer.")

    return RemoteRunResult(
        run_id=remote_run_id,
        success=True, # We don't really know
        outcome=outcome
    )
