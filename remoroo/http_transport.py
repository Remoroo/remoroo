
import requests
import time
import json
from typing import Optional
from dataclasses import asdict

# We need to import Transport base class from engine if possible, 
# or just implement the interface strictly.
# For now, we assume we can import from remoroo_offline if it is in path,
# or we just match the duck-typing of QueueTransport.
# Since CLI imports remoroo_offline, we can import Transport.

try:
    from .engine.protocol import ExecutionRequest, ExecutionResult
    # We don't need Transport base class strictly if we duck type, but good to have.
    # Defining abstract base locally if needed or import from engine if available.
    class Transport: pass 
except ImportError:
    # Fallback to remoroo_offline if running in dev/hybrid mode
    try:
        from remoroo_offline.protocol.execution_contract import ExecutionRequest, ExecutionResult
        class Transport: pass
    except ImportError:
        # Should not happen in bundled CLI
        print("CRITICAL: Could not import ExecutionRequest protocol.")
        class Transport: pass
        class ExecutionRequest: pass
        class ExecutionResult: pass

class HttpTransport(Transport):
    """
    Transport that communicates with the BrainServer via HTTP.
    """
    
    def __init__(self, base_url: str, client_id: Optional[str] = None, run_token: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.client_id = client_id or f"cli-worker-{time.time()}"
        self.run_token = run_token
        self.session = requests.Session()
        
    def send(self, request: ExecutionRequest) -> ExecutionResult:
        """
        Not used by Client Side in Polling Model usually?
        Actually, in the WorkerService model:
        - WorkerService is passive.
        - Brain is active.
        
        Wait, in the Architecture:
        Stage 2: "CLI talks to Brain via transport boundary".
        
        In `run_local.py`:
        `worker_service = WorkerService(...)`
        `while True: step = server.get_next_step()`
        
        We need to match `server.get_next_step()` interface on the CLIENT side?
        No, `run_local.py` is the ENTRYPOINT.
        
        Current `run_local.py`:
        1. Starts BrainServer (which runs Orchestrator).
        2. Starts WorkerService.
        3. Loop:
            step = server.get_next_step()
            result = worker.handle(step)
            server.submit_result(result)
            
        So we need an `HttpServerProxy` that looks like `server`:
        - `get_next_step()` -> POST /workers/poll
        - `submit_result()` -> POST /jobs/result
        
        Let's call this `HttpBrainProxy`.
        """
        raise NotImplementedError("Client does not 'send' to Brain, it polls.")

    # --- Proxy Methods matching BrainServer interface ---

    def get_next_step(self, timeout: float = 0.5, run_id: Optional[str] = None):
        """Poll the HTTP server for the next step. Returns (step, latest_metrics, baseline_metrics)."""
        try:
            body = {"client_id": self.client_id, "run_id": run_id}
            if self.run_token:
                body["run_token"] = self.run_token
            resp = self.session.post(
                f"{self.base_url}/workers/poll",
                json=body,
                timeout=timeout + 1.0
            )
            if resp.status_code != 200:
                print(f"⚠️ Poll failed: {resp.text}")
                return None, None, None
                
            data = resp.json()
            metrics = data.get("latest_metrics")
            baseline = data.get("baseline_metrics")

            if data.get("status") == "finished":
                 # Include outcome from server so CLI can display the correct decision
                 server_outcome = data.get("outcome", "UNKNOWN")
                 return ExecutionRequest(type="workflow_complete", payload={
                     "status": "finished",
                     "reason": "Run marked as completed in DB",
                     "outcome": server_outcome,
                     "decision": server_outcome,
                     "success": server_outcome == "SUCCESS",
                     "partial_success": server_outcome == "PARTIAL_SUCCESS",
                 }), metrics, baseline
                 
            step_data = data.get("step")
            if not step_data:
                return None, metrics, baseline
                
            # Reconstruct ExecutionRequest
            return ExecutionRequest(**step_data), metrics, baseline
            
        except Exception as e:
            # print(f"⚠️ Poll error: {e}")
            return None, None, None

    @staticmethod
    def _sanitize_floats(obj):
        """Recursively convert nan/inf/-inf to string representations so json.dumps won't choke."""
        import math
        if isinstance(obj, float):
            if math.isnan(obj):
                return "NaN"
            if math.isinf(obj):
                return "Infinity" if obj > 0 else "-Infinity"
            return obj
        if isinstance(obj, dict):
            return {k: HttpTransport._sanitize_floats(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [HttpTransport._sanitize_floats(v) for v in obj]
        return obj

    def submit_result(self, result: ExecutionResult) -> None:
        """Submit result to the HTTP server with retries."""
        import time
        max_retries = 5
        base_delay = 1.0
        
        # Convert to dict via asdict, ensuring JSON serializable
        payload = self._sanitize_floats(asdict(result))
        
        for attempt in range(max_retries):
            try:
                body = {"client_id": self.client_id, "result": payload}
                if self.run_token:
                    body["run_token"] = self.run_token
                resp = self.session.post(
                    f"{self.base_url}/jobs/result",
                    json=body,
                    timeout=60.0
                )
                if resp.status_code == 200:
                    return
                
                print(f"❌ Submit result failed (Attempt {attempt+1}/{max_retries}): {resp.status_code} {resp.text}")
                
            except Exception as e:
                print(f"❌ Submit result error (Attempt {attempt+1}/{max_retries}): {e}")
            
            time.sleep(base_delay * (2 ** attempt))
            
        print("CRITICAL: Failed to submit result after retries. The run will likely hang.")

    @property
    def is_finished(self) -> bool:
        # In HTTP mode, we might need to poll a status endpoint or assume logic elsewhere.
        # For now, we can check health or status.
        # Let's add a lightweight check.
        # Actually run_local loop checks this.
        # We can implement a check.
        return False # TODO: Implement proper finish check via /runs/{id}
