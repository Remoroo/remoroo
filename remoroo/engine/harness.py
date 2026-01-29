import os
import time
import signal
import threading
import subprocess
import glob
from typing import Dict, Optional, Callable, List, Set, Any
from .protocol import ExecutionResult
from .utils.system_interface import SystemInterface, RealSystem
from .utils import configs

class RemorooHarness:
    """
    Middleware to supervise interactive process execution.
    Implements:
    1. Metric-Driven Termination (stops when artifacts appear).
    2. Signal Ladder (SIGTERM -> SIGKILL).
    3. Stream Capture (non-blocking stdout/stderr).
    """

    def __init__(self, system: Optional[SystemInterface] = None):
        self.system = system or RealSystem()
        self.output_callback = None

    def run(self, 
            cmd: str, 
            runner_factory: Callable[..., subprocess.Popen], 
            timeout: float, 
            env: Dict[str, str],
            artifact_dir: str,
            repo_root: Optional[str] = None,
            stdout_buffer: Optional[List[str]] = None,
            stderr_buffer: Optional[List[str]] = None,
            output_callback: Optional[Callable[[str, str], None]] = None) -> ExecutionResult:
        """
        Execute command with supervision.
        """
        self.output_callback = output_callback
        # 1. Capture initial state for artifact watching
        if not self.system.fs.exists(artifact_dir):
            self.system.fs.makedirs(artifact_dir, exist_ok=True)
        
        # CLEAR stale artifacts (Whack-a-Mole Prevention)
        self._purify_artifacts(artifact_dir)
        if repo_root:
            self._purify_artifacts(repo_root) # Also clear CWD metrics

        initial_artifacts = self._scan_artifacts(artifact_dir)
        if repo_root:
            # Also watch root metrics.json
            root_metrics = os.path.join(repo_root, configs.METRICS_FILENAME)
            if self.system.fs.exists(root_metrics):
                 initial_artifacts[configs.METRICS_FILENAME] = self.system.fs.getmtime(root_metrics)
        
        # 2. Spawn Process
        try:
             # Pass artifact_dir in env so process knows where to write (if not already set)
             if "REMOROO_ARTIFACTS_DIR" not in env:
                 env["REMOROO_ARTIFACTS_DIR"] = artifact_dir
             process = runner_factory(cmd, env=env)
        except Exception as e:
            return ExecutionResult(success=False, error=f"Failed to spawn process: {e}")

        # 3. Spawn IO Threads
        # Use provided buffers or create local ones
        stdout_buf = stdout_buffer if stdout_buffer is not None else []
        stderr_buf = stderr_buffer if stderr_buffer is not None else []
        self._start_io_threads(process, stdout_buf, stderr_buf)

        # 4. Supervision Loop
        start_time = self.system.clock.time()
        exit_trigger = "unknown"
        metrics_captured = False
        
        # Helper to detect new or modified files
        def get_fresh_artifacts():
            current = self._scan_artifacts(artifact_dir)
            fresh = set()
            for fname, mtime in current.items():
                if fname not in initial_artifacts:
                    fresh.add(fname)
                elif mtime > initial_artifacts[fname]:
                     fresh.add(fname)
            
            # Check CWD metrics if repo_root provided
            if repo_root:
                cwd_mpath = os.path.join(repo_root, configs.METRICS_FILENAME)
                if self.system.fs.exists(cwd_mpath):
                    mtime = self.system.fs.getmtime(cwd_mpath)
                    if configs.METRICS_FILENAME not in initial_artifacts or mtime > initial_artifacts[configs.METRICS_FILENAME]:
                        fresh.add(f"legacy:{configs.METRICS_FILENAME}")
            return fresh

        try:
            while True:
                # A. Check Natural Exit
                if process.poll() is not None:
                    exit_trigger = "natural_exit"
                    break
                
                # B. Check Metrics (Early Exit)
                new_artifacts = get_fresh_artifacts()
                
                # Look for partial metrics OR standard metrics.json updates
                found_metric = any(
                    (f.startswith("partial_") or f == configs.METRICS_FILENAME or f == "current_metrics.json" or f == "baseline_metrics.json" or f == f"legacy:{configs.METRICS_FILENAME}") and f.endswith(".json") 
                    for f in new_artifacts
                )
                
                if found_metric:
                    print(f"✨ [Harness] Metric artifact detected among {len(new_artifacts)} fresh files! Initiating safe shutdown.")
                    exit_trigger = "metric_detected"
                    metrics_captured = True
                    self._terminate_ladder(process)
                    break
                
                # C. Check Timeout
                if timeout and (self.system.clock.time() - start_time > timeout):
                    print(f"⏱️ [Harness] Timeout reached ({timeout}s). Killing process.")
                    exit_trigger = "timeout"
                    self._terminate_ladder(process)
                    break
                
                # Low latency poll
                self.system.clock.sleep(0.1)
                
        except KeyboardInterrupt:
            self._terminate_ladder(process)
            raise
            
        # 5. Result Construction
        return_code = process.returncode
        
        # FINAL SCAN: Catch artifacts written just before exit
        new_artifacts = get_fresh_artifacts()
        
        # Success definition: 
        # - "metric_detected" is ALWAYS success (we got data).
        # - "natural_exit" is success if return_code == 0.
        # - "timeout" is failure.
        
        success = False
        if exit_trigger == "metric_detected":
            success = True
        elif exit_trigger == "natural_exit" and return_code == 0:
            success = True

        return ExecutionResult(
            success=success,
            data={
                "stdout": "".join(stdout_buf),
                "stderr": "".join(stderr_buf),
                "exit_code": return_code,
                "trigger": exit_trigger,
                "duration": self.system.clock.time() - start_time,
                "new_artifacts": list(new_artifacts)
            }
        )

    def _scan_artifacts(self, path: str) -> Dict[str, float]:
        """Return dict of {filename: mtime}."""
        try:
            res = {}
            for f in self.system.fs.listdir(path):
                full_path = os.path.join(path, f)
                try:
                    res[f] = self.system.fs.getmtime(full_path)
                except OSError: pass # File might disappear
            return res
        except (FileNotFoundError, PermissionError):
            return {}
        except OSError as e:
            if e.errno == 13: # EACCES
                print(f"⚠️ [Harness] PERMISSION_ERROR: Could not scan '{path}': {e}")
                return {}
            raise

    def _purify_artifacts(self, artifact_dir: str):
        """Purge stale metrics and partials to ensure deterministic capture."""
        targets = [configs.METRICS_FILENAME, "baseline_metrics.json", "current_metrics.json"]
        
        # 1. Clear explicit targets
        for t in targets:
            tp = os.path.join(artifact_dir, t)
            if self.system.fs.exists(tp):
                try:
                    self.system.fs.remove(tp)
                except Exception as e:
                    # Structured warning for permission issues (Docker Root shadow case)
                    msg = str(e)
                    if "Permission denied" in msg or "Errno 13" in msg:
                        print(f"⚠️ [Harness] PERMISSION_DENIED: Cannot delete stale '{t}'. Running in mixed-permission mode.")
                    else:
                        print(f"⚠️ [Harness] Error purging stale artifact '{t}': {e}")
        
        # 2. Clear partials (glob)
        partials = self.system.fs.glob(os.path.join(artifact_dir, "partial_*.json"))
        for p in partials:
            try:
                self.system.fs.remove(p)
            except Exception as e:
                print(f"⚠️ [Harness] PERMISSION_ERROR: Could not purge stale partial '{os.path.basename(p)}': {e}")

    def _terminate_ladder(self, process: subprocess.Popen):
        """
        Apply signal ladder: TERM -> Wait -> KILL.
        Handles process that might already be dead.
        Uses process group kill if available to ensure NO ORPHANS.
        """
        if process.poll() is not None:
            return

        # Attempt process group kill
        use_pg = True
        pgid = None
        if use_pg:
            try:
                pgid = self.system.proc.get_pgid(process.pid)
            except:
                use_pg = False

        def kill_func(sig):
            if use_pg and pgid:
                self.system.proc.kill_group(process.pid, sig)
            else:
                process.send_signal(sig)

        try:
            kill_func(signal.SIGTERM)
            try:
                process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                kill_func(signal.SIGKILL)
                process.wait(timeout=1.0)
        except OSError:
            # Process might have died in between poll and signal
            pass

    def _start_io_threads(self, process, stdout_buf, stderr_buf):
        def reader(stream, buf, stream_name):
            try:
                for line in iter(stream.readline, ''):
                    if not line: break
                    buf.append(line)
                    if self.output_callback:
                        stripped = line.rstrip()
                        # Pass raw stripped line, let callback handle stream labels
                        self.output_callback(stripped, stream_name)
            except: pass # Stream closed
            finally:
                if stream:
                    stream.close()

        if process.stdout:
            threading.Thread(target=reader, args=(process.stdout, stdout_buf, "stdout"), daemon=True).start()
        if process.stderr:
            threading.Thread(target=reader, args=(process.stderr, stderr_buf, "stderr"), daemon=True).start()
