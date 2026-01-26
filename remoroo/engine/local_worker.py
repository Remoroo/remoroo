from typing import Dict, Any, Optional, Callable
from .protocol import ExecutionRequest, ExecutionResult
from .core.worker import Worker
from .utils import fs_utils, syntax_validator
from .core import context_packer, env_setup, executor, applier
import shutil
import uuid
import os
import json

class WorkerService:
    """
    Service entrypoint for Validated Execution.
    Handles ExecutionRequests and returns ExecutionResults.
    """
    
    def __init__(self, repo_root: str, artifact_dir: str, original_repo_root: Optional[str] = None, run_id: Optional[str] = None, engine: str = "docker", output_callback: Optional[Callable] = None):
        self.output_callback = output_callback
        self._log("🔧 WorkerService (Patched) Loaded")
        import tempfile
        self.repo_root = repo_root
        self.original_repo_root = original_repo_root or repo_root # Keep reference to original
        # Infer is_ephemeral if repo_root is different from original_repo_root
        self.is_ephemeral = (self.repo_root != self.original_repo_root) 
        self.artifact_dir = artifact_dir
        self.run_id = run_id
        self.engine = engine.lower()
        self.worker = Worker(repo_root=repo_root, artifact_dir=artifact_dir)
        
        # Initialize Sandbox (Lazy start)
        # Conditional initialization: Only if engine is 'docker'
        self.sandbox = None
        if self.engine == "docker":
            from .sandbox import DockerSandbox
            self.sandbox = DockerSandbox(repo_root, artifact_dir)
        else:
            self._log(f"ℹ️  Execution Engine: {self.engine.upper()} (Sandbox Disabled)")
        
        # Async Execution Tracking
        # Dictionary mapping execution_id (str) -> subprocess.Popen object
        # Note: In a real distributed system this would be in Redis/DB,
        # but for local worker memory is fine.
        self._running_processes: Dict[str, Any] = {}
        self._execution_buffers: Dict[str, Any] = {} # Store stdout/stderr buffers
        
    def _log(self, message: str):
        """Internal logger that redirects to output_callback or standard print."""
        if self.output_callback:
            try:
                self.output_callback(message)
            except:
                print(message)
        else:
            print(message)
        
    def _robust_extract_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Robustly extract numerical metrics from various possible JSON structures.
        Handles:
        1. Flat dict: {"runtime_s": 10.2}
        2. Nested metrics: {"metrics": {"runtime_s": 10.2}}
        3. Metrics with units: {"metrics_with_units": {"runtime_s": {"value": 10.2, "unit": "s"}}}
        """
        metrics = {}
        
        # 1. Check "metrics" key (Brain standard)
        if "metrics" in data and isinstance(data["metrics"], dict):
            for k, v in data["metrics"].items():
                if isinstance(v, (int, float)):
                    metrics[k] = v
        
        # 2. Check "metrics_with_units" key (Core standard)
        if "metrics_with_units" in data and isinstance(data["metrics_with_units"], dict):
            for k, v in data["metrics_with_units"].items():
                if isinstance(v, dict) and "value" in v:
                    val = v["value"]
                    if isinstance(val, (int, float)):
                        metrics[k] = val
        
        # 3. Fallback to top-level keys (Standard monitor)
        blacklist = ["created_at", "source", "version", "phase", "metrics_with_units", "metrics", "target_files", "baseline_metrics"]
        for k, v in data.items():
            if k in blacklist: continue
            if isinstance(v, (int, float)):
                if k not in metrics: # Don't overwrite if already found in structured fields
                    metrics[k] = v
        
        return metrics

    def _extract_metrics_from_text(self, text: str) -> Dict[str, Any]:
        """Simple regex fallback to capture key=value metrics from stdout/stderr."""
        import re
        metrics = {}
        # Pattern: key=value (where value is a number)
        # We look for lines like 'runtime_s=10.25' or 'accuracy: 0.99'
        patterns = [
            r'([a-zA-Z0-9_]+)\s*[:=]\s*([0-9.]+)',
        ]
        for line in text.splitlines():
            for p in patterns:
                matches = re.findall(p, line)
                for k, v in matches:
                    try:
                        # Skip common false positives or very long numbers that might be hashes
                        if len(k) < 30 and len(v) < 20:
                             metrics[k] = float(v)
                    except: pass
        return metrics

        
    def _finalize_artifacts_internal(self, dest_filename: str = "final_patch.diff") -> list[str]:
        """Internal helper to generate diff and save artifacts. Returns list of finalized files."""
        try:
            from ..execution.repo_manager import generate_diff
            finalized = []
            
            # Use original_repo_root and current repo_root for diff
            if self.is_ephemeral and self.repo_root != self.original_repo_root:
                self._log(f"💼 Finalizing Implementation Patch ({dest_filename})...")
                self._log(f"📍 Original Repo: {self.original_repo_root}")
                self._log(f"📍 Current Repo:  {self.repo_root}")
                
                if os.path.exists(self.repo_root):
                    from ..execution.repo_manager import get_modified_files, IGNORED_PATTERNS
                    all_modified = get_modified_files(self.original_repo_root, self.repo_root)
                    
                    # Filter for code files only (reduce noise)
                    code_extensions = {
                        '.py', '.js', '.ts', '.tsx', '.jsx', '.html', '.css', 
                        '.json', '.yaml', '.yml', '.md', '.sql', '.sh', '.bash', '.zsh',
                        '.toml', '.lock', '.txt', '.cfg', '.ini',
                        '.rs', '.go', '.java', '.cpp', '.c', '.h', '.hpp', '.cc', '.cxx', '.hh', '.hxx',
                        '.make', '.cmake', '.proto', '.sql', '.xml'
                    }
                    
                    code_files = [
                        f for f in all_modified 
                        if any(f.endswith(ext) for ext in code_extensions)
                        and not any(ignored in f for ignored in IGNORED_PATTERNS)
                    ]
                    
                    if code_files:
                        self._log(f"📊 Generating filtered patch for {len(code_files)} code files...")
                        diff_content = generate_diff(self.original_repo_root, self.repo_root, files=code_files)
                        
                        if diff_content:
                            # 🎯 ROUTE TO RUN-SPECIFIC OUTPUT 
                            if self.run_id:
                                dest_dir = os.path.join(self.original_repo_root, ".remoroo", "runs", self.run_id)
                                os.makedirs(dest_dir, exist_ok=True)
                                dest_diff = os.path.join(dest_dir, dest_filename)
                            else:
                                dest_diff = os.path.join(self.original_repo_root, dest_filename)
                                
                            with open(dest_diff, 'w', encoding='utf-8') as f:
                                f.write(diff_content)
                            
                            # ALSO save to artifact_dir (for CLI transparency)
                            if self.artifact_dir:
                                cache_diff = os.path.join(self.artifact_dir, dest_filename)
                                with open(cache_diff, 'w', encoding='utf-8') as f:
                                    f.write(diff_content)
                            
                            finalized.append(dest_filename)
                            self._log(f"✅ Saved {dest_filename} to {dest_diff}")
                        else:
                            self._log("ℹ️  No significant changes detected in code files.")
                    else:
                        self._log("ℹ️  No modified code files found (skipping patch).")
                else:
                    self._log(f"⚠️  Cannot generate diff: source directory {self.repo_root} no longer exists")
                    
            return finalized
        except Exception as e:
            self._log(f"⚠️  Artifact finalization failed: {e}")
            return []

    def handle_request(self, request: ExecutionRequest) -> ExecutionResult:
        """Dispatch request to appropriate Worker method and ensure contract metadata."""
        result = self._handle_request_internal(request)
        if result is None:
            result = ExecutionResult(success=False, error="Handler returned None")
        if result.request_id is None:
            result.request_id = request.request_id
        return result

    def _resolve_repo_root(self, request: ExecutionRequest) -> str:
        """
        Resolve the target repository root for this request.
        Priority:
        1. Explicit `repo_root` in payload (Stateless)
        2. `self.repo_root` (Stateful fallback)
        """
        explicit_root = request.payload.get("repo_root")
        if explicit_root:
            # We trust the Brain's explicit instruction
            return explicit_root
        return self.repo_root

    def _get_worker_env(self, extra_env: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """
        Build the environment for local command execution.
        Injects venv PATH to ensure isolated execution even without Docker.
        """
        env = os.environ.copy()
        
        # 1. Inject Venv PATH
        # We check for 'venv' or '.venv' in repo_root
        for venv_name in ["venv", ".venv"]:
            venv_path = os.path.join(self.repo_root, venv_name)
            if os.path.isdir(venv_path):
                # Determine bin dir (bin on Linux/Mac, Scripts on Windows)
                import sys
                bin_name = "Scripts" if sys.platform == "win32" else "bin"
                bin_path = os.path.join(venv_path, bin_name)
                
                if os.path.isdir(bin_path):
                    # Prepend to PATH
                    current_path = env.get("PATH", "")
                    env["PATH"] = f"{bin_path}{os.pathsep}{current_path}"
                    # Also set VIRTUAL_ENV
                    env["VIRTUAL_ENV"] = venv_path
                    # Remove PYTHONHOME if set (interference)
                    if "PYTHONHOME" in env:
                        del env["PYTHONHOME"]
                    break
        
        # 2. Inject Remoroo defaults
        env["REMOROO_ARTIFACTS_DIR"] = os.path.join(self.repo_root, "artifacts")
        env["PYTHONUNBUFFERED"] = "1"
        
        # 3. Apply extra env from request
        if extra_env:
            for k, v in extra_env.items():
                if k:
                    env[str(k)] = str(v)
                    
        return env

    def _handle_request_internal(self, request: ExecutionRequest) -> ExecutionResult:
        """Dispatch request to appropriate Worker method."""
        # 1. Resolve Target Context
        target_root = self._resolve_repo_root(request)
        if target_root != self.repo_root:
            self._log(f"🔄 Stateless Context Switch: {target_root}")
        
        # 2. Context Switch (Temp)
        # We swap self.repo_root temporarily so that internal methods using self.repo_root
        # (like RepoIndexer, executor, etc.) work on the correct path.
        # This makes the stateless request effective even for legacy code.
        previous_root = self.repo_root
        self.repo_root = target_root
        
        try:

            if request.type == "scan_repository":
                force = request.payload.get("force_refresh", False)
                structure = fs_utils.scan_repository(self.repo_root, self.artifact_dir, force)
                return ExecutionResult(success=True, data=structure)
                
            elif request.type == "file_exists":
                return self._handle_file_exists(request)
            
            elif request.type == "is_data_file":
                path = request.payload.get("path", "")
                is_data = fs_utils.is_data_file(path, self.repo_root)
                return ExecutionResult(success=True, data={"is_data_file": is_data})
                
            elif request.type == "build_context":
                # Extract args
                p = request.payload
                context = context_packer.build_context_pack(
                    repo_root=self.repo_root,
                    turn_index=p.get("turn_index", -1),
                    focus_files=p.get("focus_files", []),
                    previous_turn_outcomes=p.get("previous_turn_outcomes"),
                    max_files=p.get("max_files", 50),
                    max_total_bytes=p.get("max_total_bytes", 200000),
                    max_total_chars=p.get("max_total_chars", 200000),
                    deny_paths=p.get("deny_paths", []),
                    deny_writing_data_folders=p.get("deny_writing_data_folders", True),
                    allowed_data_folders=p.get("allowed_data_folders", []),
                    goal=p.get("goal"),
                    use_semantic_chunking=p.get("use_semantic_chunking", False),
                    max_chars_per_file=p.get("max_chars_per_file", 50000),
                    min_relevance_threshold=p.get("min_relevance_threshold", 0.3),
                    previous_context_pack=p.get("previous_context_pack")
                )
                return ExecutionResult(success=True, data=context)
            
            elif request.type == "index_repository":
                force = request.payload.get("force", False)
                # Use explicit repo_root from payload if provided (stateless protocol)
                target_root = request.payload.get("repo_root") or self.repo_root
                # RepoIndexer is imported lazily
                from .core.repo_indexer import RepoIndexer
                indexer = RepoIndexer(target_root)
                index_data = indexer.index(force=force)
                return ExecutionResult(success=True, data=index_data)

            elif request.type == "env_setup":
                p = request.payload
                
                # P0 Safer Sandbox: Always use Sandbox if available
                if self.sandbox and self.sandbox.available:
                    # Simplified Sandbox Install
                    # We just run install commands inside container
                    install_commands = p.get("install_commands", [])
                    cmds_to_run = []
                    
                    # Basic auto-detect logic for sandbox
                    if not install_commands and not p.get("skip_auto_install", False):
                        if os.path.exists(os.path.join(self.repo_root, "requirements.txt")):
                            cmds_to_run.append("pip install -r requirements.txt")
                        elif os.path.exists(os.path.join(self.repo_root, "setup.py")):
                            cmds_to_run.append("pip install -e .")
                        elif os.path.exists(os.path.join(self.repo_root, "pyproject.toml")):
                            cmds_to_run.append("pip install .")
                    elif install_commands:
                        cmds_to_run = install_commands
                        
                    setup_log = []
                    failed = False
                    start_t = os.times().elapsed
                    
                    # Ensure container running
                    self.sandbox.start()
                    
                    for cmd in cmds_to_run:
                        self._log(f"📦 Sandbox Install: {cmd}")
                        res = self.sandbox.exec_run(cmd.split())
                        setup_log.append(f"> {cmd}\n{res['stdout']}\n{res['stderr']}")
                        if res['exit_code'] != 0:
                            failed = True
                            break
                            
                    return ExecutionResult(
                        success=not failed,
                        data={
                            "diagnosis": "Sandbox setup completed" if not failed else "Sandbox setup failed",
                            "commands_run": cmds_to_run,
                            "setup_duration_s": os.times().elapsed - start_t
                        },
                        logs="\n".join(setup_log)
                    )
                else:
                    # Fallback to local (Legacy/Unsafe)
                    result = env_setup.execute_env_setup(
                        repo_root=self.repo_root,
                        install_commands=p.get("install_commands"),
                        timeout_s=p.get("timeout", 300),
                        use_venv=p.get("use_venv", True),
                        always_create_venv=p.get("always_create_venv", False),
                        skip_auto_install=p.get("skip_auto_install", False)
                    )
                    
                    return ExecutionResult(
                        success=result.success,
                        data={
                            "diagnosis": result.diagnosis,
                            "commands_run": result.commands_run,
                            "setup_duration_s": result.setup_duration_s,
                            "venv_python": getattr(result, "venv_python", None)
                        },
                        error=result.error_message
                    )
                
            elif request.type == "env_smoke_test":
                p = request.payload
                cmd = p.get("smoke_test_cmd", "")
                timeout = p.get("timeout_s", 30)
                
                runner = None
                if self.sandbox and self.sandbox.available:
                    def sandbox_runner(cmd, env=None):
                        return self.sandbox.exec_popen(cmd, env=env or {})
                    runner = sandbox_runner
                
                outcome = executor.run_command_with_timeout(
                    cmd=cmd,
                    cwd=self.repo_root,
                    timeout_s=timeout,
                    show_progress=True,
                    output_callback=self.output_callback,
                    env=self._get_worker_env(),
                    runner_factory=runner
                )
                
                return ExecutionResult(success=True, data={
                    "smoke_test_passed": outcome.get("exit_code") == 0,
                    "error_output": outcome.get("stderr") or outcome.get("stdout") or "",
                    "output": outcome.get("stdout")
                })

            elif request.type == "env_apply_fix":
                p = request.payload
                commands = p.get("commands", [])
                timeout = p.get("timeout_s", 120)
                
                runner = None
                if self.sandbox and self.sandbox.available:
                    def sandbox_runner(cmd, env=None):
                        return self.sandbox.exec_popen(cmd, env=env or {})
                    runner = sandbox_runner
                
                exec_env = self._get_worker_env()
                
                outcomes = []
                success = True
                for cmd in commands:
                    outcome = executor.run_command_with_timeout(
                        cmd=cmd,
                        cwd=self.repo_root,
                        timeout_s=timeout,
                        show_progress=True,
                        output_callback=self.output_callback,
                        env=exec_env,
                        runner_factory=runner
                    )
                    outcomes.append(cmd)
                    if outcome.get("exit_code") != 0:
                        success = False
                        break
                
                return ExecutionResult(success=True, data={
                    "success": success,
                    "outcomes": outcomes
                })

            elif request.type == "instrumentation_prepare":
                from ..execution import instrumentation_pipeline, instrumentation_targets
                from ..engine.core.repo_indexer import RepoIndexer
                from ..execution.file_access_tracker import FileAccessTracker
                
                # Use explicit repo_root from payload if provided (stateless protocol)
                target_root = request.payload.get("repo_root") or self.repo_root
                repo_index = RepoIndexer(target_root).index(force=True)
                repo_index_summary = instrumentation_pipeline._summarize_repo_index(repo_index)
                
                contract = request.payload.get("experiment_contract") or {}
                command_plan = contract.get("command_plan") or {}
                commands_flat = instrumentation_pipeline._flatten_command_plan(command_plan)
                
                metric_specs = contract.get("metric_specs") or []
                metric_names = [m.get("name") for m in metric_specs if isinstance(m, dict) and m.get("name")]

                targets = instrumentation_targets.select_instrumentation_targets(
                    repo_root=target_root,
                    commands=commands_flat,
                    metric_names=[m for m in metric_names if isinstance(m, str)],
                    select_top_n=5
                )

                # Promote top-K files
                promoted_files = (targets.get("selected_files") or [])[:5]
                instrumentation_files_state = {}
                file_access_tracker = FileAccessTracker()
                for fp in promoted_files:
                    if not isinstance(fp, str) or not fp: continue
                    abs_path = os.path.join(target_root, fp)
                    if not os.path.exists(abs_path) or not os.path.isfile(abs_path): continue
                    try:
                        with open(abs_path, "r", encoding="utf-8", errors="replace") as f:
                             content = f.read()
                        instrumentation_files_state[fp] = {
                            "exists": True,
                            "content": content,
                            "issues": [],
                            "syntax_errors": []
                        }
                        file_access_tracker.mark_full(fp)
                    except Exception:
                        continue

                return ExecutionResult(success=True, data={
                    "repo_index_summary": repo_index_summary,
                    "instrumentation_targets": targets,
                    "instrumentation_files_state": instrumentation_files_state
                })

            elif request.type == "instrumentation_apply":
                payload = request.payload
                patch_proposal = payload.get("patch_proposal", {})
                instrumentation_manifest = payload.get("instrumentation_manifest", {})
                # Use explicit repo_root from payload if provided (stateless protocol)
                target_root = payload.get("repo_root") or self.repo_root
                
                if not patch_proposal:
                    return ExecutionResult(success=True, data={"applied": False, "reason": "No patch"})
                
                try:
                    applied, skipped = applier.apply_patchproposal(
                        repo_root=target_root,
                        patch=patch_proposal
                    )
                    
                    # Save manifest locally? Or Brain does it?
                    # Brain might want worker to save it implies artifact dir access.
                    # We can save it if needed, but for now just return success.
                    # Usually Brain sends the manifest to be saved.
                    manifest_path = os.path.join(self.artifact_dir, "instrumentation_manifest.json")
                    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
                    with open(manifest_path, 'w') as f:
                        json.dump(instrumentation_manifest, f, indent=2)

                    return ExecutionResult(success=True, data={"applied": applied, "skipped": skipped})
                except Exception as e:
                    return ExecutionResult(success=False, error=f"Patch application failed: {str(e)}")

            elif request.type == "instrumentation_run_baseline":
                p = request.payload
                commands = p.get("commands", [])
                timeout = p.get("timeout_s", 120)
                env_vars = p.get("env", {}).copy()
                
                runner = None
                if self.sandbox and self.sandbox.available:
                    # Inside Docker: use container path
                    env_vars["REMOROO_ARTIFACTS_DIR"] = "/app/workdir/artifacts"
                    def sandbox_runner(cmd, env=None):
                        return self.sandbox.exec_popen(cmd, env=env or {})
                    runner = sandbox_runner
                else:
                    # Local execution: use host path
                    exec_env = self._get_worker_env(env_vars)
                
                outcomes = []
                success = True
                for cmd in commands:
                    outcome = executor.run_command_with_timeout(
                        cmd=cmd,
                        cwd=self.repo_root,
                        timeout_s=timeout,
                        show_progress=True,
                        output_callback=self.output_callback,
                        env=env_vars,
                        runner_factory=runner
                    )
                    outcomes.append(outcome)
                    if outcome.get("exit_code") != 0:
                        success = False
                        # Don't break immediately? Baseline usually runs all.
                
                # --- AUTO-CAPTURE METRICS (SANDBOX DIRECT READ) ---
                captured_metrics = {}
                
                # 1. Read directly via Sandbox (Primary)
                # This bypasses all host permission issues since we use Docker to read Docker files.
                if self.sandbox and self.sandbox.available:
                     # Helper to read and update
                     def read_sandbox_json(path):
                         res = self.sandbox.exec_run(["cat", path])
                         if res.get("exit_code") == 0:
                            try:
                                loaded = json.loads(res["stdout"])
                                captured_metrics.update(self._robust_extract_metrics(loaded))
                            except Exception as e:
                                self._log(f"DEBUG: JSON Sandbox Error: {e}")
                                pass

                     # Check known paths
                     read_sandbox_json("/app/workdir/artifacts/metrics.json")
                     read_sandbox_json("/app/workdir/artifacts/baseline_metrics.json")
                     if not captured_metrics:
                         read_sandbox_json("/app/workdir/metrics.json")
                     
                     # Fire-and-forget permission fix for cleanup (don't block capture)
                     try:
                         uid, gid = os.getuid(), os.getgid()
                         self.sandbox.exec_run(["chown", "-R", f"{uid}:{gid}", "/app/workdir/artifacts"])
                         self.sandbox.exec_run(["chmod", "-R", "777", "/app/workdir/artifacts"])
                     except: pass

                # 2. Host Read Fallback (only if sandbox failed or empty)
                if not captured_metrics:
                    host_artifacts = os.path.join(self.repo_root, "artifacts", "metrics.json")
                    host_root = os.path.join(self.repo_root, "metrics.json")
                    host_baseline = os.path.join(self.repo_root, "artifacts", "baseline_metrics.json")
                    
                    for mpath in [host_baseline, host_artifacts, host_root]:
                        if os.path.exists(mpath):
                              try:
                                  with open(mpath, 'r') as f:
                                      loaded = json.load(f)
                                      captured_metrics.update(self._robust_extract_metrics(loaded))
                              except: pass
                if not captured_metrics:
                      # 3. Last Resort: Parse from LOGS (stdout)
                      for outcome in outcomes:
                          stdout = outcome.get("stdout", "")
                          log_metrics = self._extract_metrics_from_text(stdout)
                          if log_metrics:
                               self._log(f"📊 [DEBUG] Captured metrics from LOGS: {log_metrics}")
                               captured_metrics.update(log_metrics)

                if captured_metrics:
                     self._log(f"📊 [DEBUG] Baseline Step Captured: {captured_metrics}")
                     pass
                else:
                     self._log(f"⚠️ [DEBUG] Metrics Capture Failed (Sandbox & Host). No metrics found in artifacts")
                     pass

                # Ensure we populate baseline_metrics for protocol, but ALSO metrics for Current scoreboard
                # NOTE: baseline_metrics goes into data, metrics goes into specific field
                return ExecutionResult(success=True, data={
                    "success": success,
                    "outcomes": outcomes,
                    "baseline_metrics": captured_metrics
                }, metrics=captured_metrics)

            elif request.type == "validate_syntax":
                file_path = request.payload.get("file_path")
                if not file_path:
                    return ExecutionResult(success=False, error="file_path required")
                abs_path = os.path.join(self.repo_root, file_path)
                is_valid, error = syntax_validator.validate_python_syntax(abs_path)
                return ExecutionResult(success=True, data={"is_valid": is_valid, "error_message": error})
                
            elif request.type == "execute_plan":
                p = request.payload
                # Build execution environment with required vars
                exec_env = os.environ.copy()
                
                # Define Runner Factory and set appropriate artifacts path
                runner = None
                if self.sandbox and self.sandbox.available:
                    # Inside Docker: use container path
                    exec_env["REMOROO_ARTIFACTS_DIR"] = "/app/workdir/artifacts"
                    def sandbox_runner(cmd, env=None):
                        # Convert cmd string to list if strict? exec_popen handles str
                        # env argument to exec_popen expects Dict
                        return self.sandbox.exec_popen(cmd, env=env or {})
                    runner = sandbox_runner
                else:
                    # Local execution: use host path
                    exec_env = self._get_worker_env()
                    
                command_results = executor.run_command_plan(
                    repo_root=self.repo_root,
                    command_plan=p.get("command_plan", {}),
                    max_command_time_s=p.get("max_command_time_s"),
                    suggested_timeouts=p.get("suggested_timeouts"),
                    judge_checker_factory=None, 
                    env=exec_env,
                    runner_factory=runner,
                    show_progress=True,
                    output_callback=self.output_callback
                )
                
                # --- AUTO-CAPTURE METRICS (SANDBOX AWARE) ---
                captured_metrics = {}
                
                # Definition of paths
                host_artifacts = os.path.join(self.repo_root, "artifacts", "metrics.json")
                host_root = os.path.join(self.repo_root, "metrics.json")
                
                # 1. Try reading via Sandbox (if active) to bypass permission issues
                if self.sandbox and self.sandbox.available:
                    # Try artifacts dir
                    res = self.sandbox.exec_run(["cat", "/app/workdir/artifacts/metrics.json"])
                    if res.get("exit_code") == 0:
                         try:
                             captured_metrics.update(self._robust_extract_metrics(json.loads(res["stdout"])))
                         except: pass
                    
                    # Try root dir if empty
                    if not captured_metrics:
                        res = self.sandbox.exec_run(["cat", "/app/workdir/metrics.json"])
                        if res.get("exit_code") == 0:
                             try:
                                 captured_metrics.update(self._robust_extract_metrics(json.loads(res["stdout"])))
                             except: pass

                # 2. Fallback to Host Read (if not found in sandbox or sandbox disabled)
                if not captured_metrics:
                    search_paths = [host_artifacts, host_root]
                    for mpath in search_paths:
                        if os.path.exists(mpath):
                             try:
                                 with open(mpath, 'r') as f:
                                     data = json.load(f)
                                     captured_metrics.update(data)
                                 self._log(f"📊 [DEBUG] Loaded metrics from Host {mpath}: {data}")
                             except Exception as e:
                                 self._log(f"⚠️ [DEBUG] Failed to load from Host {mpath}: {e}")
                                 pass
                
                if not captured_metrics:
                     self._log(f"⚠️ [DEBUG] No metrics found (Sandbox or Host).")
                     pass
                         
                # ------------------------------------

                return ExecutionResult(success=True, data=command_results, metrics=captured_metrics)
            
            elif request.type == "apply_patch":
                # Accept both 'patch_proposal' (from Orchestrator RPC) and 'patch' (legacy)
                patch_proposal = request.payload.get("patch_proposal") or request.payload.get("patch", {})
                if not patch_proposal:
                    return ExecutionResult(success=False, error="No patch provided")
                
                try:
                    applied, skipped = applier.apply_patchproposal(
                        repo_root=self.repo_root,
                        patch=patch_proposal
                    )
                    return ExecutionResult(success=True, data={"applied": applied, "skipped": skipped})
                except Exception as e:
                    return ExecutionResult(success=False, error=f"Patch application failed: {str(e)}")

            elif request.type == "read_file":
                path = request.payload.get("path", "")
                max_chars = request.payload.get("max_chars")
                target_scope = request.payload.get("target_scope", "current")
                try:
                    # Determine root
                    root = self.repo_root
                    if target_scope == "artifact":
                        root = self.artifact_dir
                    elif target_scope == "original":
                         root = self.original_repo_root

                    abs_path = path if os.path.isabs(path) else os.path.join(root, path)
                    
                    if not os.path.exists(abs_path):
                        return ExecutionResult(success=True, data={"exists": False})
                    
                    with open(abs_path, 'r', encoding='utf-8') as f:
                        content = f.read(max_chars) if max_chars else f.read()
                    
                    return ExecutionResult(success=True, data={
                        "content": content, 
                        "exists": True,
                        "mtime": os.path.getmtime(abs_path)
                    })
                except Exception as e:
                    return ExecutionResult(success=False, error=str(e))
            
            elif request.type == "list_files":
                 # List dir
                 path = request.payload.get("path", ".")
                 try:
                     abs_path = os.path.join(self.repo_root, path)
                     items = []
                     for item in os.listdir(abs_path):
                         item_path = os.path.join(abs_path, item)
                         items.append({
                             "name": item,
                             "is_dir": os.path.isdir(item_path),
                             "size": os.path.getsize(item_path) if not os.path.isdir(item_path) else 0
                         })
                     return ExecutionResult(success=True, data={"files": items})
                 except Exception as e:
                     return ExecutionResult(success=False, error=str(e))

            elif request.type == "execute_command":
                # Direct command execution
                cmd = request.payload.get("command", "")
                timeout = request.payload.get("timeout_s")
                env_vars = request.payload.get("env", {})
                
                runner = None
                if self.sandbox and self.sandbox.available:
                    def sandbox_runner(cmd, env=None):
                        return self.sandbox.exec_popen(cmd, env=env or {})
                    runner = sandbox_runner

                # Ensure artifacts directory exists for the script to use
                artifacts_path = os.path.join(self.repo_root, "artifacts")
                os.makedirs(artifacts_path, exist_ok=True)

                outcome = executor.run_command_with_timeout(
                    cmd=cmd,
                    cwd=self.repo_root,
                    timeout_s=timeout,
                    show_progress=True,
                    output_callback=self.output_callback,
                    env=self._get_worker_env(env_vars),
                    runner_factory=runner
                )
                
                # --- AUTO-CAPTURE METRICS (SANDBOX DIRECT READ) ---
                captured_metrics = {}
                
                # 1. Read directly via Sandbox (Primary)
                if self.sandbox and self.sandbox.available:
                     def read_sandbox_json(path):
                         res = self.sandbox.exec_run(["cat", path])
                         if res.get("exit_code") == 0:
                             try:
                                 loaded = json.loads(res["stdout"])
                                 # Extract inner 'metrics'
                                 if "metrics" in loaded and isinstance(loaded["metrics"], dict):
                                     captured_metrics.update(loaded["metrics"])
                                 else:
                                     captured_metrics.update(loaded)
                                 self._log(f"📊 [DEBUG] Loaded metrics via Sandbox: {path}")
                             except: pass

                     read_sandbox_json("/app/workdir/artifacts/metrics.json")
                     if not captured_metrics:
                         read_sandbox_json("/app/workdir/metrics.json")
                     
                     # Fire-and-forget permission fix
                     try:
                         uid, gid = os.getuid(), os.getgid()
                         self.sandbox.exec_run(["chown", "-R", f"{uid}:{gid}", "/app/workdir/artifacts"])
                         self.sandbox.exec_run(["chmod", "-R", "777", "/app/workdir/artifacts"])
                     except: pass
                
                # 2. Host Read Fallback
                if not captured_metrics:
                    host_artifacts = os.path.join(self.repo_root, "artifacts", "metrics.json")
                    host_root = os.path.join(self.repo_root, "metrics.json")
                    
                    search_paths = [host_artifacts, host_root]
                    for mpath in search_paths:
                        if os.path.exists(mpath):
                             try:
                                 with open(mpath, 'r') as f:
                                     loaded = json.load(f)
                                     if "metrics" in loaded and isinstance(loaded["metrics"], dict):
                                         captured_metrics.update(loaded["metrics"])
                                     else:
                                         captured_metrics.update(loaded)
                                     self._log(f"📊 [DEBUG] Loaded metrics from Host {mpath}: {captured_metrics}")
                                     pass
                                     break
                             except Exception as e:
                                 self._log(f"⚠️ [DEBUG] Failed to load from Host {mpath}: {e}")
                                 pass
                
                if not captured_metrics:
                     self._log(f"⚠️ [DEBUG] No metrics found (Sandbox or Host).")
                     pass
                     
                         
                # ------------------------------------
                
                # Fallback: Parse stdout
                if not captured_metrics and outcome.get("stdout"):
                    import re
                    stdout = outcome["stdout"]
                    # Regex for "key: value" or "key=value" where value is a number
                    # We look for specific known metric keys to avoid false positives
                    patterns = [
                        r"(?i)\b(runtime_s|duration_s|time_s)\b\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)",
                        r"(?i)\b(accuracy|score|val_acc)\b\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)",
                        r"(?i)\b(error_rate|loss)\b\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)"
                    ]
                    for pat in patterns:
                        for m in re.finditer(pat, stdout):
                            key = m.group(1).lower()
                            try:
                                val = float(m.group(2))
                                captured_metrics[key] = val
                            except: pass
                    if captured_metrics:
                         self._log(f"📊 [DEBUG] Extracted metrics from stdout: {captured_metrics}")
                         pass
                # ------------------------------------

                return ExecutionResult(success=True, data={"outcome": outcome}, metrics=captured_metrics)

            elif request.type == "command_discovery":
                from .core import command_discovery
                p = request.payload
                
                runner = None
                if self.sandbox and self.sandbox.available:
                    def sandbox_runner(cmd, env=None):
                        return self.sandbox.exec_popen(cmd, env=env or {})
                    runner = sandbox_runner
                
                res = command_discovery.discover_command_plan(
                    repo_root=self.repo_root,
                    artifact_dir=self.artifact_dir,
                    metric_names=p.get("metric_names", []),
                    initial_commands=p.get("initial_commands", []),
                    venv_python=p.get("venv_python"),
                    timeout_s=p.get("timeout_s", 8.0),
                    runner_factory=runner,
                    output_callback=self.output_callback
                )
                # Persist for debugging
                command_discovery.persist_command_plan(
                    repo_root=self.repo_root,
                    artifact_dir=self.artifact_dir,
                    result=res
                )
                import dataclasses
                return ExecutionResult(success=True, data=dataclasses.asdict(res))

            elif request.type == "create_working_copy":
                run_id = request.payload.get("run_id") or f"run-{uuid.uuid4().hex[:8]}"
                source_path = request.payload.get("source_path") or self.original_repo_root
                
                # Create ephemeral path
                import tempfile
                temp_dir = os.path.join(tempfile.gettempdir(), f"remoroo_worktree_{run_id}")
                
                # Cleanup if exists (unlikely but safe)
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                
                self._log(f"🌲 Creating Ephemeral Working Copy: {temp_dir}")
                
                try:
                    # Case 1: Source is a URL (Cloud/Remote Mode)
                    if source_path.startswith("http://") or source_path.startswith("https://"):
                        import requests
                        import zipfile
                        import io
                        
                        self._log(f"📥 Downloading repository from: {source_path}")
                        resp = requests.get(source_path)
                        resp.raise_for_status()
                        
                        # Extract ZIP to temp_dir
                        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                            z.extractall(temp_dir)
                        
                        # Sometimes ZIPs have a nested folder, but we assume flat for now 
                        # to match pack_repo.py output.
                    
                    # Case 2: Source is a Local Path (Local/Hybrid Mode)
                    else:
                        # Copy Logic with Exclusions
                        # We exclude heavy/volatile dirs to keep it fast and clean
                        ignore_func = shutil.ignore_patterns('runs', 'artifacts', '.remoroo', '.git', '__pycache__', 'venv', '.env')
                        shutil.copytree(source_path, temp_dir, ignore=ignore_func)
                        
                        # Handle git: copy .git if source has it, otherwise just init empty
                        import subprocess
                        source_git = os.path.join(source_path, ".git")
                        if os.path.isdir(source_git):
                            try:
                                shutil.copytree(source_git, os.path.join(temp_dir, ".git"))
                            except Exception as e:
                                self._log(f"⚠️  Failed to copy .git folder: {e}")
                        else:
                            try:
                                # Try to init git
                                subprocess.run(["git", "init"], cwd=temp_dir, capture_output=True, check=True)
                            except (subprocess.SubprocessError, FileNotFoundError):
                                error_msg = (
                                    "❌ Git is not installed or not in PATH.\n"
                                    "Remoroo requires Git to manage working copies and generate patches.\n"
                                    "Please install Git (https://git-scm.com/downloads) or run 'remoroo run' "
                                    "to trigger the automatic installer."
                                )
                                self._log(error_msg)
                                raise RuntimeError("Git dependency missing. Please install Git.")
                    
                    # SWITCH CONTEXT
                    self.repo_root = temp_dir
                    self.is_ephemeral = True
                    
                    # Also need to update sub-components that hold repo_root refs
                    self.worker.repo_root = temp_dir
                    if self.sandbox:
                        self.sandbox.stop()
                        from .sandbox import DockerSandbox
                        self.sandbox = DockerSandbox(self.repo_root, self.artifact_dir)
                    
                    return ExecutionResult(success=True, data={"working_path": self.repo_root})
                except Exception as e:
                    self._log(f"❌ Failed to create working copy: {e}")
                    return ExecutionResult(success=False, error=f"Failed to create working copy: {str(e)}")

            elif request.type == "finalize_artifacts":
                dest_filename = request.payload.get("dest_filename", "final_patch.diff")
                finalized = self._finalize_artifacts_internal(dest_filename=dest_filename)
                return ExecutionResult(success=True, data={"artifacts_finalized": finalized})
            
            elif request.type == "cleanup_working_copy":
                # Robust check for EPC (Ephemeral Working Copy) cleanup
                import tempfile
                is_in_temp = self.repo_root.startswith(tempfile.gettempdir())
                
                if self.is_ephemeral and (is_in_temp or "remoroo_worktree" in self.repo_root):
                    # SAFETY: Finalize artifacts BEFORE deleting the directory
                    # This handles cases where Brain initiates cleanup before CLI finalization
                    dest_filename = request.payload.get("dest_filename", "final_patch.diff")
                    self._finalize_artifacts_internal(dest_filename=dest_filename)
                    
                    self._log(f"🧹 Cleaning up Ephemeral Working Copy: {self.repo_root}")
                    try:
                        # 1. PERMISSION FIX (While sandbox is still running)
                        if self.sandbox and self.sandbox.available:
                            try:
                                uid, gid = os.getuid(), os.getgid()
                                self._log(f"   👤 Reclaiming ownership: {uid}:{gid}")
                                self.sandbox.exec_run(["chown", "-R", f"{uid}:{gid}", "/app/workdir"])
                                self.sandbox.exec_run(["chmod", "-R", "777", "/app/workdir"])
                            except: pass

                        # 2. STOP SANDBOX (to release mounts)
                        if self.sandbox:
                            self._log("   🛑 Stopping Sandbox...")
                            self.sandbox.stop()
                            # Replace with fresh one for future use (if needed)
                            from .sandbox import DockerSandbox
                            self.sandbox = DockerSandbox(self.original_repo_root, self.artifact_dir)

                        # 3. DELETE DIRECTORY
                        shutil.rmtree(self.repo_root)
                        # Reset to original
                        self.repo_root = self.original_repo_root
                        self.is_ephemeral = False
                        
                        return ExecutionResult(success=True, data={"cleaned": True})
                    except Exception as e:
                        self._log(f"⚠️ Cleanup failed: {e}")
                        return ExecutionResult(success=False, error=str(e))
                else:
                    return ExecutionResult(success=True, data={"cleaned": False, "reason": "Not ephemeral"})

            elif request.type == "git_diff":
                 # Git diff support
                 files = request.payload.get("files", [])
                 staged = request.payload.get("staged", False)
                 
                 # Stage files first if requested
                 if files:
                     files_str = " ".join(f'"{f}"' for f in files)
                     executor.run_command_stepwise(f"git add {files_str}", self.repo_root, output_callback=self.output_callback)
                 
                 # Run diff
                 cmd = "git diff --cached" if staged else "git diff"
                 outcome = executor.run_command_stepwise(
                     cmd,
                     self.repo_root,
                     timeout_s=30
                 )
                 
                 diff_content = outcome.get("stdout", "")
                 return ExecutionResult(
                     success=True, 
                     data={"diff": diff_content},
                     request_id=request.request_id
                 )
                 
            elif request.type == "write_file":
                 # Simple write file handler for saving reports/artifacts
                 path = request.payload.get("path")
                 content = request.payload.get("content")
                 target_scope = request.payload.get("target_scope", "current") # current vs original
                 
                 if not path:
                     return ExecutionResult(success=False, error="path required")
                 
                 try:
                     # Determine root
                     root = self.repo_root
                     if target_scope == "original":
                         root = self.original_repo_root
                         # 🚀 AUTO-ROUTE REPORTS TO RUN-SPECIFIC FOLDER
                         if self.run_id and ("final_report.md" in path or "report" in path.lower()):
                              run_output = os.path.join(root, ".remoroo", "runs", self.run_id)
                              os.makedirs(run_output, exist_ok=True)
                              root = run_output
                              
                         self._log(f"🚚 Delivering to: {root}/{path}")
                     elif target_scope == "artifact":
                         root = self.artifact_dir

                     if not os.path.isabs(path):
                         target_path = os.path.join(root, path)
                     else:
                         target_path = path

                     # Ensure dir exists
                     os.makedirs(os.path.dirname(target_path), exist_ok=True)
                     
                     with open(target_path, 'w', encoding='utf-8') as f:
                         f.write(content)
                     
                     # ALSO save to artifact_dir (for CLI transparency)
                     if self.artifact_dir and target_scope == "original":
                         cache_path = os.path.join(self.artifact_dir, path)
                         os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                         with open(cache_path, 'w', encoding='utf-8') as f:
                             f.write(content)

                     # Enhanced logging for debugging
                     self._log(f"   ✅ File written successfully")
                     self._log(f"   📍 Full path: {target_path}")
                     self._log(f"   📊 Size: {len(content)} bytes")
                     
                     if "report" in str(path).lower():
                         self._log("   📄 Report preview (first 200 chars):")
                         self._log("   " + content[:200].replace("\n", "\n   "))
                         
                     return ExecutionResult(success=True, data={"path": target_path})
                 except Exception as e:
                     self._log(f"   ❌ Write failed: {e}")
                     import traceback
                     traceback.print_exc()
                     return ExecutionResult(success=False, error=str(e))

            elif request.type == "env_infer_config":
                from ..execution import env_doctor
                doctor = env_doctor.EnvDoctor(
                    repo_root=self.repo_root,
                    artifact_dir=self.artifact_dir,
                    venv_python=request.payload.get("venv_python"),
                    packages_to_install=request.payload.get("packages_to_install")
                )
                smoke_test = doctor._infer_smoke_test()
                install_cmds = doctor._get_initial_install_commands()
                return ExecutionResult(success=True, data={
                    "smoke_test_cmd": smoke_test,
                    "install_commands": install_cmds
                })

            elif request.type == "instrumentation_select_targets":
                from ..execution import instrumentation_targets
                # Use explicit repo_root from payload if provided (stateless protocol)
                target_root = request.payload.get("repo_root") or self.repo_root
                targets = instrumentation_targets.select_instrumentation_targets(
                    repo_root=target_root,
                    commands=request.payload.get("commands", []),
                    metric_names=request.payload.get("metric_names", [])
                )
                return ExecutionResult(success=True, data=targets)

            elif request.type == "instrumentation_is_repo_empty":
                from ..execution import instrumentation_pipeline
                # Use explicit repo_root from payload if provided (stateless protocol)
                target_root = request.payload.get("repo_root") or self.repo_root
                # Direct call to helper
                is_empty = instrumentation_pipeline._is_repo_empty(target_root)
                return ExecutionResult(success=True, data={"is_empty": is_empty})

            elif request.type == "instrumentation_inject_monitor":
                from ..execution import instrumentation_pipeline
                # Use explicit repo_root from payload if provided (stateless protocol)
                target_root = request.payload.get("repo_root") or self.repo_root
                # We need an instance to run inject? No, _inject_monitor is method but uses self.repo_root
                # We can mock instance or refactor to static.
                # Or just use the logic directly. 
                # Actually, simplified: copy logic or use Pipeline class?
                # Pipeline class __init__ requires planner_callback.
                # Creating dummy callback is fine.
                def dummy_cb(*args, **kwargs): return {}
                pipeline = instrumentation_pipeline.InstrumentationPipeline(
                    repo_root=target_root,
                    artifact_dir=self.artifact_dir,
                    planner_callback=dummy_cb
                )
                pipeline._inject_monitor()
                return ExecutionResult(success=True, data={"injected": True})

            elif request.type == "diagnosis_import_error":
                from ..execution import import_diagnostics
                # Use explicit repo_root from payload if provided (stateless protocol)
                target_root = request.payload.get("repo_root") or self.repo_root
                # Use absolute venv python if available
                venv_python = request.payload.get("venv_python")
                if not venv_python or venv_python == "python":
                    potential_venv = os.path.join(target_root, "venv", "bin", "python")
                    if os.path.exists(potential_venv):
                        venv_python = potential_venv
                    else:
                        potential_venv = os.path.join(target_root, ".venv", "bin", "python")
                        if os.path.exists(potential_venv):
                            venv_python = potential_venv

                d = import_diagnostics.diagnose_import_error(
                    error_message=request.payload.get("error_message", ""),
                    repo_root=target_root,
                    venv_python=venv_python
                )
                return ExecutionResult(success=True, data=d)
                
            elif request.type == "instrumentation_process_artifacts":
                from ..execution import instrumentation_pipeline
                # Logic: merge partials into baseline/current
                # Payload: phase ("baseline" or "current")
                phase = request.payload.get("phase")
                
                # Check both repo/artifacts and run-specific artifact_dir
                artifact_dirs = [
                    os.path.join(self.repo_root, "artifacts"),
                    self.artifact_dir
                ]
                
                result_success = True
                files_updated = []
                total_merged = 0
                final_data = {} # Final state of the primary artifact
                
                for repo_artifacts_dir in artifact_dirs:
                    if not os.path.isdir(repo_artifacts_dir):
                        continue
                        
                    merged_data = instrumentation_pipeline._collect_and_merge_partial_artifacts(repo_artifacts_dir)
                    if not merged_data:
                        continue
                        
                    total_merged += len(merged_data)
                    targets = []
                    if phase == "baseline":
                         targets.append("baseline_metrics.json")
                    elif phase == "current":
                         targets.append("current_metrics.json")
                         targets.append("metrics.json")
                    
                    for fname in targets:
                        fpath = os.path.join(repo_artifacts_dir, fname)
                        try:
                           existing = instrumentation_pipeline._read_json(fpath) or {}
                           if "metrics" not in existing:
                               existing["metrics"] = {}
                           existing["metrics"].update(merged_data)
                           instrumentation_pipeline._write_json(fpath, existing)
                           files_updated.append(f"{os.path.basename(repo_artifacts_dir)}/{fname}")
                           final_data = existing
                        except Exception:
                           result_success = False
                
                return ExecutionResult(success=result_success, data={
                    "files_updated": files_updated, 
                    "merged_count": total_merged,
                    "metrics_data": final_data
                })

            elif request.type == "env_scan_imports":
                from ..execution import env_doctor
                # Use explicit repo_root from payload if provided (stateless protocol)
                target_root = request.payload.get("repo_root") or self.repo_root
                doctor = env_doctor.EnvDoctor(
                    repo_root=target_root,
                    artifact_dir=self.artifact_dir
                )
                context = doctor.scan_import_context()
                return ExecutionResult(success=True, data=context)


            elif request.type == "run_command_async":
                # Midturn Judge v3: Start execution and return ID immediately
                cmd = request.payload.get("command", "")
                timeout_s = request.payload.get("timeout_s")
                env_vars = request.payload.get("env", {})
                
                if not cmd:
                    return ExecutionResult(success=False, error="No command provided")
                
                execution_id = f"exec-{uuid.uuid4().hex[:8]}"
                
                # Build execution environment
                exec_env = self._get_worker_env(env_vars)
                
                try:
                    import subprocess
                    import threading
                    import time
                    
                    # Log files for async output
                    stdout_buffer = []
                    stderr_buffer = []
                    
                    self._execution_buffers[execution_id] = {
                        "stdout": stdout_buffer,
                        "stderr": stderr_buffer,
                        "start_time": time.time(),
                        "command": cmd,
                        "finished": False,
                        "exit_code": None
                    }
                    
                    self._log(f"🚀 [Worker] Starting Async Command: {cmd} (ID: {execution_id})")
                    pass

                    # Running via executor helper would block, so we use subprocess directly
                    # but we need to respect sandbox if active.
                    
                    process = None
                    if self.sandbox and self.sandbox.available:
                        # Use streaming exec_popen to allow real-time output capture
                        # Path Translation: Host Path -> Container Path
                        # Sandbox mounts self.repo_root -> /app/workdir
                        # IF self.repo_root matches what the sandbox was init'd with. 
                        # (Which it should if we updated sandbox in create_working_copy)
                        
                        container_workdir = "/app/workdir"
                        # If self.repo_root is not self.sandbox.repo_path, we have a drift issue.
                        # But assuming they match.
                        
                        # Ensure artifacts dir exists for the command
                        # Sandbox mounts self.repo_root -> /app/workdir
                        exec_env["REMOROO_ARTIFACTS_DIR"] = "/app/workdir/artifacts"
                        
                        process = self.sandbox.exec_popen(
                            cmd, 
                            env={k: v for k, v in exec_env.items() if k},
                            workdir=container_workdir 
                        )
                        self._running_processes[execution_id] = process
                        
                        from rich.console import Console
                        from rich.panel import Panel
                        from rich.live import Live
                        console = Console()
                        
                        # Background threads to consume pipes (Same as local Popen)
                        def reader(stream, buffer, name="unknown"):
                            if stream:
                                try:
                                    for line in stream:
                                        # Use standard output format so User sees the content in their CLI
                                        clean_line = line.rstrip('\n')
                                        # Box the output for visibility
                                        color = "white" if name == "STDOUT" else "red"
                                        # console.self._log(Panel(clean_line, title=f"[bold {color}]{name}[/bold {color}]", border_style=color, expand=False))
                                        buffer.append(line)
                                    stream.close()
                                except Exception:
                                    pass
                            
                        threading.Thread(target=reader, args=(process.stdout, stdout_buffer, "STDOUT"), daemon=True).start()
                        threading.Thread(target=reader, args=(process.stderr, stderr_buffer, "STDERR"), daemon=True).start()
                        
                        # Monitor thread to wait for exit
                        def waiter():
                            process.wait()
                            self._execution_buffers[execution_id]["exit_code"] = process.returncode
                            self._execution_buffers[execution_id]["finished"] = True
                            
                        threading.Thread(target=waiter, daemon=True).start()
                        
                    else:
                        # Local execution using Popen
                        # Use shell=True for complex commands (like piped ones)
                        process = subprocess.Popen(
                            cmd,
                            cwd=self.repo_root,
                            env=exec_env,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=True,
                            text=True,
                            bufsize=1 # Line buffered
                        )
                        self._running_processes[execution_id] = process
                        
                        from rich.console import Console
                        from rich.panel import Panel
                        console = Console()

                        # Background threads to consume pipes
                        def reader(stream, buffer, name="OUTPUT"):
                            for line in stream:
                                try:
                                    # Box the output for visibility
                                    color = "white" if name == "STDOUT" else "red"
                                    # console.self._log(Panel(line.rstrip(), title=f"[bold {color}]{name}[/bold {color}]", border_style=color, expand=False))
                                except Exception:
                                    pass
                                buffer.append(line)
                            stream.close()
                            
                        threading.Thread(target=reader, args=(process.stdout, stdout_buffer, "STDOUT"), daemon=True).start()
                        threading.Thread(target=reader, args=(process.stderr, stderr_buffer, "STDERR"), daemon=True).start()
                        
                        # Monitor thread to wait for exit
                        def waiter():
                            process.wait()
                            self._execution_buffers[execution_id]["exit_code"] = process.returncode
                            self._execution_buffers[execution_id]["finished"] = True
                            
                        threading.Thread(target=waiter, daemon=True).start()

                    return ExecutionResult(success=True, data={"execution_id": execution_id})
                    
                except Exception as e:
                    return ExecutionResult(success=False, error=str(e))

            elif request.type == "get_output":
                # Poll output snapshot
                exec_id = request.payload.get("execution_id")
                if not exec_id or exec_id not in self._execution_buffers:
                     return ExecutionResult(success=False, error="Execution ID not found")
                
                state = self._execution_buffers[exec_id]
                import time
                elapsed = time.time() - state["start_time"]
                
                # Check if process is still running
                # If wrapped in thread (sandbox), checking thread.is_alive() is proxy
                running = not state["finished"]
                
                # Truncate output to avoid massive payloads (1MB limit per stream)
                stdout_full = "".join(state["stdout"])
                stderr_full = "".join(state["stderr"])
                limit = 1_000_000
                
                if len(stdout_full) > limit:
                    stdout_full = stdout_full[:limit] + "\n... [Truncated by Worker] ..."
                if len(stderr_full) > limit:
                     stderr_full = stderr_full[:limit] + "\n... [Truncated by Worker] ..."

                # --- AUTO-CAPTURE METRICS (SANDBOX DIRECT READ) ---
                captured_metrics = {}
                if not running:
                    # 1. Read directly via Sandbox (Primary)
                    if self.sandbox and self.sandbox.available:
                        def read_sandbox_json(path):
                            res = self.sandbox.exec_run(["cat", path])
                            if res.get("exit_code") == 0:
                                try:
                                    loaded = json.loads(res["stdout"])
                                    captured_metrics.update(self._robust_extract_metrics(loaded))
                                except: pass

                        read_sandbox_json("/app/workdir/artifacts/metrics.json")
                        if not captured_metrics:
                            read_sandbox_json("/app/workdir/metrics.json")
                        
                        # Fire-and-forget permission fix
                        try:
                            uid, gid = os.getuid(), os.getgid()
                            self.sandbox.exec_run(["chown", "-R", f"{uid}:{gid}", "/app/workdir/artifacts"])
                            self.sandbox.exec_run(["chmod", "-R", "777", "/app/workdir/artifacts"])
                        except: pass
                    
                    # 2. Host Read Fallback
                    if not captured_metrics:
                        host_artifacts = os.path.join(self.repo_root, "artifacts", "metrics.json")
                        host_root = os.path.join(self.repo_root, "metrics.json")
                        
                        search_paths = [host_artifacts, host_root]
                        for mpath in search_paths:
                            if os.path.exists(mpath):
                                 try:
                                     with open(mpath, 'r') as f:
                                         loaded = json.load(f)
                                         captured_metrics.update(self._robust_extract_metrics(loaded))
                                         break
                                 except: pass

                # 3. Last Resort: Parse from snapshot buffers
                if not captured_metrics:
                    log_metrics = self._extract_metrics_from_text(stdout_full)
                    if log_metrics:
                        captured_metrics.update(log_metrics)
                # ------------------------------------

                return ExecutionResult(success=True, data={
                    "stdout": stdout_full,
                    "stderr": stderr_full,
                    "is_running": running,
                    "exit_code": state["exit_code"],
                    "elapsed_s": elapsed
                }, metrics=captured_metrics)
                
            elif request.type == "kill_command":
                exec_id = request.payload.get("execution_id")
                if not exec_id or exec_id not in self._running_processes:
                    # Maybe already finished?
                     return ExecutionResult(success=True, data={"killed": False, "reason": "Not running"})
                
                proc_or_thread = self._running_processes[exec_id]
                import subprocess
                
                try:
                    if isinstance(proc_or_thread, subprocess.Popen):
                        proc_or_thread.terminate() # SIGTERM
                        # Give it a sec then kill?
                        self._execution_buffers[exec_id]["finished"] = True # Force semantics
                        self._execution_buffers[exec_id]["exit_code"] = -15 # SIGTERM
                        return ExecutionResult(success=True, data={"killed": True})
                    else:
                        # Thread/Sandbox - can't easily kill thread.
                        # If sandbox, we might need container.exec_run("kill ...")?
                        # For now, simplistic.
                        return ExecutionResult(success=True, data={"killed": False, "reason": "Sandbox kill not impl"})
                except Exception as e:
                     return ExecutionResult(success=False, error=str(e))

            elif request.type == "run_commands":
                # Alias for execute_plan (Brain Refactor)
                return self._handle_request_internal(ExecutionRequest(
                    type="execute_plan",
                    payload=request.payload,
                    request_id=request.request_id
                ))

            elif request.type == "apply_patch_bundle":
                # Alias for apply_patch (Brain Refactor)
                return self._handle_request_internal(ExecutionRequest(
                    type="apply_patch",
                    payload={"patch_proposal": request.payload.get("patch")}, # Brain sends "patch"
                    request_id=request.request_id
                ))

            elif request.type == "explore_repo":
                # Brain Refactor: Delegate to Core Worker
                # Payload is usually {"tool": "grep", "query": "..."}
                try:
                    outcome = self.worker.execute_exploration(request.payload)
                    return ExecutionResult(success=True, data={"output": outcome})
                except Exception as e:
                    return ExecutionResult(success=False, error=str(e))

            else:
                return ExecutionResult(success=False, error=f"Unknown request type: {request.type}")


        except Exception as e:
            import traceback
            traceback.print_exc()
            return ExecutionResult(success=False, error=str(e))
        finally:
            # Restore Context
            self.repo_root = previous_root

    def _handle_file_exists(self, request: ExecutionRequest) -> ExecutionResult:
        path = request.payload.get("path", "")
        exists = os.path.exists(os.path.join(self.repo_root, path))
        return ExecutionResult(success=True, data={"exists": exists})
