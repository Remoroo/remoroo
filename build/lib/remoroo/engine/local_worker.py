from typing import Dict, Any, Optional
from .protocol import ExecutionRequest, ExecutionResult
from .core.worker import Worker
from .utils import fs_utils, syntax_validator
from .core import context_packer, env_setup, executor, applier
import shutil
import uuid
import os

class WorkerService:
    """
    Service entrypoint for Validated Execution.
    Handles ExecutionRequests and returns ExecutionResults.
    """
    
    def __init__(self, repo_root: str, artifact_dir: str):
        print("🔧 WorkerService (Patched) Loaded")
        self.repo_root = repo_root
        self.original_repo_root = repo_root # Keep reference to original
        self.is_ephemeral = False # Track if we are in a temp copy
        self.artifact_dir = artifact_dir
        self.worker = Worker(repo_root=repo_root, artifact_dir=artifact_dir)
        
        # Initialize Sandbox (Lazy start)
        # We need check if we are in "Sandbox Mode". For now P0 = Always Sandbox.
        # But since we need to build image, maybe we optionally init.
        # Assuming image build happens externally or lazy.
        from .sandbox import DockerSandbox
        self.sandbox = DockerSandbox(repo_root, artifact_dir)
        
        # Async Execution Tracking
        # Dictionary mapping execution_id (str) -> subprocess.Popen object
        # Note: In a real distributed system this would be in Redis/DB,
        # but for local worker memory is fine.
        self._running_processes: Dict[str, Any] = {}
        self._execution_buffers: Dict[str, Any] = {} # Store stdout/stderr buffers

        
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

    def _handle_request_internal(self, request: ExecutionRequest) -> ExecutionResult:
        """Dispatch request to appropriate Worker method."""
        # 1. Resolve Target Context
        target_root = self._resolve_repo_root(request)
        if target_root != self.repo_root:
            print(f"🔄 Stateless Context Switch: {target_root}")
        
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
                        print(f"📦 Sandbox Install: {cmd}")
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
                
                outcomes = []
                success = True
                for cmd in commands:
                    outcome = executor.run_command_with_timeout(
                        cmd=cmd,
                        cwd=self.repo_root,
                        timeout_s=timeout,
                        show_progress=True,
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
                repo_index = RepoIndexer(target_root).index(force=False)
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
                    import json
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
                    env_vars["REMOROO_ARTIFACTS_DIR"] = os.path.join(self.repo_root, "artifacts")
                
                outcomes = []
                success = True
                for cmd in commands:
                    outcome = executor.run_command_with_timeout(
                        cmd=cmd,
                        cwd=self.repo_root,
                        timeout_s=timeout,
                        show_progress=True,
                        env=env_vars,
                        runner_factory=runner
                    )
                    outcomes.append(outcome)
                    if outcome.get("exit_code") != 0:
                        success = False
                        # Don't break immediately? Baseline usually runs all.
                
                return ExecutionResult(success=True, data={
                    "success": success,
                    "outcomes": outcomes
                })

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
                    exec_env["REMOROO_ARTIFACTS_DIR"] = os.path.join(self.repo_root, "artifacts")
                    
                command_results = executor.run_command_plan(
                    repo_root=self.repo_root,
                    command_plan=p.get("command_plan", {}),
                    max_command_time_s=p.get("max_command_time_s"),
                    suggested_timeouts=p.get("suggested_timeouts"),
                    judge_checker_factory=None, 
                    env=exec_env,
                    runner_factory=runner
                )
                return ExecutionResult(success=True, data=command_results)
            
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
                try:
                    abs_path = os.path.join(self.repo_root, path)
                    if not os.path.exists(abs_path):
                         return ExecutionResult(success=False, error=f"File not found: {path}")
                    with open(abs_path, 'r', encoding='utf-8', errors='replace') as f:
                        content = f.read()
                        if max_chars and len(content) > max_chars:
                            content = content[:max_chars] + "... (truncated)"
                    return ExecutionResult(success=True, data={"content": content, "path": path})
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

                outcome = executor.run_command_with_timeout(
                    cmd=cmd,
                    cwd=self.repo_root,
                    timeout_s=timeout,
                    show_progress=True,
                    env=env_vars,
                    runner_factory=runner
                )
                return ExecutionResult(success=True, data={"outcome": outcome})

            elif request.type == "create_working_copy":
                run_id = request.payload.get("run_id") or f"run-{uuid.uuid4().hex[:8]}"
                
                # Create ephemeral path
                temp_dir = f"/tmp/remoroo_worktree_{run_id}"
                
                # Cleanup if exists (unlikely but safe)
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                
                print(f"🌲 Creating Ephemeral Working Copy: {temp_dir}")
                
                # Copy Logic with Exclusions
                # We exclude heavy/volatile dirs to keep it fast and clean
                # NOTE: We exclude .git here and handle it separately below
                ignore_func = shutil.ignore_patterns('runs', 'artifacts', '.remoroo', '.git', '__pycache__', 'venv', '.env')
                
                try:
                    shutil.copytree(self.original_repo_root, temp_dir, ignore=ignore_func)
                    
                    # Handle git: copy .git if source has it, otherwise just init empty
                    import subprocess
                    source_git = os.path.join(self.original_repo_root, ".git")
                    if os.path.isdir(source_git):
                        # Copy .git directory to preserve history
                        shutil.copytree(source_git, os.path.join(temp_dir, ".git"))
                    else:
                        # Source has no git - init empty repo (don't stage anything)
                        subprocess.run(["git", "init"], cwd=temp_dir, capture_output=True)
                    
                    # SWITCH CONTEXT
                    self.repo_root = temp_dir
                    self.is_ephemeral = True
                    
                    # Also need to update sub-components that hold repo_root refs
                    self.worker.repo_root = temp_dir
                    # Sandbox? Sandbox usually mounts repo_root.
                    # If we use DockerSandbox, we need to update its mount point!
                    if self.sandbox:
                        # Stop old sandbox container first!
                        self.sandbox.stop()
                        # Re-init sandbox with new repo path
                        from .sandbox import DockerSandbox
                        self.sandbox = DockerSandbox(self.repo_root, self.artifact_dir)
                    
                    return ExecutionResult(success=True, data={"working_path": self.repo_root})
                except Exception as e:
                    return ExecutionResult(success=False, error=f"Failed to create working copy: {str(e)}")
            
            elif request.type == "cleanup_working_copy":
                if self.is_ephemeral and self.repo_root.startswith("/tmp/remoroo_worktree"):
                    print(f"🧹 Cleaning up Ephemeral Working Copy: {self.repo_root}")
                    try:
                        shutil.rmtree(self.repo_root)
                        # Reset to original
                        self.repo_root = self.original_repo_root
                        self.is_ephemeral = False
                        
                        # Restore Sandbox
                        if self.sandbox:
                            self.sandbox.stop()
                            from .sandbox import DockerSandbox
                            self.sandbox = DockerSandbox(self.repo_root, self.artifact_dir)
                            
                        return ExecutionResult(success=True, data={"cleaned": True})
                    except Exception as e:
                        print(f"⚠️ Cleanup failed: {e}")
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
                     executor.run_command_stepwise(f"git add {files_str}", self.repo_root)
                 
                 # Run diff
                 cmd = "git diff --cached" if staged else "git diff"
                 outcome = executor.run_command_stepwise(
                     cmd,
                     self.repo_root,
                     timeout_s=30
                 )
                 
                 diff_content = outcome.get("stdout", "")
                 if not diff_content and not outcome.get("exit_code") == 0:
                     # Fallback if no git repo or other error
                     pass
                 
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
                         print(f"🚚 Delivering to ORIGINAL repo: {path}")

                     if not os.path.isabs(path):
                         target_path = os.path.join(root, path)
                     else:
                         target_path = path

                     # Ensure dir exists
                     os.makedirs(os.path.dirname(target_path), exist_ok=True)
                     
                     with open(target_path, 'w', encoding='utf-8') as f:
                         f.write(content)
                     
                     if "report" in str(path):
                         print("   📄 Report Content:")
                         print(content)
                         
                     return ExecutionResult(success=True, data={"path": target_path})
                 except Exception as e:
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
                d = import_diagnostics.diagnose_import_error(
                    error_message=request.payload.get("error_message", ""),
                    repo_root=target_root,
                    venv_python=request.payload.get("venv_python")
                )
                return ExecutionResult(success=True, data=d)
                
            elif request.type == "instrumentation_process_artifacts":
                from ..execution import instrumentation_pipeline
                # Logic: merge partials into baseline/current
                # Payload: phase ("baseline" or "current")
                phase = request.payload.get("phase")
                
                repo_artifacts_dir = os.path.join(self.repo_root, "artifacts")
                
                # Use private helper from pipeline (or duplicate logic)
                # Helper is _collect_and_merge_partial_artifacts(dir) -> dict
                merged_data = instrumentation_pipeline._collect_and_merge_partial_artifacts(repo_artifacts_dir)
                
                result_success = True
                files_updated = []
                
                if merged_data:
                    # Logic adapted from InstrumentationPipeline.run Phase 1 / Phase 2 merge
                    # We need to read existing, update, write back.
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
                           files_updated.append(fname)
                        except Exception:
                           result_success = False
                
                return ExecutionResult(success=result_success, data={"files_updated": files_updated, "merged_count": len(merged_data)})

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
                exec_env = os.environ.copy()
                exec_env["REMOROO_ARTIFACTS_DIR"] = os.path.join(self.repo_root, "artifacts")
                exec_env["PYTHONUNBUFFERED"] = "1" # Ensure Python logs flush immediately
                if env_vars:
                    for k, v in env_vars.items():
                        if k:
                            exec_env[str(k)] = str(v)
                
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
                    
                    print(f"🚀 [Worker] Starting Async Command: {cmd} (ID: {execution_id})")

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
                        
                        # Now check if we want to run in a subdir
                        # We use self.repo_root which IS the cwd for the worker.
                        # So simply /app/workdir is correct.
                        # Passing workdir=self.repo_root (Host Path) to Docker is WRONG.
                        
                        process = self.sandbox.exec_popen(
                            cmd, 
                            env=env_vars,
                            workdir=container_workdir 
                        )
                        self._running_processes[execution_id] = process
                        
                        # Background threads to consume pipes (Same as local Popen)
                        def reader(stream, buffer, name="unknown"):
                            if stream:
                                try:
                                    print(f"DEBUG: Starting reader for {name}")
                                    for line in stream:
                                        # Use standard output format so User sees the content in their CLI
                                        # We strip trailing newline because print adds one
                                        clean_line = line.rstrip('\n')
                                        print(f"[{name}] {clean_line}")
                                        buffer.append(line)
                                    print(f"DEBUG: {name} stream closed")
                                    stream.close()
                                except Exception as e:
                                    print(f"DEBUG: Error in reader {name}: {e}")
                            
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
                        
                        # Background threads to consume pipes
                        def reader(stream, buffer):
                            for line in stream:
                                buffer.append(line)
                            stream.close()
                            
                        threading.Thread(target=reader, args=(process.stdout, stdout_buffer), daemon=True).start()
                        threading.Thread(target=reader, args=(process.stderr, stderr_buffer), daemon=True).start()
                        
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
                
                return ExecutionResult(success=True, data={
                    "stdout": "".join(state["stdout"]),
                    "stderr": "".join(state["stderr"]),
                    "is_running": running,
                    "exit_code": state["exit_code"],
                    "elapsed_s": elapsed
                })
                
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
