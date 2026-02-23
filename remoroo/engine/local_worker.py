from typing import Dict, Any, Optional, Callable, List
from .protocol import ExecutionRequest, ExecutionResult
from .core.worker import Worker
from .utils import fs_utils, syntax_validator, configs
from .core import context_packer, env_setup, executor, applier
import shutil
import uuid
import os
import json
import time
import threading
import signal
import subprocess
import platform
import sys
import re
import tempfile
import dataclasses
import hashlib
import traceback
import zipfile
import io
try:
    import requests
except ImportError:
    requests = None

from .venv_sandbox import VenvSandbox
from .harness import RemorooHarness
from .utils.system_interface import SystemInterface, RealSystem

class LocalWorker:
    """
    Service entrypoint for Validated Execution.
    Handles ExecutionRequests and returns ExecutionResults.
    """
    
    def __init__(self, repo_root: str, artifact_dir: str, original_repo_root: Optional[str] = None, run_id: Optional[str] = None, engine: str = "docker", output_callback: Optional[Callable] = None, system: Optional[SystemInterface] = None, persistence_dir: Optional[str] = None, cache_env: bool = False):
        self.system = system or RealSystem()
        self.output_callback = output_callback
        # self._log("🔧 LocalWorker (v4-Airtight) Loaded")
        
        self.repo_root = repo_root
        self.original_repo_root = original_repo_root or repo_root # Keep reference to original
        # Infer is_ephemeral if repo_root is different from original_repo_root
        self.is_ephemeral = (self.repo_root != self.original_repo_root) 
        self.is_ephemeral = (self.repo_root != self.original_repo_root) 
        
        # UNIVERSAL PATH LOGIC (v9)
        # If no artifact_dir provided, default to repo_root/artifacts
        if not artifact_dir:
            artifact_dir = os.path.join(repo_root, configs.ARTIFACTS_DIR_NAME)
        
        self.artifact_dir = os.path.abspath(artifact_dir)
        self.persistence_dir = os.path.abspath(persistence_dir) if persistence_dir else None
        self.system.fs.makedirs(self.artifact_dir, exist_ok=True)
        if self.persistence_dir:
            self.system.fs.makedirs(self.persistence_dir, exist_ok=True)
            
        self.run_id = run_id
        self.engine = engine.lower()
        self.cache_env = cache_env  # Enable environment caching (skip already-installed packages)
        self.worker = Worker(repo_root=repo_root, artifact_dir=artifact_dir)
        
        # Initialize Sandbox (Lazy start)
        # Conditional initialization: Only if engine is 'docker'
        self.sandbox = None
        if self.engine == "docker":
            from .sandbox import DockerSandbox
            self.sandbox = DockerSandbox(repo_root, artifact_dir, cache_env=cache_env)
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

    def _extract_metrics_from_text(self, text: str, expected_metrics: List[str] = None) -> Dict[str, Any]:
        """
        Extract metrics from stdout/stderr text.
        
        Builds targeted regex patterns FROM expected_metrics to avoid capturing noise.
        """
        if text is None or not expected_metrics:
            return {}
        metrics = {}
        
        # Build targeted patterns for each expected metric
        # e.g., "avg_reward" -> matches "avg_reward: 123" or "Avg Reward: 123"
        for metric_name in expected_metrics:
            if not isinstance(metric_name, str) or not metric_name.strip():
                continue
            
            name = metric_name.strip()
            
            # Build variations: "avg_reward" -> ["avg_reward", "Avg Reward", "avg reward"]
            # Build variations: "avg_reward" -> ["avg_reward", "Avg Reward", "avg reward"]
            var_set = {name, name.replace('_', ' '), name.replace('_', ' ').title()}
            
            # ─── New Architecture Fix: Dynamic Stemming ───
            # If name has a common unit suffix, also match the base name (stem)
            # This handles 'runtime_s' -> 'Runtime', 'accuracy_pct' -> 'Accuracy' etc.
            # WITHOUT an explicit alias map.
            parts = name.split('_')
            if len(parts) > 1:
                stem = parts[0]
                var_set.add(stem)
                var_set.add(stem.title())
                # Also handle 'total_duration_s' -> 'total duration'
                stem_full = "_".join(parts[:-1])
                var_set.add(stem_full)
                var_set.add(stem_full.replace('_', ' '))
                var_set.add(stem_full.replace('_', ' ').title())
            
            # Support slashes and dots in metric names for regex
            escaped_variations = [re.escape(v) for v in var_set]
            pattern = r'(?:' + '|'.join(escaped_variations) + r')\s*(?:\([^)]*\))?\s*[:=]\s*(-?[0-9][0-9,]*\.?[0-9]*)'
            
            # Search for this specific metric (case-insensitive)
            for match in re.finditer(pattern, text, re.IGNORECASE):
                try:
                    value_str = match.group(1).replace(',', '')
                    value = float(value_str)
                    metrics[name.lower().replace(' ', '_')] = value
                except:
                    pass
        
        return metrics

        
    def _reclaim_ownership(self, path: str):
        """Helper to reclaim host ownership of files created by Docker root."""
        if self.engine == "docker" and self.sandbox and self.sandbox.available:
            try:
                uid, gid = os.getuid(), os.getgid()
                self.sandbox.exec_run(["chown", "-R", f"{uid}:{gid}", path])
                self.sandbox.exec_run(["chmod", "-R", "777", path])
            except: pass

    def _finalize_artifacts_internal(self, dest_filename: str = "final_patch.diff") -> list[str]:
        """Internal helper to generate diff and save artifacts. Returns list of finalized files."""
        try:
            from ..execution.repo_manager import generate_diff
            finalized = []
            
            # v14: Reclaim ownership of artifact dir and repo root before diffing/copying
            # This ensures that files created by Docker (as root) are readable by the CLI
            self._reclaim_ownership(self.artifact_dir)
            self._reclaim_ownership(self.repo_root)
            
            # Use original_repo_root and current repo_root for diff
            # v14.1: Be more permissive - if we have two distinct roots, we generate a patch.
            if self.repo_root != self.original_repo_root:
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
                            
                            # ALSO save to artifact_dir (for CLI transparency in workspace)
                            if self.artifact_dir:
                                cache_diff = os.path.join(self.artifact_dir, dest_filename)
                                with open(cache_diff, 'w', encoding='utf-8') as f:
                                    f.write(diff_content)
                            
                            # v16: ALSO save to persistence_dir (for CLI transparency in summary/prompt)
                            if self.persistence_dir:
                                persist_diff = os.path.join(self.persistence_dir, dest_filename)
                                with open(persist_diff, 'w', encoding='utf-8') as f:
                                    f.write(diff_content)
                            
                            finalized.append(dest_filename)
                            self._log(f"✅ Saved {dest_filename} to {dest_diff}")
                        else:
                            self._log("ℹ️  No significant changes detected in code files.")
                    else:
                        self._log("ℹ️  No modified code files found (skipping patch).")
                    
                    # ── Transfer non-code files (models, checkpoints, data) ──
                    # Detect files that aren't covered by the code patch and copy them
                    # at their exact relative paths so code references stay valid.
                    non_code_files = [
                        f for f in all_modified
                        if not any(f.endswith(ext) for ext in code_extensions)
                        and not any(ignored in f for ignored in IGNORED_PATTERNS)
                    ]
                    
                    if non_code_files:
                        max_file_size = 500 * 1024 * 1024  # 500 MB cap per file
                        copied_count = 0
                        skipped_count = 0
                        
                        for rel_path in non_code_files:
                            src = os.path.join(self.repo_root, rel_path)
                            dst = os.path.join(self.original_repo_root, rel_path)
                            
                            if not os.path.exists(src):
                                continue  # File was deleted, not created
                            
                            # Skip files that are too large
                            skip_large_files = False
                            if skip_large_files:
                                try:
                                    file_size = os.path.getsize(src)
                                    if file_size > max_file_size:
                                        self._log(f"⚠️  Skipping large file ({file_size // (1024*1024)}MB): {rel_path}")
                                        skipped_count += 1
                                        continue
                                except OSError:
                                    continue
                                
                            try:
                                os.makedirs(os.path.dirname(dst), exist_ok=True)
                                shutil.copy2(src, dst)
                                copied_count += 1
                                
                                # Also save to run artifact dir for provenance
                                if self.run_id:
                                    run_dir = os.path.join(self.original_repo_root, ".remoroo", "runs", self.run_id)
                                    artifact_copy = os.path.join(run_dir, "transferred_files", rel_path)
                                    os.makedirs(os.path.dirname(artifact_copy), exist_ok=True)
                                    shutil.copy2(src, artifact_copy)
                            except Exception as e:
                                self._log(f"⚠️  Failed to transfer {rel_path}: {e}")
                                skipped_count += 1
                        
                        if copied_count:
                            self._log(f"📦 Transferred {copied_count} non-code file(s) to original repo")
                            finalized.append(f"{copied_count}_transferred_files")
                        if skipped_count:
                            self._log(f"⚠️  Skipped {skipped_count} file(s) (too large or error)")

                else:
                    if self.repo_root == self.original_repo_root:
                        self._log("ℹ️  Artifacts already finalized and workspace cleaned up.")
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
        env["PYTHONUNBUFFERED"] = "1"
        
        # 3. Headless Operation (Phase 1 Resilience)
        # Prevent GUI apps (like Pygame or Qt) from hanging or crashing in cloud workers
        env["SDL_VIDEODRIVER"] = "dummy"
        env["QT_QPA_PLATFORM"] = "offscreen"
        env["DISPLAY"] = ":99" # Virtual display placeholder
        
        # 4. Apply extra env from request
        if extra_env:
            for k, v in extra_env.items():
                if k:
                    env[str(k)] = str(v)
        
        # 5. Final Enforced Defaults (Override requested path if it's a Docker path in a local worker)
        # We enforce the host-path artifacts dir for the local worker.
        # CRITICAL: Use the absolute host path here to ensure robustness even if symlinks fail.
        if self.artifact_dir:
            env["REMOROO_ARTIFACTS_DIR"] = self.artifact_dir
        else:
            env["REMOROO_ARTIFACTS_DIR"] = os.path.join(self.repo_root, "artifacts")
                    
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
                
                # Enrich with symbols if possible
                try:
                    from .core.repo_indexer import RepoIndexer
                    indexer = RepoIndexer(self.repo_root)
                    repo_index = indexer.index()
                    structure["symbols"] = repo_index.get("symbols", [])
                    structure["entrypoints"] = repo_index.get("entrypoints", [])
                    print('Structure', structure)
                except Exception as e:
                    self._log(f"Error enriching repository index: {e}")
                
                return ExecutionResult(success=True, data=structure)
                
            elif request.type == "file_exists":
                return self._handle_file_exists(request)
            
            elif request.type == "is_data_file":
                path = request.payload.get("path", "")
                is_data = fs_utils.is_data_file(path, self.repo_root)
                return ExecutionResult(success=True, data={"is_data_file": is_data})
                
            elif request.type == "get_scaffolding":
                self._log(f"Executing: get_scaffolding")
                from .core.repo_indexer import RepoIndexer
                indexer = RepoIndexer(target_root)
                scaffolding = indexer.get_scaffolding()
                return ExecutionResult(success=True, data={"scaffolding": scaffolding})

            elif request.type == "get_snippet":
                self._log(f"Executing: get_snippet")
                file_path = request.payload.get("file_path", "")
                symbol_name = request.payload.get("symbol_name", "")
                from .core.repo_indexer import RepoIndexer
                indexer = RepoIndexer(target_root)
                code = indexer.get_snippet(file_path, symbol_name)
                return ExecutionResult(success=True, data={"code": code})
                
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
                timeout = p.get("timeout", 300)  # Default 5 minutes for installs
                
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
                    outcomes = []
                    failed = False
                    last_error = None
                    start_t = os.times().elapsed
                    
                    # Ensure container running
                    self.sandbox.start()
                    
                    # Use streaming execution with progress output
                    def sandbox_runner(cmd, env=None):
                        return self.sandbox.exec_popen(cmd, env=env or {})
                    
                    exec_env = self._get_worker_env()
                    
                    for cmd in cmds_to_run:
                        self._log(f"📦 Sandbox Install: {cmd}")
                        # Use executor with streaming output
                        outcome = executor.run_command_with_timeout(
                            cmd=cmd,
                            cwd=self.repo_root,
                            timeout_s=timeout,
                            show_progress=True,
                            output_callback=self.output_callback,
                            env=exec_env,
                            runner_factory=sandbox_runner
                        )
                        setup_log.append(f"> {cmd}\n{outcome.get('stdout', '')}\n{outcome.get('stderr', '')}")
                        outcomes.append({
                            "command": cmd,
                            "exit_code": outcome.get("exit_code"),
                            "stdout": outcome.get("stdout", "")[:3000],
                            "stderr": outcome.get("stderr", "")[:3000],
                            "duration_s": outcome.get("duration_s", 0.0)
                        })
                        if outcome.get("exit_code") != 0:
                            failed = True
                            last_error = outcome.get("stderr") or outcome.get("stdout") or f"Exit code {outcome.get('exit_code')}"
                            break
                            
                    return ExecutionResult(
                        success=not failed,
                        data={
                            "diagnosis": "Sandbox setup completed" if not failed else "Sandbox setup failed",
                            "commands_run": cmds_to_run,
                            "outcomes": outcomes,
                            "last_error": last_error,
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
                last_error = None
                packages_installed = False  # Track if any packages were installed
                
                for cmd in commands:
                    # Filter install commands if cache_env is enabled
                    original_cmd = cmd
                    if self.sandbox and self.sandbox.available and self.cache_env:
                        filtered_cmd = self.sandbox.filter_install_command(cmd)
                        if filtered_cmd is None:
                            # All packages already installed, skip this command
                            outcome_record = {
                                "command": original_cmd,
                                "exit_code": 0,
                                "stdout": "(cached - packages already installed)",
                                "stderr": "",
                                "duration_s": 0.0,
                                "skipped": True
                            }
                            outcomes.append(outcome_record)
                            continue
                        cmd = filtered_cmd
                        if cmd != original_cmd:
                            packages_installed = True
                    
                    # Execute the command
                    outcome = executor.run_command_with_timeout(
                        cmd=cmd,
                        cwd=self.repo_root,
                        timeout_s=timeout,
                        show_progress=True,
                        output_callback=self.output_callback,
                        env=exec_env,
                        runner_factory=runner
                    )
                    
                    # Track if packages were installed
                    if "pip install" in cmd or "pip3 install" in cmd:
                        if outcome.get("exit_code") == 0:
                            packages_installed = True
                    
                    # Store the full outcome (not just cmd) to preserve stderr/stdout
                    outcome_record = {
                        "command": original_cmd,
                        "exit_code": outcome.get("exit_code"),
                        "stdout": outcome.get("stdout", "")[:3000],  # Truncate for sanity
                        "stderr": outcome.get("stderr", "")[:3000],
                        "duration_s": outcome.get("duration_s", 0.0),
                        "skipped": False
                    }
                    outcomes.append(outcome_record)
                    if outcome.get("exit_code") != 0:
                        success = False
                        # Capture the error for easy access
                        last_error = outcome.get("stderr") or outcome.get("stdout") or f"Command failed with exit code {outcome.get('exit_code')}"
                        break
                
                # Commit Docker container state if cache_env is enabled and packages were installed
                if self.cache_env and self.sandbox and self.sandbox.available and packages_installed and success:
                    try:
                        self._log("💾 Committing Docker environment (packages installed)...")
                        self.sandbox.commit_state(success=True)
                    except Exception as e:
                        self._log(f"⚠️  Failed to commit Docker state: {e}")
                
                return ExecutionResult(success=True, data={
                    "success": success,
                    "outcomes": outcomes,
                    "last_error": last_error  # Include error output for LLM diagnosis
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

                # Planner authority: explicit instrumentation targets (if provided).
                forced_files: List[str] = []
                raw_forced = contract.get("instrumentation_targets")
                if isinstance(raw_forced, list):
                    forced_files = [f for f in raw_forced if isinstance(f, str) and f.strip()]

                # Deterministic metric-definition scan: find repo files that literally mention metric keys.
                # This is cheap and scales: it doesn't require hydrating entrypoints by count.
                metric_hit_files: List[str] = []
                metric_hit_details: List[Dict[str, Any]] = []
                try:
                    names = [m for m in metric_names if isinstance(m, str) and m.strip()]
                    if names:
                        patterns = [re.compile(re.escape(n)) for n in names]
                        # Scan python + markdown files only (small, common places for metric emitters/harnesses).
                        files_list = repo_index.get("files", []) or []
                        # Hard cap for safety
                        max_files_to_scan = 4000
                        max_bytes_per_file = 400_000  # 400KB
                        for f in files_list[:max_files_to_scan]:
                            if not isinstance(f, dict):
                                continue
                            fp = f.get("path")
                            if not isinstance(fp, str) or not fp:
                                continue
                            # Restrict extensions
                            if not (fp.endswith(".py") or fp.endswith(".md") or fp.endswith(".txt")):
                                continue
                            abs_path = os.path.join(target_root, fp)
                            if not os.path.isfile(abs_path):
                                continue
                            try:
                                if os.path.getsize(abs_path) > max_bytes_per_file:
                                    continue
                                with open(abs_path, "r", encoding="utf-8", errors="replace") as fh:
                                    content = fh.read()
                            except Exception:
                                continue
                            hits = []
                            for pat, name in zip(patterns, names):
                                if pat.search(content):
                                    hits.append(name)
                            if hits:
                                metric_hit_files.append(fp)
                                metric_hit_details.append({
                                    "file_path": fp,
                                    "metric_names": hits[:10],
                                    "hit_count": len(hits),
                                })
                except Exception:
                    metric_hit_files = []
                    metric_hit_details = []

                targets = instrumentation_targets.select_instrumentation_targets(
                    repo_root=target_root,
                    commands=commands_flat,
                    metric_names=[m for m in metric_names if isinstance(m, str)],
                    select_top_n=request.payload.get("select_top_n", 5)
                )


                # Promote top-K files
                budget = request.payload.get("select_top_n", 5)
                selected_files = list(targets.get("selected_files") or [])
                # Priority order:
                # 1) forced planner targets (authoritative)
                # 2) metric-definition hits (where metric keys are defined/emitted)
                # 3) reachability-based selection (fallback)
                promoted: List[str] = []
                for seq in (forced_files, metric_hit_files, selected_files):
                    for fp in seq:
                        if isinstance(fp, str) and fp and fp not in promoted:
                            promoted.append(fp)
                promoted_files = promoted[:budget]

                # Ensure the instrumenter is allowed to edit forced/hit files by including them in selected_files.
                targets["selected_files"] = promoted_files

                instrumentation_files_state = {}
                file_access_tracker = FileAccessTracker()
                for fp in promoted_files:
                    if not isinstance(fp, str) or not fp: continue
                    abs_path = os.path.join(target_root, fp)
                    if not os.path.exists(abs_path) or not os.path.isfile(abs_path): continue
                    try:
                        # Architecture Migration: Smart Context
                        # We NO LONGER hydrate full file contents here. 
                        # The LLM must use `get_snippet` to read the files actively.
                        # We just tell it which files exist and are promoted.
                        instrumentation_files_state[fp] = {
                            "exists": True,
                            "content": "<Content hidden. Use `get_snippet` tool to view actual file contents.>",
                            "issues": [],
                            "syntax_errors": []
                        }
                        # We do not mark_full(fp) because the content isn't in context yet.
                    except Exception:
                        continue

                return ExecutionResult(success=True, data={
                    "repo_index_summary": repo_index_summary,
                    "instrumentation_targets": targets,
                    "instrumentation_files_state": instrumentation_files_state,
                    "forced_instrumentation_files": forced_files,
                    "metric_definition_hits": metric_hit_details
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
                expected_metrics = p.get("expected_metrics", [])
                
                # Setup Sandbox/Runner
                runner_factory = None
                if self.engine == "docker" and self.sandbox and self.sandbox.available:
                    # Inside Docker: use mirrored host path (Zero-Mapping)
                    env_vars["REMOROO_ARTIFACTS_DIR"] = self.artifact_dir
                    env_vars["PYTHONUNBUFFERED"] = "1"
                    # Add headless variables to Docker too
                    env_vars["SDL_VIDEODRIVER"] = "dummy"
                    env_vars["QT_QPA_PLATFORM"] = "offscreen"
                    env_vars["DISPLAY"] = ":99"
                    runner_factory = lambda c, env=None: self.sandbox.exec_popen(c, env=env or {}, workdir=self.repo_root)
                else:
                    # Local/Venv Mode
                    env_vars = self._get_worker_env(env_vars)
                    # UNIVERSAL ENFORCEMENT: Always overwrite REMOROO_ARTIFACTS_DIR
                    env_vars["REMOROO_ARTIFACTS_DIR"] = self.artifact_dir
                    
                    venv_sandbox = VenvSandbox(self.repo_root, system=self.system)
                    runner_factory = lambda c, env=None, sp=subprocess: venv_sandbox.exec_popen(
                        c, 
                        env=env or {}, 
                        shell=True,
                        stdout=sp.PIPE,
                        stderr=sp.PIPE,
                        text=True
                    )
                
                harness = RemorooHarness(system=self.system)
                outcomes = []
                success = True

                # PROACTIVE PERMISSIONS FIX (Docker Root Fix)
                # IMPORTANT: Do NOT delete artifacts between baseline commands.
                # Baseline may include multiple commands and must be able to READ-MODIFY-WRITE
                # a shared artifacts/metrics.json without losing prior metrics.
                if self.engine == "docker" and self.sandbox and self.sandbox.available:
                    try:
                        uid, gid = os.getuid(), os.getgid()
                        self.sandbox.exec_run(["chown", "-R", f"{uid}:{gid}", self.artifact_dir])
                        self.sandbox.exec_run(["chmod", "-R", "777", self.artifact_dir])
                    except Exception:
                        pass

                    # Remove only legacy root-level metrics file (stale compatibility path).
                    try:
                        clean_cmd = ["/bin/sh", "-c", f"rm -f {self.repo_root}/{configs.METRICS_FILENAME}"]
                        self.sandbox.exec_run(clean_cmd)
                    except Exception:
                        pass

                for cmd in commands:
                    self._log(f"  ▶️  Running: {cmd} (timeout: {timeout}s)")
                    h_result = harness.run(
                        cmd=cmd,
                        runner_factory=runner_factory,
                        timeout=float(timeout),
                        env=env_vars,
                        artifact_dir=self.artifact_dir,
                        repo_root=self.repo_root, # Enable CWD scanning
                        output_callback=self.output_callback
                    )
                    
                    # Map Harness ExecutionResult to outcome dict expected by the worker
                    outcome = {
                        "cmd": cmd,
                        "exit_code": h_result.data.get("exit_code"),
                        "duration_s": h_result.data.get("duration"),
                        "stdout": h_result.data.get("stdout"),
                        "stderr": h_result.data.get("stderr"),
                        "timed_out": h_result.data.get("trigger") == "timeout",
                        "stopped_early": h_result.data.get("trigger") == "metric_detected"
                    }
                    outcomes.append(outcome)
                    if outcome.get("exit_code") != 0 and not outcome.get("stopped_early"):
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

                     # Check known paths (Universal - Mirrored Host Paths)
                     read_sandbox_json(f"{self.artifact_dir}/baseline_metrics.json")
                     if not captured_metrics:
                         read_sandbox_json(f"{self.artifact_dir}/{configs.METRICS_FILENAME}")
                         if not captured_metrics:
                             # Fallback: Check repo root
                             read_sandbox_json(f"{self.repo_root}/{configs.METRICS_FILENAME}")
                     
                     # Fire-and-forget permission fix for cleanup (don't block capture)
                     try:
                         uid, gid = os.getuid(), os.getgid()
                         self.sandbox.exec_run(["chown", "-R", f"{uid}:{gid}", self.artifact_dir])
                         self.sandbox.exec_run(["chmod", "-R", "777", self.artifact_dir])
                     except: pass

                # 2. Host Read Fallback (only if sandbox failed or empty)
                metrics_source = "none"
                if not captured_metrics:
                    # AUTHORITATIVE: Phase 1 (Baseline) only accepts baseline_metrics.json or partials
                    search_paths = []
                    
                    if self.artifact_dir:
                         search_paths.append(os.path.join(self.artifact_dir, "baseline_metrics.json"))
                    
                    # 2b. Legacy Fallback (CWD)
                    if not captured_metrics:
                         search_paths.append(os.path.join(self.repo_root, "metrics.json"))
                    
                    for mpath in search_paths:
                        if self.system.fs.exists(mpath):
                            try:
                                with self.system.fs.open(mpath, 'r') as f:
                                    loaded = json.load(f)
                                    extracted = self._robust_extract_metrics(loaded)
                                    if extracted:
                                        self._log(f"  ✅ [Worker] Host Read Success ({mpath}): {extracted}")
                                        captured_metrics.update(extracted)
                                        # Distinguish legacy vs artifact
                                        if os.path.basename(mpath) == "metrics.json" and "artifacts" not in mpath:
                                            metrics_source = "legacy:metrics.json"
                                        else:
                                            metrics_source = os.path.basename(mpath)
                                        break
                            except: pass # File might be locked or unreadable
                
                # 3. Last Resort: Parse stdout with ExperimentContract filter
                if not captured_metrics and expected_metrics:
                    combined_stdout = "\n".join(o.get("stdout", "") for o in outcomes if o.get("stdout"))
                    log_metrics = self._extract_metrics_from_text(combined_stdout, expected_metrics=expected_metrics)
                    if log_metrics:
                        self._log(f"  📊 [Worker] Baseline Log Parse Success (filtered): {log_metrics}")
                        captured_metrics.update(log_metrics)
                        metrics_source = "stdout_filtered"

                if captured_metrics:
                     self._log(f"📊 [DEBUG] Baseline Step Captured: {captured_metrics}")
                else:
                     self._log(f"⚠️ [DEBUG] Metrics Capture Failed. No metrics found in artifacts or logs (expected: {expected_metrics}).")

                # Wrap for schema consistency: { "metrics": { ... } }
                baseline_payload = {
                    "metrics": captured_metrics,
                    "success": success or bool(captured_metrics), # Mark success if we got metrics!
                    "outcomes": outcomes
                }

                # Ensure we populate baseline_metrics for protocol
                return ExecutionResult(success=True, data={
                    "baseline_metrics": baseline_payload,
                    "metrics": captured_metrics, # Back-compat
                    "metrics_captured": bool(captured_metrics),
                    "metrics_source": metrics_source
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
                    exec_env["REMOROO_ARTIFACTS_DIR"] = self.artifact_dir
                    # Add headless variables to Docker too
                    exec_env["SDL_VIDEODRIVER"] = "dummy"
                    exec_env["QT_QPA_PLATFORM"] = "offscreen"
                    exec_env["DISPLAY"] = ":99"
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
                   # Use mirrored host paths
                   exec_env["REMOROO_ARTIFACTS_DIR"] = self.artifact_dir
                   res = self.sandbox.exec_run(["cat", f"{self.artifact_dir}/metrics.json"])
                   if res.get("exit_code") != 0:
                       res = self.sandbox.exec_run(["cat", f"{self.repo_root}/metrics.json"])
                   
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
                                     loaded = json.load(f)
                                     captured_metrics.update(self._robust_extract_metrics(loaded))
                                 self._log(f"📊 [DEBUG] Loaded metrics from Host {mpath}: {captured_metrics}")
                             except Exception as e:
                                 self._log(f"⚠️ [DEBUG] Failed to load from Host {mpath}: {e}")
                                 pass
                
                # NOTE: Removed "Last Resort" log parsing from Worker.
                # The Brain handles stdout parsing with ExperimentContract filtering.
                # Worker only returns metrics from artifact files to avoid noise.
                
                if not captured_metrics:
                     self._log(f"⚠️ [DEBUG] No metrics found in artifacts (Sandbox or Host).")

                         
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
                except applier.ApplyError as e:
                    # Propagate structured error if available
                    payload = {"message": str(e)}
                    if e.patch_error:
                        payload["patch_error"] = e.patch_error.to_dict()
                    return ExecutionResult(success=False, error=str(e), data=payload)
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
                    
                    content = None
                    exists = False
                    mtime = 0.0

                    # 1. Try Sandbox first if available and relevant
                    if self.sandbox and self.sandbox.available and target_scope != "original":
                         # Construct mirrored sandbox path
                         sandbox_path = path if path.startswith("/") else os.path.join(self.repo_root, path)
                         if not self.system.fs.exists(sandbox_path):
                             sandbox_path = os.path.join(self.artifact_dir, path)
                         
                         res = self.sandbox.exec_run(["cat", sandbox_path])
                         if res.get("exit_code") == 0:
                             content = res["stdout"]
                             if max_chars: content = content[:max_chars]
                             exists = True
                             # Get mtime via stat
                             stat_res = self.sandbox.exec_run(["stat", "-c", "%Y", sandbox_path])
                             if stat_res.get("exit_code") == 0:
                                 try: mtime = float(stat_res["stdout"].strip())
                                 except: pass

                    # 2. Host Fallback
                    if not exists:
                        if os.path.exists(abs_path):
                            exists = True
                            with open(abs_path, 'r', encoding='utf-8') as f:
                                content = f.read(max_chars) if max_chars else f.read()
                            mtime = os.path.getmtime(abs_path)
                    
                    if not exists:
                        return ExecutionResult(success=True, data={"exists": False})

                    return ExecutionResult(success=True, data={
                        "content": content, 
                        "exists": True,
                        "mtime": mtime
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

            elif request.type == "snapshot_files":
                # Recursively snapshot the working directory for artifact manifest diffing.
                # Returns lightweight manifest: [{path, size_bytes, mtime}]
                # Excludes: excluded dirs, source code extensions (tracked by context pack).
                SOURCE_CODE_EXTENSIONS = {
                    ".py", ".json", ".yaml", ".yml", ".toml", ".txt", ".md", ".sh", ".bash", ".zsh",
                    ".js", ".ts", ".tsx", ".jsx", ".go", ".rs", ".java",
                    ".cpp", ".c", ".h", ".hpp", ".cc", ".cxx", ".hh", ".hxx",
                    ".make", ".cmake", ".sql", ".proto", ".ini", ".cfg", ".xml",
                    ".css", ".html", ".htm", ".lock",
                }
                SNAPSHOT_EXCLUDED_DIRS = configs.DEFAULT_EXCLUDED_DIRS - {"artifacts"}  # DO scan artifacts/
                SNAPSHOT_EXCLUDED_DIRS.add(".git")  # Always exclude .git

                try:
                    snapshot = []
                    scan_root = self.repo_root
                    for dirpath, dirnames, filenames in os.walk(scan_root):
                        # Prune excluded dirs in-place
                        dirnames[:] = [
                            d for d in dirnames
                            if d not in SNAPSHOT_EXCLUDED_DIRS
                            and "venv" not in d.lower()
                            and "site-packages" not in d.lower()
                        ]
                        for fname in filenames:
                            ext = os.path.splitext(fname)[1].lower()
                            if ext in SOURCE_CODE_EXTENSIONS:
                                continue
                            if fname in configs.DEFAULT_EXCLUDED_FILES or fname == ".DS_Store":
                                continue
                            fpath = os.path.join(dirpath, fname)
                            try:
                                stat = os.stat(fpath)
                                relpath = os.path.relpath(fpath, scan_root)
                                snapshot.append({
                                    "path": relpath,
                                    "size_bytes": stat.st_size,
                                    "mtime": stat.st_mtime,
                                })
                            except OSError:
                                continue
                    return ExecutionResult(success=True, data={"files": snapshot})
                except Exception as e:
                    return ExecutionResult(success=False, error=str(e))

            elif request.type == "persist_runtime_artifacts":
                # Copy runtime artifacts from ephemeral worktree to original_repo_root
                # so they survive worktree cleanup and are available in the next turn.
                artifact_paths = request.payload.get("artifact_paths", [])
                if not artifact_paths or not self.is_ephemeral:
                    return ExecutionResult(success=True, data={"persisted": 0, "reason": "nothing to persist" if not artifact_paths else "not ephemeral"})

                persisted = 0
                errors = []
                for relpath in artifact_paths:
                    src = os.path.join(self.repo_root, relpath)
                    dst = os.path.join(self.original_repo_root, relpath)
                    try:
                        if os.path.isfile(src):
                            os.makedirs(os.path.dirname(dst), exist_ok=True)
                            shutil.copy2(src, dst)
                            persisted += 1
                    except Exception as e:
                        errors.append({"path": relpath, "error": str(e)})

                self._log(f"📦 Persisted {persisted}/{len(artifact_paths)} runtime artifacts to original repo")
                return ExecutionResult(success=True, data={
                    "persisted": persisted,
                    "total": len(artifact_paths),
                    "errors": errors[:5] if errors else []
                })

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
                                 captured_metrics.update(self._robust_extract_metrics(loaded))
                                 self._log(f"📊 [DEBUG] Loaded metrics via Sandbox (Incremental): {path}")
                             except: pass

                     read_sandbox_json(f"{self.artifact_dir}/metrics.json")
                     if not captured_metrics:
                         read_sandbox_json(f"{self.repo_root}/metrics.json")
                     
                     try:
                         uid, gid = os.getuid(), os.getgid()
                         self.sandbox.exec_run(["chown", "-R", f"{uid}:{gid}", self.artifact_dir])
                         self.sandbox.exec_run(["chmod", "-R", "777", self.artifact_dir])
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
                                     self._log(f"📊 [DEBUG] Loaded metrics from Host {mpath}: {captured_metrics}")
                                     break
                             except Exception as e:
                                 self._log(f"⚠️ [DEBUG] Failed to load from Host {mpath}: {e}")
                                 pass
                
                if not captured_metrics:
                     self._log(f"⚠️ [DEBUG] No metrics found (Sandbox or Host).")

                     
                         
                # ------------------------------------
                
                # Fallback: Parse stdout
                if not captured_metrics and outcome.get("stdout"):
                    stdout = outcome["stdout"]
                    # Regex for "key: value" or "key=value" where value is a number
                    # We look for specific known metric keys to avoid false positives
                    patterns = [
                        r"(?i)\b(runtime_s|duration_s|time_s)\b\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)",
                        r"(?i)\b(accuracy|score|val_acc)\b\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)",
                        r"(?i)\b(error_rate|loss)\b\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)"
                    ]
                    found_metric = False
                    for pat in patterns:
                        for m in re.finditer(pat, stdout):
                            key = m.group(1).lower()
                            try:
                                val = float(m.group(2))
                                captured_metrics[key] = val
                                found_metric = True
                            except: pass
                    if found_metric:
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
                    timeout_s=p.get("timeout_s", 4.0),
                    include_repo_index_entrypoints=p.get("include_repo_index_entrypoints", True),
                    runner_factory=runner,
                    output_callback=self.output_callback
                )
                # Persist for debugging
                command_discovery.persist_command_plan(
                    repo_root=self.repo_root,
                    artifact_dir=self.artifact_dir,
                    result=res
                )
                return ExecutionResult(success=True, data=dataclasses.asdict(res))

            elif request.type == "create_working_copy":
                run_id = request.payload.get("run_id") or f"run-{uuid.uuid4().hex[:8]}"
                source_path = request.payload.get("source_path") or self.original_repo_root
                
                # Create ephemeral path
                temp_dir = os.path.join(tempfile.gettempdir(), f"remoroo_worktree_{run_id}")
                
                # Cleanup if exists (unlikely but safe)
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                
                self._log(f"🌲 Creating Ephemeral Working Copy: {temp_dir}")
                
                try:
                    # Case 1: Source is a URL (Cloud/Remote Mode)
                    if source_path.startswith("http://") or source_path.startswith("https://"):
                        if not requests:
                            raise RuntimeError("requests library missing. Cannot download repository.")
                        
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
                        ignore_func = shutil.ignore_patterns('runs', 'artifacts', '.remoroo', '.git', '__pycache__', 'venv', '.env')
                        shutil.copytree(source_path, temp_dir, ignore=ignore_func)
                        
                        # Fix Venv Mode: Symlink venv from original repo if it exists
                        for venv_name in ["venv", ".venv"]:
                            source_venv = os.path.join(source_path, venv_name)
                            if os.path.isdir(source_venv):
                                try:
                                    sym_target = os.path.join(temp_dir, venv_name)
                                    if not os.path.exists(sym_target):
                                        os.symlink(source_venv, sym_target)
                                except OSError: pass
                                break
                        
                        # Handle git: copy .git if source has it, otherwise just init empty
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
                        
                        # v18: Pre-populate artifacts directory with existing files from source
                        # This ensures the model/baseline files are available in the working copy
                        # while still using the isolated run-specific outputs directory.
                        source_artifacts = os.path.join(source_path, "artifacts")
                        if os.path.isdir(source_artifacts) and self.artifact_dir:
                             self._log(f"📦 Pre-populating artifacts from source: {source_artifacts}")
                             try:
                                 # Re-ensure artifact_dir exists
                                 os.makedirs(self.artifact_dir, exist_ok=True)
                                 # Copy all items from source artifacts to run artifacts
                                 for item in os.listdir(source_artifacts):
                                     s = os.path.join(source_artifacts, item)
                                     d = os.path.join(self.artifact_dir, item)
                                     if os.path.isfile(s):
                                         shutil.copy2(s, d)
                                     elif os.path.isdir(s):
                                         shutil.copytree(s, d, dirs_exist_ok=True)
                             except Exception as e:
                                 self._log(f"⚠️  Failed to pre-populate artifacts: {e}")
                    
                    # Create Symlink for artifacts to match Docker behavior (bind mount simulation)
                    # This implies relative writes to ./artifacts/ go to the run's artifact dir.
                    if self.artifact_dir:
                        try:
                            # Re-ensure it exists just in case
                            os.makedirs(self.artifact_dir, exist_ok=True)
                            sym_target = os.path.join(temp_dir, "artifacts")
                            if not os.path.exists(sym_target):
                                os.symlink(self.artifact_dir, sym_target)
                        except OSError:
                            pass 

                    # SWITCH CONTEXT
                    self.repo_root = temp_dir
                    self.is_ephemeral = True
                    
                    # Also need to update sub-components that hold repo_root refs
                    self.worker.repo_root = temp_dir
                    if self.sandbox:
                        self.sandbox.stop()
                        from .sandbox import DockerSandbox
                        self.sandbox = DockerSandbox(self.repo_root, self.artifact_dir, cache_env=self.cache_env)
                    
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
                is_in_temp = self.repo_root.startswith(tempfile.gettempdir())
                
                # FIXED: Always clean up the injection monitor, even if running in-place
                monitor_path = os.path.join(self.repo_root, "remoroo_monitor.py")
                if os.path.exists(monitor_path):
                    try:
                        os.unlink(monitor_path)
                        self._log(f"🧹 Removed injected monitor: {monitor_path}")
                    except Exception as e:
                        self._log(f"⚠️ Failed to remove monitor: {e}")
                
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
                                self.sandbox.exec_run(["chown", "-R", f"{uid}:{gid}", self.repo_root])
                                self.sandbox.exec_run(["chmod", "-R", "777", self.repo_root])
                            except: pass

                        # 2. STOP SANDBOX (to release mounts)
                        if self.sandbox:
                            self._log("   🛑 Stopping Sandbox...")
                            self.sandbox.stop()
                            # Replace with fresh one for future use (if needed)
                            from .sandbox import DockerSandbox
                            self.sandbox = DockerSandbox(self.original_repo_root, self.artifact_dir, cache_env=self.cache_env)

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
                     # Since we staged specific files, we must look at cached diff to see them
                     staged = True
                 
                 # Run diff
                 cmd = "git diff --cached" if staged else "git diff"
                 outcome = executor.run_command_stepwise(
                     cmd,
                     self.repo_root,
                     timeout_s=30,
                     show_progress=False
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
                         is_report = "final_report.md" in path or "report" in path.lower()
                         is_diagram = "system_diagram.md" in path
                         
                         if self.run_id and (is_report or is_diagram):
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
                         
                     # ALSO save to artifact_dir (for CLI transparency in workspace)
                     # v14.1: Mirror to cache if scope is 'original' OR if we are in non-ephemeral mode 
                     # (meaning original and current are the same host dir).
                     if self.artifact_dir and (target_scope == "original" or not self.is_ephemeral):
                         cache_path = os.path.join(self.artifact_dir, path)
                         os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                         with open(cache_path, 'w', encoding='utf-8') as f:
                             f.write(content)

                     # v16: ALSO save to persistence_dir (for CLI transparency in summary/prompt)
                     if self.persistence_dir and (target_scope == "original" or not self.is_ephemeral):
                         persist_path = os.path.join(self.persistence_dir, path)
                         os.makedirs(os.path.dirname(persist_path), exist_ok=True)
                         with open(persist_path, 'w', encoding='utf-8') as f:
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
                
                result_args = {
                    "success": result_success,
                    "data": {
                        "files_updated": files_updated, 
                        "merged_count": total_merged,
                        "metrics_data": final_data
                    },
                    "metrics": final_data
                }
                if hasattr(ExecutionResult, "metrics_changed"):
                    result_args["metrics_changed"] = bool(final_data)
                
                return ExecutionResult(**result_args)

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
                # Get expected metrics from ExperimentContract for filtered extraction
                expected_metrics = request.payload.get("expected_metrics", [])
                is_baseline = request.payload.get("is_baseline", False)
                
                if not cmd:
                    return ExecutionResult(success=False, error="No command provided")
                
                execution_id = f"exec-{uuid.uuid4().hex[:8]}"
                
                # Build execution environment
                # CRITICAL: Separate Host vs Docker env logic
                
                # Default to safe empty env for Docker, overlay payload
                docker_env = request.payload.get("env", {}).copy()
                docker_env["PYTHONUNBUFFERED"] = "1"
                
                # For Local, we need full host env + venv
                local_env = self._get_worker_env(request.payload.get("env", {}))
                
                try:
                    # Log files for async output
                    stdout_buffer = []
                    stderr_buffer = []
                    
                    self._execution_buffers[execution_id] = {
                        "stdout": stdout_buffer,
                        "stderr": stderr_buffer,
                        "start_time": time.time(),
                        "command": cmd,
                        "expected_metrics": expected_metrics,  # For filtered extraction
                        "is_baseline": is_baseline,  # For CLI scoreboard routing
                        "finished": False,
                        "exit_code": None
                    }
                    
                    self._log(f"🚀 [Worker] Starting Async Command: {cmd} (ID: {execution_id})")
                    # self._log(f"   📂 Repo Root: {self.repo_root}")
                    # self._log(f"   📂 Artifact Dir: {self.artifact_dir} (Exists: {os.path.exists(self.artifact_dir)})")

                    # Select Sandbox Factory
                    sandbox_factory = None
                    final_env = {}
                    
                    if self.engine == "docker" and self.sandbox and self.sandbox.available:
                        # Docker Mode
                        # v13: Mirrored Host Path (Zero-Mapping)
                        docker_env["REMOROO_ARTIFACTS_DIR"] = self.artifact_dir
                        docker_env["SDL_VIDEODRIVER"] = "dummy"
                        docker_env["QT_QPA_PLATFORM"] = "offscreen"
                        docker_env["DISPLAY"] = ":99"
                        
                        final_env = docker_env
                        # self._log(f"   🐳 Docker Mode. Env artifact dir: {docker_env['REMOROO_ARTIFACTS_DIR']}")
                        
                        sandbox_factory = lambda c, env=None: self.sandbox.exec_popen(c, env=final_env, workdir=self.repo_root)
                    else:
                        # Local/Venv Mode
                        # CRITICAL: Must point to absolute path on host so code writes where Harness looks
                        local_env["REMOROO_ARTIFACTS_DIR"] = self.artifact_dir
                        final_env = local_env
                        
                        venv_sandbox = VenvSandbox(self.repo_root, system=self.system)
                        # VenvSandbox calls proc.spawn.
                        # CRITICAL: Harness needs stdout/stderr PIPEs to capture output!
                        sandbox_factory = lambda c, env=None: venv_sandbox.exec_popen(
                            c, 
                            env=env or {}, 
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True
                        )

                    # Thread-safe kill bridge: Brain sets this event via kill_command RPC,
                    # Harness checks it in its 0.1s supervision loop and executes _terminate_ladder.
                    kill_event = threading.Event()
                    self._execution_buffers[execution_id]["kill_event"] = kill_event

                    # Background Thread for Harness
                    def harness_runner():
                        harness = RemorooHarness(system=self.system)
                        
                        # Artifact dir is needed for Metric Watching
                        
                        # PROACTIVE PERMISSIONS FIX (Docker Root Fix)
                        # IMPORTANT: Do NOT delete artifacts between commands.
                        # Multi-command workflows rely on read-modify-write into a shared artifacts/metrics.json.
                        if self.engine == "docker" and self.sandbox and self.sandbox.available:
                             try:
                                 uid, gid = os.getuid(), os.getgid()
                                 self.sandbox.exec_run(["chown", "-R", f"{uid}:{gid}", self.artifact_dir])
                                 self.sandbox.exec_run(["chmod", "-R", "777", self.artifact_dir])
                             except Exception:
                                 pass
                             # Remove only legacy root-level metrics file (stale compatibility path).
                             try:
                                 clean_cmd = ["/bin/sh", "-c", f"rm -f {self.repo_root}/{configs.METRICS_FILENAME}"]
                                 self.sandbox.exec_run(clean_cmd)
                             except Exception:
                                 pass

                        result = harness.run(
                            cmd=cmd,
                            runner_factory=sandbox_factory,
                            # "Let it Rain": Default to 24h for async commands. 
                            # This ignores LLM caps and lets the Mid-Turn Judge be the authority.
                            timeout=86400.0,
                            env=final_env.copy(), # CRITICAL: Copy to prevent Harness from mutating our captured env (e.g. overwriting REMOROO_ARTIFACTS_DIR)
                            artifact_dir=self.artifact_dir,
                            stdout_buffer=stdout_buffer,
                            stderr_buffer=stderr_buffer,
                            output_callback=self.output_callback,
                            kill_event=kill_event  # Brain-controlled kill: Brain sets event, Harness executes signal ladder
                        )
                        
                        # Update state when finished
                        # CRITICAL: Populate results BEFORE setting finished=True to avoid race condition with polling
                        
                        if result.success:
                             self._execution_buffers[execution_id]["exit_code"] = 0
                        else:
                             # Safety: result.data might be None if internal error
                             data = result.data or {}
                             self._execution_buffers[execution_id]["exit_code"] = data.get("exit_code", -1)
                             if result.error:
                                 self._log(f"  ❌ [Worker] Harness Error: {result.error}")
                        
                        trigger = result.data.get("trigger") if result.data else "error"
                        
                        # DOCKER ORPHAN CLEANUP (safety net):
                        # When Harness kills a docker exec process (brain_kill or timeout),
                        # the process INSIDE the container survives as an orphan.
                        # Clean it up here as a secondary defense (primary is in kill_command handler).
                        if trigger in ("brain_kill", "timeout") and self.engine == "docker" and self.sandbox and self.sandbox.available:
                            try:
                                self.sandbox.kill_process_by_command(cmd)
                            except Exception as e:
                                self._log(f"  ⚠️  Docker orphan cleanup failed: {e}")
                        
                        # CRITICAL: Store new_artifacts to prevent reading stale metrics
                        new_artifacts = result.data.get("new_artifacts", []) if result.data else []
                        self._execution_buffers[execution_id]["new_artifacts"] = new_artifacts
                        
                        self._log(f"  🏁 [Worker] Async Command Finished: {execution_id} (Trigger: {trigger}, New Artifacts: {len(new_artifacts)})")
                        
                        # MARK FINISHED LAST
                        self._execution_buffers[execution_id]["finished"] = True
                        
                        self._log(f"  🏁 [Worker] Async Command Finished: {execution_id} (Trigger: {trigger}, New Artifacts: {len(new_artifacts)})")
                        
                        if len(new_artifacts) == 0:
                            self._log("  ⚠️ [Worker] NO ARTIFACTS captured. Dumping output for debugging:")
                            stdout_dump = "".join(stdout_buffer[-20:]) # Last 20 lines
                            stderr_dump = "".join(stderr_buffer[-20:])
                            self._log(f"  📜 STDOUT (Tail):\n{stdout_dump}")
                            self._log(f"  📜 STDERR (Tail):\n{stderr_dump}")

                    threading.Thread(target=harness_runner, daemon=True).start()

                    return ExecutionResult(success=True, data={"execution_id": execution_id})
                    
                except Exception as e:
                    return ExecutionResult(success=False, error=str(e))


            elif request.type == "get_output":
                # Poll output snapshot
                exec_id = request.payload.get("execution_id")
                if not exec_id or exec_id not in self._execution_buffers:
                     return ExecutionResult(success=False, error="Execution ID not found")
                
                state = self._execution_buffers[exec_id]

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
                metrics_source = "none"
                
                # Read metrics both when finished AND while still running (for long-lived async commands).
                # When running, we attempt to read live artifact files written by the process.
                if not running:
                    self._log(f"🔎 [Worker] Scanning for metrics (ExecId: {exec_id})...")
                    
                    # Get provably new artifacts from Harness
                    new_artifacts = state.get("new_artifacts", [])
                    
                    # Filter for metric files - AUTHORITATIVE Phase 2
                    is_baseline = state.get("is_baseline", False)
                    # We usually ignore baseline_metrics.json to prevent leakage,
                    # but we must ALLOW it specifically during the baseline phase.
                    metric_candidates = [
                        f for f in new_artifacts 
                        if f.endswith('.json') and ('metric' in f or 'partial' in f) 
                        and (f != "baseline_metrics.json" or is_baseline)
                    ]
                    
                    # Prioritize 'current_metrics.json' if present
                    if "current_metrics.json" in metric_candidates:
                        metric_candidates.remove("current_metrics.json")
                        metric_candidates.insert(0, "current_metrics.json")
                    
                    if not metric_candidates:
                         self._log(f"  ⚠️ [Worker] No new non-baseline metric artifacts found in this run.")
                    
                    # 1. Read directly via Sandbox (Primary)
                    if self.sandbox and self.sandbox.available:
                        def read_sandbox_json(path):
                            res = self.sandbox.exec_run(["cat", path])
                            if res.get("exit_code") == 0:
                                try:
                                    loaded = json.loads(res["stdout"])
                                    extracted = self._robust_extract_metrics(loaded)
                                    if extracted:
                                        self._log(f"  ✅ [Worker] Sandbox Read Success ({path}): {extracted}")
                                        captured_metrics.update(extracted)
                                        return os.path.basename(path)
                                except Exception as e:
                                    self._log(f"  ⚠️ [Worker] Sandbox Read Failed ({path}): {e}")
                            return None

                        # Only read CANDIDATES
                        for fname in metric_candidates:
                             path = os.path.join(self.artifact_dir, fname)
                             source = read_sandbox_json(path)
                             if source:
                                 metrics_source = source
                                 break # Done if found authoritative sandbox file
                        
                        # Docker Legacy Fallback
                        if (not metrics_source or metrics_source == "none") and not captured_metrics:
                             source = read_sandbox_json(os.path.join(self.repo_root, "metrics.json"))
                             if source:
                                 metrics_source = "legacy:metrics.json"
                    
                    # 2. Host Read Fallback
                    if not captured_metrics:
                        for fname in metric_candidates:
                             if self.artifact_dir:
                                 mpath = os.path.join(self.artifact_dir, fname)
                                 if self.system.fs.exists(mpath):
                                     try:
                                         with self.system.fs.open(mpath, 'r') as f:
                                             loaded = json.load(f)
                                             extracted = self._robust_extract_metrics(loaded)
                                             if extracted:
                                                 self._log(f"  ✅ [Worker] Host Read Success ({mpath}): {extracted}")
                                                 captured_metrics.update(extracted)
                                                 metrics_source = fname
                                                 break 
                                     except Exception as e:
                                         self._log(f"  ⚠️ [Worker] Host Read Failed ({mpath}): {e}")
                    
                    if not captured_metrics:
                        # 3. Last Resort Fallback (Host/Repo Root)
                        # MUTANT: STALE_FALLBACK (Restored in v7)
                        cwd_metric = os.path.join(self.repo_root, "metrics.json")
                        if self.system.fs.exists(cwd_metric):
                             try:
                                 with self.system.fs.open(cwd_metric, 'r') as f:
                                     loaded = json.load(f)
                                     extracted = self._robust_extract_metrics(loaded)
                                     if extracted:
                                         self._log(f"  ✅ [Worker] Host Read Success (Legacy CWD): {extracted}")
                                         captured_metrics.update(extracted)
                                         metrics_source = "legacy:metrics.json"
                             except Exception as e:
                                 self._log(f"  ⚠️ [Worker] Host Read Failed (Legacy): {e}")

                # 2.5. Live Artifact Read (while process is STILL running)
                # For long-running async commands (ML training), metrics are written
                # to artifact files periodically. Read them directly.
                # ─── Closing-the-loops Fix 2: dedup live metric reads ───
                # Only attempt live read if enough time passed since last read or
                # if we haven't read yet.  Prevents flooding the brain with
                # duplicate metric snapshots every 5s.
                _last_live_read_hash = state.get("_last_live_metrics_hash", "")
                _live_read_interval = state.get("_live_read_interval", 15.0)  # start at 15s
                _last_live_read_time = state.get("_last_live_read_time", 0)
                _now_lr = time.time()
                _skip_live_read = running and (_now_lr - _last_live_read_time < _live_read_interval)
                if running and not captured_metrics and not _skip_live_read:
                    live_candidates = ["current_metrics.json", configs.METRICS_FILENAME]
                    # Try sandbox read first (Docker)
                    if self.sandbox and self.sandbox.available:
                        for fname in live_candidates:
                            path = os.path.join(self.artifact_dir, fname)
                            try:
                                res = self.sandbox.exec_run(["cat", path])
                                if res.get("exit_code") == 0 and res.get("stdout", "").strip():
                                    loaded = json.loads(res["stdout"])
                                    extracted = self._robust_extract_metrics(loaded)
                                    if extracted:
                                        captured_metrics.update(extracted)
                                        metrics_source = f"live:{fname}"
                                        break
                            except Exception:
                                pass
                    # Host fallback for live read
                    if not captured_metrics and self.artifact_dir:
                        for fname in live_candidates:
                            mpath = os.path.join(self.artifact_dir, fname)
                            if self.system.fs.exists(mpath):
                                try:
                                    with self.system.fs.open(mpath, 'r') as f:
                                        loaded = json.load(f)
                                        extracted = self._robust_extract_metrics(loaded)
                                        if extracted:
                                            captured_metrics.update(extracted)
                                            metrics_source = f"live:{fname}"
                                            break
                                except Exception:
                                    pass

                # ─── Fix 2 continued: dedup captured metrics ───
                if running and captured_metrics:
                    _metrics_hash = hashlib.sha256(
                        json.dumps(captured_metrics, sort_keys=True).encode()
                    ).hexdigest()
                    if _metrics_hash == _last_live_read_hash:
                        # Same metrics as last poll — drop and increase backoff
                        captured_metrics = {}
                        metrics_source = "dedup_skip"
                        state["_live_read_interval"] = min(_live_read_interval * 1.5, 60.0)
                    else:
                        # Genuinely new metrics — reset backoff
                        state["_last_live_metrics_hash"] = _metrics_hash
                        state["_live_read_interval"] = 15.0
                    state["_last_live_read_time"] = _now_lr

                # 3. Last Resort: Parse stdout with ExperimentContract filter
                if not captured_metrics:
                    expected = state.get("expected_metrics", [])
                    #self._log(f"  🔍 [DEBUG] expected_metrics = {expected}")
                    # DEBUG: Show buffer state to diagnose timing issues
                    #self._log(f"  🔍 [DEBUG] stdout_buffer_lines = {len(state['stdout'])}, last_line = {repr(state['stdout'][-1][:80]) if state['stdout'] else 'empty'}")
                    if expected:
                        log_metrics = self._extract_metrics_from_text(stdout_full, expected_metrics=expected)
                        if log_metrics:
                            self._log(f"  📊 [Worker] Log Parse Success (filtered): {log_metrics}")
                            captured_metrics.update(log_metrics)
                            metrics_source = "stdout_filtered"
                    #     else:
                    #         self._log(f"  ⚠️ [Worker] No metrics found in artifacts or logs (expected: {expected}).")
                    # else:
                    #     self._log(f"  ⚠️ [Worker] No metrics found in artifacts (no expected_metrics filter).")
                # ------------------------------------

                # Determine phase for CLI scoreboard routing
                phase = "baseline" if state.get("is_baseline") else "current"
                
                result_args = {
                    "success": True,
                    "data": {
                        "stdout": stdout_full,
                        "stderr": stderr_full,
                        "is_running": running,
                        "exit_code": state["exit_code"],
                        "elapsed_s": elapsed,
                        "metrics_captured": bool(captured_metrics),
                        "metrics_source": metrics_source,
                        "phase": phase
                    },
                    "metrics": captured_metrics
                }
                # Architecture Migration: Only pass metrics_changed if the protocol supports it
                if hasattr(ExecutionResult, "metrics_changed"):
                    result_args["metrics_changed"] = bool(captured_metrics)
                
                return ExecutionResult(**result_args)
                
            elif request.type == "kill_command":
                exec_id = request.payload.get("execution_id")
                
                # Primary path: use kill_event from _execution_buffers (Harness-managed async commands)
                buf = self._execution_buffers.get(exec_id)
                if buf:
                    kill_event = buf.get("kill_event")
                    if kill_event:
                        kill_event.set()  # Harness checks this in its 0.1s loop and executes _terminate_ladder
                        
                        # CRITICAL: Docker orphan kill.
                        # When running in Docker, SIGTERM/SIGKILL to the host-side `docker exec`
                        # process does NOT propagate to the process INSIDE the container.
                        # The container process survives as an orphan, continuing to write to
                        # artifact files (metrics.json, baseline_metrics.json) and corrupting
                        # subsequent turn executions.
                        # Fix: directly kill the process inside the container by command pattern.
                        if self.engine == "docker" and self.sandbox and self.sandbox.available:
                            cmd_to_kill = buf.get("command", "")
                            if cmd_to_kill:
                                try:
                                    self.sandbox.kill_process_by_command(cmd_to_kill)
                                except Exception as e:
                                    self._log(f"⚠️  Docker orphan kill failed: {e}")
                        
                        return ExecutionResult(success=True, data={"killed": True, "mechanism": "event"})
                
                # Fallback: legacy direct-process kill (non-harness commands)
                if exec_id and exec_id in self._running_processes:
                    proc_or_thread = self._running_processes[exec_id]
                    try:
                        if isinstance(proc_or_thread, subprocess.Popen):
                            proc_or_thread.terminate()  # SIGTERM
                            try:
                                proc_or_thread.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                proc_or_thread.kill()  # SIGKILL
                                proc_or_thread.wait(timeout=3)
                            if exec_id in self._execution_buffers:
                                self._execution_buffers[exec_id]["finished"] = True
                                self._execution_buffers[exec_id]["exit_code"] = -15  # SIGTERM
                            return ExecutionResult(success=True, data={"killed": True, "mechanism": "direct"})
                        else:
                            return ExecutionResult(success=True, data={"killed": False, "reason": "Sandbox kill not impl"})
                    except Exception as e:
                         return ExecutionResult(success=False, error=str(e))
                
                # Not found anywhere
                return ExecutionResult(success=True, data={"killed": False, "reason": "Not found"})

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

            elif request.type == "get_worker_info":
                # Enhanced hardware capability detection
                cpu_count = os.cpu_count()
                memory_gb = 0
                gpu_info = "none"
                
                try:
                    if sys.platform == 'darwin':
                        # macOS
                        mem_bytes = subprocess.check_output(['sysctl', '-n', 'hw.memsize'], stderr=subprocess.DEVNULL).decode().strip()
                        memory_gb = round(int(mem_bytes) / (1024**3), 1)
                        if platform.machine() == 'arm64':
                            gpu_info = "Apple Silicon GPU (MPS available)"
                    elif sys.platform == 'win32':
                        # Windows (simplified)
                        gpu_info = "unknown (windows)"
                    else:
                        # Linux
                        if os.path.exists('/proc/meminfo'):
                            with open('/proc/meminfo', 'r') as f:
                                for line in f:
                                    if 'MemTotal' in line:
                                        mem_kb = line.split()[1]
                                        memory_gb = round(int(mem_kb) / (1024**2), 1)
                                        break
                        # Basic nvidia-smi check
                        try:
                            gpu_info = subprocess.check_output(['nvidia-smi', '--query-gpu=gpu_name', '--format=csv,noheader'], stderr=subprocess.DEVNULL).decode().strip()
                        except:
                            pass

                except Exception:
                    pass

                info = {
                    "platform": platform.platform(),
                    "machine": platform.machine(),
                    "processor": platform.processor(),
                    "python_version": platform.python_version(),
                    "os": platform.system(),
                    "release": platform.release(),
                    "version": platform.version(),
                    "cpu_count": cpu_count,
                    "memory_gb": memory_gb,
                    "gpu_info": gpu_info
                }
                return ExecutionResult(success=True, data=info)

            else:
                return ExecutionResult(success=False, error=f"Unknown request type: {request.type}")


        except Exception as e:
            traceback.print_exc()
            return ExecutionResult(success=False, error=str(e))
        finally:
            # Restore Context (Stateless behavior)
            # v15: Safety check - if the command was to PERMANENTLY change/cleanup the root, 
            # we do NOT restore the old now-deleted/old root.
            if request.type not in ["cleanup_working_copy", "create_working_copy"]:
                self.repo_root = previous_root

    def _handle_file_exists(self, request: ExecutionRequest) -> ExecutionResult:
        path = request.payload.get("path", "")
        exists = os.path.exists(os.path.join(self.repo_root, path))
        return ExecutionResult(success=True, data={"exists": exists})

# Alias for backward compatibility with CLI and Remote Worker
WorkerService = LocalWorker
