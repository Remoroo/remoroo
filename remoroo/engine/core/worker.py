import os
import shlex
import time
import json
from typing import Dict, Any, List, Optional, Tuple, Callable, Union
from dataclasses import dataclass

from .executor import run_command_stepwise, run_command_plan
from .applier import apply_patchproposal, ApplyError
from ..utils.file_access_tracker import FileAccessTracker

@dataclass
class Worker:
    """
    The Worker (Execution Layer).
    
    Responsibilities:
    1. Execute command plans (shell commands).
    2. Apply patch bundles (file edits).
    3. Execute exploration probes (grep/find, scripts).
    """
    def __init__(self, repo_root: str, artifact_dir: str, file_access_tracker: Optional[FileAccessTracker] = None):
        self.repo_root = repo_root
        self.artifact_dir = artifact_dir
        self.file_access_tracker = file_access_tracker or FileAccessTracker()

    def run_python_script(self, script: str, python_path: Optional[str] = None, filename: str = "__remoroo_probe_worker.py") -> Tuple[str, int]:
        """
        Write and run a Python script in the repo root.
        """
        import os
        path = os.path.join(self.repo_root, filename)
        try:
            with open(path, "w") as f:
                f.write(script)
            
            cmd_python = python_path if python_path else "python"
            cmd = f"{cmd_python} {filename}"
            
            # Use empty env but inject ARTIFACTS_DIR
            probe_env = {}
            probe_env["REMOROO_ARTIFACTS_DIR"] = os.path.join(self.repo_root, "artifacts")
            
            outcome = run_command_stepwise(
                cmd=cmd,
                repo_root=self.repo_root,
                timeout_s=30,
                env=probe_env
            )
            
            stdout = outcome.get("stdout", "")
            stderr = outcome.get("stderr", "")
            exit_code = outcome.get("exit_code", 1)
            
            combined = stdout
            if stderr:
                combined += f"\n[STDERR]\n{stderr}"
                
            return combined.strip(), exit_code
        finally:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except:
                    pass

    def normalize_command_plan(
        self,
        command_plan: Dict[str, Any],
        repo_index_summary: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Normalize Planner-provided command_plan against repo reality.
        Only drops commands when highly confident they cannot work due to missing paths.
        """
        if not isinstance(command_plan, dict):
            return command_plan
        
        def _path_exists(rel_or_abs: str) -> bool:
            if not rel_or_abs:
                return False
            p = rel_or_abs.strip()
            if not p:
                return False
            if os.path.isabs(p):
                return os.path.exists(p)
            return os.path.exists(os.path.join(self.repo_root, p))
        
        def _should_drop_command(cmd: str) -> Tuple[bool, str]:
            """Return (drop, reason)."""
            try:
                tokens = shlex.split(cmd)
            except Exception:
                tokens = cmd.split()
            
            low = cmd.lower()
            
            # Pattern 1: unittest discover -s/--start-directory <dir>
            if "unittest" in low and "discover" in low:
                for flag in ("-s", "--start-directory"):
                    if flag in tokens:
                        idx = tokens.index(flag)
                        if idx + 1 < len(tokens):
                            start_dir = tokens[idx + 1]
                            if not _path_exists(start_dir):
                                return True, f"unittest discover start dir missing: {start_dir}"
            
            # Pattern 2: generic path existence check for commands that reference local paths.
            for i, tok in enumerate(tokens[1:], start=1):
                if not tok or tok.startswith("-"):
                    continue
                if tokens[i - 1] in ("-c",):  # python -c "..."
                    continue
                # Common test folder patterns
                if tok in ("tests", "tests/", "test", "test/"):
                    if not _path_exists(tok.rstrip("/")):
                        return True, f"referenced path missing: {tok}"
                # Any explicit relative path (contains /) should exist
                if "/" in tok and not tok.startswith(("http://", "https://")):
                    if not _path_exists(tok):
                        return True, f"referenced path missing: {tok}"
            
            return False, ""
        
        normalized: Dict[str, Any] = {}
        dropped_cmds: List[Tuple[str, str, str]] = []
        
        for stage_name, cmds in command_plan.items():
            if stage_name == "diagnostics_on_failure":
                normalized[stage_name] = cmds
                continue
            if not isinstance(cmds, list):
                normalized[stage_name] = cmds
                continue
            
            kept: List[Any] = []
            for cmd in cmds:
                if not isinstance(cmd, str):
                    kept.append(cmd)
                    continue
                drop, reason = _should_drop_command(cmd)
                if drop:
                    dropped_cmds.append((stage_name, cmd, reason))
                    continue
                kept.append(cmd)
            
            if kept:
                normalized[stage_name] = kept
        
        if dropped_cmds:
            print("  🧹 Normalized command_plan: dropped command(s) that reference missing paths:")
            for stage_name, cmd, reason in dropped_cmds[:10]:
                print(f"    - [{stage_name}] {cmd}  ({reason})")
        
        # If all executable stages were dropped, fall back to repo_index_summary entrypoints (if available)
        executable_stages = [k for k in normalized.keys() if k != "diagnostics_on_failure"]
        if not executable_stages and repo_index_summary:
            entrypoints = repo_index_summary.get("entrypoints", []) or []
            best = next((ep for ep in entrypoints if ep.get("kind") == "python_main" and ep.get("how_to_run")), None)
            if not best:
                best = next((ep for ep in entrypoints if ep.get("how_to_run")), None)
            if best and best.get("how_to_run"):
                normalized["Stage_1"] = [best["how_to_run"]]
                print(f"  🧭 command_plan was empty after normalization; using entrypoint fallback: {best['how_to_run']}")
        
        return normalized

    def execute_plan(
        self,
        # Legacy args (kept for compatibility during migration, or passed as None if request obj used)
        turn: Optional[int] = None,
        command_plan: Optional[Dict[str, Any]] = None,
        suggested_timeouts: Optional[Dict[str, float]] = None,
        timeout_failures: Optional[Dict[str, Dict[str, Any]]] = None,
        instrumentation_enabled: bool = False,
        metrics_phase_env_var: str = "REMOROO_METRICS_PHASE",
        step_callback: Callable[[int, str, Dict[str, Any], str], Dict[str, Any]] = None,
        # New Contract Arg
        request: Optional['ExecutionRequest'] = None
    ) -> Union[Dict[str, Any], 'ExecutionResult']:
        """
        Execute commands stepwise. Supports both legacy args and ExecutionRequest object.
        """
        # Resolve Request
        from ..protocol.execution_contract import ExecutionRequest, ExecutionResult
        
        if request:
            turn = request.turn
            command_plan = request.command_plan
            suggested_timeouts = request.suggested_timeouts
            timeout_failures = request.timeout_failures
            instrumentation_enabled = request.instrumentation_enabled
            metrics_phase_env_var = request.metrics_phase_env_var
        
        # Ensure mandatory args are present
        if turn is None or command_plan is None:
             raise ValueError("Missing turn or command_plan in execute_plan")

        # Get all commands from all stages (flatten command plan)
        all_commands = []
        stage_commands_map = {}
        for stage_name, stage_commands in command_plan.items():
            if stage_name != "diagnostics_on_failure" and isinstance(stage_commands, list):
                for cmd in stage_commands:
                    all_commands.append(cmd)
                    stage_commands_map[len(all_commands) - 1] = stage_name
        
        if not all_commands:
            result_dict = {
                "decision": "ITERATE",
                "reason": "No commands to execute",
                "all_outcomes": [],
                "commands_executed": [],
                "executed_stages": [],
                "final_output": "No commands in plan."
            }
            if request:
                return ExecutionResult(**result_dict)
            return result_dict
        
        # Build output callback
        output_buffer = []
        def output_callback(line: str, stream_type: str):
            output_buffer.append((stream_type, line))
            if len(output_buffer) > 500:
                output_buffer.pop(0)
        
        executed_commands = []
        executed_outcomes = []
        executed_stages = []
        
        # Ensure artifacts dir exists
        try:
            os.makedirs(os.path.join(self.repo_root, "artifacts"), exist_ok=True)
        except Exception:
            pass

        # Instrumentation phase env
        exec_env = None
        if instrumentation_enabled:
            exec_env = {str(metrics_phase_env_var): "current"}
        
        for cmd_idx, cmd in enumerate(all_commands):
            stage_name = stage_commands_map.get(cmd_idx, "unknown")
            stage_timeout = suggested_timeouts.get(stage_name, None) if suggested_timeouts else None
            
            # Adjust timeout based on previous failures
            if stage_name in timeout_failures:
                failure_info = timeout_failures[stage_name]
                last_timeout = failure_info.get("last_timeout")
                failure_count = failure_info.get("failure_count", 0)
                last_duration = failure_info.get("last_duration", 0)
                
                if stage_timeout and last_timeout and last_duration >= last_timeout * 0.9:
                    multiplier = 1.5 + (failure_count * 0.2)
                    new_timeout = max(stage_timeout, last_timeout * multiplier)
                    if new_timeout > stage_timeout:
                        print(f"  ⏱️  Adjusting timeout for {stage_name}: {stage_timeout}s → {new_timeout}s (previous failure)")
                        stage_timeout = new_timeout
            
            print(f"\n📦 [{stage_name.upper()}] Command {cmd_idx + 1}/{len(all_commands)}: {cmd}")
            
            # Execute command
            outcome = run_command_stepwise(
                cmd=cmd,
                repo_root=self.repo_root,
                timeout_s=stage_timeout,
                output_callback=output_callback,
                env=exec_env
            )
            
            # Track timeout failures
            exit_code = outcome.get("exit_code", 0)
            timed_out = outcome.get("timed_out", False)
            if exit_code == -9 or timed_out:
                duration = outcome.get("duration_s", 0)
                if stage_name not in timeout_failures:
                    timeout_failures[stage_name] = {
                        "last_timeout": stage_timeout,
                        "failure_count": 0,
                        "last_duration": duration
                    }
                timeout_failures[stage_name]["failure_count"] += 1
                timeout_failures[stage_name]["last_timeout"] = stage_timeout
                timeout_failures[stage_name]["last_duration"] = duration
                print(f"  ⚠️  Timeout failure recorded for {stage_name}")
            elif exit_code == 0 and stage_name in timeout_failures:
                del timeout_failures[stage_name]
                print(f"  ✅ Timeout failure tracking reset for {stage_name}")
            
            executed_commands.append(cmd)
            executed_outcomes.append(outcome)
            if stage_name not in executed_stages:
                executed_stages.append(stage_name)
            
            # Build command output for callback
            cmd_output = ""
            if outcome.get("stdout"):
                cmd_output += f"[stdout]\n{outcome['stdout']}\n"
            if outcome.get("stderr"):
                cmd_output += f"[stderr]\n{outcome['stderr']}\n"
            
            # Invoke callback (Judge)
            if step_callback:
                judge_result = step_callback(
                    cmd_idx, 
                    len(all_commands), 
                    cmd, 
                    outcome, 
                    cmd_output, 
                    executed_commands, 
                    executed_outcomes
                )
                
                decision = judge_result.get("decision", "UNKNOWN")
                reason = judge_result.get("reason", "")
                
                if decision in ["SUCCESS", "FAIL", "REPLAN_NOW"]:
                    result_dict = {
                        "decision": decision,
                        "reason": reason,
                        "all_outcomes": executed_outcomes,
                        "commands_executed": executed_commands,
                        "executed_stages": executed_stages, # Add missing field
                        "final_output": cmd_output # Add missing field
                    }
                    if request:
                        return ExecutionResult(**result_dict)
                    return result_dict
                elif decision == "CONTINUE":
                    continue
                else:
                    print(f"  ⚠️ Unknown decision: {decision}, continuing")
                    continue
        
        # Helper to format output
        final_out = ""
        if executed_outcomes:
            last = executed_outcomes[-1]
            if last.get("stdout"): final_out += f"[stdout]\n{last['stdout']}\n"
            if last.get("stderr"): final_out += f"[stderr]\n{last['stderr']}\n"

        result_dict = {
            "decision": "ITERATE",
            "reason": "Plan completed",
            "all_outcomes": executed_outcomes,
            "commands_executed": executed_commands,
            "executed_stages": executed_stages,
            "final_output": final_out
        }
        if request:
             return ExecutionResult(**result_dict)
        return result_dict

    def execute_exploration(self, request: Dict[str, Any], toolsmith_agent: Any = None) -> str:
        """
        Execute exploration search (grep/find).
        """
        tool = request.get("tool")
        query = request.get("query")
        path = request.get("path") or "."
        
        if path.startswith("/") or ".." in path:
            path = "."
        
        cmd = ""
        if tool == "grep":
            safe_query = shlex.quote(query)
            safe_path = shlex.quote(path)
            cmd = f"grep -rnI {safe_query} {safe_path} | head -n 20"
        elif tool == "find":
            safe_query = shlex.quote(query)
            safe_path = shlex.quote(path)
            cmd = f"find {safe_path} -name {safe_query} -not -path '*/.*' | head -n 20"
        elif tool == "python_script":
            probe_filename = "__remoroo_probe.py"
            probe_path = os.path.join(self.repo_root, probe_filename)
            try:
                with open(probe_path, "w") as f:
                    f.write(query)
                import sys
                cmd = f"{sys.executable} {probe_filename}"
            except Exception as e:
                return f"Failed to prepare probe script: {e}"
        elif tool == "toolsmith":
            if toolsmith_agent:
                # Milestone 3: 'toolsmith_agent' is now ToolsmithPlanner
                # It generates intent, we execute.
                script, rationale = toolsmith_agent.generate_probe(query)
                if not script:
                    return f"Error: Toolsmith failed to generate probe ({rationale})"
                
                output, exit_code = self.run_python_script(script, filename="__remoroo_probe_toolsmith.py")
                
                # Feed back result to update planner context/memory
                return toolsmith_agent.register_result(query, output, exit_code)
            else:
                return "Error: Toolsmith agent is not available."
        else:
            return f"Error: Unknown tool '{tool}'"
        
        print(f"  🔍 Executing exploration search: {cmd}")
        
        outcome = run_command_stepwise(
            cmd=cmd,
            repo_root=self.repo_root,
            timeout_s=10,
            env={}
        )
        
        output = outcome.get("stdout", "").strip()
        stderr = outcome.get("stderr", "").strip()
        
        if outcome.get("exit_code") != 0 and stderr:
             result = f"Search failed (exit code {outcome.get('exit_code')}): {stderr}"
        elif not output:
            result = "No output produced."
        else:
            result = output

        if tool == "python_script":
            try:
                probe_path = os.path.join(self.repo_root, "__remoroo_probe.py")
                if os.path.exists(probe_path):
                    os.remove(probe_path)
            except Exception:
                pass
                
        return result

    def apply_patch_bundle(self, patch_proposal: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Apply a patch proposal to the repository.
        Returns (applied_edits, skipped_edits).
        Raises ApplyError on failure.
        """
        return apply_patchproposal(
            repo_root=self.repo_root,
            patch=patch_proposal,
            file_access_tracker=self.file_access_tracker
        )
