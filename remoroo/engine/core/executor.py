"""
Executor module for running commands.
"""
from __future__ import annotations
import subprocess
import time
import signal
import threading
import sys
import os
from typing import List, Dict, Any, Optional, Callable, Union

def _print(message: str, show_progress: bool = True, output_callback: Optional[Callable] = None, **kwargs):
    """Internal helper to route prints through callback or standard print."""
    if output_callback:
        # Remove print-specific kwargs that callbacks (like console.print) might reject
        cb_kwargs = kwargs.copy()
        cb_kwargs.pop("flush", None)
        cb_kwargs.pop("file", None)
        
        try:
            # Try to pass as single message if it's a simple logger
            output_callback(message, **cb_kwargs)
        except Exception as e1:
            try:
                # Fallback to executor-style (message, stream)
                output_callback(message, "stdout")
            except Exception as e2:
                # If both fail, print the original message to stderr so we don't lose it
                sys.__stderr__.write(f"Callback failed: {e1} / {e2}\nMessage: {message}\n")
    elif show_progress:
        print(message, **kwargs)

def run_command_with_timeout(
    cmd: str,
    cwd: str,
    timeout_s: Optional[float] = None,
    show_progress: bool = True,
    output_callback: Optional[Callable[[str, str], None]] = None,
    convergence_checker: Optional[Callable[[str, float], Optional[Dict[str, Any]]]] = None,
    judge_checker: Optional[Callable[[str, float], Optional[Dict[str, Any]]]] = None,
    min_runtime_s: float = 30.0,
    env: Optional[Dict[str, str]] = None,
    runner_factory: Optional[Callable[..., subprocess.Popen]] = None

) -> Dict[str, Any]:
    """
    Run a single command with optional timeout, streaming output in real-time.
    runner_factory: Optional execution backend (default: subprocess.Popen)
    """
    start_time = time.time()
    timed_out = False
    # Use a mutable container to share stopped_early across threads
    stopped_early_container = {"value": False}
    # Make timeout mutable so Judge can extend it during execution
    timeout_container = {"value": timeout_s}
    convergence_info = None
    
    if show_progress:
        timeout_str = f" (timeout: {timeout_s}s)" if timeout_s else ""
        _print(f"  ▶️  Running: {cmd}{timeout_str}", show_progress, output_callback)
        _print("  " + "-" * 60, show_progress, output_callback)
    
    stdout_lines = []
    stderr_lines = []
    output_buffer = []  # For convergence checking and judge checking
    last_check_time = start_time
    judge_info = None  # Store judge decision if judge_checker stops execution
    
    # Dynamic parameters (will be set by LLM on first convergence check)
    current_min_runtime = min_runtime_s
    current_check_interval = 30.0  # Default, will be updated by LLM
    current_confidence_threshold = 0.8  # Default, will be updated by LLM
    pattern_keywords = ['metric', 'progress', 'complete', 'success', 'error']  # Generic defaults, will be updated by LLM
    
    # Thread-safe flag for early stopping
    should_stop = threading.Event()
    
    try:
        # Set PYTHONUNBUFFERED=1 to disable Python's output buffering for real-time output
        proc_env = os.environ.copy()
        proc_env['PYTHONUNBUFFERED'] = '1'
        proc_env['REMOROO_START_TIME'] = str(start_time)
        if timeout_s:
            proc_env['REMOROO_TIMEOUT_LIMIT'] = str(timeout_s)
        if env:
            # Per-command env overlay (values must be strings for subprocess)
            for k, v in env.items():
                if k:
                    proc_env[str(k)] = str(v)


        if runner_factory:
            p = runner_factory(cmd, env=proc_env)
        else:
            p = subprocess.Popen(
                cmd,
                cwd=cwd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True,
                env=proc_env,
                preexec_fn=os.setsid if sys.platform != 'win32' else None
            )
        
        def read_stdout():
            nonlocal last_check_time, current_min_runtime, current_check_interval, current_confidence_threshold, pattern_keywords, convergence_info
            for line in iter(p.stdout.readline, ''):
                if not line:
                    break
                
                line = line.rstrip()
                stdout_lines.append(line)
                output_buffer.append(("stdout", line))
                
                if show_progress:
                    _print(f"  {line}", show_progress, output_callback)
                elif output_callback:
                    _print(line, False, output_callback)
                
                # Convergence check logic here
                current_time = time.time()
                elapsed = current_time - start_time
                
                should_check = False
                if convergence_checker:
                    time_since_last_check = current_time - last_check_time
                    # pattern_triggered = any(keyword in line.lower() for keyword in pattern_keywords)
                    interval_triggered = time_since_last_check >= current_check_interval
                    
                    min_throttle_seconds = 10.0
                    if elapsed >= current_min_runtime and time_since_last_check >= min_throttle_seconds:
                        if interval_triggered:
                            should_check = True
                    
                    if should_check:
                        last_check_time = current_time
                        combined_output = "\n".join([f"[{s}] {l}" for s, l in output_buffer[-100:]])
                        try:
                            decision = convergence_checker(combined_output, elapsed)
                            if decision:
                                params = decision.get("convergence_parameters", {})
                                if params:
                                    if "min_runtime_s" in params:
                                        current_min_runtime = max(current_min_runtime, params["min_runtime_s"])
                                    if "check_interval_s" in params:
                                        current_check_interval = max(5.0, params["check_interval_s"])
                                    if "confidence_threshold" in params:
                                        current_confidence_threshold = max(0.5, min(1.0, params["confidence_threshold"]))
                                    if "pattern_keywords" in params and isinstance(params["pattern_keywords"], list):
                                        pattern_keywords = params["pattern_keywords"]
                                
                                should_stop_flag = decision.get("should_stop", False)
                                confidence = decision.get("confidence", 0.0)
                                
                                if should_stop_flag and confidence >= current_confidence_threshold:
                                    if show_progress:
                                        _print(f"\n  🎯 Early stopping detected (confidence: {confidence:.2f}, threshold: {current_confidence_threshold:.2f})", show_progress, output_callback)
                                        _print(f"     Reason: {decision.get('reason', 'N/A')[:100]}", show_progress, output_callback)
                                    convergence_info = decision
                                    should_stop.set()
                                    stopped_early_container["value"] = True
                                    
                                    # Terminate process group
                                    if sys.platform != 'win32':
                                        try:
                                            os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                                        except:
                                            p.terminate()
                                    else:
                                        p.terminate()
                                        
                                    time.sleep(1)
                                    if p.poll() is None:
                                        if sys.platform != 'win32' and is_safe_to_killpg:
                                            try:
                                                os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                                            except:
                                                p.kill()
                                        else:
                                            p.kill()
                                    break
                        except Exception as e:
                            if show_progress:
                                error_str = str(e).lower()
                                if "json" not in error_str and "parse" not in error_str:
                                    _print(f"  ⚠️  Convergence check error: {str(e)[:80]}", show_progress, output_callback, file=sys.stderr)
            p.stdout.close()
        
        def read_stderr():
            for line in iter(p.stderr.readline, ''):
                if not line:
                    break
                
                line = line.rstrip()
                stderr_lines.append(line)
                output_buffer.append(("stderr", line))
                
                if show_progress:
                    _print(f"  ⚠️  {line}", show_progress, output_callback)
                elif output_callback:
                    _print(line, False, output_callback)
            p.stderr.close()
        
        t1 = threading.Thread(target=read_stdout)
        t2 = threading.Thread(target=read_stderr)
        t1.daemon = True
        t2.daemon = True
        t1.start()
        t2.start()
        
        # Wait for process with timeout
        try:
            while p.poll() is None:
                if should_stop.is_set():
                    break
                
                if timeout_container["value"] and (time.time() - start_time) > timeout_container["value"]:
                    # Suicide Prevention: Don't killpg if it's our own group
                    is_safe_to_killpg = False
                    if sys.platform != 'win32':
                        try:
                            # Verify we are not killing ourselves
                            proc_pg = os.getpgid(p.pid)
                            my_pg = os.getpgrp()
                            is_safe_to_killpg = (proc_pg != my_pg)
                        except Exception:
                            # If we can't determine PGID, assume unsafe
                            is_safe_to_killpg = False

                    if sys.platform != 'win32' and is_safe_to_killpg:
                        try:
                            # Try graceful termination first
                            os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                            # Give it a moment to exit
                            wait_start = time.time()
                            while time.time() - wait_start < 1.0:
                                if p.poll() is not None:
                                    break
                                time.sleep(0.1)
                            
                            # Force kill if still running
                            if p.poll() is None:
                                os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                        except:
                            p.kill()
                    else:
                        p.terminate()
                        time.sleep(1)
                        if p.poll() is None:
                            p.kill()
                    timed_out = True
                    break
                
                if judge_checker:
                    current_time = time.time()
                    elapsed = current_time - start_time
                    combined_output = "\n".join([f"[{s}] {l}" for s, l in output_buffer])
                    try:
                        judge_decision = judge_checker(combined_output, elapsed)
                        if judge_decision:
                            decision = judge_decision.get("decision", "CONTINUE")
                            
                            if decision == "CONTINUE":
                                reason = judge_decision.get("reason", "")
                                import re
                                timeout_match = re.search(r'(?:extend|extended|timeout).*?(?:to|:)\s*(\d+)\s*s(?:ec(?:ond)?s?)?', reason.lower())
                                if timeout_match:
                                    new_timeout = float(timeout_match.group(1))
                                    current_timeout = timeout_container["value"]
                                    if current_timeout is None or new_timeout > current_timeout:
                                        timeout_container["value"] = new_timeout
                                        if show_progress:
                                            _print(f"    ⏱️  Judge extends timeout to {new_timeout}s", show_progress, output_callback)
                            
                            if decision in ["SUCCESS", "FAIL", "REPLAN_NOW", "PARTIAL_SUCCESS"]:
                                judge_info = judge_decision
                                should_stop.set()
                                stopped_early_container["value"] = True
                                
                                # Terminate safely
                                is_safe_to_killpg = False
                                if sys.platform != 'win32':
                                    try:
                                        proc_pg = os.getpgid(p.pid)
                                        my_pg = os.getpgrp()
                                        is_safe_to_killpg = (proc_pg != my_pg)
                                    except:
                                        is_safe_to_killpg = False

                                if sys.platform != 'win32' and is_safe_to_killpg:
                                    try:
                                        os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                                    except:
                                        p.terminate()
                                else:
                                    p.terminate()
                                    
                                time.sleep(1)
                                if p.poll() is None:
                                    if sys.platform != 'win32' and is_safe_to_killpg:
                                        try:
                                            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                                        except:
                                            p.kill()
                                    else:
                                        p.kill()
                                break
                    except Exception as e:
                        if show_progress:
                            error_str = str(e).lower()
                            if "json" not in error_str and "parse" not in error_str:
                                _print(f"  ⚠️  Judge check error: {str(e)}", show_progress, output_callback, file=sys.stderr)
                
                # Fast polling (20Hz) provides "instant" detection without the performance hit
                # of checking disk for every single line of stdout.
                time.sleep(0.05)
            
            t1.join(timeout=2)
            t2.join(timeout=2)
        except Exception as e:
            if show_progress:
                _print(f"  ⚠️  Error waiting for process: {e}", show_progress, output_callback)
        finally:
            if p.poll() is None:
                # Cleanup safely
                is_safe_to_killpg = False
                if sys.platform != 'win32':
                    try:
                        proc_pg = os.getpgid(p.pid)
                        my_pg = os.getpgrp()
                        is_safe_to_killpg = (proc_pg != my_pg)
                    except:
                        is_safe_to_killpg = False

                if sys.platform != 'win32' and is_safe_to_killpg:
                    try:
                        os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                        time.sleep(0.5)
                        if p.poll() is None:
                            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                    except:
                        p.terminate()
                        time.sleep(0.5)
                        if p.poll() is None:
                            p.kill()
                else:
                    p.terminate()
                    time.sleep(0.5)
                    if p.poll() is None:
                        p.kill()
        
        stopped_early = stopped_early_container["value"]
        
        if stopped_early:
            returncode = 0
        else:
            returncode = p.returncode if p.returncode is not None else -1
            
        duration_s = round(time.time() - start_time, 3)
        
        if show_progress:
            _print("  " + "-" * 60, show_progress, output_callback)
            if stopped_early:
                status = "🎯"
                decision_info = judge_info if judge_info else convergence_info
                
                if decision_info:
                    decision_type = "Judge decision" if judge_info else "convergence detected"
                    _print(f"  {status} Stopped early in {duration_s:.1f}s ({decision_type} - SUCCESS)", show_progress, output_callback)
                if decision_info:
                    decision = decision_info.get("decision")
                    if decision:
                        _print(f"     Decision: {decision}", show_progress, output_callback)
            else:
                status = "✅" if returncode == 0 and not timed_out else "❌"
                _print(f"  {status} Completed in {duration_s:.1f}s (exit code: {returncode})", show_progress, output_callback)
        
        stderr_final = "\n".join(stderr_lines)
        
        result = {
            "cmd": cmd,
            "exit_code": int(returncode),
            "duration_s": duration_s,
            "stdout": "\n".join(stdout_lines),
            "stderr": stderr_final,
            "timed_out": timed_out,
            "stopped_early": stopped_early
        }
        if judge_info:
            result["convergence_info"] = judge_info
        elif convergence_info:
            result["convergence_info"] = convergence_info
        return result
    except subprocess.TimeoutExpired:
        timed_out = True
        duration_s = round(time.time() - start_time, 3)
        if show_progress:
            _print("  " + "-" * 60, show_progress, output_callback)
            _print(f"  ⏱️  TIMEOUT after {duration_s:.1f}s (timeout limit: {timeout_s}s)", show_progress, output_callback)
        return {
            "cmd": cmd,
            "exit_code": -1,
            "duration_s": duration_s,
            "stdout": "\n".join(stdout_lines),
            "stderr": f"Command timed out after {timeout_s}s",
            "timed_out": True,
            "stopped_early": False,
            "convergence_info": None
        }
    except Exception as e:
        duration_s = round(time.time() - start_time, 3)
        if show_progress:
            print("  " + "-" * 60)
            print(f"  ❌ ERROR after {duration_s:.1f}s: {str(e)[:100]}")
        return {
            "cmd": cmd,
            "exit_code": -1,
            "duration_s": duration_s,
            "stdout": "\n".join(stdout_lines),
            "stderr": str(e),
            "timed_out": False,
            "stopped_early": False,
            "convergence_info": None
        }

def run_command_stepwise(
    cmd: str,
    repo_root: str,
    timeout_s: Optional[float] = None,
    output_callback: Optional[Callable[[str, str], None]] = None,
    env: Optional[Dict[str, str]] = None,
    runner_factory: Optional[Callable] = None
) -> Dict[str, Any]:
    """
    Runs a single command without convergence checking.
    """
    return run_command_with_timeout(
        cmd=cmd,
        cwd=repo_root,
        timeout_s=timeout_s,
        show_progress=True,
        output_callback=output_callback,
        convergence_checker=None,
        min_runtime_s=0.0,
        env=env,
        runner_factory=runner_factory
    )

def run_commands(
    repo_root: str,
    commands: List[str],
    timeout_s: Optional[float] = None,
    stage_name: str = "",
    output_callback: Optional[Callable[[str, str], None]] = None,
    convergence_checker: Optional[Callable[[str, float], Optional[Dict[str, Any]]]] = None,
    judge_checker: Optional[Callable[[str, float], Optional[Dict[str, Any]]]] = None,
    judge_checker_factory: Optional[Callable[[str, int, int], Optional[Callable[[str, float], Optional[Dict[str, Any]]]]]] = None,
    min_runtime_s: float = 30.0,
    env: Optional[Dict[str, str]] = None,
    runner_factory: Optional[Callable] = None
) -> List[Dict[str, Any]]:
    """Run multiple commands sequentially."""
    outcomes = []
    total = len(commands)
    for i, cmd in enumerate(commands, 1):
        if stage_name and total > 1:
            _print(f"\n[{stage_name}] Command {i}/{total}:", True, output_callback)
        
        cmd_judge_checker = judge_checker
        if judge_checker_factory:
            cmd_judge_checker = judge_checker_factory(cmd, i - 1, total)
        
        outcome = run_command_with_timeout(
            cmd, 
            repo_root, 
            timeout_s,
            show_progress=True,
            output_callback=output_callback,
            convergence_checker=convergence_checker if cmd_judge_checker is None else None,
            judge_checker=cmd_judge_checker,
            min_runtime_s=min_runtime_s,
            env=env,
            runner_factory=runner_factory
        )
        outcomes.append(outcome)
        
        if cmd_judge_checker and outcome.get("convergence_info"):
            judge_decision = outcome["convergence_info"].get("decision")
            if judge_decision in ["REPLAN_NOW", "SUCCESS", "FAIL", "PARTIAL_SUCCESS"]:
                if stage_name:
                    _print(f"  ⚖️  Stopping {stage_name} due to Judge decision: {judge_decision}", True, output_callback)
                break
        if outcome["exit_code"] != 0 and not outcome.get("timed_out", False) and not outcome.get("stopped_early", False):
            if stage_name:
                _print(f"  ⚠️  Stopping {stage_name} due to failure", True, output_callback)
            break
    return outcomes

def run_command_plan(
    repo_root: str,
    command_plan: Dict[str, Any],
    max_command_time_s: Optional[float] = None,
    diagnostics_on_failure: Optional[List[str]] = None,
    max_diagnostics: int = 3,
    suggested_timeouts: Optional[Dict[str, float]] = None,
    output_callback: Optional[Callable[[str, str], None]] = None,
    convergence_checker: Optional[Callable[[str, float], Optional[Dict[str, Any]]]] = None,
    judge_checker_factory: Optional[Callable[[str, str, int, int], Optional[Callable[[str, float], Optional[Dict[str, Any]]]]]] = None,
    min_runtime_s: float = 30.0,
    env: Optional[Dict[str, str]] = None,
    runner_factory: Optional[Callable] = None
) -> Dict[str, Any]:
    """Execute command plan with stages."""
    results = {}
    
    stages = [k for k in command_plan.keys() if k != "diagnostics_on_failure" and isinstance(command_plan[k], list)]
    
    _print(f"\n{'='*60}", True, output_callback)
    _print("🚀 EXECUTING COMMAND PLAN", True, output_callback)
    if suggested_timeouts:
        timeout_strs = []
        for stage in stages:
            timeout = suggested_timeouts.get(stage, None)
            if timeout is not None:
                timeout_strs.append(f"{stage}={timeout}s")
            else:
                timeout_strs.append(f"{stage}=no timeout")
        _print(f"   Using LLM-suggested timeouts: {', '.join(timeout_strs)}", True, output_callback)
    
    _print(f"{'='*60}", True, output_callback)
    
    all_outcomes = []
    for stage_name in stages:
        if stage_name in command_plan and command_plan[stage_name]:
            commands = command_plan[stage_name]
            stage_timeout = suggested_timeouts.get(stage_name, None) if suggested_timeouts else None
            
            # Fallback to max_command_time_s if no specific timeout suggested
            if stage_timeout is None and max_command_time_s:
                stage_timeout = max_command_time_s
            
            use_convergence = convergence_checker is not None
            if use_convergence:
                stage_commands = command_plan.get(stage_name, [])
                if isinstance(stage_commands, list):
                    has_install_commands = any(
                        isinstance(cmd, str) and any(keyword in cmd.lower() for keyword in ["pip install", "npm install", "install", "setup"])
                        for cmd in stage_commands
                    )
                    if has_install_commands:
                        use_convergence = False
            
            _print(f"\n📦 {stage_name.upper()} ({len(commands)} command(s)):", True, output_callback)
            
            stage_judge_checker_factory = None
            if judge_checker_factory:
                def create_stage_judge_checker_factory(stage: str):
                    def factory_for_command(cmd: str, cmd_idx: int, total_cmds: int):
                        return judge_checker_factory(cmd, stage, cmd_idx, total_cmds)
                    return factory_for_command
                stage_judge_checker_factory = create_stage_judge_checker_factory(stage_name)
            
            stage_results = run_commands(
                repo_root,
                commands,
                timeout_s=stage_timeout,
                stage_name=stage_name.upper(),
                output_callback=output_callback,
                convergence_checker=convergence_checker if use_convergence and not stage_judge_checker_factory else None,
                judge_checker_factory=stage_judge_checker_factory,
                min_runtime_s=min_runtime_s if use_convergence and not stage_judge_checker_factory else 0.0,
                env=env,
                runner_factory=runner_factory
            )
            results[stage_name] = stage_results
            all_outcomes.extend(stage_results)
            
            stage_stopped_due_to_judge = False
            judge_decision_from_stage = None
            for outcome in stage_results:
                convergence_info = outcome.get("convergence_info")
                if convergence_info:
                    decision = convergence_info.get("decision")
                    if decision in ["SUCCESS", "FAIL", "PARTIAL_SUCCESS"]:
                        stage_stopped_due_to_judge = True
                        judge_decision_from_stage = convergence_info
                        break
            
            if stage_stopped_due_to_judge:
                decision = judge_decision_from_stage.get("decision")
                _print(f"  ⚖️  {stage_name.capitalize()} stopped due to Judge decision: {decision}", True, output_callback)
                results["_judge_decision"] = judge_decision_from_stage
                return results
            
            if all(o.get("exit_code", 0) == 0 or o.get("stopped_early", False) for o in stage_results):
                _print(f"  ✅ {stage_name.capitalize()} completed successfully", True, output_callback)
            else:
                _print(f"  ⚠️  {stage_name.capitalize()} had failures", True, output_callback)
    
    if diagnostics_on_failure and any(o.get("exit_code", 0) != 0 and not o.get("stopped_early", False) for o in all_outcomes):
        diagnostics_to_run = diagnostics_on_failure[:max_diagnostics]
        _print(f"\n🔍 DIAGNOSTICS ({len(diagnostics_to_run)} command(s)):", True, output_callback)
        results["diagnostics"] = run_commands(
            repo_root,
            diagnostics_to_run,
            timeout_s=None,
            stage_name="DIAGNOSTICS",
            output_callback=output_callback,
            convergence_checker=None,
            min_runtime_s=0.0,
            env=env,
            runner_factory=runner_factory
        )
    
    total_commands = len(all_outcomes)
    successful = sum(1 for o in all_outcomes if o.get("exit_code", 0) == 0 or o.get("stopped_early", False))
    early_stops = sum(1 for o in all_outcomes if o.get("stopped_early", False))
    _print(f"\n{'='*60}", True, output_callback)
    _print(f"📈 COMMAND PLAN SUMMARY: {successful}/{total_commands} commands succeeded", True, output_callback)
    if early_stops > 0:
        _print(f"   🎯 {early_stops} command(s) stopped early (convergence detected)", True, output_callback)
    _print(f"{'='*60}\n", True, output_callback)
    
    return results

def all_ok(outcomes: List[Dict[str, Any]]) -> bool:
    """Check if all command outcomes are successful."""
    return all(
        (o.get("exit_code", 0) == 0 or o.get("stopped_early", False)) 
        and not o.get("timed_out", False) 
        for o in outcomes
    )

def get_last_stdout(outcomes: List[Dict[str, Any]]) -> str:
    return outcomes[-1].get("stdout", "") if outcomes else ""

def get_last_stderr(outcomes: List[Dict[str, Any]]) -> str:
    return outcomes[-1].get("stderr", "") if outcomes else ""
