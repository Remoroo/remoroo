"""Process Supervisor: minimal watchdog for spawned processes.

Every process started by the Worker is wrapped in a SupervisedJob.
The watchdog thread does 4 things every 5 seconds:
  1. Check readiness (STARTING -> RUNNING)
  2. Scan new output for error signatures + metric targets
  3. Check watched files for growth (resets silence timer)
  4. Fire SILENT_TIMEOUT if past the LLM-declared limit

All LLM parameters are printed loudly at creation time.
Missing params get warnings, not silent defaults.
Exceptions are logged, not swallowed.
"""
from __future__ import annotations

import collections
import dataclasses
import os
import signal
import re
import socket
import threading
import time
import traceback
import subprocess
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Deque, Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Child process enumeration (portable)
# ---------------------------------------------------------------------------


def _get_child_pids(parent_pid: int) -> Set[int]:
    """Return direct child PIDs of parent_pid. Works on Linux and macOS."""
    try:
        # ps ax -o pid=,ppid= is widely portable; we filter for ppid==parent
        result = subprocess.run(
            ["ps", "ax", "-o", "pid=,ppid="],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return set()
        children: Set[int] = set()
        for line in result.stdout.strip().splitlines():
            parts = line.split(None, 1)
            if len(parts) >= 2:
                try:
                    pid, ppid = int(parts[0]), int(parts[1])
                    if ppid == parent_pid:
                        children.add(pid)
                except ValueError:
                    pass
        return children
    except (OSError, subprocess.TimeoutExpired):
        return set()


def _count_processes_in_group(pgid: int) -> int:
    """Count processes in the given process group. Returns 0 when group is gone (all exited).
    Uses ps -o pid=,pgid= and filters; portable on Linux and macOS (BSD ps -g behaves differently)."""
    try:
        result = subprocess.run(
            ["ps", "ax", "-o", "pid=,pgid="],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return 0
        count = 0
        for line in result.stdout.strip().splitlines():
            parts = line.split(None, 1)
            if len(parts) >= 2:
                try:
                    if int(parts[1]) == pgid:
                        count += 1
                except ValueError:
                    pass
        return count
    except (OSError, subprocess.TimeoutExpired):
        return 0


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class JobState(str, Enum):
    STARTING = "starting"
    RUNNING = "running"
    BACKGROUND_FOLLOW = "background_follow"  # shell exited but background children still running
    FINISHED = "finished"
    FAILED = "failed"
    KILLED = "killed"
    BACKOFF = "backoff"


@dataclass
class TargetMetric:
    """A metric the task is trying to achieve."""
    name: str
    operator: str   # ">" | ">=" | "<" | "<=" | "==" | "!="
    value: float

    def check(self, observed: float) -> bool:
        ops = {">": observed > self.value, ">=": observed >= self.value,
               "<": observed < self.value, "<=": observed <= self.value,
               "==": observed == self.value, "!=": observed != self.value}
        return ops.get(self.operator, False)


@dataclass
class JobMetadata:
    """Hints from the LLM about the workload."""
    expected_kind: str = "unknown"
    cost_sensitivity: str = "medium"
    gpu_expected: bool = False
    progress_hint_paths: List[str] = field(default_factory=list)
    file_watch_paths: List[str] = field(default_factory=list)
    silent_phase_ok: bool = False  # legacy, unused
    target_metrics: List[TargetMetric] = field(default_factory=list)
    max_silent_s: Optional[float] = None
    readiness_regex: Optional[str] = None
    readiness_port: Optional[int] = None
    redirect_output_file: Optional[str] = None


@dataclass
class SupervisorEvent:
    """An event the watchdog wants to communicate."""
    job_id: str
    timestamp: float
    event_type: str
    severity: str
    confidence: float
    reason: str
    evidence: Dict[str, Any] = field(default_factory=dict)
    log_tail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


# Primary events — these wake the LLM.
EVENT_CONFIDENCE_THRESHOLDS: Dict[str, float] = {
    "COMPLETED": 0.0,
    "FAILED": 0.0,
    "METRIC_TARGET_REACHED": 0.0,
    "ERROR_SIGNATURE": 0.7,
    "SILENT_TIMEOUT": 0.0,
    "CRASH_LOOP": 0.0,
}

PRIMARY_EVENT_TYPES = set(EVENT_CONFIDENCE_THRESHOLDS.keys())


# ---------------------------------------------------------------------------
# Metric extraction
# ---------------------------------------------------------------------------

_METRIC_KV_RE = re.compile(
    r"(\w[\w_]*)\s*[=:]\s*(-?[\d]+\.[\d]+|-?[\d]+(?:\.\d+)?(?:[eE][+-]?\d+)?)"
)


def _redirect_target_from_command(command: str) -> Optional[str]:
    """Infer stdout redirect filepath from shell command (aligned with bash tool auto-detect).

    Matches `> file`, `>>file`, `&> file`, `| tee file`. Skips /dev/null.
    Uses `\\s*` after the operator so `>run.log` works, not only `> run.log`.
    """
    if not command or not command.strip():
        return None
    matches = re.findall(
        r"(?:^|\s)(?:>|>>|&>|\|\s*tee(?:\s+-a)?)\s*([^\s>&|;]+)",
        command,
    )
    if not matches:
        return None
    last = matches[-1]
    if last in ("/dev/null", "NUL"):
        return None
    return last


def extract_metric_values(line: str) -> Dict[str, float]:
    """Extract all key=value numeric pairs from a line."""
    metrics: Dict[str, float] = {}

    rm = re.match(r"REMOROO_METRIC\s+(\w+)\s*=\s*(.+)", line.strip())
    if rm:
        name, raw = rm.group(1).lower(), rm.group(2).strip()
        if raw.lower() == "true":
            metrics[name] = 1.0
        elif raw.lower() == "false":
            metrics[name] = 0.0
        else:
            try:
                metrics[name] = float(raw)
            except ValueError:
                pass
        return metrics

    for m in _METRIC_KV_RE.finditer(line):
        name = m.group(1).lower()
        try:
            metrics[name] = float(m.group(2))
        except ValueError:
            pass
    return metrics


def check_targets(line_metrics: Dict[str, float], targets: List[TargetMetric]) -> List[Tuple[TargetMetric, float]]:
    """Check extracted metrics against targets."""
    hits = []
    for target in targets:
        target_name = target.name.lower()
        for obs_name, obs_val in line_metrics.items():
            if target_name == obs_name or target_name.replace("_", "") == obs_name.replace("_", ""):
                if target.check(obs_val):
                    hits.append((target, obs_val))
    return hits


# ---------------------------------------------------------------------------
# Error signatures
# ---------------------------------------------------------------------------

ERROR_SIGNATURES: List[Tuple[re.Pattern, str, float]] = [
    (re.compile(r"(MemoryError|OOM|CUDA out of memory|out of memory)", re.I), "oom", 0.95),
    (re.compile(r"(ModuleNotFoundError|ImportError)", re.I), "missing_dep", 0.90),
    (re.compile(r"(PermissionError|Permission denied)", re.I), "permission", 0.90),
    (re.compile(r"(Segmentation fault|core dumped|SIGSEGV)", re.I), "segfault", 0.95),
    (re.compile(r"(Address already in use)", re.I), "port_conflict", 0.85),
    (re.compile(r"\b(nan|inf)\b.*(?:loss|reward|gradient)", re.I), "divergence", 0.80),
    (re.compile(r"(MPS backend out of memory|MPS framework error)", re.I), "mps_oom", 0.95),
    (re.compile(r"(MPSNDArray error|metal\.MTLCompileError)", re.I), "mps_compile", 0.90),
    (re.compile(r"(mps\.?driver\.abort|GPU Timeout|GPU Hang)", re.I), "mps_hang", 0.90),
    (re.compile(r"not (currently )?supported on (the )?MPS backend", re.I), "mps_unsupported_op", 0.85),
]


def detect_error_signature(line: str) -> Optional[Tuple[str, float]]:
    """Check a line for known error patterns. Returns (signature_id, confidence) or None."""
    for pattern, sig_id, confidence in ERROR_SIGNATURES:
        if pattern.search(line):
            return (sig_id, confidence)
    return None


# ---------------------------------------------------------------------------
# SupervisedJob — the minimal watchdog
# ---------------------------------------------------------------------------

class SupervisedJob:
    """Minimal process watchdog.

    Monitors output activity, file watches, error signatures, and metric
    targets.  Fires events only for hard facts: completion, failure,
    silence timeout, error signature, metric target reached, crash loop.
    """

    POLL_INTERVAL_S = 5.0

    def __init__(
        self,
        job_id: str,
        pid: int,
        command: str,
        cwd: str,
        stdout_buf: collections.deque,
        stderr_buf: collections.deque,
        metadata: Optional[JobMetadata] = None,
        artifact_log_path: Optional[str] = None,
    ):
        self.job_id = job_id
        self.pid = pid
        self.command = command
        self.cwd = cwd
        self.stdout_buf = stdout_buf
        self.stderr_buf = stderr_buf
        self.metadata = metadata or JobMetadata()
        self.artifact_log_path = artifact_log_path
        self.start_time = time.time()

        self.state: JobState = JobState.STARTING
        self.exit_code: Optional[int] = None
        self.exit_signal: Optional[int] = None

        self.event_queue: Deque[SupervisorEvent] = collections.deque(maxlen=50)

        self._last_new_output_time: float = time.time()
        self._last_stdout_len: int = 0
        self._last_stderr_len: int = 0
        self._last_file_sizes: Dict[str, int] = {}
        self._satisfied_targets: Dict[str, float] = {}
        self._last_event_times: Dict[str, float] = {}

        self._readiness_re = re.compile(self.metadata.readiness_regex) if self.metadata.readiness_regex else None
        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self._background_pids: Set[int] = set()  # child PIDs when shell uses &; fallback when no PGID
        self._shell_pgid: Optional[int] = None   # process group of shell; used to wait for whole tree

        for path in self.metadata.file_watch_paths:
            try:
                self._last_file_sizes[path] = os.stat(path).st_size
            except OSError:
                self._last_file_sizes[path] = 0
        if self.metadata.redirect_output_file and self.metadata.redirect_output_file not in self._last_file_sizes:
            try:
                self._last_file_sizes[self.metadata.redirect_output_file] = os.stat(self.metadata.redirect_output_file).st_size
            except OSError:
                self._last_file_sizes[self.metadata.redirect_output_file] = 0

        # self._log_config()

    # ------------------------------------------------------------------
    # Transparent config logging
    # ------------------------------------------------------------------

    def _log_config(self) -> None:
        print(f"  [watch] job={self.job_id} cmd={self.command[:100]}", flush=True)
        if self.metadata.max_silent_s is not None:
            print(f"  [watch] job={self.job_id} max_silent_s={self.metadata.max_silent_s}", flush=True)
        else:
            print(f"  [watch] job={self.job_id} WARNING: max_silent_s not set — no hang detection", flush=True)
        if self.metadata.redirect_output_file:
            exists = os.path.exists(self.metadata.redirect_output_file)
            print(f"  [watch] job={self.job_id} redirect_file={self.metadata.redirect_output_file} exists={exists}", flush=True)
        for path in self.metadata.file_watch_paths:
            exists = os.path.exists(path)
            print(f"  [watch] job={self.job_id} file_watch={path} exists={exists}", flush=True)
        for t in self.metadata.target_metrics:
            print(f"  [watch] job={self.job_id} target: {t.name} {t.operator} {t.value}", flush=True)
        if self.metadata.readiness_regex:
            print(f"  [watch] job={self.job_id} readiness_regex={self.metadata.readiness_regex}", flush=True)
        if self.metadata.readiness_port:
            print(f"  [watch] job={self.job_id} readiness_port={self.metadata.readiness_port}", flush=True)
        if self._output_deferred_until_exit():
            print(f"  [watch] job={self.job_id} output_deferred_until_exit (pipe to tail) — SILENT_TIMEOUT disabled", flush=True)
        if self._has_background_ampersand():
            print(f"  [watch] job={self.job_id} background_jobs=true — will track children after shell exits", flush=True)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start_monitoring(self) -> None:
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, daemon=True, name=f"watch-{self.job_id}"
        )
        self._monitor_thread.start()

    def stop_monitoring(self) -> None:
        self._stop_event.set()

    def mark_finished(self, exit_code: int, signal_num: Optional[int] = None) -> None:
        if self.state in (JobState.KILLED, JobState.FINISHED, JobState.FAILED, JobState.BACKGROUND_FOLLOW):
            return
        self.exit_code = exit_code
        self.exit_signal = signal_num

        # If command used & and we have background children, don't emit yet — keep monitoring them
        if self._has_background_ampersand() and (self._shell_pgid is not None or self._background_pids):
            self.state = JobState.BACKGROUND_FOLLOW
            track_msg = f"pgid={self._shell_pgid}" if self._shell_pgid else f"{len(self._background_pids)} PIDs"
            print(f"  [watch] job={self.job_id} shell exited (code={exit_code}) — tracking {track_msg}", flush=True)
            return  # Don't set _stop_event or emit; monitor loop continues

        self.state = JobState.FINISHED if exit_code == 0 else JobState.FAILED
        self._stop_event.set()
        etype = "COMPLETED" if exit_code == 0 else "FAILED"
        self._emit(etype, f"Process exited with code {exit_code}",
                   {"exit_code": exit_code, "signal": signal_num,
                    "elapsed_s": round(time.time() - self.start_time, 1)})

    def kill_background_children(self) -> None:
        """Send SIGTERM then SIGKILL to background process group. Used when user kills during BACKGROUND_FOLLOW."""
        if self._shell_pgid is not None:
            try:
                os.killpg(self._shell_pgid, signal.SIGTERM)
            except OSError:
                pass
            time.sleep(2)
            try:
                os.killpg(self._shell_pgid, signal.SIGKILL)
            except OSError:
                pass
            self._shell_pgid = None
        for pid in list(self._background_pids):
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                pass
        time.sleep(1)
        for pid in list(self._background_pids):
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        self._background_pids.clear()

    def mark_killed(self) -> None:
        if self.state in (JobState.KILLED, JobState.FINISHED, JobState.FAILED):
            return
        if self.state == JobState.BACKGROUND_FOLLOW:
            self.kill_background_children()
        self.state = JobState.KILLED
        self._stop_event.set()

    def drain_events(self) -> List[SupervisorEvent]:
        events = []
        while self.event_queue:
            try:
                events.append(self.event_queue.popleft())
            except IndexError:
                break
        return events

    def peek_events(self) -> List[SupervisorEvent]:
        return list(self.event_queue)

    # ------------------------------------------------------------------
    # Monitor loop — 4 checks, nothing else
    # ------------------------------------------------------------------

    def _monitor_loop(self) -> None:
        if not self.metadata.readiness_regex and not self.metadata.readiness_port:
            self.state = JobState.RUNNING

        while not self._stop_event.is_set():
            try:
                if self.state == JobState.STARTING:
                    self._check_readiness()
                elif self.state == JobState.BACKGROUND_FOLLOW:
                    self._check_background_follow()
                else:
                    # RUNNING: capture PGID and children for background-follow (in case shell exits)
                    if self._has_background_ampersand() and self.pid and self.pid > 0:
                        self._background_pids = _get_child_pids(self.pid)
                        try:
                            self._shell_pgid = os.getpgid(self.pid)
                        except (OSError, AttributeError):
                            self._shell_pgid = None
                    self._check_output()
                    self._check_files()
                    self._check_silent()
            except Exception:
                print(f"  [watch] ERROR job={self.job_id}:\n{traceback.format_exc()}", flush=True)
            self._stop_event.wait(timeout=self.POLL_INTERVAL_S)

    def _check_readiness(self) -> None:
        ready = False
        if self.metadata.readiness_port:
            try:
                with socket.create_connection(("127.0.0.1", self.metadata.readiness_port), timeout=0.1):
                    ready = True
            except (OSError, ConnectionRefusedError):
                pass
        if not ready and self._readiness_re:
            try:
                for line in list(self.stdout_buf)[-500:]:  # transport cap only; no policy
                    if self._readiness_re.search(line):
                        ready = True
                        break
            except RuntimeError:
                pass
        if ready:
            self.state = JobState.RUNNING
            print(f"  [watch] job={self.job_id} READY -> RUNNING", flush=True)

    def _check_output(self) -> None:
        """Scan new stdout/stderr for errors and metrics; reset silence timer on new output."""
        current_stdout = len(self.stdout_buf)
        current_stderr = len(self.stderr_buf)

        if current_stdout > self._last_stdout_len:
            self._last_new_output_time = time.time()
            try:
                new_lines = list(self.stdout_buf)[-max(1, current_stdout - self._last_stdout_len):]
            except RuntimeError:
                new_lines = []
            self._last_stdout_len = current_stdout

            for line in new_lines:
                stripped = line.strip()
                if not stripped:
                    continue

                err = detect_error_signature(stripped)
                if err:
                    sig_id, confidence = err
                    if confidence >= EVENT_CONFIDENCE_THRESHOLDS.get("ERROR_SIGNATURE", 0.7):
                        self._emit("ERROR_SIGNATURE", f"Detected {sig_id} in stdout",
                                   {"signature_id": sig_id, "line": stripped})  # full line; brain truncates with notice

                if self.metadata.target_metrics:
                    metrics = extract_metric_values(stripped)
                    if metrics:
                        hits = check_targets(metrics, self.metadata.target_metrics)
                        for target, observed in hits:
                            self._satisfied_targets[target.name.lower()] = observed

                        all_names = {t.name.lower() for t in self.metadata.target_metrics}
                        if all_names and all_names <= set(self._satisfied_targets.keys()):
                            summary = ", ".join(
                                f"{t.name} {t.operator} {t.value} (observed: {self._satisfied_targets.get(t.name.lower(), '?')})"
                                for t in self.metadata.target_metrics
                            )
                            self._emit("METRIC_TARGET_REACHED", f"All targets met: {summary}",
                                       {"satisfied_targets": dict(self._satisfied_targets)})

        if current_stderr > self._last_stderr_len:
            try:
                new_lines = list(self.stderr_buf)[-max(1, current_stderr - self._last_stderr_len):]
            except RuntimeError:
                new_lines = []
            self._last_stderr_len = current_stderr

            for line in new_lines:
                stripped = line.strip()
                if not stripped:
                    continue
                err = detect_error_signature(stripped)
                if err:
                    sig_id, confidence = err
                    if confidence >= EVENT_CONFIDENCE_THRESHOLDS.get("ERROR_SIGNATURE", 0.7):
                        self._emit("ERROR_SIGNATURE", f"Detected {sig_id} in stderr",
                                   {"signature_id": sig_id, "line": stripped})  # full line; brain truncates with notice

    def _paths_to_watch(self) -> List[str]:
        """All paths that count as 'activity' for silence detection (file_watch_paths + redirect_output_file)."""
        paths = list(self.metadata.file_watch_paths)
        if self.metadata.redirect_output_file and self.metadata.redirect_output_file not in paths:
            paths.append(self.metadata.redirect_output_file)
        return paths

    def _check_files(self) -> None:
        """Check watched files for growth — resets silence timer."""
        for path in self._paths_to_watch():
            try:
                size = os.stat(path).st_size
                prev = self._last_file_sizes.get(path, 0)
                if size > prev:
                    self._last_new_output_time = time.time()
                    self._last_file_sizes[path] = size
            except OSError:
                pass

    def _output_deferred_until_exit(self) -> bool:
        """True if stdout only appears when the pipeline exits (e.g. cmd | tail -30)."""
        return "| tail" in self.command

    def _has_background_ampersand(self) -> bool:
        """True if the command backgrounds work with & (e.g. cmd1 & sleep 5). Excludes && (logical AND)."""
        # Match " &" not followed by &, or "& " not preceded by &
        return bool(re.search(r" &(?!&)|(?<![&])& ", self.command))

    def _check_silent(self) -> None:
        """Fire SILENT_TIMEOUT if no output or file activity past the LLM-declared limit."""
        if self.metadata.max_silent_s is None:
            return
        # Pipelines like "python train.py | tail -30" produce no stdout until the pipeline
        # closes; do not treat expected silence as a hang.
        if self._output_deferred_until_exit():
            return
        effective_limit = self.metadata.max_silent_s * 1.25
        silent_for = time.time() - self._last_new_output_time
        if silent_for <= effective_limit:
            return

        # Double-check all watched paths (file_watch_paths + redirect_output_file)
        for path in self._paths_to_watch():
            try:
                size = os.stat(path).st_size
                prev = self._last_file_sizes.get(path, 0)
                if size > prev:
                    self._last_new_output_time = time.time()
                    self._last_file_sizes[path] = size
                    return
            except OSError:
                pass

        # When stdout is redirected to a file, the process may be line-buffered or block-buffered;
        # the file might not have grown at our last poll. Give it one short delay then re-check
        # to avoid false SILENT_TIMEOUT while the process is actively appending.
        if self.metadata.redirect_output_file:
            self._stop_event.wait(timeout=2.0)
            if self._stop_event.is_set():
                return
            for path in self._paths_to_watch():
                try:
                    size = os.stat(path).st_size
                    prev = self._last_file_sizes.get(path, 0)
                    if size > prev:
                        self._last_new_output_time = time.time()
                        self._last_file_sizes[path] = size
                        return
                except OSError:
                    pass

        self._emit("SILENT_TIMEOUT",
                    f"No stdout or file activity for {silent_for:.0f}s (limit: {self.metadata.max_silent_s:.0f}s + 25% margin)",
                    {"silent_for_s": round(silent_for, 1), "max_silent_s": self.metadata.max_silent_s})
        self._last_new_output_time = time.time()  # debounce

    def _check_background_follow(self) -> None:
        """While in BACKGROUND_FOLLOW: wait for process group (or fallback PIDs) to empty, then COMPLETED."""
        self._check_files()

        if self.metadata.max_silent_s is not None and not self._output_deferred_until_exit():
            self._check_silent()

        all_done = False
        if self._shell_pgid is not None:
            # Primary: process group — wait until no processes in shell's group remain
            n = _count_processes_in_group(self._shell_pgid)
            if n == 0:
                all_done = True
        else:
            # Fallback: direct children (when PGID unavailable, e.g. Windows)
            for pid in list(self._background_pids):
                try:
                    os.kill(pid, 0)
                except OSError:
                    self._background_pids.discard(pid)
            if not self._background_pids:
                all_done = True

        if all_done:
            self.state = JobState.FINISHED
            self._stop_event.set()
            self._emit("COMPLETED", "All background processes exited",
                       {"elapsed_s": round(time.time() - self.start_time, 1)})
            print(f"  [watch] job={self.job_id} BACKGROUND_FOLLOW -> COMPLETED", flush=True)

    # ------------------------------------------------------------------
    # Event emission — always printed
    # ------------------------------------------------------------------

    def _emit(self, event_type: str, reason: str, evidence: Optional[Dict[str, Any]] = None) -> None:
        if event_type not in PRIMARY_EVENT_TYPES:
            return

        threshold = EVENT_CONFIDENCE_THRESHOLDS.get(event_type, 0.7)
        # For ERROR_SIGNATURE the caller already checked confidence; for
        # everything else confidence is 1.0 and threshold is 0.0.

        now = time.time()
        if event_type not in ("COMPLETED", "FAILED", "METRIC_TARGET_REACHED"):
            last = self._last_event_times.get(event_type, 0.0)
            if now - last < 30.0:
                return
        self._last_event_times[event_type] = now

        try:
            # Generous slice only; no truncation notice (brain applies via output_interceptor)
            _event_log_cap = 500
            log_tail = "".join(list(self.stdout_buf)[-_event_log_cap:])
        except RuntimeError:
            log_tail = ""

        event = SupervisorEvent(
            job_id=self.job_id,
            timestamp=now,
            event_type=event_type,
            severity="critical" if event_type in ("FAILED", "ERROR_SIGNATURE") else "info",
            confidence=1.0,
            reason=reason,
            evidence=evidence or {},
            log_tail=log_tail,
        )
        self.event_queue.append(event)
        print(f"  [watch] job={self.job_id} EVENT: {event_type} — {reason}", flush=True)

    # ------------------------------------------------------------------
    # Poll response
    # ------------------------------------------------------------------

    # Chunk size for reading redirect file from the end (I/O only; no policy cap).
    # Size policy is in output_interceptor; we return last tail_lines lines and let it trim.
    _REDIRECT_TAIL_CHUNK_BYTES = 64 * 1024

    def _read_redirect_tail(self, tail_lines: int, redirect_path: Optional[str] = None) -> str:
        """When stdout was redirected to a file, read last tail_lines lines (same semantics as pipe stdout_tail).
        No byte cap here — output_interceptor trims large results.
        redirect_path: if set, read this file (absolute or relative to cwd); else use metadata.redirect_output_file.
        """
        path = (redirect_path or self.metadata.redirect_output_file or "").strip()
        if not path:
            return ""
        if not os.path.isabs(path):
            path = os.path.join(self.cwd, path)
        try:
            if not os.path.exists(path):
                return ""
            size = os.path.getsize(path)
            if size == 0:
                return ""
            collected: List[str] = []
            with open(path, "rb") as f:
                chunk_size = min(size, self._REDIRECT_TAIL_CHUNK_BYTES)
                offset = max(0, size - chunk_size)
                while offset < size and len(collected) < tail_lines:
                    f.seek(offset)
                    raw = f.read(chunk_size)
                    try:
                        text = raw.decode("utf-8", errors="replace")
                    except Exception:
                        text = raw.decode("latin-1", errors="replace")
                    parts = text.splitlines()
                    # If this chunk doesn't end with newline, its last part continues in collected[0]
                    if collected and parts and not raw.endswith(b"\n"):
                        collected = parts[:-1] + [parts[-1] + collected[0]] + collected[1:]
                    else:
                        collected = parts + collected
                    if offset == 0:
                        break
                    offset = max(0, offset - chunk_size)
            last = collected[-tail_lines:] if len(collected) > tail_lines else collected
            return "\n".join(last) + ("\n" if last else "")
        except (OSError, IOError):
            return ""

    def build_poll_response(self, tail_lines: int = 80, drain: bool = True) -> Dict[str, Any]:
        try:
            stdout_lines = list(self.stdout_buf)
            stderr_lines = list(self.stderr_buf)
        except RuntimeError:
            stdout_lines, stderr_lines = [], []

        stdout_tail = "".join(stdout_lines[-tail_lines:])
        # When command redirected to file, always include redirect tail so the agent gets the log
        # (even if the pipe has wrapper output like echo "Exit code: $?" — then we send both).
        # If metadata omitted redirect_output_file, infer from command (same patterns as bash tool).
        redir = (self.metadata.redirect_output_file or "").strip()
        if not redir:
            inferred = _redirect_target_from_command(self.command)
            if inferred:
                redir = inferred
        if redir:
            redirect_tail = self._read_redirect_tail(tail_lines, redirect_path=redir)
            if redirect_tail:
                if stdout_tail.strip():
                    stdout_tail = stdout_tail.rstrip() + "\n\n" + redirect_tail
                else:
                    stdout_tail = redirect_tail

        events = self.drain_events() if drain else self.peek_events()
        return {
            "status": self.state.value,
            "exit_code": self.exit_code,
            "elapsed_s": round(time.time() - self.start_time, 1),
            "stdout_tail": stdout_tail,
            "stderr_tail": "".join(stderr_lines[-tail_lines:]),
            "output_lines": len(stdout_lines),
            "events": [e.to_dict() for e in events],
        }
