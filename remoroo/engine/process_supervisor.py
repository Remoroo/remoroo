"""Process Supervisor: monitors spawned processes with resource sampling,
forward-progress scoring, waste detection, and event generation.

Every process started by the Worker is wrapped in a SupervisedJob.  The
Supervisor runs monitoring threads that sample resources, parse progress,
and generate events.  Events are queued and drained by v2_exec_poll.
"""
from __future__ import annotations

import collections
import dataclasses
import os
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

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
    """Lightweight hints from the LLM to improve classification accuracy."""
    expected_kind: str = "unknown"       # tests | build | training | server | interactive | unknown
    cost_sensitivity: str = "medium"     # low | medium | high
    gpu_expected: bool = False
    progress_hint_paths: List[str] = field(default_factory=list)
    file_watch_paths: List[str] = field(default_factory=list)
    silent_phase_ok: bool = False
    target_metrics: List[TargetMetric] = field(default_factory=list)


@dataclass
class ResourceSnapshot:
    """Point-in-time resource reading for a process."""
    timestamp: float = 0.0
    cpu_pct: Optional[float] = None
    rss_mb: Optional[float] = None
    disk_io_mb_s: Optional[float] = None
    gpu_util_pct: Optional[float] = None
    gpu_mem_mb: Optional[float] = None


@dataclass
class ProgressState:
    """Parsed progress extracted from output or structured hints."""
    pct: Optional[float] = None      # 0–100
    step: Optional[str] = None       # "epoch 5/15", "test 23/100"
    phase: Optional[str] = None      # "training", "evaluation", "downloading"
    eta_s: Optional[float] = None
    last_update: float = 0.0


@dataclass
class SupervisorEvent:
    """An observation the Supervisor wants to communicate."""
    job_id: str
    timestamp: float
    event_type: str          # COMPLETED, FAILED, STALL_SUSPECTED, RUNAWAY_COST, ...
    severity: str            # info, warning, critical
    confidence: float        # 0.0 – 1.0
    reason: str
    evidence: Dict[str, Any] = field(default_factory=dict)
    log_tail: str = ""
    resource_snapshot: Optional[ResourceSnapshot] = None
    progress_snapshot: Optional[ProgressState] = None

    def to_dict(self) -> Dict[str, Any]:
        d = dataclasses.asdict(self)
        if d.get("resource_snapshot") is None:
            d["resource_snapshot"] = {}
        if d.get("progress_snapshot") is None:
            d["progress_snapshot"] = {}
        return d


# Confidence thresholds for primary events (wake the LLM).
# Only include events backed by RELIABLE evidence. Speculative events
# (stall, runaway cost, GPU) depend on resource metrics that don't work
# in Docker/sandbox and cause the LLM to kill working processes.
EVENT_CONFIDENCE_THRESHOLDS: Dict[str, float] = {
    "COMPLETED": 0.0,             # always wake — process exited 0
    "FAILED": 0.0,                # always wake — process exited non-zero
    "METRIC_TARGET_REACHED": 0.0, # always wake — task goal achieved
    "ERROR_SIGNATURE": 0.7,       # OOM, missing deps, stack traces
}

# These event types are "primary" — they should wake the LLM
PRIMARY_EVENT_TYPES = set(EVENT_CONFIDENCE_THRESHOLDS.keys())

# Secondary events — stored as milestones, do NOT wake the LLM
SECONDARY_EVENT_TYPES = {
    "PROGRESS_MILESTONE", "OUTPUT_MILESTONE", "HEARTBEAT",
    "STALL_SUSPECTED", "RUNAWAY_COST", "THRASHING_DETECTED",
    "GPU_UNDERUTILIZED", "INTERACTIVE_OR_DAEMON_LIKELY", "RESOURCE_RISK",
}


# ---------------------------------------------------------------------------
# Resource sampler
# ---------------------------------------------------------------------------

def _try_import_psutil():
    try:
        import psutil
        return psutil
    except ImportError:
        return None


def sample_resources(pid: int) -> ResourceSnapshot:
    """Best-effort resource snapshot for a process."""
    snap = ResourceSnapshot(timestamp=time.time())
    psutil = _try_import_psutil()
    if psutil is None:
        return snap

    try:
        proc = psutil.Process(pid)
        snap.cpu_pct = proc.cpu_percent(interval=0.1)
        mem = proc.memory_info()
        snap.rss_mb = round(mem.rss / (1024 * 1024), 1)
    except (psutil.NoSuchProcess, psutil.AccessDenied, Exception):
        pass

    # GPU sampling (best-effort)
    try:
        import subprocess as _sp
        result = _sp.run(
            ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=3,
        )
        if result.returncode == 0 and result.stdout.strip():
            parts = result.stdout.strip().split("\n")[0].split(",")
            if len(parts) >= 2:
                snap.gpu_util_pct = float(parts[0].strip())
                snap.gpu_mem_mb = float(parts[1].strip())
    except Exception:
        pass

    return snap


# ---------------------------------------------------------------------------
# Progress parsers
# ---------------------------------------------------------------------------

# Training patterns
_EPOCH_RE = re.compile(
    r"[Ee]poch\s+(\d+)[/:](\d+)", re.IGNORECASE
)
_LOSS_RE = re.compile(
    r"(?:loss|train_loss|val_loss)\s*[=:]\s*([\d.eE+-]+)", re.IGNORECASE
)
_ACCURACY_RE = re.compile(
    r"(?:accuracy|acc|val_accuracy|val_acc)\s*[=:]\s*([\d.eE+-]+)", re.IGNORECASE
)
_STEP_RE = re.compile(
    r"[Ss]tep\s+(\d+)[/:](\d+)", re.IGNORECASE
)

# Pytest patterns
_PYTEST_COLLECTED_RE = re.compile(r"collected\s+(\d+)\s+items?")
_PYTEST_PROGRESS_RE = re.compile(r"(\d+)%")

# Download patterns
_DOWNLOAD_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%", re.IGNORECASE)

# Generic structured progress
_REMOROO_PROGRESS_RE = re.compile(r"REMOROO_PROGRESS\s+({.+})")

# Generic metric value extraction: "metric_name=0.983" or "metric_name: 0.983"
_METRIC_KV_RE = re.compile(
    r"(\w[\w_]*)\s*[=:]\s*(-?[\d]+\.[\d]+|-?[\d]+(?:\.\d+)?(?:[eE][+-]?\d+)?)"
)

# Error signatures
ERROR_SIGNATURES: List[Tuple[re.Pattern, str, float]] = [
    (re.compile(r"(MemoryError|OOM|CUDA out of memory|out of memory)", re.I), "oom", 0.95),
    (re.compile(r"(ModuleNotFoundError|ImportError)", re.I), "missing_dep", 0.90),
    (re.compile(r"(PermissionError|Permission denied)", re.I), "permission", 0.90),
    (re.compile(r"(Segmentation fault|core dumped|SIGSEGV)", re.I), "segfault", 0.95),
    (re.compile(r"(Address already in use)", re.I), "port_conflict", 0.85),
    (re.compile(r"\b(nan|inf)\b.*(?:loss|reward|gradient)", re.I), "divergence", 0.80),
]


def parse_progress_line(line: str, state: ProgressState) -> bool:
    """Try to extract progress from a single output line.
    Returns True if state was updated."""
    changed = False

    # Structured progress
    m = _REMOROO_PROGRESS_RE.search(line)
    if m:
        try:
            import json
            data = json.loads(m.group(1))
            if "pct" in data:
                state.pct = float(data["pct"])
            if "step" in data:
                state.step = str(data["step"])
            if "phase" in data:
                state.phase = str(data["phase"])
            if "eta_s" in data:
                state.eta_s = float(data["eta_s"])
            state.last_update = time.time()
            return True
        except Exception:
            pass

    # Epoch progress (training)
    m = _EPOCH_RE.search(line)
    if m:
        current, total = int(m.group(1)), int(m.group(2))
        state.step = f"epoch {current}/{total}"
        state.pct = round(100.0 * current / total, 1) if total > 0 else None
        state.phase = "training"
        state.last_update = time.time()
        changed = True

    # Step progress
    if not changed:
        m = _STEP_RE.search(line)
        if m:
            current, total = int(m.group(1)), int(m.group(2))
            state.step = f"step {current}/{total}"
            state.pct = round(100.0 * current / total, 1) if total > 0 else None
            state.last_update = time.time()
            changed = True

    # Pytest collected
    if not changed:
        m = _PYTEST_COLLECTED_RE.search(line)
        if m:
            state.phase = "testing"
            state.step = f"collected {m.group(1)} tests"
            state.last_update = time.time()
            changed = True

    return changed


def extract_metric_values(line: str) -> Dict[str, float]:
    """Extract all key=value numeric pairs from a line."""
    metrics: Dict[str, float] = {}
    for m in _METRIC_KV_RE.finditer(line):
        name = m.group(1).lower()
        try:
            metrics[name] = float(m.group(2))
        except ValueError:
            pass
    return metrics


def check_targets(line_metrics: Dict[str, float], targets: List[TargetMetric]) -> List[Tuple[TargetMetric, float]]:
    """Check extracted metrics against targets. Returns list of (target, observed_value) hits."""
    hits = []
    for target in targets:
        target_name = target.name.lower()
        for obs_name, obs_val in line_metrics.items():
            if target_name == obs_name or target_name.replace("_", "") == obs_name.replace("_", ""):
                if target.check(obs_val):
                    hits.append((target, obs_val))
    return hits


def detect_error_signature(line: str) -> Optional[Tuple[str, float]]:
    """Check a line for known error patterns. Returns (signature_id, confidence) or None."""
    for pattern, sig_id, confidence in ERROR_SIGNATURES:
        if pattern.search(line):
            return (sig_id, confidence)
    return None


# ---------------------------------------------------------------------------
# SupervisedJob
# ---------------------------------------------------------------------------

class SupervisedJob:
    """Wraps a subprocess with monitoring, scoring, and event generation."""

    EVAL_WINDOW_S = 30.0        # how often to recompute FPS/WS
    SAMPLE_INTERVAL_S = 5.0     # resource sampling frequency
    STALL_WINDOWS = 2           # consecutive low-FPS windows before STALL_SUSPECTED

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

        # Monitoring state
        self.resource_samples: Deque[ResourceSnapshot] = collections.deque(maxlen=120)
        self.progress = ProgressState()
        self.watched_files: Dict[str, Tuple[int, float]] = {}  # path → (size, mtime)

        # Event queue — drained by poll
        self.event_queue: Deque[SupervisorEvent] = collections.deque(maxlen=50)
        self.milestones: Deque[str] = collections.deque(maxlen=20)

        # Scores
        self.fps: float = 1.0   # start optimistic
        self.ws: float = 0.0

        # Tracking for FPS computation
        self._last_eval_time: float = time.time()
        self._last_output_lines: int = 0
        self._last_stderr_lines: int = 0
        self._last_progress_update: float = 0.0
        self._last_file_sizes: Dict[str, int] = {}
        self._consecutive_low_fps: int = 0
        self._emitted_event_types: Dict[str, float] = {}  # event_type → last_emit_time

        # Multi-metric: track which targets have been individually satisfied.
        # Only fire METRIC_TARGET_REACHED when ALL are met.
        self._satisfied_targets: Dict[str, float] = {}  # target_name → observed_value

        # Terminal state
        self.exit_code: Optional[int] = None
        self.exit_signal: Optional[int] = None
        self.status: str = "running"

        # Threads
        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None

        # Initialize file watches
        for path in self.metadata.file_watch_paths:
            self._snapshot_file(path)

    def start_monitoring(self) -> None:
        """Start the background monitoring thread."""
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, daemon=True, name=f"supervisor-{self.job_id}"
        )
        self._monitor_thread.start()

    def stop_monitoring(self) -> None:
        """Signal the monitoring thread to stop."""
        self._stop_event.set()

    def mark_finished(self, exit_code: int, signal_num: Optional[int] = None) -> None:
        """Called when the process exits."""
        self.exit_code = exit_code
        self.exit_signal = signal_num
        self.status = "failed" if exit_code != 0 else "finished"
        self._stop_event.set()

        event_type = "COMPLETED" if exit_code == 0 else "FAILED"
        self._emit_event(
            event_type=event_type,
            severity="info" if exit_code == 0 else "critical",
            confidence=1.0,
            reason=f"Process exited with code {exit_code}",
            evidence={"exit_code": exit_code, "signal": signal_num,
                      "elapsed_s": round(time.time() - self.start_time, 1)},
        )

    def mark_killed(self) -> None:
        self.status = "killed"
        self._stop_event.set()

    def drain_events(self) -> List[SupervisorEvent]:
        """Return and clear all queued primary events."""
        events = []
        while self.event_queue:
            try:
                events.append(self.event_queue.popleft())
            except IndexError:
                break
        return events

    def peek_events(self) -> List[SupervisorEvent]:
        """Return queued primary events WITHOUT clearing them."""
        return list(self.event_queue)

    def drain_milestones(self) -> List[str]:
        """Return and clear secondary milestone messages."""
        milestones = []
        while self.milestones:
            try:
                milestones.append(self.milestones.popleft())
            except IndexError:
                break
        return milestones

    def get_resource_snapshot(self) -> ResourceSnapshot:
        """Return latest resource snapshot, or sample now if stale."""
        if self.resource_samples:
            latest = self.resource_samples[-1]
            if time.time() - latest.timestamp < self.SAMPLE_INTERVAL_S * 2:
                return latest
        return sample_resources(self.pid)

    def get_progress_dict(self) -> Dict[str, Any]:
        p = self.progress
        return {
            "pct": p.pct,
            "step": p.step,
            "phase": p.phase,
            "eta_s": p.eta_s,
        }

    # ------------------------------------------------------------------
    # Internal monitoring loop
    # ------------------------------------------------------------------

    def _monitor_loop(self) -> None:
        """Background thread: sample resources, parse output, compute scores."""
        while not self._stop_event.is_set():
            try:
                # Resource sample
                snap = sample_resources(self.pid)
                self.resource_samples.append(snap)

                # Parse recent output for progress + errors
                self._scan_output()

                # Check watched files
                self._check_file_watches()

                # Evaluate FPS/WS on schedule
                now = time.time()
                if now - self._last_eval_time >= self.EVAL_WINDOW_S:
                    self._evaluate_scores()
                    self._last_eval_time = now

            except Exception:
                pass

            self._stop_event.wait(timeout=self.SAMPLE_INTERVAL_S)

    def _scan_output(self) -> None:
        """Scan new stdout/stderr lines for progress and errors."""
        current_stdout_len = len(self.stdout_buf)
        current_stderr_len = len(self.stderr_buf)

        # Scan new stdout lines for progress, metrics, and errors
        if current_stdout_len > self._last_output_lines:
            new_lines = list(self.stdout_buf)[-max(1, current_stdout_len - self._last_output_lines):]
            print(f"  [DBG-SUPERVISOR] _scan_output: {len(new_lines)} new lines (total={current_stdout_len}, last={self._last_output_lines}), targets={len(self.metadata.target_metrics)}", flush=True)  # DBG-SUPERVISOR
            for line in new_lines:
                stripped = line.strip()
                if not stripped:
                    continue
                if parse_progress_line(stripped, self.progress):
                    self.milestones.append(f"Progress: {self.progress.step or self.progress.phase or stripped[:80]}")

                # Check for target metric hits (multi-metric aware)
                if self.metadata.target_metrics:
                    line_metrics = extract_metric_values(stripped)
                    if line_metrics:
                        print(f"  [DBG-SUPERVISOR] Extracted metrics: {line_metrics} from: {stripped[:100]}", flush=True)  # DBG-SUPERVISOR
                        hits = check_targets(line_metrics, self.metadata.target_metrics)
                        print(f"  [DBG-SUPERVISOR] Target check hits: {len(hits)} (targets={[(t.name,t.operator,t.value) for t in self.metadata.target_metrics]})", flush=True)  # DBG-SUPERVISOR
                        for target, observed in hits:
                            self._satisfied_targets[target.name.lower()] = observed
                            print(f"  [DBG-SUPERVISOR] >>> METRIC HIT: {target.name} {target.operator} {target.value}, observed={observed} (satisfied={len(self._satisfied_targets)}/{len(self.metadata.target_metrics)})", flush=True)  # DBG-SUPERVISOR

                        # Fire METRIC_TARGET_REACHED only when ALL targets are met
                        all_target_names = {t.name.lower() for t in self.metadata.target_metrics}
                        if all_target_names and all_target_names <= set(self._satisfied_targets.keys()):
                            evidence_all = {
                                name: self._satisfied_targets[name]
                                for name in sorted(self._satisfied_targets)
                            }
                            summary = ", ".join(
                                f"{t.name} {t.operator} {t.value} (observed: {self._satisfied_targets.get(t.name.lower(), '?')})"
                                for t in self.metadata.target_metrics
                            )
                            self._emit_event(
                                event_type="METRIC_TARGET_REACHED",
                                severity="info",
                                confidence=1.0,
                                reason=f"All target metrics reached: {summary}",
                                evidence={
                                    "satisfied_targets": evidence_all,
                                    "target_count": len(self.metadata.target_metrics),
                                },
                            )

                sig = detect_error_signature(stripped)
                if sig:
                    sig_id, confidence = sig
                    self._emit_event(
                        event_type="ERROR_SIGNATURE",
                        severity="warning" if confidence < 0.9 else "critical",
                        confidence=confidence,
                        reason=f"Detected {sig_id} in stdout",
                        evidence={"signature_id": sig_id, "line": stripped[:200]},
                    )

        # Scan new stderr lines for errors
        if current_stderr_len > self._last_stderr_lines:
            new_lines = list(self.stderr_buf)[-max(1, current_stderr_len - self._last_stderr_lines):]
            for line in new_lines:
                stripped = line.strip()
                if not stripped:
                    continue
                sig = detect_error_signature(stripped)
                if sig:
                    sig_id, confidence = sig
                    self._emit_event(
                        event_type="ERROR_SIGNATURE",
                        severity="warning" if confidence < 0.9 else "critical",
                        confidence=confidence,
                        reason=f"Detected {sig_id} in stderr",
                        evidence={"signature_id": sig_id, "line": stripped[:200]},
                    )

        self._last_output_lines = current_stdout_len
        self._last_stderr_lines = current_stderr_len

    def _check_file_watches(self) -> None:
        """Check watched file paths for growth."""
        for path in self.metadata.file_watch_paths:
            try:
                st = os.stat(path)
                size = st.st_size
                prev_size = self._last_file_sizes.get(path, 0)
                if size > prev_size:
                    self._last_file_sizes[path] = size
                    basename = os.path.basename(path)
                    self.milestones.append(f"File growth: {basename} ({prev_size} → {size} bytes)")
            except OSError:
                pass

    def _snapshot_file(self, path: str) -> None:
        try:
            st = os.stat(path)
            self.watched_files[path] = (st.st_size, st.st_mtime)
            self._last_file_sizes[path] = st.st_size
        except OSError:
            self.watched_files[path] = (0, 0.0)
            self._last_file_sizes[path] = 0

    # ------------------------------------------------------------------
    # FPS / WS scoring
    # ------------------------------------------------------------------

    def _evaluate_scores(self) -> None:
        """Recompute Forward Progress Score and Waste Score."""
        now = time.time()

        # Signal 1: Progress changed since last eval?
        progress_signal = 1.0 if self.progress.last_update > self._last_progress_update else 0.0
        self._last_progress_update = self.progress.last_update

        # Signal 2: Meaningful output growth?
        # Use a dedicated counter so _scan_output doesn't steal our baseline
        current_lines = len(self.stdout_buf)
        if not hasattr(self, '_eval_output_lines'):
            self._eval_output_lines = 0
        output_growth = current_lines - self._eval_output_lines
        self._eval_output_lines = current_lines
        output_signal = min(1.0, output_growth / 5.0) if output_growth > 0 else 0.0

        # Signal 3: File growth in watched paths?
        file_growth = False
        for path in self.metadata.file_watch_paths:
            try:
                st = os.stat(path)
                prev = self.watched_files.get(path, (0, 0.0))
                if st.st_size > prev[0] or st.st_mtime > prev[1]:
                    file_growth = True
                    self.watched_files[path] = (st.st_size, st.st_mtime)
            except OSError:
                pass
        file_signal = 1.0 if file_growth else 0.0

        # Signal 4: Sustained compute activity?
        compute_signal = 0.0
        if self.resource_samples:
            recent = list(self.resource_samples)[-6:]  # last ~30s
            cpu_vals = [s.cpu_pct for s in recent if s.cpu_pct is not None]
            gpu_vals = [s.gpu_util_pct for s in recent if s.gpu_util_pct is not None]
            avg_cpu = sum(cpu_vals) / len(cpu_vals) if cpu_vals else 0
            avg_gpu = sum(gpu_vals) / len(gpu_vals) if gpu_vals else 0
            compute_signal = min(1.0, max(avg_cpu, avg_gpu) / 30.0)  # 30% threshold

        # Weights: adjust if resource sampling unavailable
        has_resources = any(s.cpu_pct is not None for s in list(self.resource_samples)[-3:])
        if has_resources:
            w_progress, w_output, w_file, w_compute = 0.40, 0.25, 0.20, 0.15
        else:
            w_progress, w_output, w_file, w_compute = 0.45, 0.35, 0.20, 0.00

        self.fps = (
            progress_signal * w_progress +
            output_signal * w_output +
            file_signal * w_file +
            compute_signal * w_compute
        )

        # Waste Score: high resource utilization with no forward signal
        resource_util = 0.0
        if self.resource_samples:
            recent = list(self.resource_samples)[-6:]
            cpu_vals = [s.cpu_pct for s in recent if s.cpu_pct is not None]
            gpu_vals = [s.gpu_util_pct for s in recent if s.gpu_util_pct is not None]
            resource_util = max(
                (sum(cpu_vals) / len(cpu_vals) / 100.0) if cpu_vals else 0,
                (sum(gpu_vals) / len(gpu_vals) / 100.0) if gpu_vals else 0,
            )
        self.ws = resource_util * (1.0 - self.fps)

        # Stall detection — skip entirely when target metrics are set (we wait
        # for METRIC_TARGET_REACHED or process exit, not arbitrary silence).
        has_targets = bool(self.metadata.target_metrics)
        if self.fps < 0.15 and not has_targets:
            self._consecutive_low_fps += 1
        else:
            self._consecutive_low_fps = 0

        if (self._consecutive_low_fps >= self.STALL_WINDOWS
                and not self.metadata.silent_phase_ok):
            self._emit_event(
                event_type="STALL_SUSPECTED",
                severity="warning",
                confidence=min(0.95, 0.6 + 0.1 * self._consecutive_low_fps),
                reason=(
                    f"No forward progress for {self._consecutive_low_fps * self.EVAL_WINDOW_S:.0f}s "
                    f"(FPS={self.fps:.2f}, WS={self.ws:.2f})"
                ),
                evidence={
                    "fps": round(self.fps, 3),
                    "ws": round(self.ws, 3),
                    "consecutive_low_fps_windows": self._consecutive_low_fps,
                    "output_lines": len(self.stdout_buf),
                    "resource_util": round(resource_util, 3),
                },
            )

        # Runaway cost detection
        if self.ws > 0.7 and not self.metadata.silent_phase_ok:
            self._emit_event(
                event_type="RUNAWAY_COST",
                severity="warning",
                confidence=min(0.95, 0.7 + 0.05 * self._consecutive_low_fps),
                reason=(
                    f"High resource burn with no forward signal "
                    f"(WS={self.ws:.2f}, FPS={self.fps:.2f}, CPU/GPU util={resource_util:.0%})"
                ),
                evidence={
                    "fps": round(self.fps, 3),
                    "ws": round(self.ws, 3),
                    "resource_util": round(resource_util, 3),
                },
            )

        # GPU underutilization
        if (self.metadata.gpu_expected and self.resource_samples):
            recent = list(self.resource_samples)[-6:]
            gpu_vals = [s.gpu_util_pct for s in recent if s.gpu_util_pct is not None]
            cpu_vals = [s.cpu_pct for s in recent if s.cpu_pct is not None]
            if gpu_vals and cpu_vals:
                avg_gpu = sum(gpu_vals) / len(gpu_vals)
                avg_cpu = sum(cpu_vals) / len(cpu_vals)
                if avg_gpu < 5.0 and avg_cpu > 30.0:
                    self._emit_event(
                        event_type="GPU_UNDERUTILIZED",
                        severity="warning",
                        confidence=0.8,
                        reason=f"GPU util ~{avg_gpu:.0f}% while CPU ~{avg_cpu:.0f}% (expected GPU usage)",
                        evidence={"avg_gpu_pct": round(avg_gpu, 1), "avg_cpu_pct": round(avg_cpu, 1)},
                    )

    # ------------------------------------------------------------------
    # Event emission with debouncing
    # ------------------------------------------------------------------

    def _emit_event(
        self,
        event_type: str,
        severity: str,
        confidence: float,
        reason: str,
        evidence: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Create and queue an event, with debouncing and confidence gating."""
        threshold = EVENT_CONFIDENCE_THRESHOLDS.get(event_type, 0.7)
        if confidence < threshold:
            print(f"  [DBG-SUPERVISOR] _emit_event({event_type}): BLOCKED by confidence ({confidence} < {threshold})", flush=True)  # DBG-SUPERVISOR
            return

        # Debounce: don't emit the same event type more than once per eval window
        now = time.time()
        last_emit = self._emitted_event_types.get(event_type, 0.0)
        if event_type not in ("COMPLETED", "FAILED", "METRIC_TARGET_REACHED") and (now - last_emit) < self.EVAL_WINDOW_S:
            print(f"  [DBG-SUPERVISOR] _emit_event({event_type}): DEBOUNCED ({now - last_emit:.1f}s < {self.EVAL_WINDOW_S}s)", flush=True)  # DBG-SUPERVISOR
            return
        self._emitted_event_types[event_type] = now
        print(f"  [DBG-SUPERVISOR] _emit_event({event_type}): QUEUED (confidence={confidence}, reason={reason[:80]})", flush=True)  # DBG-SUPERVISOR

        # Build log tail
        stdout_lines = list(self.stdout_buf)
        log_tail = "".join(stdout_lines[-20:]) if stdout_lines else ""

        event = SupervisorEvent(
            job_id=self.job_id,
            timestamp=now,
            event_type=event_type,
            severity=severity,
            confidence=confidence,
            reason=reason,
            evidence=evidence or {},
            log_tail=log_tail,
            resource_snapshot=self.get_resource_snapshot() if self.resource_samples else None,
            progress_snapshot=ProgressState(
                pct=self.progress.pct, step=self.progress.step,
                phase=self.progress.phase, eta_s=self.progress.eta_s,
                last_update=self.progress.last_update,
            ),
        )

        if event_type in PRIMARY_EVENT_TYPES:
            self.event_queue.append(event)
        else:
            self.milestones.append(f"[{event_type}] {reason[:100]}")

    # ------------------------------------------------------------------
    # Enriched poll response
    # ------------------------------------------------------------------

    def build_poll_response(self, tail_lines: int = 80, drain: bool = True) -> Dict[str, Any]:
        """Build the enriched poll response for v2_exec_poll.

        Args:
            drain: If True, events are removed from the queue (watch mode).
                   If False, events are peeked without clearing (turn context).
        """
        stdout_lines = list(self.stdout_buf)
        stderr_lines = list(self.stderr_buf)
        elapsed_s = round(time.time() - self.start_time, 1)

        snap = self.get_resource_snapshot()
        events = self.drain_events() if drain else self.peek_events()
        print(f"  [DBG-SUPERVISOR] build_poll_response(drain={drain}): status={self.status}, events={len(events)}, queue_remaining={len(self.event_queue)}, fps={self.fps:.3f}, ws={self.ws:.3f}", flush=True)  # DBG-SUPERVISOR
        if events:  # DBG-SUPERVISOR
            for e in events:  # DBG-SUPERVISOR
                print(f"  [DBG-SUPERVISOR]   event: {e.event_type} conf={e.confidence} reason={e.reason[:100]}", flush=True)  # DBG-SUPERVISOR

        return {
            "status": self.status,
            "exit_code": self.exit_code,
            "elapsed_s": elapsed_s,
            "stdout_tail": "".join(stdout_lines[-tail_lines:]),
            "stderr_tail": "".join(stderr_lines[-min(20, tail_lines):]),
            "output_lines": len(stdout_lines),
            "events": [e.to_dict() for e in events],
            "fps": round(self.fps, 3),
            "ws": round(self.ws, 3),
            "resource_snapshot": {
                "cpu_pct": snap.cpu_pct,
                "rss_mb": snap.rss_mb,
                "disk_io_mb_s": snap.disk_io_mb_s,
                "gpu_util_pct": snap.gpu_util_pct,
                "gpu_mem_mb": snap.gpu_mem_mb,
            },
            "progress": self.get_progress_dict(),
            "milestones": self.drain_milestones(),
        }
