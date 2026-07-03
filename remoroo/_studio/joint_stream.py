"""The INDEPENDENT joint-state stream — telemetry decoupled from the calibration pipeline.

Joint state is TELEMETRY: the live mirror, the routine recorder, and any future realtime
consumer all need "where is the rig, continuously" — and none of them may be held hostage by
the calibration pipeline (camera grabs, bundle solves, the bridge lock those hold). ONE
producer owns the joint feed; every consumer SUBSCRIBES. The payload is a
{joint_name: value} map over EVERY actuated joint of the rig — any morphology, any DOF.

Two producer modes, chosen by what the cell Bridge offers:
  * PUSH (the bridge contract, `bridge_primitives.md`): `stream_joints(on_sample, should_stop)`
    subscribes to each controller's NATIVE push/report stream and calls back per report — the
    rate is the SDK's own (e.g. an xArm reports at 250 Hz). No polling, no rate chosen here.
  * POLL (degraded fallback, pre-`stream_joints` cells): `get_observation()` is camera-bundled
    and not concurrent-safe, so it is polled under the shared bridge lock, non-blocking, a few
    Hz at best — still ONE centralized poller instead of several competing ones.

Pure stdlib; engine-testable off-robot (inject any subscribe/read function).
"""
from __future__ import annotations

import threading
import time
from typing import Callable, Dict, List, Optional, Tuple

# The degraded (get_observation-under-lock) path is bounded by the observation cost itself
# (a camera grab), not by the rig: polling faster than this only adds lock contention.
DEGRADED_HZ = 5.0


class JointStream:
    def __init__(self, read_joints: Optional[Callable[[], Optional[Dict[str, float]]]] = None, *,
                 subscribe: Optional[Callable] = None, hz: Optional[float] = None,
                 lock=None, name: str = "joint-stream", retry_s: float = 3.0,
                 probe_s: float = 3.0):
        """PUSH mode: `subscribe(on_sample, should_stop)` — the Bridge's `stream_joints`
        contract; the SDK paces the stream. POLL mode: `read_joints()` returns
        {joint_name: value}; `hz` required; a `lock` is acquired non-blocking (a busy lock
        skips the poll — telemetry never queues behind the pipeline).

        With BOTH given, push is primary and poll is the BEHAVIORAL fallback: a cell may
        carry a stub `stream_joints` (the seed template's TODO), so method EXISTENCE proves
        nothing — if the subscription produces no sample within `probe_s`, the poll fallback
        starts (LOUDLY, on stdout → the edge log) so the mirror never dies silently."""
        if subscribe is None and read_joints is None:
            raise ValueError("JointStream needs subscribe= (push) and/or read_joints= (poll)")
        if read_joints is not None and not hz:
            raise ValueError("poll mode needs the poll rate (hz)")
        self._subscribe = subscribe
        self._read = read_joints
        self.hz = float(hz) if hz else None
        self._lock = lock
        self.name = name
        self.retry_s = float(retry_s)
        self.probe_s = float(probe_s)
        self.latest: Optional[Tuple[float, Dict[str, float]]] = None
        self.error: Optional[str] = None
        self._subs: List[Callable[[float, Dict[str, float]], None]] = []
        self._mu = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._fallback_thread: Optional[threading.Thread] = None

    def subscribe(self, cb: Callable[[float, Dict[str, float]], None]) -> None:
        """`cb(t, joints)` fires on the producer thread for every sample. A raising subscriber
        is dropped from that event only — it must never kill telemetry."""
        with self._mu:
            self._subs.append(cb)

    def start(self) -> "JointStream":
        if self._thread is None or not self._thread.is_alive():
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, daemon=True, name=self.name)
            self._thread.start()
            if self._subscribe is not None and self._read is not None:
                threading.Thread(target=self._probe_push, daemon=True,
                                 name=f"{self.name}-probe").start()
        return self

    def _probe_push(self) -> None:
        """The behavioral probe: if push has produced NOTHING after probe_s (a stubbed or
        broken `stream_joints` — even one that blocks forever), start the poll fallback and
        say so on stdout (→ the edge log). The push thread keeps retrying; if a real push
        stream comes alive later it simply publishes alongside (consumers dedup)."""
        self._stop.wait(self.probe_s)
        if self._stop.is_set() or self.latest is not None:
            return
        print(f"[joint-stream] stream_joints produced no samples in {self.probe_s:.0f}s "
              f"({self.error or 'no callback fired'}) — falling back to observation polling. "
              "Fix primitives.py stream_joints (the bridge seed's G2 criterion measures it).")
        self._fallback_thread = threading.Thread(target=self._poll_loop, daemon=True,
                                                 name=f"{self.name}-fallback")
        self._fallback_thread.start()

    def stop(self) -> None:
        self._stop.set()

    # ── publishing (shared by both modes) ────────────────────────────────────
    def _publish(self, joints: Dict[str, float]) -> None:
        # Re-stamp on OUR clock domain (time.time): the recorder's capture marks and dt math
        # all live there; the SDK's monotonic report stamp is not comparable across domains.
        evt_t = time.time()
        self.latest = (evt_t, dict(joints))
        self.error = None
        with self._mu:
            subs = list(self._subs)
        for cb in subs:
            try:
                cb(evt_t, joints)
            except Exception:  # noqa: BLE001 — a consumer must never kill telemetry
                pass

    # ── producer loops ───────────────────────────────────────────────────────
    def _loop(self) -> None:
        if self._subscribe is not None:
            self._push_loop()
        else:
            self._poll_loop()

    def _push_loop(self) -> None:
        """Run the Bridge's native subscription (`stream_joints(on_sample, should_stop)`).
        It blocks until should_stop; if it exits/raises (controller hiccup, a stubbed cell),
        surface the error and retry with backoff — the mirror shows WHY instead of freezing."""
        while not self._stop.is_set():
            try:
                self._subscribe(lambda t, joints: self._publish(joints), self._stop.is_set)
                if not self._stop.is_set():
                    self.error = "stream_joints returned — no push stream; retrying"
            except Exception as e:  # noqa: BLE001
                self.error = f"stream_joints: {type(e).__name__}: {e}"
            self._stop.wait(self.retry_s)

    def _poll_loop(self) -> None:
        period = 1.0 / max(0.1, self.hz or 1.0)
        while not self._stop.is_set():
            t0 = time.time()
            joints = None
            try:
                joints = self._poll_once()
            except Exception as e:  # noqa: BLE001 — a bad poll is reported, never fatal
                self.error = f"{type(e).__name__}: {e}"
            if joints:
                self._publish(joints)
            self._stop.wait(max(0.0, period - (time.time() - t0)))

    def _poll_once(self) -> Optional[Dict[str, float]]:
        if self._lock is not None:
            if not self._lock.acquire(blocking=False):
                return None                      # busy — skip, never queue behind the pipeline
            try:
                return self._read()
            finally:
                self._lock.release()
        return self._read()
