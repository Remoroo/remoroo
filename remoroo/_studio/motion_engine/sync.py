"""sync.py — the capture-time joint SYNCHRONIZER: the keystone of *synchronized* realtime perception.

An eye-in-hand camera's pose is `cam_pose(t) = FK(q(t)) ∘ X`. A depth frame captured at time `t` must
be fused with the joints the arm was AT at `t` — not "now" — or the world smears while the arm moves.
Nothing in the engine does this today (`update_world_live` fuses at one current-joints seed, only
correct when parked). This module is that alignment.

The approach is the standard one for asynchronous sensor fusion on a moving robot (ARM-SLAM; ROS
`message_filters` ApproximateTime): buffer the high-rate joint stream as timestamped samples, and for
each depth-frame timestamp recover the config by **bracketing it between two samples and linearly
interpolating** — within a `max_gap` "slop" tolerance, else SKIP the frame. We interpolate the JOINTS
(scalars) and let the caller FK them, which respects kinematics and avoids interpolating the camera
pose (no SLERP). Linear interpolation of joint *positions* is exact-enough over the ~ms bracket the
tolerance enforces.

PURE (numpy/stdlib, no cuRobo/torch) → unit-tested off-robot with synthetic streams. It reads no clock
and no device: it consumes timestamped samples on a COMMON time base (establishing that base — relating
each device's stamp to a host clock on receipt — is the bridge gate's job) and answers one question:
"what joints was the arm at, at time `t`?".

Miss policy = SKIP. A misregistered frame is worse than a dropped one, so with no tight bracket
(a stream gap, or `t` outside the window) we return `ok=False` and the caller skips that frame.
"""
from __future__ import annotations

import bisect
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

import numpy as np


def max_gap_for(freq_hz: float, slack_s: float = 0.001) -> float:
    """The bracket-width tolerance ("slop"): two adjacent samples from a controller reporting at
    `freq_hz` are `1/freq_hz` apart, so a valid bracket around a capture time is at most
    `1/freq_hz + slack`; the slack absorbs jitter. (xArm @ 250 Hz → 0.004 + 0.001 = 5 ms.)"""
    return 1.0 / float(freq_hz) + float(slack_s)


def host_capture_time(host_now: float, device_now: float, device_capture: float) -> float:
    """Put a DEVICE capture timestamp on the HOST monotonic clock — the robust clock-sync primitive.

    The two streams (joints, depth) must share ONE clock or `sync_at` is meaningless. The reliable way
    is NOT to compare a device's absolute clock to the host's (their epochs differ): it is to use the
    device clock only for a *difference* — how long ago the frame was captured — and place that on the
    host clock. So: capture_latency = device_now - device_capture (both in the DEVICE's own clock, so
    the absolute offset cancels), and the capture time on the host monotonic clock is
    `host_now - capture_latency`. Stamp joints with `time.monotonic()` at receipt and convert each
    depth frame with this, and both live on the host monotonic clock — independent of any device clock.

    `host_now`  = `time.monotonic()` read right after the grab.
    `device_now`/`device_capture` = the device's "now" and the frame's capture stamp, SAME units/clock
    (e.g. ZED `TIME_REFERENCE.CURRENT` and `TIME_REFERENCE.IMAGE`, both seconds).
    """
    return float(host_now) - (float(device_now) - float(device_capture))


@dataclass
class SyncResult:
    """The answer to "joints at time t". `ok=False` means SKIP this frame (do not integrate)."""
    ok: bool
    q: Optional[Dict[str, float]]      # interpolated joints (combined-URDF names) at the query time
    reason: str = ""                   # "" when ok; else WHY we skipped (legibility for the gate)
    gap: float = 0.0                   # the bracket width (t2 - t1), seconds — diagnostics
    alpha: float = 0.0                 # interpolation fraction within the bracket [0,1]
    held: bool = False                 # True = zero-order hold on the newest sample (freshest frame)
    age: float = 0.0                   # seconds the query time is PAST the newest sample (hold/late)


class JointSync:
    """A timestamped ring of joint samples + capture-time interpolation.

    Push `(t, q)` as the controller streams; query `sync_at(t_capture)` to recover the config the arm
    was at when a frame was captured. Samples are keyed to a fixed `joint_names` schema, so a cell may
    push a partial dict (gripper-only, say) and missing entries hold 0.0 without reordering.
    """

    def __init__(self, joint_names: Sequence[str], *, capacity: int = 2048):
        self._names: List[str] = [str(n) for n in joint_names]
        self._idx: Dict[str, int] = {n: i for i, n in enumerate(self._names)}
        self._cap = max(2, int(capacity))
        self._t: List[float] = []          # sample times, non-decreasing
        self._q: List[np.ndarray] = []     # each [D] in _names order

    @property
    def joint_names(self) -> List[str]:
        return list(self._names)

    def __len__(self) -> int:
        return len(self._t)

    @property
    def span(self) -> float:
        """Seconds currently buffered (newest - oldest); 0 if < 2 samples."""
        return (self._t[-1] - self._t[0]) if len(self._t) >= 2 else 0.0

    def push(self, t: float, q) -> None:
        """Append a sample. `q` is a `{name: pos}` dict (reordered to the schema; missing → 0.0) or an
        array already in `joint_names` order. Samples must arrive with non-decreasing `t`; an
        out-of-order sample is DROPPED (a monotonic common clock is the contract — silently keeping a
        backwards sample would corrupt every bracket after it)."""
        t = float(t)
        if self._t and t < self._t[-1]:
            return
        self._t.append(t)
        self._q.append(self._to_array(q))
        if len(self._t) > self._cap:           # evict oldest (small ring → cheap)
            drop = len(self._t) - self._cap
            del self._t[:drop]
            del self._q[:drop]

    def sync_at(self, t: float, *, max_gap: float, hold_last: bool = True) -> SyncResult:
        """The joint config at time `t`. Normally we BRACKET `t` (t1 ≤ t ≤ t2, (t2-t1) < `max_gap`) and
        interpolate. But the FRESHEST frame in a real-time loop is captured slightly AFTER the newest
        buffered joint (there is no future joint yet), so requiring a two-sided bracket would skip every
        live frame. So when `t` is past the newest sample by ≤ `max_gap` and `hold_last` is set, we
        zero-order HOLD the newest config: the arm moved at most one tolerance-worth, a bounded error of
        the same order as the bracket case (velocity × age) and vastly better than dropping the frame.
        `ok=False` (skip) only when `t` is older than the buffer, or past the newest by MORE than
        `max_gap` (the stream is genuinely too slow / clocks unaligned), or the bracket itself is wide."""
        n = len(self._t)
        if n == 0:
            return SyncResult(False, None, "no joint samples buffered")
        if n == 1:
            return SyncResult(False, None, "only one joint sample (cannot bracket)")
        if t < self._t[0]:
            return SyncResult(False, None, f"t={t:.4f}s before oldest sample {self._t[0]:.4f}s")
        if t > self._t[-1]:
            age = t - self._t[-1]
            if hold_last and age <= max_gap:
                return SyncResult(True, self._as_dict(self._q[-1]), "", 0.0, 1.0, held=True, age=age)
            return SyncResult(False, None,
                              f"newest joint is {age*1e3:.1f}ms stale > max_gap {max_gap*1e3:.1f}ms "
                              f"(joint stream too slow or clocks unaligned)", age=age)
        i = bisect.bisect_right(self._t, t) - 1     # largest i with _t[i] <= t
        if i >= n - 1:                               # exact hit on the newest sample
            return SyncResult(True, self._as_dict(self._q[-1]), "", 0.0, 0.0)
        t1, t2 = self._t[i], self._t[i + 1]
        gap = t2 - t1
        if gap > max_gap:
            return SyncResult(False, None,
                              f"bracket gap {gap*1e3:.1f}ms > max {max_gap*1e3:.1f}ms (joint-stream gap)", gap)
        alpha = 0.0 if gap <= 0.0 else (t - t1) / gap
        q = self._q[i] * (1.0 - alpha) + self._q[i + 1] * alpha
        return SyncResult(True, self._as_dict(q), "", gap, float(alpha))

    def latest(self) -> Optional[Dict[str, float]]:
        """The newest joint sample (no interpolation) — for the planner's *current-state* seed (the
        control loop reads this; perception reads `sync_at`). None if empty."""
        return self._as_dict(self._q[-1]) if self._q else None

    @property
    def latest_t(self) -> Optional[float]:
        """Timestamp of the newest joint sample (host clock), or None — for diagnostics: a realtime
        loop can compare `t_capture - latest_t` to see how stale the joint stream is."""
        return self._t[-1] if self._t else None

    def rate_hz(self) -> float:
        """Measured median sample rate over the buffer (Hz), 0 if < 3 samples — so a loop can set
        `max_gap` from the stream's ACTUAL rate instead of an assumed one (robust to a slow stream)."""
        if len(self._t) < 3:
            return 0.0
        dt = np.diff(np.asarray(self._t, dtype=float))
        med = float(np.median(dt))
        return 1.0 / med if med > 0 else 0.0

    # --- internals -----------------------------------------------------------
    def _to_array(self, q) -> np.ndarray:
        if isinstance(q, dict):
            a = np.zeros(len(self._names), dtype=float)
            for name, v in q.items():
                j = self._idx.get(str(name))
                if j is not None:
                    a[j] = float(v)
            return a
        a = np.asarray(q, dtype=float).reshape(-1)
        if a.shape[0] != len(self._names):
            raise ValueError(f"JointSync expects {len(self._names)} joints, got {a.shape[0]}")
        return a.astype(float, copy=True)

    def _as_dict(self, a: np.ndarray) -> Dict[str, float]:
        return {n: float(a[i]) for i, n in enumerate(self._names)}
