"""Recorded calibration ROUTINES — record the first (supervised) calibration's joint
trajectory, then re-run the whole calibration automatically by REPLAYING it.

The safety premise: at calibration time the world model isn't validated yet, so cuRobo-planned
fresh paths are risky — but a path the arm ALREADY physically traversed under operator
supervision is a trusted path. So the recorder passively logs dense joint waypoints while the
operator collects (hand-moved or guided), marks which waypoints were captures, and the replay
walks the SAME waypoints with direct joint hops (each hop tiny by construction) — never a
planner. Pure numpy + json: engine-testable off-robot; the edge only feeds `tick()`.
"""
from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import numpy as np

SCHEMA = 1
# Consecutive waypoints closer than this (any joint, rad) are deduped — an arm at rest must
# not spam waypoints. ~0.5 deg.
DEDUP_RAD = 0.008
# The per-hop REPLAY limit (any joint, rad): replay drives direct hops, so each must be tiny.
# Recordings are densified to half this; a post-densify violation means the file is corrupt.
# Also the default start-proximity tolerance (the arm must be near the first waypoint).
HOP_TOL_RAD = 0.15
# The DENSIFY limit: a recording gap whose joint excursion is at most this (any joint, rad) is
# bridged by joint-space interpolation — REGARDLESS of how long the gap took. On a real rig
# joint reads arrive ~1 Hz at best (get_observation grabs a camera frame under the bridge
# lock; capture/solve verbs hold it for many seconds), so time-based gating starves and normal
# hand-move speeds record 0.3–0.8 rad "jumps". What makes interpolation safe is not the gap's
# DURATION but its EXCURSION: within this bound a straight joint path stays in the neighborhood
# the operator was supervising. Beyond it the arm may have detoured arbitrarily → refuse.
MAX_INTERP_HOP_RAD = 0.8


@dataclass
class Routine:
    """One step's recorded trajectory: dense joint waypoints + which of them were captures."""
    step_id: str
    camera: str
    arm: str
    joint_names: List[str]              # bridge joint order at record time ([] if unknown)
    waypoints: np.ndarray               # (N, J)
    dt: np.ndarray                      # (N,) seconds since the previous waypoint (dt[0]=0)
    capture_marks: List[int]            # waypoint indices where a capture happened
    target: dict = field(default_factory=dict)   # the target spec used (operator sanity info)
    created_at: str = ""
    schema: int = SCHEMA

    def save(self, path: str) -> str:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps({
            "schema": self.schema, "step_id": self.step_id, "camera": self.camera,
            "arm": self.arm, "joint_names": list(self.joint_names),
            "waypoints": np.asarray(self.waypoints, float).round(6).tolist(),
            "dt": np.asarray(self.dt, float).round(4).tolist(),
            "capture_marks": [int(i) for i in self.capture_marks],
            "target": self.target, "created_at": self.created_at,
        }, indent=1), encoding="utf-8")
        return str(path)

    @staticmethod
    def load(path: str) -> "Routine":
        d = json.loads(Path(path).read_text(encoding="utf-8"))
        return Routine(
            step_id=str(d.get("step_id", "")), camera=str(d.get("camera", "")),
            arm=str(d.get("arm", "")), joint_names=[str(n) for n in (d.get("joint_names") or [])],
            waypoints=np.asarray(d.get("waypoints") or [], float),
            dt=np.asarray(d.get("dt") or [], float),
            capture_marks=[int(i) for i in (d.get("capture_marks") or [])],
            target=dict(d.get("target") or {}), created_at=str(d.get("created_at", "")),
            schema=int(d.get("schema", 0)),
        )

    def densified(self) -> "Routine":
        """HEAL a sparse recording: bridge every bounded-excursion hop (≤ MAX_INTERP_HOP_RAD)
        with joint-space interpolation so all hops are replayable (≤ HOP_TOL_RAD/2), remapping
        the capture marks. Applied at replay-load so routines recorded at a slow effective tick
        rate (a real rig reads joints ~1 Hz) replay without re-recording. Excursions beyond the
        bound are kept raw — validate_against refuses them (the arm may have detoured)."""
        W = np.asarray(self.waypoints, float)
        if W.ndim != 2 or W.shape[0] < 2:
            return self
        new_w: List[np.ndarray] = [W[0]]
        new_dt: List[float] = [float(self.dt[0]) if len(self.dt) else 0.0]
        idx_map = {0: 0}
        for i in range(1, W.shape[0]):
            q, prev = W[i], new_w[-1]
            dt = float(self.dt[i]) if i < len(self.dt) else 0.0
            hop = float(np.max(np.abs(q - prev)))
            if HOP_TOL_RAD / 2.0 < hop <= MAX_INTERP_HOP_RAD:
                n = int(np.ceil(hop / (HOP_TOL_RAD / 2.0)))
                for k in range(1, n):
                    new_w.append(prev + (q - prev) * (k / n))
                    new_dt.append(dt / n)
                dt = dt / n
            new_w.append(q)
            new_dt.append(dt)
            idx_map[i] = len(new_w) - 1
        return Routine(
            step_id=self.step_id, camera=self.camera, arm=self.arm,
            joint_names=list(self.joint_names), waypoints=np.asarray(new_w, float),
            dt=np.asarray(new_dt, float),
            capture_marks=[idx_map[int(m)] for m in self.capture_marks if int(m) in idx_map],
            target=dict(self.target), created_at=self.created_at, schema=self.schema,
        )

    def validate_against(self, joint_names: Sequence[str], n_joints: int,
                         *, hop_tol: float = HOP_TOL_RAD) -> Optional[str]:
        """The refuse-to-replay guards. Returns an error string, or None when safe to replay.
        `joint_names` is the CURRENT bridge order ([] when the bridge doesn't expose names —
        then only the DOF count is checked); `n_joints` the session chain's DOF."""
        if self.schema != SCHEMA:
            return f"routine schema {self.schema} != {SCHEMA} — re-record it"
        W = np.asarray(self.waypoints, float)
        if W.ndim != 2 or W.shape[0] < 2:
            return "routine has no trajectory — re-record it"
        if len(self.capture_marks) < 2:
            return "routine has fewer than 2 capture marks — re-record it"
        if any(not (0 <= i < W.shape[0]) for i in self.capture_marks):
            return "routine capture marks out of range — file is corrupt"
        if W.shape[1] != n_joints:
            return (f"routine has {W.shape[1]} joints but this arm has {n_joints} — "
                    "the rig changed; re-record it")
        if self.joint_names and list(joint_names) and list(self.joint_names) != list(joint_names):
            return ("routine joint order/names differ from the current bridge "
                    f"({self.joint_names} vs {list(joint_names)}) — re-record it")
        hop = np.abs(np.diff(W, axis=0)).max() if W.shape[0] > 1 else 0.0
        if hop > hop_tol:
            return (f"routine contains a {hop:.3f} rad jump the recording cannot account for "
                    f"(beyond the {MAX_INTERP_HOP_RAD} rad safe-interpolation bound) — the arm "
                    "may have detoured during a recording gap; re-record this calibration")
        return None


class RoutineRecorder:
    """Passively accumulates dense joint waypoints while ARMED. The edge feeds `tick()` at
    ~10 Hz from its poll loop; off-robot tests call it directly. `mark_capture()` force-appends
    the exact capture joints and remembers the index, tying the trajectory to capture events."""

    def __init__(self, joint_names: Optional[Sequence[str]] = None, *, dedup_rad: float = DEDUP_RAD):
        self.joint_names = [str(n) for n in (joint_names or [])]
        self.dedup_rad = float(dedup_rad)
        self.armed = False
        self._w: List[np.ndarray] = []
        self._dt: List[float] = []
        self._t_last: Optional[float] = None
        self.capture_marks: List[int] = []
        # Two feeders share this recorder — the edge's poll ticker (its own thread) and the
        # bridge's on_move hook (the verb thread, fired mid-move) — so appends + mark indices
        # must not interleave.
        self._mu = threading.Lock()

    @property
    def n_waypoints(self) -> int:
        return len(self._w)

    def _append(self, q: np.ndarray, t: float, *, commanded: bool = False) -> None:
        dt = 0.0 if self._t_last is None else max(0.0, t - self._t_last)
        # Densify a hop into sub-HOP_TOL_RAD/2 interpolated waypoints so the routine stays
        # replayable in tiny direct hops. A SAMPLED hop (a blind recording gap) is densified
        # only within MAX_INTERP_HOP_RAD — beyond it the arm may have detoured, so it stays
        # raw and the replay guard refuses. A COMMANDED hop is straight in joint space BY
        # CONSTRUCTION (the SDK executes a direct move linearly), so it densifies at any size.
        if self._w:
            prev = self._w[-1]
            hop = float(np.max(np.abs(q - prev)))
            if commanded or hop <= MAX_INTERP_HOP_RAD:
                n = int(np.ceil(hop / (HOP_TOL_RAD / 2.0)))
                for k in range(1, n):
                    self._w.append(prev + (q - prev) * (k / n))
                    self._dt.append(dt / n)
                if n > 1:
                    dt = dt / n
        self._w.append(q)
        self._dt.append(dt)
        self._t_last = t

    def tick_map(self, m: Dict[str, float], t: Optional[float] = None, *,
                 commanded: bool = False) -> bool:
        """Feed a NAMED joint sample covering the WHOLE BODY (the joint stream's map). On the
        first full sample the recorder ADOPTS its names (sorted, stable) — a commissioned rig
        is ONE body: we record all so replay can restore all. Later samples are reindexed by
        name (a missing name holds its last value)."""
        if not self.armed:
            return False
        with self._mu:
            if not self.joint_names:
                self.joint_names = sorted(str(k) for k in m.keys())
                if self._w and len(self._w[-1]) != len(self.joint_names):
                    # a few pre-adoption vector waypoints (chain-width hooks fired before the
                    # stream's first full sample) — drop them; the body-wide tape starts here
                    self._w, self._dt, self.capture_marks, self._t_last = [], [], [], None
            if self._w is not None and len(self._w):
                last = self._w[-1]
                q = np.array([float(m.get(n, last[i])) for i, n in enumerate(self.joint_names)])
            else:
                if any(n not in m for n in self.joint_names):
                    return False                    # first sample must cover the adopted names
                q = np.array([float(m[n]) for n in self.joint_names])
            if self._w and np.max(np.abs(q - self._w[-1])) < self.dedup_rad:
                return False
            self._append(q, time.time() if t is None else float(t), commanded=commanded)
        return True

    def tick_named(self, names: Sequence[str], joints, t: Optional[float] = None, *,
                   commanded: bool = False, mark: bool = False) -> bool:
        """Feed a PARTIAL sample (e.g. the step chain's commanded waypoint) by NAME — merged
        into the last known whole-body state. Falls back to the plain vector path when the
        recorder has no names to merge by (off-robot / name-less bridges). `mark=True` also
        marks a capture at the appended waypoint."""
        if not self.armed:
            return False
        names = [str(n) for n in (names or [])]
        if self.joint_names and names and set(names) <= set(self.joint_names) and self._w:
            m = dict(zip(self.joint_names, (float(v) for v in self._w[-1])))
            m.update({n: float(v) for n, v in zip(names, np.asarray(joints, float).reshape(-1))})
            with self._mu:
                q = np.array([m[n] for n in self.joint_names])
                if mark:
                    self._append(q, time.time() if t is None else float(t), commanded=commanded)
                    self.capture_marks.append(len(self._w) - 1)
                    return True
                if np.max(np.abs(q - self._w[-1])) < self.dedup_rad:
                    return False
                self._append(q, time.time() if t is None else float(t), commanded=commanded)
            return True
        if mark:
            self.mark_capture(joints, t)
            return True
        return self.tick(joints, t, commanded=commanded)

    def tick(self, joints, t: Optional[float] = None, *, commanded: bool = False) -> bool:
        """Record the live joints (when armed). Dedups a resting arm. `commanded=True` marks a
        waypoint the engine COMMANDED (the bridge's on_move hook) — a straight joint move by
        construction, safe to densify at any excursion. Returns whether a waypoint was appended."""
        if not self.armed:
            return False
        q = np.asarray(joints, float).reshape(-1)
        with self._mu:
            if self._w and np.max(np.abs(q - self._w[-1])) < self.dedup_rad:
                return False
            self._append(q, time.time() if t is None else float(t), commanded=commanded)
        return True

    def mark_capture(self, joints, t: Optional[float] = None) -> None:
        """A capture happened at `joints`: force-append (exact pose, no dedup) + mark it."""
        if not self.armed:
            return
        q = np.asarray(joints, float).reshape(-1)
        with self._mu:
            self._append(q, time.time() if t is None else float(t))
            self.capture_marks.append(len(self._w) - 1)

    def to_routine(self, *, step_id: str, camera: str, arm: str, target: Optional[dict] = None) -> Routine:
        return Routine(
            step_id=step_id, camera=camera, arm=arm, joint_names=list(self.joint_names),
            waypoints=np.asarray(self._w, float) if self._w else np.zeros((0, 0)),
            dt=np.asarray(self._dt, float), capture_marks=list(self.capture_marks),
            target=dict(target or {}), created_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
        )


class ReplayCursor:
    """Walks a Routine as SEGMENTS — each segment is the dense waypoint span ending at the
    next capture mark. One `replay_step` executes one segment: hop through its waypoints,
    then capture. Keeps replay verbs short (HTTP-friendly) and abortable between captures."""

    def __init__(self, routine: Routine):
        self.routine = routine
        marks = sorted(set(int(i) for i in routine.capture_marks))
        self.segments: List[Tuple[int, int]] = []
        prev = 0
        for m in marks:
            self.segments.append((prev, m))          # waypoints (prev..m], capture at m
            prev = m
        self.i = 0                                    # next segment to execute

    @property
    def done(self) -> bool:
        return self.i >= len(self.segments)

    def start_check(self, q_now, *, tol: float = HOP_TOL_RAD):
        """The arm must already be NEAR the routine's first waypoint (the operator jogs it
        there; replay never plans an approach). Returns (ok, per-joint deltas)."""
        q0 = np.asarray(self.routine.waypoints, float)[0]
        d = np.abs(np.asarray(q_now, float).reshape(-1) - q0)
        return bool(d.max() <= tol), d.round(4).tolist()

    def next_segment(self):
        """The next segment's waypoints (inclusive of the capture waypoint) + its dt's, or
        None when done. Advances the cursor."""
        if self.done:
            return None
        a, b = self.segments[self.i]
        self.i += 1
        W = np.asarray(self.routine.waypoints, float)
        dt = np.asarray(self.routine.dt, float)
        lo = a if self.i == 1 else a + 1              # later segments start AFTER the visited mark
        return W[lo:b + 1], dt[lo:b + 1]
