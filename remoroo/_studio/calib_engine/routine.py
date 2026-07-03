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
# A hop between consecutive RECORDED waypoints larger than this (any joint, rad) means the
# file is corrupt / recorded across a teleport — refuse to replay it. Also the default
# start-proximity tolerance: the arm must already be near the routine's first waypoint.
HOP_TOL_RAD = 0.15
# Ticks closer together than this are DENSIFIED by joint-space interpolation when the hop is
# large (a fast hand-move / a skipped poll): the arm physically traversed that span within the
# gap, so interpolation reconstructs the real path. A LONGER gap could hide an arbitrary
# excursion — those stay raw, and the replay hop-guard refuses them.
MAX_INTERP_GAP_S = 0.5


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
            return (f"routine contains a {hop:.3f} rad jump between consecutive waypoints "
                    f"(limit {hop_tol}) — file is corrupt or recording skipped; re-record it")
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

    def _append(self, q: np.ndarray, t: float) -> None:
        dt = 0.0 if self._t_last is None else max(0.0, t - self._t_last)
        # Densify a large hop recorded across a SHORT gap: split it into sub-HOP_TOL_RAD/2
        # interpolated waypoints so the routine stays replayable in tiny direct hops. dt==0
        # (back-to-back reads on a coarse clock) is a short gap by definition.
        if self._w and 0.0 <= dt < MAX_INTERP_GAP_S:
            prev = self._w[-1]
            hop = float(np.max(np.abs(q - prev)))
            n = int(np.ceil(hop / (HOP_TOL_RAD / 2.0)))
            for k in range(1, n):
                self._w.append(prev + (q - prev) * (k / n))
                self._dt.append(dt / n)
            if n > 1:
                dt = dt / n
        self._w.append(q)
        self._dt.append(dt)
        self._t_last = t

    def tick(self, joints, t: Optional[float] = None) -> bool:
        """Record the live joints (when armed). Dedups a resting arm. Returns whether a
        waypoint was appended."""
        if not self.armed:
            return False
        q = np.asarray(joints, float).reshape(-1)
        with self._mu:
            if self._w and np.max(np.abs(q - self._w[-1])) < self.dedup_rad:
                return False
            self._append(q, time.time() if t is None else float(t))
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
