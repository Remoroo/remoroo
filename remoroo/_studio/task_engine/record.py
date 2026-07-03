"""TrialRecord (IFACE-04 / DATA-01) — one schema, four consumers: replay, trace-grounded
debugging, model training, and the sellable dataset. Assertions run AT WRITE TIME: a record
missing a version field never lands quietly (DEC-05: every trial stamps what judged it).
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


REQUIRED_VERSIONS = ("judge_version", "perception_version")


@dataclass
class TrialRecord:
    trial_id: str
    task_slug: str
    backend: str                              # "real" | "sibling"
    program_ref: str                          # content hash / git ref of the actor
    knobs: Dict[str, float]
    knob_ranges: Dict[str, Any]
    judge_version: str
    perception_version: str
    seed: int
    t_start: float                            # monotonic
    t_end: float
    wall_time: float                          # wall clock at write
    scene_pre: Dict[str, Any]
    scene_post: Dict[str, Any]
    trace: List[dict]
    verdict: Dict[str, Any]
    outcome: str                              # stated, always (CON-04 discipline)
    reset_ok: Optional[bool] = None
    anomaly_flags: List[str] = field(default_factory=list)
    evidence_refs: Dict[str, str] = field(default_factory=dict)   # frames etc, by reference

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "TrialRecord":
        return cls(**d)


def new_trial_id() -> str:
    return "t_" + uuid.uuid4().hex[:10]


class TrialStore:
    """One directory per task; one json per trial; an append-only index. Edge-buffered,
    synced opportunistically (the sync is transport's job, not this class's)."""

    def __init__(self, root: str) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.index = self.root / "index.jsonl"

    def write(self, rec: TrialRecord) -> Path:
        self._assert_valid(rec)
        path = self.root / f"{rec.trial_id}.json"
        path.write_text(json.dumps(rec.to_dict(), indent=0, sort_keys=True))
        with self.index.open("a") as f:
            f.write(json.dumps({"trial_id": rec.trial_id, "backend": rec.backend,
                                "score": rec.verdict.get("score"),
                                "ok": rec.verdict.get("ok"),
                                "outcome": rec.outcome, "wall_time": rec.wall_time}) + "\n")
        return path

    def load(self, trial_id: str) -> TrialRecord:
        return TrialRecord.from_dict(json.loads((self.root / f"{trial_id}.json").read_text()))

    def all_ids(self) -> List[str]:
        if not self.index.exists():
            return []
        return [json.loads(l)["trial_id"] for l in self.index.read_text().splitlines() if l]

    @staticmethod
    def _assert_valid(rec: TrialRecord) -> None:
        problems = []
        for f_ in REQUIRED_VERSIONS:
            if not getattr(rec, f_):
                problems.append(f"missing {f_}")
        if rec.t_end < rec.t_start:
            problems.append("t_end before t_start (clock misalignment)")
        if not rec.outcome:
            problems.append("no stated outcome")
        if not rec.program_ref:
            problems.append("missing program_ref")
        if problems:
            raise ValueError("TrialRecord rejected at write time: " + "; ".join(problems))


def replay_score(rec: TrialRecord, judge_fn: Any) -> Dict[str, Any]:
    """Re-judge a stored trial from its recorded evidence (scenes + trace). This is what
    makes 'nothing frozen' safe: a better judge retroactively cleans the score history."""
    from .scene.state import SceneState
    from .trace import Trace, TraceStep

    pre = SceneState.from_dict(rec.scene_pre)
    post = SceneState.from_dict(rec.scene_post)
    tr = Trace()
    for s in rec.trace:
        tr.steps.append(TraceStep(name=s["name"], args=s.get("args", {}),
                                  t_start=s.get("t_start", 0.0), t_end=s.get("t_end"),
                                  ok=s.get("ok"), stop_cause=s.get("stop_cause"),
                                  evidence=s.get("evidence", {})))
    return judge_fn(pre, post, tr).to_dict()
