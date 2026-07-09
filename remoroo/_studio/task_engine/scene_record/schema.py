"""Scene record schemas (DATA-01, DATA-02) — the run's written account of the scene:
every object, where, how big, and WHY we believe it (proof lines), plus what we can't
see yet (unknown volumes). Plain dataclasses, JSON round-trip, no engine logic here."""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

PROOF_KINDS = ("view", "touch", "lift", "trial")
CONTACT_KINDS = ("touch", "lift", "trial")     # physical proof outranks any look


@dataclass
class Proof:
    kind: str                                   # view | touch | lift | trial
    source: str                                 # camera name, probe id, trial id
    viewpoint: str = ""                         # viewpoint hash (camera pose bucket)
    modality: str = ""                          # rgb | depth | contact
    t: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RecordObject:
    object_id: str
    label: str
    pose: List[float]                           # [x,y,z] (+ optional quat)
    pose_sigma: float = 0.02                    # meters, isotropic v1
    dims: Optional[List[float]] = None
    dims_sigma: float = 0.3                     # relative
    proofs: List[Proof] = field(default_factory=list)
    task_role: str = "unknown"                  # target|container|obstacle|fixture|unknown
    retired: str = ""                           # non-empty = why it was rejected
    sightings: List[dict] = field(default_factory=list)   # {t, centroid, arm_poses}
    ranges: Dict[str, List[float]] = field(default_factory=dict)  # param -> [lo, hi]

    def proof_level(self) -> str:
        """touched > corroborated > seen_once. Corroborated = two proofs that are
        INDEPENDENT: distinct viewpoint hashes, or distinct modalities."""
        if any(p.kind in CONTACT_KINDS for p in self.proofs):
            return "touched"
        views = [p for p in self.proofs if p.kind == "view"]
        if len({(p.viewpoint or p.source) for p in views}) >= 2:
            return "corroborated"
        if len({p.modality for p in views if p.modality}) >= 2:
            return "corroborated"
        return "seen_once"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["proof_level"] = self.proof_level()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "RecordObject":
        d = dict(d)
        d.pop("proof_level", None)
        d["proofs"] = [Proof(**p) for p in d.get("proofs") or []]
        return cls(**d)


@dataclass
class UnknownVolume:
    aabb: List[float]                           # [x0,y0,z0,x1,y1,z1]
    reason: str                                 # "occluded by box" | "never viewed"
    since: float = field(default_factory=time.time)
    resolved: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


def contains(aabb: List[float], p: List[float]) -> bool:
    return all(aabb[i] <= p[i] <= aabb[i + 3] for i in range(3))
