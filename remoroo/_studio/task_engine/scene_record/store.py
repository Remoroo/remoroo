"""SceneRecord store (COMP-01, IFACE-01, ART-01) — the single written truth about the
scene. The agent's perception SUBMITS scenes here with provenance; the engine merges,
grades (grading.py), and answers queries. Agent claims are inadmissible: only
submissions carrying capture provenance become proof lines (DEC-08).

File: .remoroo/task/record/<slug>.json — single-writer (the edge process), load/save
whole-file like every other task_engine store.
"""
from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from .grading import grade
from .schema import Proof, RecordObject, UnknownVolume

MATCH_RADIUS_M = 0.05        # a submitted entity within this of a known object IS it


class SceneRecord:
    def __init__(self, path: str, task_slug: str = "") -> None:
        self.path = Path(path)
        self.task_slug = task_slug
        self.objects: Dict[str, RecordObject] = {}
        self.unknowns: List[UnknownVolume] = []
        self.goal_region: Optional[dict] = None
        self.look_requests: List[dict] = []
        self.grade_history: List[dict] = []

    # ---- persistence -------------------------------------------------------------
    @classmethod
    def load(cls, path: str, task_slug: str = "") -> "SceneRecord":
        rec = cls(path, task_slug)
        p = Path(path)
        if p.exists():
            d = json.loads(p.read_text(encoding="utf-8"))
            rec.task_slug = d.get("task_slug", task_slug)
            rec.objects = {o["object_id"]: RecordObject.from_dict(o)
                           for o in d.get("objects", [])}
            rec.unknowns = [UnknownVolume(**u) for u in d.get("unknowns", [])]
            rec.goal_region = d.get("goal_region")
            rec.look_requests = d.get("look_requests", [])
            rec.grade_history = d.get("grade_history", [])[-20:]
        return rec

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.to_dict(), indent=1, default=str),
                             encoding="utf-8")

    def to_dict(self) -> dict:
        return {"task_slug": self.task_slug,
                "objects": [o.to_dict() for o in self.objects.values()],
                "unknowns": [u.to_dict() for u in self.unknowns],
                "goal_region": self.goal_region,
                "look_requests": self.look_requests,
                "grade_history": self.grade_history[-20:],
                "t": time.time()}

    # ---- submission (the ONLY way objects enter) -----------------------------------
    def submit(self, entities: List[dict], provenance: Dict[str, Any]) -> Dict[str, Any]:
        """entities: [{label, pose[, dims, confidence]}] from the agent's perception.
        provenance: {source, viewpoint, modality, arm_poses{name:[xyz]}, t?} — REQUIRED;
        a submission without capture provenance is rejected whole (agent claims are
        not evidence)."""
        for key in ("source", "viewpoint", "modality"):
            if not provenance.get(key):
                return {"error": f"submission rejected: provenance needs {key!r} "
                                 "(which camera, from where, rgb/depth/contact)"}
        t = float(provenance.get("t", time.time()))
        proof = Proof(kind=provenance.get("kind", "view"),
                      source=str(provenance["source"]),
                      viewpoint=str(provenance["viewpoint"]),
                      modality=str(provenance["modality"]), t=t)
        for e in entities:
            pose = [float(v) for v in e["pose"][:3]]
            obj = self._match(pose)
            if obj is None:
                oid = f"obj_{len(self.objects):03d}"
                obj = RecordObject(object_id=oid, label=str(e.get("label", "object")),
                                   pose=pose, dims=e.get("dims"))
                self.objects[oid] = obj
            else:
                n = len(obj.proofs)  # running mean keeps the pose from jumping
                obj.pose = [(obj.pose[k] * n + pose[k]) / (n + 1) for k in range(3)]
                if e.get("dims"):
                    obj.dims = e["dims"]
            obj.proofs.append(Proof(**proof.to_dict()))
            obj.sightings.append({"t": t, "centroid": pose,
                                  "viewpoint": proof.viewpoint,
                                  "arm_poses": provenance.get("arm_poses") or {}})
        g = grade(list(self.objects.values()))
        g["t"] = t
        self.grade_history.append(g)
        self.save()
        return g

    def _match(self, pose: List[float]) -> Optional[RecordObject]:
        # retired objects still match: a bounced phantom must keep absorbing its
        # sightings (evidence accumulates) instead of respawning fresh every frame
        best, best_d = None, MATCH_RADIUS_M
        for o in self.objects.values():
            d = math.dist(o.pose[:3], pose[:3])
            if d < best_d:
                best, best_d = o, d
        return best

    # ---- physical proof + narrowing (probes/trials call these) ----------------------
    def confirm_touch(self, object_id: str, *, kind: str, source: str) -> None:
        o = self.objects[object_id]
        o.proofs.append(Proof(kind=kind, source=source, modality="contact"))
        self.save()

    def narrow(self, object_id: str, param: str, lo: float, hi: float) -> None:
        """A probe measured something: shrink the imagination range (rule 5)."""
        o = self.objects[object_id]
        cur = o.ranges.get(param)
        o.ranges[param] = ([max(cur[0], lo), min(cur[1], hi)] if cur else [lo, hi])
        self.save()

    def retro_confirm(self, object_id: str, trial_id: str) -> None:
        """A real trial's grasp held: the detection was real — retro-label (DEC-07)."""
        self.confirm_touch(object_id, kind="trial", source=trial_id)

    # ---- queries ---------------------------------------------------------------------
    def entities(self, min_proof: str = "corroborated") -> List[RecordObject]:
        order = {"seen_once": 0, "corroborated": 1, "touched": 2}
        floor = order[min_proof]
        return [o for o in self.objects.values()
                if not o.retired and order[o.proof_level()] >= floor]

    def request_look(self, question: str, aabb: Optional[List[float]] = None) -> None:
        self.look_requests.append({"question": question, "aabb": aabb,
                                   "t": time.time(), "done": False})
        self.save()

    def add_unknown(self, aabb: List[float], reason: str) -> None:
        self.unknowns.append(UnknownVolume(aabb=aabb, reason=reason))
        self.save()

    def last_grade(self) -> dict:
        return self.grade_history[-1] if self.grade_history else {}
