"""Ground-truth library + replay harness (COMP-03, IFACE-04, ART-02, DATA-03).

Every frame where a touch, lift, or real trial PROVED what was on the table becomes a
labeled test case, forever. Perception programs are then scored OFFLINE against the
whole library: hits, misses, hallucinations — the free, robot-less fitness that lets
perception evolve in the same archive as actors (DEC-06). Hallucination dies in
replay, never on the robot.

Layout (ART-02): <data_dir>/groundtruth/labels.jsonl + frames/<case_id>.npz
"""
from __future__ import annotations

import json
import math
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

HIT_RADIUS_M = 0.05


@dataclass
class GroundTruthCase:
    case_id: str
    frame_ref: str                        # frames/<id>.npz (depth[, rgb, K])
    entities: List[dict]                  # [{label, pose}] physically confirmed
    provenance: str                       # probe id / trial id that proved it
    camera: str = ""
    t: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return asdict(self)


class GroundTruthLibrary:
    def __init__(self, root: str) -> None:
        self.root = Path(root)
        self.cases: List[GroundTruthCase] = []

    @classmethod
    def load(cls, root: str) -> "GroundTruthLibrary":
        lib = cls(root)
        p = lib.root / "labels.jsonl"
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    lib.cases.append(GroundTruthCase(**json.loads(line)))
        return lib

    def add_case(self, *, frame: Dict[str, Any], entities: List[dict],
                 provenance: str, camera: str = "") -> GroundTruthCase:
        """Persist the frame (npz when numpy arrays present, json otherwise) + label."""
        self.root.joinpath("frames").mkdir(parents=True, exist_ok=True)
        cid = "gt_" + uuid.uuid4().hex[:10]
        frame_ref = f"frames/{cid}.npz"
        try:
            import numpy as np
            arrays = {k: np.asarray(v) for k, v in frame.items()
                      if v is not None and hasattr(v, "__len__") and k != "camera"}
            np.savez_compressed(self.root / frame_ref, **arrays)
        except Exception:                              # noqa: BLE001 - json fallback
            frame_ref = f"frames/{cid}.json"
            (self.root / frame_ref).write_text(json.dumps(frame, default=str))
        case = GroundTruthCase(case_id=cid, frame_ref=frame_ref,
                               entities=entities, provenance=provenance,
                               camera=camera or str(frame.get("camera", "")))
        with open(self.root / "labels.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(case.to_dict(), default=str) + "\n")
        self.cases.append(case)
        return case

    def load_frame(self, case: GroundTruthCase) -> Dict[str, Any]:
        p = self.root / case.frame_ref
        if p.suffix == ".json":
            return json.loads(p.read_text(encoding="utf-8"))
        import numpy as np
        with np.load(p, allow_pickle=False) as z:
            return {k: z[k] for k in z.files}


# --- replay (IFACE-04): run a perception program against the library ------------------
class ReplayBridge:
    """A bridge that serves exactly one stored frame — the perception program under
    test runs against it exactly as it would against the live cell."""

    def __init__(self, frame: Dict[str, Any], camera: str) -> None:
        self._frame = dict(frame)
        self._camera = camera

    def grab_camera(self, camera: str) -> Dict[str, Any]:
        return dict(self._frame)

    def camera_frame(self, camera: str) -> Any:
        return dict(self._frame)


def _compile_perceive(program_src: str, shared: Dict[str, Any]) -> Callable:
    ns: Dict[str, Any] = {}
    exec(compile(program_src, "<perception_candidate>", "exec"), ns)  # noqa: S102
    if "make_perceive" not in ns:
        raise ValueError("perception candidate must define make_perceive(shared)")
    return ns["make_perceive"](shared)


def score_perception(program_src: str, library: GroundTruthLibrary,
                     *, hallucination_penalty: float = 1.0) -> Dict[str, Any]:
    """Score = (hits - penalty*hallucinations) / total_truth over all cases, clamped
    to [0,1]. Errors are stated per case, not raised (a crashing candidate scores 0
    on that case with the traceback tail attached)."""
    per_case = []
    hits = misses = halls = total = 0
    for case in library.cases:
        truth = case.entities
        total += len(truth)
        try:
            frame = library.load_frame(case)
            perceive = _compile_perceive(
                program_src, {"bridge": ReplayBridge(frame, case.camera),
                              "stack": None, "data_dir": str(library.root)})
            scene = perceive()
            found = [list(e.pose[:3]) for e in scene.entities()]
        except Exception as e:                        # noqa: BLE001 - stated
            per_case.append({"case_id": case.case_id,
                             "error": f"{type(e).__name__}: {e}"})
            misses += len(truth)
            continue
        used = set()
        case_hits = 0
        for t_ent in truth:
            match = next((i for i, f in enumerate(found) if i not in used and
                          math.dist(f, t_ent["pose"][:3]) < HIT_RADIUS_M), None)
            if match is not None:
                used.add(match)
                case_hits += 1
        case_halls = len(found) - len(used)
        hits += case_hits
        misses += len(truth) - case_hits
        halls += case_halls
        per_case.append({"case_id": case.case_id, "hits": case_hits,
                         "misses": len(truth) - case_hits,
                         "hallucinations": case_halls})
    score = 0.0 if total == 0 else max(
        0.0, min(1.0, (hits - hallucination_penalty * halls) / total))
    return {"score": round(score, 4), "hits": hits, "misses": misses,
            "hallucinations": halls, "cases": len(library.cases),
            "per_case": per_case}
