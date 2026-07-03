"""RegressionBank (COMP-10 / DATA-02) — ground truth from physics, not people.

Cases accumulate automatically: a grasp outcome labels the pose that predicted it; multi-view
or temporal inconsistencies write negatives; verified end-states back-fill positives. A
perception/judge update is ADOPTED only if it passes the bank (DEC-05); adoption gating is a
test, not a human.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


@dataclass
class BankCase:
    case_id: str
    kind: str                     # "pos" | "neg"
    source: str                   # "grasp_outcome" | "multiview" | "temporal" | "backfill"
    payload: Dict[str, Any]       # whatever the predictor needs to reproduce the prediction
    t: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {"case_id": self.case_id, "kind": self.kind, "source": self.source,
                "payload": self.payload, "t": self.t}


class RegressionBank:
    def __init__(self) -> None:
        self.cases: List[BankCase] = []
        self._n = 0

    # collectors ------------------------------------------------------------------
    def add(self, kind: str, source: str, payload: Dict[str, Any]) -> BankCase:
        self._n += 1
        case = BankCase(case_id=f"case_{self._n:06d}", kind=kind, source=source,
                        payload=payload)
        self.cases.append(case)
        return case

    def label_from_grasp(self, *, predicted_xyz: List[float], held: bool,
                         closed_width: float, frame_ref: Optional[str] = None) -> BankCase:
        """A grasp attempt at a perceived pose is a label for that pose, free, no vision."""
        return self.add("pos" if held else "neg", "grasp_outcome",
                        {"xyz": predicted_xyz, "closed_width": closed_width,
                         "frame_ref": frame_ref})

    def label_multiview_negative(self, *, xyz: List[float], camera: str,
                                 frame_ref: Optional[str] = None) -> BankCase:
        return self.add("neg", "multiview", {"xyz": xyz, "camera": camera,
                                             "frame_ref": frame_ref})

    # adoption gate ------------------------------------------------------------------
    def evaluate(self, predict: Callable[[BankCase], bool]) -> float:
        """predict(case) -> True means the candidate says 'positive'. Returns accuracy over
        the bank."""
        if not self.cases:
            return 1.0
        correct = 0
        for c in self.cases:
            want = c.kind == "pos"
            if bool(predict(c)) == want:
                correct += 1
        return correct / len(self.cases)

    @staticmethod
    def adopt_ok(candidate_acc: float, incumbent_acc: float, *,
                 tolerance: float = 0.02) -> bool:
        """Adopt only if the candidate does not regress the incumbent (small tolerance for
        bank noise). A regression is a stated rollback, not an emergency."""
        return candidate_acc + tolerance >= incumbent_acc

    # persistence ------------------------------------------------------------------
    def save(self, path: str) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w") as f:
            for c in self.cases:
                f.write(json.dumps(c.to_dict()) + "\n")

    @classmethod
    def load(cls, path: str) -> "RegressionBank":
        bank = cls()
        p = Path(path)
        if p.exists():
            with p.open() as f:
                for line in f:
                    d = json.loads(line)
                    bank.cases.append(BankCase(**d))
            bank._n = len(bank.cases)
        return bank
