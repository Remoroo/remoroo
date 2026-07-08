"""Sibling asset catalog (DATA-09) — retrieval, not reconstruction. A JSON catalog of
sim-ready assets with class tags, nominal dims, DR ranges, and license provenance. The
authored sibling_scene.py queries by perceived class + dims; scale is FITTED to perception.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

STARTER_CATALOG: List[Dict[str, Any]] = [
    # class tags are generic geometry families, never task nouns (DEC-01)
    {"id": "box_small", "tags": ["box", "block", "part", "cube"],
     "dims": [0.03, 0.03, 0.03], "asset": None, "dr": {"friction": [0.4, 1.0],
     "mass_kg": [0.02, 0.3]}, "license": "generated-primitive"},
    {"id": "cylinder_small", "tags": ["cylinder", "can", "bottle", "peg"],
     "dims": [0.03, 0.03, 0.1], "asset": None, "dr": {"friction": [0.3, 0.9],
     "mass_kg": [0.05, 0.5]}, "license": "generated-primitive"},
    {"id": "walled_box", "tags": ["container", "bin", "box_open"],
     "dims": [0.3, 0.22, 0.06], "asset": None, "dr": {"friction": [0.5, 1.0],
     "mass_kg": [0.2, 1.5]}, "license": "generated-primitive"},
    {"id": "sheet_square", "tags": ["sheet", "flexible", "deformable", "fabric"],
     "dims": [0.4, 0.4, 0.002], "asset": None, "dr": {"stretch": [0.8, 1.2],
     "bend": [0.5, 2.0]}, "license": "generated-primitive", "solver": "vbd"},
]


class AssetCatalog:
    def __init__(self, entries: Optional[List[Dict[str, Any]]] = None) -> None:
        self.entries = list(entries if entries is not None else STARTER_CATALOG)

    @classmethod
    def load(cls, path: str) -> "AssetCatalog":
        p = Path(path)
        if p.exists():
            return cls(json.loads(p.read_text()))
        return cls()

    def save(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(self.entries, indent=1))

    def retrieve(self, *, label: str, dims: Optional[List[float]] = None,
                 limit: int = 1) -> List[Dict[str, Any]]:
        """Rank by tag overlap with the perceived label words, tie-break by dims proximity;
        the caller scales the chosen asset to the PERCEIVED dims regardless."""
        words = set(label.lower().replace("_", " ").split())
        scored = []
        for e in self.entries:
            tag_score = len(words & set(e["tags"]))
            d_score = 0.0
            if dims and e.get("dims"):
                d_score = -sum(abs(a - b) for a, b in zip(dims, e["dims"]))
            scored.append((tag_score, d_score, e))
        scored.sort(key=lambda t: (t[0], t[1]), reverse=True)
        return [dict(e, fitted_dims=dims or e["dims"]) for _t, _d, e in scored[:limit]
                if _t > 0 or dims]
