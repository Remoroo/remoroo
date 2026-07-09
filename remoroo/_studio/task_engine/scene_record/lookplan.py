"""Look planner (COMP-14, rule 4): the next viewpoint that settles the biggest open
question. Not seeing something is recorded (unknown volumes, single-look objects,
explicit look requests) — this module turns those debts into ranked, executable
viewpoint proposals (atoms.look / the VLA scout consume them).

Ranking: explicit look requests first (the search asked: PROB-07), then unknown
volumes by size (biggest ignorance first), then single-look objects (cheapest
upgrade to actionable). Every proposal says WHY in one sentence.
"""
from __future__ import annotations

from typing import Any, Dict, List

STANDOFF_M = 0.35            # camera standoff above/beside the question
LATERAL_TILT = 0.25          # side-view offset for height/backside questions


def propose(record: Any, *, max_proposals: int = 5) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for req in record.look_requests:
        if req.get("done"):
            continue
        aabb = req.get("aabb")
        center = ([(aabb[i] + aabb[i + 3]) / 2 for i in range(3)] if aabb
                  else None)
        out.append({"kind": "requested", "look_at": center,
                    "camera_pose": _above(center) if center else None,
                    "reason": f"asked for: {req['question']}", "rank": 0})
    for u in record.unknowns:
        if u.resolved:
            continue
        c = [(u.aabb[i] + u.aabb[i + 3]) / 2 for i in range(3)]
        size = ((u.aabb[3] - u.aabb[0]) * (u.aabb[4] - u.aabb[1])
                * max(0.01, u.aabb[5] - u.aabb[2]))
        out.append({"kind": "unknown_volume", "look_at": c,
                    "camera_pose": _lateral(c),
                    "reason": f"never seen: {u.reason} "
                              f"({size * 1e6:.0f} cm3 of ignorance)",
                    "rank": 1, "size": size})
    for o in record.objects.values():
        if o.retired or o.proof_level() != "seen_once":
            continue
        out.append({"kind": "second_look", "look_at": list(o.pose[:3]),
                    "camera_pose": _lateral(o.pose[:3]),
                    "object_id": o.object_id,
                    "reason": f"{o.object_id} has one look; a second from a "
                              "different viewpoint makes it actionable", "rank": 2})
    out.sort(key=lambda p: (p["rank"], -p.get("size", 0.0)))
    return out[:max_proposals]


def _above(c: List[float]) -> List[float]:
    return [c[0], c[1], c[2] + STANDOFF_M, 1.0, 0.0, 0.0, 0.0]


def _lateral(c: List[float]) -> List[float]:
    return [c[0] - LATERAL_TILT, c[1], c[2] + STANDOFF_M * 0.7, 1.0, 0.0, 0.0, 0.0]
