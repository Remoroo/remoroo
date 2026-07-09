"""Grading (COMP-02, DEC-02) — the engine grades what the agent's perception submits;
it never runs detectors itself (DEC-01). Three rules, all mechanical:

1. TWO LOOKS OR A TOUCH: an object is actionable only at proof_level corroborated or
   touched. One camera's opinion is a hypothesis, never a fact (PROB-01).
2. ARM CORRELATION: a detection that only ever appears near the robot's own arms is
   the robot seeing itself; bounced back with the correlation shown (the live run's
   phantom #5).
3. CALIBRATION ATTRIBUTION (PROB-08): when the residuals BETWEEN viewpoints are large
   but COHERENT (everything shifted the same way), the world isn't wrong, the
   calibration is; flag it, never let perception be edited to compensate.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from .schema import RecordObject

ARM_NEAR_M = 0.12            # detection centroid within this of an arm link
ARM_CORR_FRAC = 0.8          # in at least this fraction of sightings, with >=2 sightings
CALIB_MEAN_TOL = 0.02        # coherent cross-view offset above this ...
CALIB_VAR_EPS = 0.0004       # ... with variance below this = calibration, not objects


def _dist(a: List[float], b: List[float]) -> float:
    return math.dist(a[:3], b[:3])


def arm_correlated(obj: RecordObject) -> Optional[float]:
    """Fraction of sightings with an arm nearby; None when not enough evidence."""
    sights = [s for s in obj.sightings if s.get("arm_poses")]
    if len(sights) < 2:
        return None
    near = 0
    for s in sights:
        arms = list(s["arm_poses"].values())
        if any(_dist(s["centroid"], a) <= ARM_NEAR_M for a in arms):
            near += 1
    return near / len(sights)


def cross_view_residuals(obj: RecordObject) -> List[List[float]]:
    """Pairwise centroid offsets between sightings from DIFFERENT viewpoints."""
    by_vp: Dict[str, List[float]] = {}
    for s in obj.sightings:
        vp = s.get("viewpoint", "")
        if vp and vp not in by_vp:
            by_vp[vp] = s["centroid"]
    vps = list(by_vp.values())
    return [[vps[j][k] - vps[i][k] for k in range(3)]
            for i in range(len(vps)) for j in range(i + 1, len(vps))]


def calibration_suspect(objects: List[RecordObject]) -> bool:
    """Coherent shift across MANY objects' cross-view residuals = calibration."""
    res = [r for o in objects for r in cross_view_residuals(o)]
    if len(res) < 3:
        return False
    mean = [sum(r[k] for r in res) / len(res) for k in range(3)]
    var = sum(_dist(r, mean) ** 2 for r in res) / len(res)
    return math.sqrt(sum(m * m for m in mean)) > CALIB_MEAN_TOL and var < CALIB_VAR_EPS


def grade(objects: List[RecordObject]) -> Dict[str, Any]:
    """The grade the agent's submission gets back: what's actionable, what's bounced
    and WHY (with the evidence, so the fix is obvious), and the calibration flag."""
    accepted: List[str] = []
    pending: List[str] = []
    bounced: List[Dict[str, Any]] = []
    for o in objects:
        if o.retired:                       # stays visible in every grade, not one-shot
            bounced.append({"object_id": o.object_id, "reason": o.retired})
            continue
        corr = arm_correlated(o)
        if corr is not None and corr >= ARM_CORR_FRAC:
            o.retired = (f"arm-correlated: seen near an arm in {corr:.0%} of "
                         f"{len(o.sightings)} sightings — likely the robot seeing "
                         "itself; look again with the arms elsewhere")
            bounced.append({"object_id": o.object_id, "reason": o.retired})
            continue
        level = o.proof_level()
        (accepted if level in ("corroborated", "touched") else pending).append(
            o.object_id)
    return {"accepted": accepted, "pending_second_look": pending, "bounced": bounced,
            "calibration_suspect": calibration_suspect(objects)}
