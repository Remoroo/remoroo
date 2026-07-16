"""Probe-to-record glue (COMP-13, rule 5): touching IS perceiving. One probe feeds
three gates at once — the scene record (physical proof + narrowed ranges), the
ground-truth library (a labeled case from the frame at probe time), and the cousins
(the narrowed ranges are what generation samples from). PROB-11's prescription lives
here too: touch-to-locate when cameras disagree.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from . import probe_mech
from .perceive.groundtruth import GroundTruthLibrary

PROBE_FNS = {"touch": probe_mech.probe_touch,
             "push": probe_mech.probe_push,
             "lift": probe_mech.probe_lift_place}
PROOF_KIND = {"touch": "touch", "push": "touch", "lift": "lift"}


def probe_and_record(kind: str, ctx: Any, tcp: str, *, record: Any,
                     object_id: str, library: GroundTruthLibrary,
                     frame: Optional[Dict[str, Any]] = None,
                     narrow: Optional[List[dict]] = None,
                     params: Optional[dict] = None) -> Dict[str, Any]:
    """Run one probe on a recorded object; on success write proof + case + ranges."""
    if kind not in PROBE_FNS:
        return {"error": f"unknown probe kind {kind!r} (touch | push | lift)"}
    obj = record.objects.get(object_id)
    if obj is None:
        return {"error": f"no recorded object {object_id!r} — probe what the record "
                         "knows, not coordinates from thin air"}
    probe_id = f"probe_{kind}_{int(time.time())}"
    # TOUCH THE TOP, never the centroid (live 2026-07-15: a screwdriver's centroid
    # sat at z=-0.018 — at/below the table plane — so every descend target was in
    # collision and BOTH arms "failed IK" on a plainly reachable object; the run
    # wedged at the looking exit). Top = pose.z + dims_z/2 + a contact margin;
    # params.touch_z overrides when the agent knows the support plane better.
    kwargs = dict(params or {})
    at = [float(v) for v in obj.pose[:3]]
    if "touch_z" in kwargs:
        at[2] = float(kwargs.pop("touch_z"))
    else:
        dims = list(getattr(obj, "dims", None) or [])
        half_h = float(dims[2]) / 2.0 if len(dims) >= 3 and dims[2] else 0.02
        at[2] = at[2] + half_h + 0.005
    res = PROBE_FNS[kind](ctx, tcp, at, **kwargs)
    out: Dict[str, Any] = {"ok": bool(res.get("ok")), "evidence": dict(res),
                           "probe_id": probe_id,
                           "touch_point": [round(v, 4) for v in at]}
    if not out["ok"]:
        why = res.get("why")
        out["note"] = ("probe failed; nothing recorded (absence of proof is stated)"
                       + (f" — {why}" if why else ""))
        return out
    record.confirm_touch(object_id, kind=PROOF_KIND[kind], source=probe_id)
    for n in narrow or []:
        record.narrow(object_id, n["param"], float(n["lo"]), float(n["hi"]))
    if frame is not None:                        # the frame at probe time becomes truth
        case = library.add_case(frame=frame, provenance=probe_id,
                                entities=[{"label": obj.label,
                                           "pose": list(obj.pose[:3])}])
        out["case_id"] = case.case_id
    out["proof_level"] = obj.proof_level()
    return out
