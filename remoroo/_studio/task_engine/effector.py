"""Effector contract — ONE declaration, every world (incident b12f5aa1).

The b12f5aa1 failure was a private-dialect disease: atoms sent metres, the real
bridge read closure-fraction (0=open, 1=closed), and every sim read `width <
0.03 -> attach`. The sim scored 1.0 while the metal never closed. The cure is
this module: cell.yaml `groups[].effector` declares what the effector's NATIVE
command space MEANS, atoms speak CANONICAL aperture-metres, and every bridge —
real and all sims — converts through these functions. No consumer may interpret
a gripper command with its own literal.

Declaration (cell.yaml `groups[].effector`, agent-authored at setup):

    effector:
      kind: parallel_gripper        # | suction | magnet | hand
      units: fraction               # bridge-native space: fraction | width_m | qpos
      open: 0.0                     # native value that commands FULLY OPEN
      closed: 1.0                   # native value that commands FULLY CLOSED
      width_max_m: 0.086            # physical aperture (m) at `open`
      holding_band: [0.05, 0.97]    # measured closure-frac band = "stalled on object"

`units: width_m` is the identity space (native == aperture metres); `fraction`/
`qpos` interpolate linearly between the declared open/closed endpoints by
closure fraction c in [0,1] (c=0 fully open, c=1 fully closed).
"""
from __future__ import annotations

from typing import Any, Dict, Optional

# Matches the historic atoms defaults so an undeclared cell behaves as before —
# EXCEPT that consumers now share one meaning instead of three private ones.
DEFAULT_EFFECTOR: Dict[str, Any] = {
    "kind": "parallel_gripper",
    "units": "width_m",
    "open": 0.04,
    "closed": 0.0,
    "width_max_m": 0.08,
    "holding_band": [0.05, 0.97],
}


def normalize(eff: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge a (possibly partial / None) declaration over the default."""
    out = dict(DEFAULT_EFFECTOR)
    if isinstance(eff, dict):
        out.update({k: v for k, v in eff.items() if v is not None})
    return out


def width_max(eff: Dict[str, Any]) -> float:
    return float(normalize(eff)["width_max_m"])


def _clamp01(v: float) -> float:
    return min(max(float(v), 0.0), 1.0)


def closure_of_width(eff: Dict[str, Any], width_m: float) -> float:
    """Canonical aperture (m) -> closure fraction [0,1]."""
    wmax = width_max(eff)
    return _clamp01(1.0 - float(width_m) / wmax) if wmax > 0 else 1.0


def width_of_closure(eff: Dict[str, Any], closure: float) -> float:
    return width_max(eff) * (1.0 - _clamp01(closure))


def command_native(eff: Optional[Dict[str, Any]], *, width_m: Optional[float] = None,
                   engage: Optional[bool] = None) -> float:
    """Canonical command -> the bridge's NATIVE value. width_m=None uses the
    declared endpoint for engage(True->closed / False->open)."""
    e = normalize(eff)
    if e["units"] == "width_m":
        # identity space: the native value IS aperture metres; engage endpoints are
        # the DECLARED open/closed values (0.04/0.0 by default), never wmax
        if width_m is None:
            return float(e["closed"] if engage else e["open"])
        return min(max(float(width_m), 0.0), width_max(e))
    c = (1.0 if engage else 0.0) if width_m is None else closure_of_width(e, width_m)
    o, cl = float(e["open"]), float(e["closed"])
    return o + c * (cl - o)


def closure_of_native(eff: Optional[Dict[str, Any]], native: float) -> float:
    """A native command/measurement -> closure fraction, via the declaration."""
    e = normalize(eff)
    if e["units"] == "width_m":
        return closure_of_width(e, float(native))
    o, cl = float(e["open"]), float(e["closed"])
    if cl == o:
        return 0.0
    return _clamp01((float(native) - o) / (cl - o))


def aperture_of_native(eff: Optional[Dict[str, Any]], native: float) -> float:
    """A native command/measurement -> canonical aperture metres. THE function a
    sim must use to decide whether commanded fingers could hold an object."""
    return width_of_closure(normalize(eff), closure_of_native(eff, native))


def holding_from_closure(eff: Optional[Dict[str, Any]],
                         measured_closure: float) -> bool:
    """Parallel-gripper holding inference: a close that STALLED partway (fingers
    blocked by an object) lands inside holding_band; a full close (empty) lands
    above it; an open gripper below it."""
    lo, hi = (normalize(eff).get("holding_band") or [0.05, 0.97])[:2]
    return float(lo) <= float(measured_closure) <= float(hi)
