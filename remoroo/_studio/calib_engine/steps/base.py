"""Step-executor registry — the extension point that dissolves the closed `kind` enum.

A calibration step's KIND maps to a factory that builds the executor for that step; the
service never branches on a hardcoded set. A new rig variant (a pan-tilt head, a
camera-to-camera step, …) registers a kind here and the engine + service carry it as data.

Each kind also declares a `family` — which verb-set drives it:
  * "supervised"  — the motion_check→detect→suggest/capture→solve→validate→accept loop
                    (eye_in_hand / static / eye_to_hand; the `CalibSession` executor).
  * "b2b"         — the shared-marker capture→solve→accept loop (`BaseToBaseSession`).
The service stores the executor in the slot the family names, so it never says `if kind ==`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

from ..geometry import Chain
from ..types import Target


class StepError(Exception):
    """A step can't be built yet (unknown kind, or an unsatisfied dependency such as a
    base-to-base step whose two eye-in-hand cameras aren't accepted)."""


@dataclass
class StepContext:
    """What a factory needs to build an executor, injected by the service so the engine
    layer stays testable. `accepted` maps an already-calibrated camera to its CalibResult
    (a base-to-base step reads its partners' X from here); `plan_items` is the authored
    pipeline's steps (to resolve a partner camera's chain/bridge)."""
    target: Target
    K: np.ndarray
    chain_provider: Callable[[str], Chain]
    bridge_factory: Callable[[object], object]
    wh: Tuple[int, int] = (1280, 720)
    accept_heldout_px: float = 1.5
    accept_tip_mm: float = 3.0
    accept_rot_sigma_deg: float = 0.5
    accept_trans_sigma_mm: float = 2.0
    accepted: Dict[str, object] = field(default_factory=dict)
    plan_items: List[object] = field(default_factory=list)


# kind -> (factory, family)
_REGISTRY: Dict[str, Tuple[Callable[[object, StepContext], object], str]] = {}


def register(kind: str, family: str = "supervised"):
    def deco(fn: Callable[[object, StepContext], object]):
        _REGISTRY[kind] = (fn, family)
        return fn
    return deco


def known_kinds() -> List[str]:
    return sorted(_REGISTRY)


def family_of(kind: str) -> Optional[str]:
    return _REGISTRY[kind][1] if kind in _REGISTRY else None


def make_executor(item, ctx: StepContext) -> Tuple[object, str]:
    """Build (executor, family) for a step. Raises StepError on an unknown kind so the
    service surfaces it instead of silently doing nothing."""
    if item.kind not in _REGISTRY:
        raise StepError(f"unknown step kind {item.kind!r}; known: {', '.join(known_kinds())}")
    fn, family = _REGISTRY[item.kind]
    return fn(item, ctx), family
