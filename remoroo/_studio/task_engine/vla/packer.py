"""Observation packer — the profile's client half. Builds the exact observation dict the
policy server expects ({"image": {slot: uint8 HxWx3}, "state": float32[state_dim]}) from
the live cell, using the SAME vla_profile.yaml the server-side configs were generated
from — the mapping cannot drift between the two sides.

Missing instrumentation degrades and is STATED (zeros in the state block + a `degraded`
list), never guessed: the VLA still sees the images; the judge still owns the truth.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

import numpy as np

from .profile import VlaProfile


def _rgb_of(bridge: Any, camera: str) -> Optional[np.ndarray]:
    grab = getattr(bridge, "grab_camera", None)
    if callable(grab):
        d = grab(camera) or {}
        rgb = d.get("rgb")
        if rgb is not None:
            return np.asarray(rgb, dtype=np.uint8)
    return None                                     # depth-only camera_frame can't feed a VLA


def _joints_of(bridge: Any, chain: str, dims: int) -> Optional[List[float]]:
    for name in ("joint_state", "joint_positions", "get_joints"):   # authored dialects
        fn = getattr(bridge, name, None)
        if callable(fn):
            try:
                vals = list(fn(chain) or [])
                return ([float(v) for v in vals] + [0.0] * dims)[:dims]
            except Exception:                       # noqa: BLE001 - keep trying dialects
                continue
    return None


def _ee_of(stack: Any, chain: str, dims: int) -> Optional[List[float]]:
    if stack is None:
        return None
    try:
        pose = list(stack.link_pose(chain))
        return ([float(v) for v in pose] + [0.0] * dims)[:dims]
    except Exception:                               # noqa: BLE001
        return None


def _gripper_of(bridge: Any, chain: str) -> Optional[float]:
    gs = getattr(bridge, "gripper_state", None)
    if callable(gs):
        try:
            return float((gs(chain) or {}).get("closed_width", 0.0))
        except Exception:                           # noqa: BLE001
            return None
    return None


def make_observation_packer(profile: VlaProfile, *, bridge: Any,
                            stack: Any = None) -> Callable[[], Dict[str, Any]]:
    def observe() -> Dict[str, Any]:
        degraded: List[str] = []
        images: Dict[str, np.ndarray] = {}
        for slot, cam in profile.cameras.items():
            rgb = _rgb_of(bridge, cam)
            if rgb is None:
                degraded.append(f"image:{slot}({cam})")
            else:
                images[slot] = rgb
        state = np.zeros(profile.state_dim, dtype=np.float32)
        for s in profile.state:
            vals: Optional[List[float]] = None
            if s.source == "joints":
                vals = _joints_of(bridge, s.chain, s.dims)
            elif s.source == "ee_pose":
                vals = _ee_of(stack, s.chain, s.dims)
            elif s.source == "gripper":
                g = _gripper_of(bridge, s.chain)
                vals = None if g is None else [g]
            if vals is None:
                degraded.append(f"state:{s.group}/{s.chain}")
            else:
                state[s.start:s.end] = np.asarray(vals[:s.dims], dtype=np.float32)
        # robo_name IS the transform selector on multi-robot checkpoints (rig-decoded
        # 2026-07-14): the deploy policy builds its FeatureTransform LAZILY from
        # configs/robot_configs/{robo_name}.yaml + norm stats on the first observation
        # that names the robot. Without it: feature_transform stays None and the
        # server crashes at resize_image ('NoneType' has no 'org_features').
        obs: Dict[str, Any] = {"image": images, "state": state,
                               "robo_name": profile.robo_name}
        if degraded:
            obs["degraded"] = degraded              # stated, travels in the trace too
        return obs

    return observe
