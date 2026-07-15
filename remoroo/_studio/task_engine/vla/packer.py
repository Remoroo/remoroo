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


def _joints_of(bridge: Any, chain: str, dims: int,
               groups: Optional[Dict[str, Any]] = None) -> Optional[List[float]]:
    for name in ("joint_state", "joint_positions", "get_joints"):   # authored dialects
        fn = getattr(bridge, name, None)
        if callable(fn):
            try:
                vals = list(fn(chain) or [])
                return ([float(v) for v in vals] + [0.0] * dims)[:dims]
            except Exception:                       # noqa: BLE001 - keep trying dialects
                continue
    # THE dialect this rig actually speaks (and the stack already uses):
    # get_observation().joint_positions is a dict of ALL joints; the cell's groups
    # say which names belong to this chain. Live 2026-07-15: without this path the
    # packer zeroed every state and the policy was BLIND to its own configuration.
    try:
        obs = bridge.get_observation()
        jp = dict(getattr(obs, "joint_positions", None) or {})
        names = list(((groups or {}).get(chain) or {}).get("joint_names") or [])
        if jp and names:
            vals = [float(jp[n]) for n in names if n in jp]
            if len(vals) >= min(dims, len(names)):
                return (vals + [0.0] * dims)[:dims]
    except Exception:                               # noqa: BLE001
        pass
    return None


def _ee_of(stack: Any, chain: str, dims: int) -> Optional[List[float]]:
    if stack is None:
        return None
    try:
        pose = stack.link_pose(chain)
        if pose is None:
            return None
        if isinstance(pose, tuple) and len(pose) == 2:   # (xyz, wxyz) stack convention
            flat = list(pose[0])[:3] + list(pose[1])[:4]  # the tuple bug's THIRD site:
        else:                                             # float(list) raised -> silent
            flat = list(pose)                             # None -> zero states (2026-07-15)
        return ([float(v) for v in flat] + [0.0] * dims)[:dims]
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
                            stack: Any = None,
                            groups: Optional[Dict[str, Any]] = None
                            ) -> Callable[[], Dict[str, Any]]:
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
                vals = _joints_of(bridge, s.chain, s.dims, groups=groups)
            elif s.source == "ee_pose":
                vals = _ee_of(stack, s.chain, s.dims)
            elif s.source == "gripper":
                g = _gripper_of(bridge, s.chain)
                vals = None if g is None else [g]
            if vals is None:
                degraded.append(f"state:{s.group}/{s.chain}")
            else:
                state[s.start:s.end] = np.asarray(vals[:s.dims], dtype=np.float32)
        # WIRE SCHEMA READ FROM THE VENDOR SOURCE (2026-07-15, deploy/
        # lingbot_vla_v2_policy.py resize_image + FeatureTransform.apply): the obs is
        # a FLAT dict — each image under its full feature name
        # ("observation.images.<slot>", HxWx3), the state vector under
        # "observation.state" (their origin_keys slice exactly that key), "prompt"
        # injected by the runtime at act time, and "robo_name" (the lazy-reset
        # transform selector on multi-robot checkpoints). The old nested
        # {"image": {...}, "state": ...} would have failed resize_image's
        # `assert image_feature in observation`.
        obs: Dict[str, Any] = {"observation.state": state,
                               "robo_name": profile.robo_name}
        for slot, rgb in images.items():
            obs[f"observation.images.{slot}"] = rgb
        if degraded:
            obs["degraded"] = degraded              # stated, travels in the trace too
        return obs

    return observe
