"""Atoms (L1) — the shipped instruction set. Geometry + a MotionStack call + an envelope
check + a trace write. No task logic, no robot logic (the Bridge is the only per-robot seam,
authored at Setup; MotionStack is robot-agnostic via the cell's URDF/spheres/calibration).

Every atom returns AtomResult and logs exactly one TraceStep. Knob-shaped arguments are plain
floats here; programs obtain them via ctx.knobs.declare(...) at their call sites (params.py).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence

from .envelope import STOP_CURRENT, STOP_FT, STOP_TIMEOUT, STOP_Z, Envelope
from .params import Knobs
from .trace import Trace


# --- pose helpers (mirror motion_engine._norm_pose accepted shapes) -----------------------
def _xyzq(pose: Any) -> List[float]:
    """Accept [x,y,z] | [x,y,z,qw,qx,qy,qz] | {'position','quaternion'} -> 7-list."""
    if isinstance(pose, dict):
        p = list(pose["position"])[:3]
        q = list(pose.get("quaternion") or [1.0, 0.0, 0.0, 0.0])[:4]
        return p + q
    p = list(pose)
    if len(p) == 7:
        return [float(v) for v in p]
    if len(p) == 3:
        return [float(v) for v in p] + [1.0, 0.0, 0.0, 0.0]
    raise ValueError(f"unrecognised pose shape: {pose!r}")


def _quat_mul(a: Sequence[float], b: Sequence[float]) -> List[float]:
    aw, ax, ay, az = a
    bw, bx, by, bz = b
    return [
        aw * bw - ax * bx - ay * by - az * bz,
        aw * bx + ax * bw + ay * bz - az * by,
        aw * by - ax * bz + ay * bw + az * bx,
        aw * bz + ax * by - ay * bx + az * bw,
    ]


def _axis_quat(axis: str, deg: float) -> List[float]:
    h = math.radians(deg) / 2.0
    s = math.sin(h)
    v = {"x": [s, 0.0, 0.0], "y": [0.0, s, 0.0], "z": [0.0, 0.0, s]}[axis]
    return [math.cos(h)] + v


@dataclass
class GraspEvidence:
    held: bool
    closed_width: float

    def to_dict(self) -> dict:
        return {"held": self.held, "closed_width": self.closed_width}


@dataclass
class AtomResult:
    ok: bool
    stop_cause: Optional[str] = None
    evidence: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Ctx:
    """Everything an atom needs. stack/bridge are duck-typed (MotionStack/Bridge or fakes)."""
    stack: Any
    bridge: Any
    envelope: Envelope
    trace: Trace
    knobs: Knobs
    arm_of: Callable[[str], str] = lambda tcp: tcp     # tcp -> gripper/arm name
    home_poses: Dict[str, Any] = field(default_factory=dict)


# --- atoms ---------------------------------------------------------------------------------
def reach(ctx: Ctx, tcp: str, pose: Any, *, speed: Optional[float] = None,
          via: Sequence[Any] = ()) -> AtomResult:
    tgt = _xyzq(pose)
    legs = [_xyzq(p) for p in via] + [tgt]
    ctx.envelope.check_points(legs)
    spd = ctx.envelope.clamp_speed(speed)
    h = ctx.trace.begin("reach", tcp=tcp, pose=tgt, via=len(via), speed=spd)
    if len(legs) == 1:
        res = ctx.stack.move_to_pose(tcp, tgt)
    else:
        res = ctx.stack.move_through_poses(tcp, legs)
    h.done(ok=bool(res.ok), stop_cause=None if res.ok else "plan_failed")
    return AtomResult(ok=bool(res.ok), stop_cause=None if res.ok else "plan_failed")


def descend(ctx: Ctx, tcp: str, to_z: float, *, speed: Optional[float] = None,
            stop_on: str = STOP_Z, step_m: float = 0.005,
            max_steps: int = 200) -> AtomResult:
    """Guarded vertical approach: step down, poll the stop signal between steps.
    The stop signal is a Bridge fact (currents/FT), never computed here."""
    ctx.envelope.require_stop(stop_on)
    spd = ctx.envelope.clamp_speed(speed)
    cur = _xyzq(ctx.stack.link_pose(tcp))
    h = ctx.trace.begin("descend", tcp=tcp, frm_z=cur[2], to_z=to_z, stop_on=stop_on, speed=spd)
    z = cur[2]
    steps = 0
    cause = STOP_TIMEOUT
    while steps < max_steps:
        if stop_on in (STOP_CURRENT, STOP_FT):
            # stop_signal is OPTIONAL bridge instrumentation (the authored contract does
            # not require it): without it, contact stops degrade to the z target.
            sig = getattr(ctx.bridge, "stop_signal", None)
            if callable(sig) and sig(stop_on, tcp):
                cause = stop_on
                break
        if z <= to_z + 1e-9:
            cause = STOP_Z
            break
        z = max(to_z, z - abs(step_m))
        nxt = [cur[0], cur[1], z] + cur[3:7]
        ctx.envelope.check_xyz(nxt[:3])
        res = ctx.stack.move_to_pose(tcp, nxt)
        if not res.ok:
            h.done(ok=False, stop_cause="plan_failed", z_reached=z)
            return AtomResult(ok=False, stop_cause="plan_failed", evidence={"z": z})
        steps += 1
    ok = cause in (STOP_Z, STOP_CURRENT, STOP_FT)
    h.done(ok=ok, stop_cause=cause, z_final=z, steps=steps)
    return AtomResult(ok=ok, stop_cause=cause, evidence={"z": z, "steps": steps})


def grasp(ctx: Ctx, tcp: str, *, width: float, force_hint: Optional[float] = None) -> AtomResult:
    arm = ctx.arm_of(tcp)
    h = ctx.trace.begin("grasp", tcp=tcp, width=width, force_hint=force_hint)
    ctx.bridge.set_gripper(arm, width)
    gs = getattr(ctx.bridge, "gripper_state", None)   # OPTIONAL instrumentation
    if callable(gs):
        state = gs(arm) or {}
        closed = float(state.get("closed_width", 0.0))
        held = bool(state.get("holding", closed > 1e-3))
        ev = GraspEvidence(held=held, closed_width=closed)
        h.done(ok=held, stop_cause=None if held else "grasp_empty", **ev.to_dict())
        return AtomResult(ok=held, stop_cause=None if held else "grasp_empty",
                          evidence=ev.to_dict())
    # No gripper feedback on this bridge: the grasp PROCEEDS (the verifier judges the
    # outcome from perception — the actual truth); evidence states held=UNKNOWN so the
    # judge's proprioceptive channel abstains instead of lying.
    h.done(ok=True, stop_cause=None, held=None, closed_width=None, instrumented=False)
    return AtomResult(ok=True, stop_cause=None,
                      evidence={"held": None, "closed_width": None,
                                "instrumented": False})


def release(ctx: Ctx, tcp: str, *, open_width: float = 0.04,
            at: Optional[Any] = None) -> AtomResult:
    if at is not None:
        r = reach(ctx, tcp, at)
        if not r.ok:
            return r
    arm = ctx.arm_of(tcp)
    h = ctx.trace.begin("release", tcp=tcp, open_width=open_width)
    ctx.bridge.set_gripper(arm, open_width)
    h.done(ok=True)
    return AtomResult(ok=True)


def carry(ctx: Ctx, tcp: str, to: Any, *, arc_h: float, speed: Optional[float] = None,
          via: Sequence[Any] = ()) -> AtomResult:
    """Transport while holding: lift-over arc so the payload clears obstacles between here
    and there. arc peak z = max(start_z, end_z) + arc_h."""
    p0 = _xyzq(ctx.stack.link_pose(tcp))
    p1 = _xyzq(to)
    peak_z = max(p0[2], p1[2]) + abs(arc_h)
    mid = [(p0[0] + p1[0]) / 2.0, (p0[1] + p1[1]) / 2.0, peak_z] + p1[3:7]
    legs = [mid] + [_xyzq(p) for p in via] + [p1]
    ctx.envelope.check_points(legs)
    spd = ctx.envelope.clamp_speed(speed)
    h = ctx.trace.begin("carry", tcp=tcp, to=p1, arc_h=arc_h, peak_z=peak_z, speed=spd)
    res = ctx.stack.move_through_poses(tcp, legs)
    h.done(ok=bool(res.ok), stop_cause=None if res.ok else "plan_failed")
    return AtomResult(ok=bool(res.ok), stop_cause=None if res.ok else "plan_failed",
                      evidence={"peak_z": peak_z})


def sweep(ctx: Ctx, tcp: str, frm: Any, to: Any, *, press_z: float,
          speed: Optional[float] = None) -> AtomResult:
    """Planar move in contact with a surface at constant z (press_z)."""
    a = _xyzq(frm)
    b = _xyzq(to)
    a[2] = b[2] = float(press_z)
    ctx.envelope.check_points([a, b])
    spd = ctx.envelope.clamp_speed(speed)
    h = ctx.trace.begin("sweep", tcp=tcp, frm=a[:3], to=b[:3], press_z=press_z, speed=spd)
    res = ctx.stack.move_through_poses(tcp, [a, b])
    h.done(ok=bool(res.ok), stop_cause=None if res.ok else "plan_failed")
    return AtomResult(ok=bool(res.ok), stop_cause=None if res.ok else "plan_failed")


def turn(ctx: Ctx, tcp: str, *, deg: float, axis: str = "z",
         speed: Optional[float] = None) -> AtomResult:
    cur = _xyzq(ctx.stack.link_pose(tcp))
    q = _quat_mul(_axis_quat(axis, deg), cur[3:7])
    tgt = cur[:3] + q
    h = ctx.trace.begin("turn", tcp=tcp, deg=deg, axis=axis)
    res = ctx.stack.move_to_pose(tcp, tgt)
    h.done(ok=bool(res.ok), stop_cause=None if res.ok else "plan_failed")
    return AtomResult(ok=bool(res.ok), stop_cause=None if res.ok else "plan_failed")


def hold(ctx: Ctx, tcp: str) -> AtomResult:
    h = ctx.trace.begin("hold", tcp=tcp)
    h.done(ok=True)
    return AtomResult(ok=True)


def look(ctx: Ctx, camera: str, at: Any, *, tcp: Optional[str] = None) -> AtomResult:
    """Point a wrist camera by moving its mount tcp to a viewpoint pose; static cameras are a
    no-op (they already look where they look)."""
    h = ctx.trace.begin("look", camera=camera, tcp=tcp)
    if tcp is None:
        h.done(ok=True, static=True)
        return AtomResult(ok=True, evidence={"static": True})
    r = reach(ctx, tcp, at)
    h.done(ok=r.ok, stop_cause=r.stop_cause)
    return r


def home(ctx: Ctx, tcp: str) -> AtomResult:
    pose = ctx.home_poses.get(tcp)
    if pose is None:
        h = ctx.trace.begin("home", tcp=tcp)
        h.done(ok=False, stop_cause="no_home_pose")
        return AtomResult(ok=False, stop_cause="no_home_pose")
    return reach(ctx, tcp, pose)


def sync(ctx: Ctx, targets: Dict[str, Any], *, speed: Optional[float] = None) -> AtomResult:
    """Two tcps to two poses as ONE jointly-planned motion (v1 supports exactly 2; the
    MotionStack goal set is already N-arm, lift the cap when a third arm exists)."""
    if len(targets) != 2:
        raise ValueError("sync v1 supports exactly two tcps")
    goals = {t: _xyzq(p) for t, p in targets.items()}
    ctx.envelope.check_points(list(goals.values()))
    spd = ctx.envelope.clamp_speed(speed)
    h = ctx.trace.begin("sync", tcps=sorted(goals.keys()), speed=spd)
    res = ctx.stack.move_to_poses(goals)
    h.done(ok=bool(res.ok), stop_cause=None if res.ok else "plan_failed")
    return AtomResult(ok=bool(res.ok), stop_cause=None if res.ok else "plan_failed")
