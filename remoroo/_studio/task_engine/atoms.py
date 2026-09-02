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

from . import effector as _eff
from .effector import DEFAULT_EFFECTOR
from .envelope import STOP_CURRENT, STOP_FT, STOP_TIMEOUT, STOP_Z, Envelope
from .params import Knobs
from .trace import Trace


# --- pose helpers (mirror motion_engine._norm_pose accepted shapes) -----------------------
def _xyzq(pose: Any) -> List[float]:
    """Accept [x,y,z] | [x,y,z,qw,qx,qy,qz] | {'position','quaternion'} -> 7-list.
    NOTE: a bare position gets the IDENTITY quat here — callers with a ctx must use
    _xyzq_live instead, which fills the CURRENT tool orientation (operator-diagnosed
    2026-07-15: identity = "tool aligned with the base", near-infeasible at tabletop
    reach points — both arms "failed IK" at positions the arm demonstrably reaches;
    probes were the only bare-position caller, so it looked like a probe curse)."""
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


def _xyzq_live(ctx: "Ctx", tcp: str, pose: Any) -> List[float]:
    """_xyzq, but a bare position keeps the tool's CURRENT orientation. ONLY for the
    verbs whose PHYSICS constrain orientation: carry (a held payload must not be
    reoriented) and sweep (contact continuation at the approach orientation). Free-
    space reach/sync pass bare positions through as POSITION-ONLY planner goals."""
    if not isinstance(pose, dict):
        p = list(pose)
        if len(p) == 3:
            return [float(v) for v in p] + _live_pose(ctx, tcp)[3:7]
    return _xyzq(pose)


def _live_pose(ctx: "Ctx", tcp: str) -> List[float]:
    """The tcp's live pose via stack FK — STATED when the stack can't FK this name
    (None used to surface as `TypeError: 'NoneType' object is not iterable`, which cost
    a live run several turns to trace)."""
    pose = ctx.stack.link_pose(tcp)
    if pose is None:
        raise RuntimeError(f"stack.link_pose({tcp!r}) returned None — this stack can't "
                           f"FK that name; use the cell's planner group name (the stack "
                           f"accepts a group or its tip link)")
    if isinstance(pose, tuple) and len(pose) == 2:      # (xyz, wxyz) stack convention
        return list(pose[0])[:3] + list(pose[1])[:4]
    return _xyzq(pose)


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


def _planned(res, h, **evidence) -> "AtomResult":
    """Fold a MoveResult into an AtomResult WITH the planner's message — a failed plan the
    agent can read in the result, never only in edge.log (live lesson 2026-07-13)."""
    ok = bool(res.ok)
    ev = dict(evidence)
    if not ok:
        ev["planner"] = str(getattr(res, "message", "") or "plan failed")
    h.done(ok=ok, stop_cause=None if ok else "plan_failed", **({"planner": ev["planner"]} if not ok else {}))
    return AtomResult(ok=ok, stop_cause=None if ok else "plan_failed", evidence=ev)


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
    # EMBODIMENT FRAMES (cell-declared, agent-authored like `groups`). Defaults = a
    # tabletop manipulator, so shipped cells are byte-for-byte unchanged; a wall arm /
    # suction head / humanoid declares its own and the same atoms just work.
    up_of: Callable[[str], Any] = lambda tcp: (0.0, 0.0, 1.0)        # anti-gravity
    approach_of: Callable[[str], Any] = lambda tcp: (0.0, 0.0, -1.0)  # tool advance dir
    # FULL default declaration (effector.DEFAULT_EFFECTOR) — never a bare kind, so
    # every consumer downstream can convert units without guessing (b12f5aa1).
    effector_of: Callable[[str], Dict[str, Any]] = \
        lambda tcp: dict(DEFAULT_EFFECTOR)


def _unit(v) -> tuple:
    import math
    v = [float(x) for x in list(v)[:3]]
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return (v[0] / n, v[1] / n, v[2] / n)


def _along(p, u) -> float:
    return sum(float(p[i]) * u[i] for i in range(3))


def _actuate(ctx: "Ctx", tcp: str, *, engage: bool, width: Optional[float] = None,
             **params) -> Any:
    """General effector command. `width` is CANONICAL aperture-METRES (or None for
    the declared engage/disengage endpoint). Prefers bridge.actuate_effector(arm,
    engage, effector, ...); the set_gripper fallback converts canonical->NATIVE via
    the cell's effector declaration (effector.command_native) — NEVER raw metres
    into a fraction bridge (incident b12f5aa1: sim 1.0, metal never closed)."""
    eff = _eff.normalize(ctx.effector_of(tcp))
    fn = getattr(ctx.bridge, "actuate_effector", None)
    if callable(fn):
        return fn(ctx.arm_of(tcp), engage=engage, effector=eff, width=width, **params)
    native = _eff.command_native(eff, width_m=width, engage=engage)
    return ctx.bridge.set_gripper(ctx.arm_of(tcp), native)


def _settled_effector_state(ctx: "Ctx", tcp: str, *, timeout_s: float = 3.0,
                            poll_s: float = 0.25) -> Optional[dict]:
    """effector state AFTER the fingers stop moving. Run 48236590 (2026-07-21):
    the xArm close command is wait=False and evidence was read 0.5s later — the
    measured aperture was ~fully open MID-TRAVEL, poisoning `held` both ways
    (holding-band false positive + expectation false negative). Poll until the
    closure measurement stops changing; a bridge that reports instantly-stable
    values (every sim) exits after one cheap re-read."""
    import time as _t
    st = _effector_state(ctx, tcp)
    if st is None:
        return None
    last = st.get("closed_frac", st.get("closed_width"))
    t0 = _t.monotonic()
    while _t.monotonic() - t0 < timeout_s:
        _t.sleep(0.05 if _t.monotonic() - t0 < 0.06 else poll_s)
        st = _effector_state(ctx, tcp) or st
        cur = st.get("closed_frac", st.get("closed_width"))
        if cur is not None and last is not None and abs(float(cur) - float(last)) < 0.004:
            return st                               # fingers stopped moving
        last = cur
    return st


def _effector_state(ctx: "Ctx", tcp: str) -> Optional[dict]:
    """None = NOT INSTRUMENTED (grasp evidence abstains). A bridge whose read
    fails at runtime returns a falsy value — that is None here too, never {}
    (an empty dict would masquerade as 'instrumented and empty' and raise a
    false grasp_empty)."""
    fn = (getattr(ctx.bridge, "effector_state", None)
          or getattr(ctx.bridge, "gripper_state", None))
    if not callable(fn):
        return None
    st = fn(ctx.arm_of(tcp))
    return st if st else None


# --- atoms ---------------------------------------------------------------------------------
def reach(ctx: Ctx, tcp: str, pose: Any, *, speed: Optional[float] = None,
          via: Sequence[Any] = ()) -> AtomResult:
    # A bare [x,y,z] is a POSITION-ONLY goal: the stack marks it and the planner
    # SOLVES the orientation (ToolPoseCriteria.track_position — cuRobo's designed
    # mode). Neither identity (infeasible, the 2026-07-15 probe bug) nor "current
    # orientation" (arbitrary mid-task state) is ever substituted. Envelope checks
    # need only xyz; the raw pose passes through to the stack untouched.
    # dict poses carry agent-declared constraints ({position, quaternion, axes}) — the
    # stack reads them RAW (_axes_of); flattening here dropped the axes key (audit 2026-07-18)
    tgt = (pose if isinstance(pose, dict)
           else (list(pose) if len(list(pose)) == 3 else _xyzq(pose)))
    legs = [(p if not isinstance(p, dict) and len(list(p)) == 3 else _xyzq(p))
            for p in via] + [tgt]
    ctx.envelope.check_points(legs)
    spd = ctx.envelope.clamp_speed(speed)
    h = ctx.trace.begin("reach", tcp=tcp, pose=tgt, via=len(via), speed=spd)
    if len(legs) == 1:
        res = ctx.stack.move_to_pose(tcp, tgt)
    else:
        res = ctx.stack.move_through_poses(tcp, legs)
    return _planned(res, h)


def descend(ctx: Ctx, tcp: str, to_z: Optional[float] = None, *,
            distance: Optional[float] = None, axis: Optional[Any] = None,
            speed: Optional[float] = None, stop_on: str = STOP_Z,
            step_m: float = 0.005, max_steps: int = 200) -> AtomResult:
    """APPROACH along an axis. TWO regimes, chosen by what stop_on needs:

    - stop_on=z_reached (no guard signal): ONE planned trajectory straight to the
      target — there is NOTHING to poll between steps, so stepping is pure waste
      (operator, run 48236590: 'our descend atom is stupid — it should go directly
      into the object, not in small chunks'; 17 plans x 1.3s = 22s per 8cm).
    - stop_on=current/FT: the guarded loop — step, polling the stop signal (a
      Bridge fact, never computed here) between steps; the stepping IS the guard.

    BACKWARD-COMPATIBLE: descend(tcp, to_z=Z) advances world -Z until z==Z (the
    tabletop default). A wall/suction/humanoid TCP passes axis=<its approach> (or
    the cell declares approach_of) for `distance` metres."""
    ctx.envelope.require_stop(stop_on)
    spd = ctx.envelope.clamp_speed(speed)
    cur = _live_pose(ctx, tcp)
    p0 = cur[:3]
    if to_z is not None and axis is None:
        a = (0.0, 0.0, -1.0)                          # COMPAT: straight down
        total = max(0.0, p0[2] - float(to_z))
    else:
        a = _unit(axis if axis is not None else ctx.approach_of(tcp))
        total = abs(float(distance if distance is not None else 0.0))
    h = ctx.trace.begin("descend", tcp=tcp, frm=list(p0), axis=list(a),
                        total=round(total, 4), to_z=to_z, stop_on=stop_on, speed=spd)
    if stop_on not in (STOP_CURRENT, STOP_FT):
        # UNGUARDED: one planned move. A midpoint waypoint keeps the path close to
        # the approach line (a free plan may bow); still ONE trajectory, ONE plan.
        mid = [p0[i] + a[i] * total * 0.5 for i in range(3)] + cur[3:7]
        end = [p0[i] + a[i] * total for i in range(3)] + cur[3:7]
        ctx.envelope.check_points([mid, end])
        res = (ctx.stack.move_through_poses(tcp, [mid, end]) if total > 0.02
               else ctx.stack.move_to_pose(tcp, end))
        if not res.ok:
            return _planned(res, h, travelled=0.0)
        h.done(ok=True, stop_cause=STOP_Z, z_final=end[2], steps=1)
        return AtomResult(ok=True, stop_cause=STOP_Z,
                          evidence={"z": end[2], "steps": 1})
    travelled, steps, cause = 0.0, 0, STOP_TIMEOUT
    while steps < max_steps:
        if stop_on in (STOP_CURRENT, STOP_FT):
            sig = getattr(ctx.bridge, "stop_signal", None)
            if callable(sig) and sig(stop_on, tcp):
                cause = stop_on
                break
        if travelled >= total - 1e-9:
            cause = STOP_Z                            # target distance reached
            break
        travelled = min(total, travelled + abs(step_m))
        nxt = [p0[i] + a[i] * travelled for i in range(3)] + cur[3:7]
        ctx.envelope.check_xyz(nxt[:3])
        res = ctx.stack.move_to_pose(tcp, nxt)
        if not res.ok:
            return _planned(res, h, travelled=round(travelled, 4))
        steps += 1
    ok = cause in (STOP_Z, STOP_CURRENT, STOP_FT)
    z_final = p0[2] + a[2] * travelled
    h.done(ok=ok, stop_cause=cause, z_final=z_final, steps=steps)
    return AtomResult(ok=ok, stop_cause=cause, evidence={"z": z_final, "steps": steps})


def grasp(ctx: Ctx, tcp: str, *, width: Optional[float] = None,
          force_hint: Optional[float] = None, **params) -> AtomResult:
    """ENGAGE the effector — width for a parallel gripper, vacuum for suction, current
    for magnetic, a finger config for a hand — via the effector abstraction. `held` is
    generic evidence from effector_state (a suction cup holds at width~0 and is NOT
    misread as empty). width=None -> the effector's own closed value."""
    h = ctx.trace.begin("grasp", tcp=tcp, width=width, force_hint=force_hint,
                        effector=(ctx.effector_of(tcp) or {}).get("kind"))
    _actuate(ctx, tcp, engage=True, width=width, force_hint=force_hint, **params)
    state = _settled_effector_state(ctx, tcp)       # evidence AFTER travel, never mid
    if state is not None:
        closed = float(state.get("closed_width", 0.0))
        held = bool(state.get("holding", state.get("engaged", closed > 1e-3)))
        ev = GraspEvidence(held=held, closed_width=closed)
        h.done(ok=held, stop_cause=None if held else "grasp_empty", **ev.to_dict())
        return AtomResult(ok=held, stop_cause=None if held else "grasp_empty",
                          evidence=ev.to_dict())
    # No effector feedback: proceed; the verifier judges the OUTCOME from perception,
    # and evidence states held=UNKNOWN so the proprio channel abstains, never lies.
    h.done(ok=True, stop_cause=None, held=None, closed_width=None, instrumented=False)
    return AtomResult(ok=True, stop_cause=None,
                      evidence={"held": None, "closed_width": None,
                                "instrumented": False})


def release(ctx: Ctx, tcp: str, *, open_width: Optional[float] = None,
            at: Optional[Any] = None, **params) -> AtomResult:
    """DISENGAGE the effector — open a gripper, vacuum-off a suction cup, current-off a
    magnet. open_width=None -> the effector's own open value (0.04 m for a gripper)."""
    if at is not None:
        r = reach(ctx, tcp, at)
        if not r.ok:
            return r
    h = ctx.trace.begin("release", tcp=tcp, open_width=open_width,
                        effector=(ctx.effector_of(tcp) or {}).get("kind"))
    _actuate(ctx, tcp, engage=False, width=open_width, **params)
    h.done(ok=True)
    return AtomResult(ok=True)


def carry(ctx: Ctx, tcp: str, to: Any, *, arc_h: float, speed: Optional[float] = None,
          via: Sequence[Any] = (), up: Optional[Any] = None) -> AtomResult:
    """Transport while holding: a clearance arc along the cell's UP (anti-gravity) axis
    so the payload clears obstacles. Default up=+Z (tabletop: peak = max(z0,z1)+arc_h);
    a wall/ceiling/mobile embodiment declares up_of and the arc bows along THAT axis."""
    p0 = _live_pose(ctx, tcp)
    p1 = _xyzq_live(ctx, tcp, to)
    u = _unit(up if up is not None else ctx.up_of(tcp))
    mid_xyz = [(p0[i] + p1[i]) / 2.0 for i in range(3)]
    peak = max(_along(p0[:3], u), _along(p1[:3], u)) + abs(arc_h)
    lift = peak - _along(mid_xyz, u)                  # raise midpoint to the arc peak
    mid = [mid_xyz[i] + u[i] * lift for i in range(3)] + p1[3:7]
    legs = [mid] + [_xyzq_live(ctx, tcp, p) for p in via] + [p1]
    ctx.envelope.check_points(legs)
    spd = ctx.envelope.clamp_speed(speed)
    h = ctx.trace.begin("carry", tcp=tcp, to=p1, arc_h=arc_h, up=list(u),
                        peak=round(peak, 4), speed=spd)
    res = ctx.stack.move_through_poses(tcp, legs)
    return _planned(res, h, peak_z=mid[2])


def sweep(ctx: Ctx, tcp: str, frm: Any, to: Any, *, press_z: Optional[float] = None,
          press: Optional[float] = None, normal: Optional[Any] = None,
          speed: Optional[float] = None) -> AtomResult:
    """Move in contact with a surface, holding a constant press ALONG the surface normal
    while sliding in its tangent plane. Default normal=+Z with press_z pins world z (the
    tabletop wipe); a vertical wall / tilted panel declares a normal + press and the two
    endpoints hold that normal offset instead of world z."""
    a = _xyzq_live(ctx, tcp, frm)
    b = _xyzq_live(ctx, tcp, to)
    if normal is None and press_z is not None:
        a[2] = b[2] = float(press_z)                  # COMPAT: constant world z
        n, pv = (0.0, 0.0, 1.0), float(press_z)
    else:
        n = _unit(normal if normal is not None else ctx.up_of(tcp))
        pv = float(press if press is not None else (press_z or 0.0))
        for p in (a, b):                              # set the along-normal component to pv
            delta = pv - _along(p[:3], n)
            for i in range(3):
                p[i] += n[i] * delta
    ctx.envelope.check_points([a, b])
    spd = ctx.envelope.clamp_speed(speed)
    h = ctx.trace.begin("sweep", tcp=tcp, frm=a[:3], to=b[:3], normal=list(n),
                        press=pv, speed=spd)
    res = ctx.stack.move_through_poses(tcp, [a, b])
    return _planned(res, h)


def turn(ctx: Ctx, tcp: str, *, deg: float, axis: str = "z",
         speed: Optional[float] = None) -> AtomResult:
    cur = _live_pose(ctx, tcp)
    q = _quat_mul(_axis_quat(axis, deg), cur[3:7])
    tgt = cur[:3] + q
    h = ctx.trace.begin("turn", tcp=tcp, deg=deg, axis=axis)
    res = ctx.stack.move_to_pose(tcp, tgt)
    return _planned(res, h)


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
    """N tcps to N poses as ONE jointly-planned motion. Count-agnostic (1, 2, or a
    humanoid's many) — the MotionStack goal set is already N-tool; sync just delegates
    the whole set to one trajectory."""
    if not targets:
        raise ValueError("sync needs at least one tcp target")
    goals = {t: (p if isinstance(p, dict)                       # dicts RAW: axes survive
                 else (list(p) if len(list(p)) == 3 else _xyzq(p)))
             for t, p in targets.items()}          # bare = position-only (stack marks it)
    ctx.envelope.check_points([
        (list(p["position"])[:3] if isinstance(p, dict) else list(p)[:3])
        for p in goals.values()])
    spd = ctx.envelope.clamp_speed(speed)
    h = ctx.trace.begin("sync", tcps=sorted(goals.keys()), speed=spd)
    res = ctx.stack.move_to_poses(goals)
    return _planned(res, h)
