"""The authored calibration pipeline — the engine CONSUMES what the agent authored.

Across gates 1–5 the Remoroo agent models the rig and authors a declarative pipeline:
ordered steps, each binding a kind + camera(s) + arm(s) + a named target, with `depends_on`
edges. The engine never guesses the steps from URDF strings; it parses, VALIDATES (loudly),
topologically orders, and resolves each step's URDF kinematics — but the structure is the
agent's.

Three layers, the first two pure-data so they're fully testable off-robot:
  * `parse(spec)`            dict (loaded from pipeline.yaml) -> [PipelineStep] + target specs
  * `validate` / `ordered`   structural checks + dependency topo-sort (no URDF, no cv2)
  * `resolve(...)`           fills each step's URDF kinematics (flange/nominal transforms) and
                             builds the Targets -> engine `PlanItem`s + a {id: Target} map
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Sequence, Tuple

import numpy as np

from . import urdf_io
from .types import PlanItem, Target

# Which bindings each step FAMILY requires. A supervised step binds one camera + (for an
# arm-driven kind) one arm; a base_to_base step binds exactly two cameras + two arms. Kept
# as data so a new family is a table entry, not a new branch.
_FAMILY_BINDINGS = {
    "supervised": {"cameras": 1, "arms_max": 1},
    "b2b": {"cameras": 2, "arms_max": 2},
}


class PipelineError(ValueError):
    """An authored pipeline is malformed — unknown kind/target/camera/arm, a missing or
    duplicate binding, or an unsatisfiable / cyclic dependency. Always precise, never silent."""


@dataclass
class PipelineStep:
    id: str
    kind: str
    camera: str = ""
    cameras: List[str] = field(default_factory=list)
    arm: str = ""
    arms: List[str] = field(default_factory=list)
    target: str = ""
    depends_on: List[str] = field(default_factory=list)
    board_source: str = "handheld"

    def all_cameras(self) -> List[str]:
        return ([self.camera] if self.camera else []) + list(self.cameras)

    def all_arms(self) -> List[str]:
        return ([self.arm] if self.arm else []) + list(self.arms)


@dataclass
class TargetSpec:
    id: str
    type: str
    params: dict


def parse(spec: dict) -> Tuple[List[PipelineStep], Dict[str, TargetSpec]]:
    """Pure-structure parse of an authored pipeline dict. Raises PipelineError on a missing
    id/kind or a duplicate step id — the structural floor before deeper validation."""
    if not isinstance(spec, dict):
        raise PipelineError(f"pipeline must be a mapping, got {type(spec).__name__}")
    targets: Dict[str, TargetSpec] = {}
    for tid, t in (spec.get("targets") or {}).items():
        if not isinstance(t, dict) or "type" not in t:
            raise PipelineError(f"target {tid!r} must be a mapping with a 'type'")
        targets[str(tid)] = TargetSpec(id=str(tid), type=str(t["type"]), params=dict(t.get("params") or {}))

    steps: List[PipelineStep] = []
    seen = set()
    for raw in (spec.get("steps") or []):
        if not isinstance(raw, dict):
            raise PipelineError(f"each step must be a mapping, got {type(raw).__name__}")
        sid = str(raw.get("id") or "")
        kind = str(raw.get("kind") or "")
        if not sid or not kind:
            raise PipelineError(f"step needs both 'id' and 'kind' (got id={sid!r}, kind={kind!r})")
        if sid in seen:
            raise PipelineError(f"duplicate step id {sid!r}")
        seen.add(sid)
        steps.append(PipelineStep(
            id=sid, kind=kind,
            camera=str(raw.get("camera") or ""), cameras=[str(c) for c in (raw.get("cameras") or [])],
            arm=str(raw.get("arm") or ""), arms=[str(a) for a in (raw.get("arms") or [])],
            target=str(raw.get("target") or ""),
            depends_on=[str(d) for d in (raw.get("depends_on") or [])],
            board_source=str(raw.get("board_source") or "handheld"),
        ))
    if not steps:
        raise PipelineError("pipeline has no steps")
    return steps, targets


def validate(
    steps: Sequence[PipelineStep],
    targets: Dict[str, TargetSpec],
    *,
    kinds: Sequence[str],
    family_of: Callable[[str], str],
    cameras: Sequence[str],
    arms: Sequence[str],
) -> None:
    """Validate the authored pipeline against the registered step kinds and the cell's real
    cameras/arms/targets. Every failure is a precise PipelineError — never a silent default."""
    kinds = set(kinds)
    cams = set(cameras)
    arm_set = set(arms)
    ids = {s.id for s in steps}
    for s in steps:
        if s.kind not in kinds:
            raise PipelineError(f"step {s.id!r}: unknown kind {s.kind!r}; known: {', '.join(sorted(kinds))}")
        fam = family_of(s.kind)
        need = _FAMILY_BINDINGS.get(fam)
        if need is None:
            raise PipelineError(f"step {s.id!r}: kind {s.kind!r} has no known family bindings")
        sc = s.all_cameras()
        if len(sc) != need["cameras"]:
            raise PipelineError(f"step {s.id!r} ({s.kind}) binds {len(sc)} camera(s), needs {need['cameras']}")
        if len(s.all_arms()) > need["arms_max"]:
            raise PipelineError(f"step {s.id!r} ({s.kind}) binds too many arms ({len(s.all_arms())} > {need['arms_max']})")
        for c in sc:
            if cams and c not in cams:
                raise PipelineError(f"step {s.id!r}: camera {c!r} is not in the cell ({', '.join(sorted(cams))})")
        for a in s.all_arms():
            if arm_set and a not in arm_set:
                raise PipelineError(f"step {s.id!r}: arm {a!r} is not in the cell ({', '.join(sorted(arm_set))})")
        if s.target and s.target not in targets:
            raise PipelineError(f"step {s.id!r}: unknown target {s.target!r}; defined: {', '.join(sorted(targets)) or '(none)'}")
        if not s.target and targets and len(targets) > 1:
            raise PipelineError(f"step {s.id!r}: must name a target (pipeline defines {len(targets)})")
        for d in s.depends_on:
            if d not in ids:
                raise PipelineError(f"step {s.id!r}: depends_on unknown step {d!r}")
            if d == s.id:
                raise PipelineError(f"step {s.id!r}: depends on itself")
    ordered(steps)  # raises on a cycle


def ordered(steps: Sequence[PipelineStep]) -> List[PipelineStep]:
    """Dependency topological order (stable: authored order breaks ties). Raises on a cycle."""
    by_id = {s.id: s for s in steps}
    state: Dict[str, int] = {}     # 0=unseen, 1=on-stack, 2=done
    out: List[PipelineStep] = []

    def visit(sid: str, trail: List[str]) -> None:
        st = state.get(sid, 0)
        if st == 2:
            return
        if st == 1:
            raise PipelineError(f"dependency cycle: {' -> '.join(trail + [sid])}")
        state[sid] = 1
        for d in by_id[sid].depends_on:
            visit(d, trail + [sid])
        state[sid] = 2
        out.append(by_id[sid])

    for s in steps:           # iterate in authored order for a stable result
        visit(s.id, [])
    return out


def build_targets(target_specs: Dict[str, TargetSpec],
                  build_target: Callable[[dict], Target]) -> Dict[str, Target]:
    """Build the named Targets from their specs (may need cv2 for some detector types). Kept
    separate from `resolve_items` so the URDF resolution stays cv2-free and testable."""
    return {tid: build_target({"type": t.type, "params": t.params})
            for tid, t in target_specs.items()}


def resolve(
    steps: Sequence[PipelineStep],
    target_specs: Dict[str, TargetSpec],
    urdf_path: str,
    build_target: Callable[[dict], Target],
) -> Tuple[List[PlanItem], Dict[str, Target]]:
    """Edge convenience: resolve the URDF kinematics AND build the Targets together."""
    return resolve_items(steps, urdf_path), build_targets(target_specs, build_target)


def resolve_items(steps: Sequence[PipelineStep], urdf_path: str,
                  arm_flanges: Dict[str, str] | None = None) -> List[PlanItem]:
    """Fill each step's URDF kinematics (flange + nominal transforms, derived per bound
    camera link) → engine `PlanItem`s in dependency order. cv2-free (no detector building).
    The KIND is the agent's (authored), never re-guessed; the URDF supplies only the geometry
    for the camera the step bound.

    `arm_flanges` maps a bound arm name → its flange/tip URDF link. It is REQUIRED for the
    eye-to-hand `board_source="arm"` case (a FIXED camera an arm presents a board to): there the
    thing that moves is the ARM, so the kinematic chain must be the presenting arm's — NOT the
    camera's, which traces to the world root and has no movable joint. The camera still supplies
    the X = base->camera seed and the optical-frame write-back; only the CHAIN comes from the arm."""
    arm_flanges = dict(arm_flanges or {})
    items: List[PlanItem] = []
    for s in ordered(steps):
        if family_is_b2b(s):
            ca, cb = s.all_cameras()
            items.append(PlanItem(
                camera_link=s.id, optical_frame="", kind=s.kind, flange_link="",
                nominal_flange_body=np.eye(4), nominal_T=np.eye(4),
                arm=(s.all_arms()[0] if s.all_arms() else ""),
                partner_camera=ca, secondary_camera=cb,
                id=s.id, target_id=s.target, depends_on=list(s.depends_on),
            ))
            continue
        cam = s.all_cameras()[0]
        cam_flange = urdf_io.find_flange_link(urdf_path, cam)              # the CAMERA's mount link
        flange_body = urdf_io.link_chain_transform(urdf_path, cam_flange, cam)
        body_optical = urdf_io.read_nominal_optical(urdf_path, cam)
        # The CHAIN link positions whatever the calibration tracks. A moving camera tracks itself
        # (its own flange). An arm-PRESENTED board (eye-to-hand) rides the arm, so the chain is the
        # presenting arm's flange; the camera nominals above stay camera-based (X seed + write-back).
        chain_flange = cam_flange
        if s.board_source == "arm" and s.arm:
            chain_flange = arm_flanges.get(s.arm, s.arm)
        items.append(PlanItem(
            camera_link=cam, optical_frame=f"{cam}_optical_frame", kind=s.kind,
            flange_link=chain_flange, nominal_flange_body=flange_body,
            nominal_T=flange_body @ body_optical, arm=(s.arm or cam_flange),
            board_source=s.board_source,
            id=s.id, target_id=s.target, depends_on=list(s.depends_on),
        ))
    return items


def family_is_b2b(step: PipelineStep) -> bool:
    """A step binds two cameras ⇒ a shared base-to-base step (used during resolve before the
    step registry is consulted)."""
    return len(step.all_cameras()) == 2
