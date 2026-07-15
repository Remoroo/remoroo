"""VLA embodiment profile — the ONE wiring diagram between this cell and a VLA.

LingBot (like every cross-embodiment VLA) needs a per-robot config: which camera is
"camera_top", which state indices are which joints, which action dims mean what. That
wiring cannot ship with the product — it IS the customer's cell — so the AGENT authors
it, once, as `remoroo_cell/vla_profile.yaml`, from what it already knows (cell.yaml
groups, camera mounts, the Bridge's gripper surface).

Everything else derives from that single file, so the two sides of the websocket can
never disagree about what dim 12 means:
  * the observation PACKER (packer.py) builds {"image", "state", "prompt"} from it;
  * the action DECODER (runtime.py) unpacks returned chunks with it;
  * the GENERATORS here emit the yaml files LingBot's OWN code reads (robot config +
    the cli config the policy server loads) — the agent never hand-writes LingBot's
    format. Same doctrine as bake_calibration: the agent decides, code translates.

Profile shape (authored):

    robo_name: hitbot_dual_01
    cameras:                    # canonical slot <- cell camera name (omit absent slots)
      camera_top: overhead
      camera_wrist_left: wrist_a
    state:                      # ORDER defines the state vector, group by group
      - {group: arm.position,      chain: arm_a, source: joints,  dims: 6}
      - {group: arm.position,      chain: arm_b, source: joints,  dims: 6}
      - {group: end.position,      chain: arm_a, source: ee_pose, dims: 7}
      - {group: end.position,      chain: arm_b, source: ee_pose, dims: 7}
      - {group: effector.position, chain: arm_a, source: gripper, dims: 1}
      - {group: effector.position, chain: arm_b, source: gripper, dims: 1}
    actions:                    # mirrors state; subtract_state=true -> deltas in training
      - {group: end.position,      chain: arm_a, dims: 7, subtract_state: false}
      - {group: end.position,      chain: arm_b, dims: 7, subtract_state: false}
      - {group: effector.position, chain: arm_a, dims: 1, subtract_state: false}
      - {group: effector.position, chain: arm_b, dims: 1, subtract_state: false}
    gripper_close_width: 0.012  # decode: effector value < close_threshold -> this width
    close_threshold: 0.5

Live decode consumes `end.position` (EE pose -> delta_xyz) and `effector.position`
(-> gripper) groups; `arm.position` rows inform training only (our executor's dialect
is EE-space — the GuardedExecutor clamps deltas, never raw joints).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

# The unified feature space's group names (LingBot's canonical vocabulary).
KNOWN_GROUPS = {"arm.position", "end.position", "effector.position", "hand.position",
                "waist.position", "head.position", "base.position"}
KNOWN_CAMERA_SLOTS = {"camera_top", "camera_wrist_left", "camera_wrist_right"}
KNOWN_STATE_SOURCES = {"joints", "ee_pose", "gripper"}


@dataclass
class Slice:
    group: str
    chain: str
    source: str          # state only: joints | ee_pose | gripper
    dims: int
    start: int           # computed offset into the flat vector
    end: int
    subtract_state: bool = False   # actions only


@dataclass
class VlaProfile:
    robo_name: str
    cameras: Dict[str, str]                  # canonical slot -> cell camera
    state: List[Slice] = field(default_factory=list)
    actions: List[Slice] = field(default_factory=list)
    gripper_close_width: float = 0.012
    close_threshold: float = 0.5

    @property
    def state_dim(self) -> int:
        return self.state[-1].end if self.state else 0

    @property
    def action_dim(self) -> int:
        return self.actions[-1].end if self.actions else 0

    def action_slices(self, group: str) -> List[Slice]:
        return [s for s in self.actions if s.group == group]


def _layout(entries: List[dict], *, actions: bool) -> List[Slice]:
    out: List[Slice] = []
    off = 0
    for e in entries:
        dims = int(e["dims"])
        if dims <= 0:
            raise ValueError(f"profile entry {e!r}: dims must be positive")
        group = str(e["group"])
        if group not in KNOWN_GROUPS:
            raise ValueError(f"profile entry {e!r}: unknown group {group!r} "
                             f"(known: {sorted(KNOWN_GROUPS)})")
        src = str(e.get("source", "joints"))
        if not actions and src not in KNOWN_STATE_SOURCES:
            raise ValueError(f"profile entry {e!r}: unknown source {src!r} "
                             f"(known: {sorted(KNOWN_STATE_SOURCES)})")
        out.append(Slice(group=group, chain=str(e.get("chain", "")), source=src,
                         dims=dims, start=off, end=off + dims,
                         subtract_state=bool(e.get("subtract_state", False))))
        off += dims
    return out


def parse_profile(raw: Dict[str, Any]) -> VlaProfile:
    if not raw.get("robo_name"):
        raise ValueError("vla_profile needs a robo_name (the cell's identity in configs)")
    cams = dict(raw.get("cameras") or {})
    bad = set(cams) - KNOWN_CAMERA_SLOTS
    if bad:
        raise ValueError(f"unknown camera slots {sorted(bad)} "
                         f"(known: {sorted(KNOWN_CAMERA_SLOTS)})")
    if not cams:
        raise ValueError("vla_profile needs at least one camera mapping (a VLA sees)")
    state = _layout(list(raw.get("state") or []), actions=False)
    actions = _layout(list(raw.get("actions") or []), actions=True)
    if not state or not actions:
        raise ValueError("vla_profile needs non-empty state and actions layouts")
    return VlaProfile(robo_name=str(raw["robo_name"]), cameras=cams,
                      state=state, actions=actions,
                      gripper_close_width=float(raw.get("gripper_close_width", 0.012)),
                      close_threshold=float(raw.get("close_threshold", 0.5)))


def load_profile(path: str) -> Optional[VlaProfile]:
    """None when the cell has no authored profile (that's a state, not an error)."""
    p = Path(path)
    if not p.exists():
        return None
    import yaml

    return parse_profile(yaml.safe_load(p.read_text(encoding="utf-8")) or {})


# --- generators: OUR profile -> LingBot's OWN config formats ------------------------------
# Structure mirrored from the repo's shipped examples (configs/robot_configs/robotwin.yaml,
# configs/vla/real_robot/real_robot.yaml). The FORM is CI-locked here; exact server-side
# acceptance is rig-validated, like the wire schema.

def to_lingbot_robot_config(profile: VlaProfile, *, norm_stats_path: str) -> Dict[str, Any]:
    """Mirrors the vendor's OWN robotwin.yaml shape (rig-verified 2026-07-14):
    `states`/`actions` are LISTS of single-key mappings — their loader iterates list
    items, not dict keys. Our earlier dict emission parsed fine as yaml and then
    crashed three layers into FeatureTransform."""
    def slices_of(entries: List[Slice], key: str, *, with_subtract: bool) -> List[Dict[str, Any]]:
        grouped: List[Dict[str, Any]] = []
        for group in dict.fromkeys(e.group for e in entries):     # keep declaration order
            origin = [{key: {"start": s.start, "end": s.end}}
                      for s in entries if s.group == group]
            item: Dict[str, Any] = {"origin_keys": origin}
            if with_subtract:
                item["subtract_state"] = any(s.subtract_state for s in entries
                                             if s.group == group)
            grouped.append(
                {f"{'action' if with_subtract else 'observation.state'}.{group}": item})
        return grouped

    # images: STRING entries (their check accepts str|dict; robotwin.yaml uses the
    # dict form to RENAME robot streams to canonical slots — our packer already emits
    # canonical slot names, so target == org and the plain string is the whole truth).
    # Read from the vendor source 2026-07-15, not inferred.
    return {
        "states": slices_of(profile.state, "observation.state", with_subtract=False),
        "actions": slices_of(profile.actions, "action", with_subtract=True),
        "images": [f"observation.images.{slot}" for slot in profile.cameras],
        "norm_stats": norm_stats_path,
    }


# NOTE (rig-decoded 2026-07-14, second pass): the data section's `joints` list is
# the CHECKPOINT'S trained feature-space registry (max padding dims per canonical
# group: arm.position 14, end.position 14, ...), NOT the robot's dims — it belongs
# to the vendor base config and must never be overridden per-robot (our dims would
# under-pad the features the checkpoint expects). The base file ships those entries
# UNQUOTED (broken for their own ast.literal_eval); the integration fix is quoting
# them IN THE BASE (seed vla_serving rung 11), not emitting our own.


def to_lingbot_cli_config(profile: VlaProfile, *, checkpoint: str, qwen_path: str,
                          robot_config_root: str) -> Dict[str, Any]:
    """The training/CLI yaml the policy server reads from the checkpoint's parent
    (lingbotvla_cli.yaml). Minimal: dims + paths; training hyperparams belong to the
    finetune job, not here."""
    return {
        "model": {
            "model_path": checkpoint,
            "tokenizer_path": qwen_path,
            "config_key": "LingbotVLAV2Config",
            "action_dim": profile.action_dim,
            "max_action_dim": max(profile.action_dim, 55),
            "max_state_dim": max(profile.state_dim, 55),
        },
        "data": {
            "datasets_type": "vla",
            "data_name": profile.robo_name,
            "robot_config_root": robot_config_root,
            # per-robot camera SLOT selection (update_info reads it); `joints` is
            # deliberately NOT emitted — see the registry note above. norm_type
            # entries are literal_eval'd strings like joints (get_normalizer);
            # every group this profile uses must carry one.
            "cameras": list(profile.cameras.keys()),
            "norm_type": [str({g: "meanstd"}) for g in dict.fromkeys(
                [s.group for s in profile.state] + [s.group for s in profile.actions])],
        },
    }


def write_lingbot_configs(profile: VlaProfile, *, workdir: str, checkpoint: str,
                          qwen_path: str, norm_stats_path: str,
                          config_path: Optional[str] = None) -> Dict[str, Any]:
    """Materialize both files where LingBot's code looks for them. Returns the written
    paths AND the config dicts themselves — the finetune job ships the dicts to the
    trainer box (which regenerates nothing; the profile decided once, here)."""
    import yaml

    robot_dir = Path(workdir) / "configs" / "robot_configs"
    robot_dir.mkdir(parents=True, exist_ok=True)
    robot = to_lingbot_robot_config(profile, norm_stats_path=norm_stats_path)
    robot_cfg = robot_dir / f"{profile.robo_name}.yaml"
    robot_cfg.write_text(yaml.safe_dump(robot, sort_keys=False), encoding="utf-8")

    cli = to_lingbot_cli_config(profile, checkpoint=checkpoint, qwen_path=qwen_path,
                                robot_config_root=str(robot_dir))
    # DEFAULT beside the checkpoint is a GUESS; the commissioning gate probes the
    # rig's REAL path (the Hitbot's server read it three dirs above) and callers
    # pass it through config_path.
    cli_cfg = (Path(config_path) if config_path
               else Path(checkpoint).resolve().parent / "lingbotvla_cli.yaml")
    cli_cfg.write_text(yaml.safe_dump(cli, sort_keys=False), encoding="utf-8")
    return {"robot_config_path": str(robot_cfg), "cli_config_path": str(cli_cfg),
            "robot_config": robot, "cli_config": cli}
