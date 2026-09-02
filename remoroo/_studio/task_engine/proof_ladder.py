"""Evidence-bound manager for the Newton robotics proof ladder.

The language-model agent may decide *how* to repair a failed gate.  This module
decides only whether the claimed evidence exists, is current, and is sufficient
to advance.  It is deliberately deterministic and backend-specific so a stale
ManiSkill/proxy result cannot accidentally graduate a program to the real cell.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional


SCHEMA_VERSION = 1
BACKENDS = {"newton", "mujoco", "mujoco_warp", "newton_mujoco_warp"}
GATE_ORDER = (
    "scene_ingestion",
    "gravity_contact",
    "curobo_actuation",
    "mass_evaluation",
    "agent_management",
)


class ProofLadderError(RuntimeError):
    pass


_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_sha256(value: Any, field: str) -> str:
    text = str(value)
    if not _SHA256.fullmatch(text):
        raise ProofLadderError(f"{field} must be a lowercase SHA-256 digest")
    return text


def _validate_manifest(manifest: Mapping[str, Any]) -> None:
    _require(manifest, (
        "schema_version", "task", "backend_family", "inputs", "matrix",
        "tunables", "campaign_sha256",
    ), "campaign manifest")
    if int(manifest["schema_version"]) != SCHEMA_VERSION:
        raise ProofLadderError("campaign manifest schema version is unsupported")
    if not str(manifest["task"]).strip():
        raise ProofLadderError("campaign task cannot be empty")
    if manifest["backend_family"] != "newton_mujoco_warp":
        raise ProofLadderError(
            "campaign backend must be Newton/MuJoCo-Warp")

    inputs = manifest["inputs"]
    if not isinstance(inputs, Mapping):
        raise ProofLadderError("campaign inputs must be an object")
    _require(inputs, (
        "cell_yaml_sha256", "urdf_sha256", "world_sha256",
        "fitness_sha256", "perception_sha256", "planner_sha256",
    ), "campaign inputs")
    for field in (
        "cell_yaml_sha256", "urdf_sha256", "fitness_sha256",
        "perception_sha256", "planner_sha256",
    ):
        _require_sha256(inputs[field], f"campaign inputs.{field}")
    worlds = list(inputs["world_sha256"])
    if not worlds:
        raise ProofLadderError("campaign requires at least one world hash")
    for index, value in enumerate(worlds):
        _require_sha256(value, f"campaign inputs.world_sha256[{index}]")
    if len(worlds) != len(set(worlds)):
        raise ProofLadderError("campaign world hashes must be unique")

    matrix = manifest["matrix"]
    if not isinstance(matrix, Mapping):
        raise ProofLadderError("campaign matrix must be an object")
    _require(matrix, ("scenes", "seeds"), "campaign matrix")
    scenes = list(matrix["scenes"])
    seeds = list(matrix["seeds"])
    if not scenes or not seeds:
        raise ProofLadderError("campaign requires scenes and seeds")
    if len(scenes) != len(set(scenes)) or len(seeds) != len(set(seeds)):
        raise ProofLadderError("campaign scene and seed values must be unique")
    if any(isinstance(seed, bool) or not isinstance(seed, int)
           for seed in seeds):
        raise ProofLadderError("campaign seeds must be integers (not bool)")

    tunables = manifest["tunables"]
    if not isinstance(tunables, Mapping):
        raise ProofLadderError("campaign tunables must be an object")
    _require(tunables, ("N", "T", "W"), "campaign tunables")
    if int(tunables["N"]) < 1000:
        raise ProofLadderError("robotics_program.md requires N >= 1000")
    if int(tunables["T"]) != 2 or int(tunables["W"]) != 2:
        raise ProofLadderError("robotics_program.md fixes T=2 and W=2")

    expected = canonical_sha256({
        key: value for key, value in manifest.items()
        if key != "campaign_sha256"
    })
    if str(manifest["campaign_sha256"]) != expected:
        raise ProofLadderError("campaign manifest hash is missing or invalid")


def build_campaign_manifest(*, task: str, cell_yaml_sha256: str,
                            urdf_sha256: str, world_sha256: Iterable[str],
                            fitness_sha256: str, perception_sha256: str,
                            planner_sha256: str, scenes: Iterable[str],
                            seeds: Iterable[int], n: int = 1000,
                            top_t: int = 2, world_attempts: int = 2
                            ) -> Dict[str, Any]:
    worlds = sorted(str(v) for v in world_sha256)
    scene_ids = sorted(str(v) for v in scenes)
    seed_values = sorted({int(v) for v in seeds})
    if not str(task).strip():
        raise ProofLadderError("campaign task cannot be empty")
    if not worlds or not scene_ids or not seed_values:
        raise ProofLadderError("campaign requires worlds, scenes, and seeds")
    if int(n) < 1000:
        raise ProofLadderError("robotics_program.md requires N >= 1000")
    if int(top_t) != 2 or int(world_attempts) != 2:
        raise ProofLadderError("robotics_program.md fixes T=2 and W=2")
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "task": str(task).strip(),
        "backend_family": "newton_mujoco_warp",
        "inputs": {
            "cell_yaml_sha256": str(cell_yaml_sha256),
            "urdf_sha256": str(urdf_sha256),
            "world_sha256": worlds,
            "fitness_sha256": str(fitness_sha256),
            "perception_sha256": str(perception_sha256),
            "planner_sha256": str(planner_sha256),
        },
        "matrix": {"scenes": scene_ids, "seeds": seed_values},
        "tunables": {"N": int(n), "T": int(top_t), "W": int(world_attempts)},
    }
    manifest["campaign_sha256"] = canonical_sha256(manifest)
    _validate_manifest(manifest)
    return manifest


def new_ladder(manifest: Mapping[str, Any]) -> Dict[str, Any]:
    _validate_manifest(manifest)
    expected = str(manifest["campaign_sha256"])
    return {
        "schema_version": SCHEMA_VERSION,
        "campaign_sha256": expected,
        "manifest": dict(manifest),
        "gates": {},
        "tool_events": [],
    }


def _load_evidence(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ProofLadderError(f"cannot read evidence {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ProofLadderError(f"evidence {path} must be a JSON object")
    return value


def _require(value: Mapping[str, Any], keys: Iterable[str], gate: str) -> None:
    missing = [key for key in keys if key not in value]
    if missing:
        raise ProofLadderError(f"{gate} evidence missing {missing}")


def _validate_gate(gate: str, evidence: Mapping[str, Any],
                   manifest: Mapping[str, Any], *,
                   expected_scene_fingerprint: Optional[str] = None,
                   expected_tool_trace_sha256: Optional[str] = None) -> None:
    if evidence.get("passed") is not True:
        raise ProofLadderError(f"{gate} did not pass")
    campaign_hash = str(manifest["campaign_sha256"])
    if gate == "scene_ingestion":
        _require(evidence, (
            "scene_fingerprint", "cell_yaml_sha256", "urdf_sha256",
            "world_sha256", "obstacle_count", "asset_count",
        ), gate)
        inputs = manifest["inputs"]
        if str(evidence["cell_yaml_sha256"]) != str(inputs["cell_yaml_sha256"]):
            raise ProofLadderError("scene evidence is for a different cell.yaml")
        if str(evidence["urdf_sha256"]) != str(inputs["urdf_sha256"]):
            raise ProofLadderError("scene evidence is for a different URDF")
        if str(evidence["world_sha256"]) not in set(inputs["world_sha256"]):
            raise ProofLadderError("scene evidence is for an unbound world")
        if int(evidence["obstacle_count"]) <= 0 or int(evidence["asset_count"]) <= 0:
            raise ProofLadderError("scene must contain measured obstacles and generated assets")
    elif gate == "gravity_contact":
        _require(evidence, (
            "had_support_contact", "downward_m", "finite",
            "robot_contacts_enabled", "isolated_dynamic_assets",
            "scene_fingerprint",
        ), gate)
        if (evidence["had_support_contact"] is not True
                or evidence["finite"] is not True
                or float(evidence["downward_m"]) <= 0.01):
            raise ProofLadderError("gravity/contact evidence is not consequential")
        if evidence["robot_contacts_enabled"] is not False:
            raise ProofLadderError("gravity diagnostic must isolate robot contacts")
        if int(evidence["isolated_dynamic_assets"]) != 1:
            raise ProofLadderError("gravity diagnostic must isolate one dynamic asset")
        if (expected_scene_fingerprint is not None
                and str(evidence["scene_fingerprint"])
                != expected_scene_fingerprint):
            raise ProofLadderError(
                "gravity evidence is for a different assembled scene")
    elif gate == "curobo_actuation":
        _require(evidence, (
            "trajectory_sha256", "scene_fingerprint", "finite",
            "max_joint_motion_rad", "tracking_rms_rad", "tracking_max_rad",
            "state_write_policy", "require_robot_asset_contact",
            "novel_task_contact_assets", "robot_asset_impulse_ns",
            "asset_displacement_m", "articulation_integrity",
            "curobo_mujoco_fk_parity",
        ), gate)
        writes = evidence["state_write_policy"]
        if (writes.get("command_path") != "torque_actuators"
                or int(writes.get("runtime_joint_qpos_writes", -1)) != 0
                or int(writes.get("runtime_object_pose_writes", -1)) != 0):
            raise ProofLadderError("trajectory was not executed solely through actuators")
        if evidence["finite"] is not True or float(
                evidence["max_joint_motion_rad"]) <= 1e-3:
            raise ProofLadderError("trajectory produced no finite robot motion")
        articulation = evidence["articulation_integrity"]
        if not isinstance(articulation, Mapping):
            raise ProofLadderError(
                "actuation articulation_integrity must be an object")
        _require(articulation, (
            "schema", "passed", "urdf_mimic_joint_count",
            "compiled_mimic_equality_count",
            "reset_mimic_qpos_writes", "runtime_mimic_qpos_writes",
            "mimic_error_tolerance_rad",
            "maximum_abs_mimic_error_rad",
            "articulation_contract_sha256",
        ), "curobo_actuation articulation")
        if (
            articulation["schema"]
            != "remoroo.urdf-mujoco-articulation-integrity/v1"
            or articulation["passed"] is not True
            or int(articulation["compiled_mimic_equality_count"])
            != int(articulation["urdf_mimic_joint_count"])
            or int(articulation["reset_mimic_qpos_writes"])
            != int(articulation["urdf_mimic_joint_count"])
            or int(articulation["runtime_mimic_qpos_writes"]) != 0
            or float(articulation["maximum_abs_mimic_error_rad"])
            > float(articulation["mimic_error_tolerance_rad"])
        ):
            raise ProofLadderError(
                "URDF mimic joints were not preserved as bounded physics "
                "constraints")
        _require_sha256(
            articulation["articulation_contract_sha256"],
            "curobo_actuation articulation contract",
        )
        fk_parity = evidence["curobo_mujoco_fk_parity"]
        if not isinstance(fk_parity, Mapping):
            raise ProofLadderError(
                "actuation cuRobo/MuJoCo FK parity must be an object")
        _require(fk_parity, (
            "schema", "passed", "sample_count",
            "position_tolerance_m", "orientation_tolerance_rad",
            "max_position_error_m", "max_orientation_error_rad",
            "runtime_qpos_writes",
        ), "curobo_actuation FK parity")
        if (
            fk_parity["schema"]
            != "remoroo.curobo-mujoco-fk-parity/v1"
            or fk_parity["passed"] is not True
            or int(fk_parity["sample_count"]) <= 0
            or int(fk_parity["runtime_qpos_writes"]) != 0
            or float(fk_parity["max_position_error_m"])
            > float(fk_parity["position_tolerance_m"])
            or float(fk_parity["max_orientation_error_rad"])
            > float(fk_parity["orientation_tolerance_rad"])
        ):
            raise ProofLadderError(
                "MuJoCo articulation does not match cuRobo waypoint FK")
        if (float(evidence["tracking_rms_rad"]) > 0.15
                or float(evidence["tracking_max_rad"]) > 0.5):
            raise ProofLadderError("torque execution exceeded the tracking envelope")
        novel = list(evidence["novel_task_contact_assets"])
        displacement = dict(evidence["asset_displacement_m"])
        if (evidence["require_robot_asset_contact"] is not True
                or not novel
                or float(evidence["robot_asset_impulse_ns"]) <= 0.0
                or not any(float(displacement.get(name, 0.0)) > 0.01
                           for name in novel)):
            raise ProofLadderError(
                "blind cuRobo path did not produce consequential novel asset contact")
        if (expected_scene_fingerprint is not None
                and str(evidence["scene_fingerprint"])
                != expected_scene_fingerprint):
            raise ProofLadderError(
                "actuation evidence is for a different assembled scene")
    elif gate == "mass_evaluation":
        _require(evidence, (
            "campaign_sha256", "episodes", "matrix_complete",
            "num_envs", "worst_case_ranking", "winners",
            "all_scene_seed_success_rate", "promotion_guard",
        ), gate)
        if str(evidence["campaign_sha256"]) != campaign_hash:
            raise ProofLadderError("mass evidence is stale or cross-campaign")
        if int(evidence["episodes"]) < int(manifest["tunables"]["N"]):
            raise ProofLadderError("mass evidence is below the N=1000 floor")
        if evidence["matrix_complete"] is not True:
            raise ProofLadderError("scene×seed matrix is incomplete")
        if int(evidence["num_envs"]) < 32:
            raise ProofLadderError("mass proof requires at least 32 concurrent worlds")
        if float(evidence["all_scene_seed_success_rate"]) < 1.0:
            raise ProofLadderError(
                "robotics_program.md requires 100% across all scenes and seeds")
        ranking = list(evidence["worst_case_ranking"])
        winners = list(evidence["winners"])
        if len(winners) < int(manifest["tunables"]["T"]):
            raise ProofLadderError("mass evidence does not bind the top T winners")
        promotion = evidence["promotion_guard"]
        if not isinstance(promotion, Mapping):
            raise ProofLadderError("mass promotion_guard must be an object")
        _require(promotion, (
            "schema", "passed", "required_top_t", "control_ref",
            "control_valid", "control_positive_rate",
            "selected_program_sha256",
        ), "mass promotion_guard")
        if (
            promotion["schema"] != "remoroo.campaign-promotion-guard/v1"
            or promotion["passed"] is not True
            or int(promotion["required_top_t"])
            != int(manifest["tunables"]["T"])
            or promotion["control_ref"] != "no_op_hold"
            or promotion["control_valid"] is not True
            or float(promotion["control_positive_rate"]) != 0.0
        ):
            raise ProofLadderError(
                "mass no-op control or top-T promotion guard did not pass")
        hashes = [str(v.get("program_sha256") or "") for v in winners]
        if any(not value for value in hashes) or len(hashes) != len(set(hashes)):
            raise ProofLadderError("winner program hashes are missing or duplicated")
        if any(v.get("typed_program_output") is not True for v in winners):
            raise ProofLadderError(
                "top T winners must be typed perception/planning program outputs")
        promoted_hashes = [
            str(value) for value in promotion["selected_program_sha256"]]
        if promoted_hashes != hashes[:int(manifest["tunables"]["T"])]:
            raise ProofLadderError(
                "promotion guard is not bound to the recorded top T winners")
        ranked_hashes = [str(v.get("program_sha256") or "") for v in ranking]
        robustness = [float(v.get("robustness")) for v in ranking]
        if (len(ranking) < len(winners)
                or ranked_hashes[:len(winners)] != hashes
                or robustness != sorted(robustness, reverse=True)
                or any(value <= 0.0 for value in robustness[:len(winners)])):
            raise ProofLadderError(
                "winners must be the positive top T by worst-case robustness")
    elif gate == "agent_management":
        _require(evidence, (
            "campaign_sha256", "tool_trace_sha256", "results_tsv_sha256",
            "next_action_deterministic", "failure_policy_enforced",
        ), gate)
        if str(evidence["campaign_sha256"]) != campaign_hash:
            raise ProofLadderError("agent evidence is stale or cross-campaign")
        _require_sha256(evidence["tool_trace_sha256"], "agent tool trace")
        _require_sha256(evidence["results_tsv_sha256"], "agent results.tsv")
        if (expected_tool_trace_sha256 is not None
                and str(evidence["tool_trace_sha256"])
                != expected_tool_trace_sha256):
            raise ProofLadderError(
                "agent evidence is not bound to the recorded tool trace")
        if (evidence["next_action_deterministic"] is not True
                or evidence["failure_policy_enforced"] is not True):
            raise ProofLadderError("agent did not enforce the robotics program")
    else:
        raise ProofLadderError(f"unknown proof gate {gate!r}")


def record_tool_event(ladder: Dict[str, Any], *, tool: str,
                      request: Mapping[str, Any], outcome: str,
                      evidence_path: Optional[str] = None) -> None:
    event = {
        "sequence": len(ladder["tool_events"]),
        "tool": str(tool),
        "request_sha256": canonical_sha256(dict(request)),
        "outcome": str(outcome),
    }
    if evidence_path:
        path = Path(evidence_path).resolve()
        if not path.is_file():
            raise ProofLadderError(f"tool evidence does not exist: {path}")
        event.update(
            evidence_path=str(path), evidence_sha256=file_sha256(path))
    ladder["tool_events"].append(event)


def record_gate(ladder: Dict[str, Any], *, gate: str, backend: str,
                evidence_path: str) -> None:
    if gate not in GATE_ORDER:
        raise ProofLadderError(f"unknown proof gate {gate!r}")
    if backend not in BACKENDS:
        raise ProofLadderError(
            f"backend {backend!r} is not Newton/MuJoCo-Warp evidence")
    expected = next_action(ladder)["gate"]
    if gate != expected:
        raise ProofLadderError(
            f"gate {gate!r} is out of order; next gate is {expected!r}")
    path = Path(evidence_path).resolve()
    if not path.is_file():
        raise ProofLadderError(f"gate evidence does not exist: {path}")
    evidence = _load_evidence(path)
    expected_scene = None
    if gate in ("gravity_contact", "curobo_actuation"):
        scene_record = ladder["gates"].get("scene_ingestion")
        if scene_record:
            scene_path = Path(str(scene_record["evidence_path"]))
            if (scene_path.is_file()
                    and file_sha256(scene_path)
                    == str(scene_record["evidence_sha256"])):
                expected_scene = str(
                    _load_evidence(scene_path)["scene_fingerprint"])
    expected_tool_trace = (
        canonical_sha256(list(ladder.get("tool_events") or []))
        if gate == "agent_management" else None
    )
    _validate_gate(
        gate, evidence, ladder["manifest"],
        expected_scene_fingerprint=expected_scene,
        expected_tool_trace_sha256=expected_tool_trace,
    )
    ladder["gates"][gate] = {
        "backend": backend,
        "evidence_path": str(path),
        "evidence_sha256": file_sha256(path),
        "tool_event_sequence": len(ladder["tool_events"]),
    }
    record_tool_event(
        ladder, tool=f"proof:{gate}",
        request={"backend": backend, "campaign": ladder["campaign_sha256"]},
        outcome="passed", evidence_path=str(path))


def verify_ladder(ladder: Mapping[str, Any]) -> Dict[str, Any]:
    failures = []
    try:
        _validate_manifest(ladder["manifest"])
        if str(ladder.get("campaign_sha256")) != str(
                ladder["manifest"]["campaign_sha256"]):
            raise ProofLadderError(
                "ladder campaign hash does not match its manifest")
    except (KeyError, TypeError, ProofLadderError) as exc:
        failures.append(f"manifest: {exc}")

    tool_events = list(ladder.get("tool_events") or [])
    for index, event in enumerate(tool_events):
        if not isinstance(event, Mapping) or event.get("sequence") != index:
            failures.append(f"tool_events: invalid sequence at index {index}")
            continue
        evidence_path = event.get("evidence_path")
        if evidence_path:
            path = Path(str(evidence_path))
            if not path.is_file():
                failures.append(f"tool_events: evidence deleted at index {index}")
            elif file_sha256(path) != str(event.get("evidence_sha256")):
                failures.append(f"tool_events: evidence changed at index {index}")

    expected_scene = None
    for gate in GATE_ORDER:
        recorded = (ladder.get("gates") or {}).get(gate)
        if not recorded:
            failures.append(f"{gate}: missing")
            continue
        path = Path(str(recorded["evidence_path"]))
        if not path.is_file():
            failures.append(f"{gate}: evidence deleted")
            continue
        if file_sha256(path) != str(recorded["evidence_sha256"]):
            failures.append(f"{gate}: evidence changed after recording")
            continue
        if recorded.get("backend") not in BACKENDS:
            failures.append(f"{gate}: backend is not Newton/MuJoCo-Warp")
            continue
        try:
            evidence = _load_evidence(path)
            sequence = recorded.get("tool_event_sequence")
            expected_tool_trace = None
            if gate == "agent_management":
                if (isinstance(sequence, bool)
                        or not isinstance(sequence, int)
                        or sequence < 0 or sequence > len(tool_events)):
                    raise ProofLadderError(
                        "agent gate has no valid tool-trace boundary")
                expected_tool_trace = canonical_sha256(
                    tool_events[:sequence])
            _validate_gate(
                gate, evidence, ladder["manifest"],
                expected_scene_fingerprint=expected_scene,
                expected_tool_trace_sha256=expected_tool_trace,
            )
            if gate == "scene_ingestion":
                expected_scene = str(evidence["scene_fingerprint"])
        except ProofLadderError as exc:
            failures.append(f"{gate}: {exc}")
    return {
        "valid": not failures,
        "promotion_ready": not failures,
        "failures": failures,
        "completed": [
            gate for gate in GATE_ORDER
            if gate in (ladder.get("gates") or {})],
    }


def next_action(ladder: Mapping[str, Any]) -> Dict[str, Any]:
    verification = verify_ladder(ladder)
    recorded = set((ladder.get("gates") or {}).keys())
    invalid_recorded = [
        failure for failure in verification["failures"]
        if failure.split(":", 1)[0] in recorded
    ]
    if invalid_recorded:
        first = invalid_recorded[0].split(":", 1)[0]
        return {
            "finished": False,
            "gate": first,
            "action": f"repair_and_rerun:{first}",
            "reason": invalid_recorded[0],
        }
    for gate in GATE_ORDER:
        if gate not in (ladder.get("gates") or {}):
            return {
                "finished": False,
                "gate": gate,
                "action": f"run_and_record:{gate}",
            }
    if not verification["valid"]:
        first = verification["failures"][0].split(":", 1)[0]
        return {
            "finished": False,
            "gate": first,
            "action": f"repair_and_rerun:{first}",
            "reason": verification["failures"][0],
        }
    return {
        "finished": True,
        "gate": None,
        "action": "promote_top_t_to_camera_certified_real_trials",
    }


def report(ladder: Mapping[str, Any]) -> Dict[str, Any]:
    verification = verify_ladder(ladder)
    action = next_action(ladder)
    return {
        "schema_version": SCHEMA_VERSION,
        "campaign_sha256": ladder["campaign_sha256"],
        "backend_family": ladder["manifest"]["backend_family"],
        "completed_gates": verification["completed"],
        "promotion_ready": verification["promotion_ready"],
        "failures": verification["failures"],
        "next_action": action,
        "tool_events": len(ladder.get("tool_events") or []),
        "tool_trace_sha256": canonical_sha256(
            list(ladder.get("tool_events") or [])),
    }


def save_ladder(ladder: Mapping[str, Any], path: str) -> None:
    Path(path).write_text(
        json.dumps(ladder, indent=2, sort_keys=True), encoding="utf-8")
