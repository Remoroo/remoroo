"""Build a fail-closed manager audit from the local Newton proof artifacts.

This is evidence plumbing, not a simulator or a success generator.  It records
the three currently valid physical gates and presents the exact typed-program
evolution report to the same deterministic ladder used for graduation.  A red,
stale, or structurally incomplete campaign is recorded as a rejected tool
attempt and is never converted into mass-evaluation success.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

from task_engine.proof_ladder import (
    ProofLadderError,
    build_campaign_manifest,
    canonical_sha256,
    file_sha256,
    new_ladder,
    next_action,
    record_gate,
    record_tool_event,
    report,
    verify_ladder,
)


AUDIT_SCHEMA = "remoroo.newton-manager-audit/v1"
SCALE_SCHEMA = "remoroo.mujoco-warp-scaling-benchmark/v1"
EVOLUTION_SCHEMA = "remoroo.mujoco-warp-evolution-campaign-result/v2"


class ProofAuditError(ProofLadderError):
    """The local proof bundle is absent, inconsistent, or cross-bound."""


def _load_json(path: Path, field: str) -> Dict[str, Any]:
    if not path.is_file():
        raise ProofAuditError(f"{field} artifact does not exist: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ProofAuditError(f"cannot read {field} artifact {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ProofAuditError(f"{field} artifact must be a JSON object")
    return value


def _nested(value: Mapping[str, Any], *keys: str) -> Any:
    current: Any = value
    for key in keys:
        if not isinstance(current, Mapping) or key not in current:
            raise ProofAuditError(
                f"artifact is missing {'.'.join(keys)}")
        current = current[key]
    return current


def _require_equal(actual: Any, expected: Any, message: str) -> None:
    if actual != expected:
        raise ProofAuditError(
            f"{message}: expected {expected!r}, got {actual!r}")


def _mass_rejection(
    mass: Mapping[str, Any],
    *,
    campaign_sha256: str,
) -> Dict[str, Any]:
    """Return explicit reasons the submitted mass evidence cannot graduate."""
    if mass.get("schema") == EVOLUTION_SCHEMA:
        manifest = (
            mass.get("manifest")
            if isinstance(mass.get("manifest"), Mapping) else {}
        )
        candidates = list(manifest.get("candidates") or [])
        worlds = list(manifest.get("worlds") or [])
        seeds = list(manifest.get("seeds") or [])
        repeats = list(mass.get("campaign_repeats") or [])
        promotion = (
            mass.get("promotion_guard")
            if isinstance(mass.get("promotion_guard"), Mapping) else {}
        )
        unique_units = len(candidates) * len(worlds) * len(seeds)
        expected = unique_units * len(repeats)
        expected_keys = {
            (
                str(candidate.get("candidate_ref") or ""),
                str(world.get("world_id") or ""),
                int(seed),
            )
            for candidate in candidates if isinstance(candidate, Mapping)
            for world in worlds if isinstance(world, Mapping)
            for seed in seeds
        }
        exact_repeat_matrix = bool(
            unique_units
            and len(expected_keys) == unique_units
            and len(repeats) == 2
        )
        if exact_repeat_matrix:
            for repeat in repeats:
                if not isinstance(repeat, Mapping):
                    exact_repeat_matrix = False
                    break
                actual_keys = []
                for trial in repeat.get("trials") or []:
                    if not isinstance(trial, Mapping):
                        exact_repeat_matrix = False
                        break
                    unit = trial.get("unit")
                    if not isinstance(unit, Mapping):
                        exact_repeat_matrix = False
                        break
                    try:
                        actual_keys.append((
                            str(unit.get("candidate_ref") or ""),
                            str(unit.get("world_id") or ""),
                            int(unit.get("seed")),
                        ))
                    except (TypeError, ValueError):
                        exact_repeat_matrix = False
                        break
                if (
                    not exact_repeat_matrix
                    or len(actual_keys) != unique_units
                    or len(set(actual_keys)) != unique_units
                    or set(actual_keys) != expected_keys
                ):
                    exact_repeat_matrix = False
                    break
        reported_expected = int(mass.get("episodes_expected") or 0)
        completed = int(mass.get("episodes_completed") or 0)
        failed = int(mass.get("episodes_failed") or 0)
        batch_metrics = list(mass.get("batch_metrics") or [])
        articulation_batches = []
        reasons = []
        if mass.get("passed") is not True:
            reasons.append("campaign_report_passed_is_not_true")
        if mass.get("campaign_complete") is not True:
            reasons.append("campaign_matrix_incomplete")
        replay_contract = (
            mass.get("gpu_replay_contract")
            if isinstance(mass.get("gpu_replay_contract"), Mapping) else {}
        )
        if (
            mass.get("gpu_replay_contract_passed") is not True
            or replay_contract.get("passed") is not True
        ):
            reasons.append("gpu_replay_provenance_contract_failed")
        if mass.get("evaluation_stable") is not True:
            reasons.append("outcome_sign_or_ranking_stability_failed")
        if mass.get("promotion_ready") is not True:
            reasons.append("typed_top_t_promotion_failed")
        if promotion.get("control_valid") is not True:
            reasons.append("no_op_control_false_positive")
        reasons.extend(
            f"promotion:{reason}"
            for reason in promotion.get("reasons") or []
        )
        if len(repeats) != 2:
            reasons.append("campaign_requires_two_independent_repeats")
        if not exact_repeat_matrix:
            reasons.append("campaign_repeat_matrix_is_not_exact")
        if unique_units < 1000:
            reasons.append("campaign_has_fewer_than_1000_unique_units")
        if (
            reported_expected != expected
            or completed != expected
            or failed != 0
        ):
            reasons.append("campaign_episode_accounting_incomplete")
        if not candidates:
            reasons.append("campaign_has_no_candidate_bindings")
        if not any(
            isinstance(candidate, Mapping)
            and candidate.get("candidate_ref") == "no_op_hold"
            for candidate in candidates
        ):
            reasons.append("campaign_has_no_no_op_control")
        if len(batch_metrics) != len(repeats):
            reasons.append("campaign_batch_metrics_do_not_match_repeats")
        for index, metrics in enumerate(batch_metrics):
            if not isinstance(metrics, Mapping):
                reasons.append(
                    f"repeat_{index}:articulation_metrics_missing")
                continue
            try:
                mimic_count = int(metrics.get("mimic_joint_count", 0))
                maximum_error = float(
                    metrics.get("maximum_abs_mimic_error_rad", float("inf")))
                tolerance = float(
                    metrics.get("mimic_error_tolerance_rad", -1.0))
                runtime_joint_writes = int(
                    metrics.get("runtime_joint_qpos_writes", -1))
                runtime_object_writes = int(
                    metrics.get("runtime_object_pose_writes", -1))
            except (TypeError, ValueError):
                reasons.append(
                    f"repeat_{index}:articulation_metrics_malformed")
                continue
            articulation_batches.append({
                "repeat": index,
                "passed": metrics.get(
                    "articulation_integrity_passed") is True,
                "mimic_joint_count": mimic_count,
                "maximum_abs_mimic_error_rad": maximum_error,
                "mimic_error_tolerance_rad": tolerance,
                "runtime_joint_qpos_writes": runtime_joint_writes,
                "runtime_object_pose_writes": runtime_object_writes,
            })
            if (
                metrics.get("articulation_integrity_passed") is not True
                or mimic_count <= 0
                or tolerance < 0.0
                or maximum_error > tolerance
            ):
                reasons.append(
                    f"repeat_{index}:urdf_mimic_integrity_failed")
            if runtime_joint_writes != 0 or runtime_object_writes != 0:
                reasons.append(
                    f"repeat_{index}:runtime_state_writes_detected")
        calculated_manifest_sha256 = canonical_sha256(manifest)
        if mass.get("manifest_sha256") != calculated_manifest_sha256:
            reasons.append("campaign_manifest_hash_invalid")
        if not reasons:
            reasons.append("campaign_is_not_normalized_mass_gate_evidence")

        return {
            "accepted": False,
            "schema": EVOLUTION_SCHEMA,
            "reasons": list(dict.fromkeys(reasons)),
            "manager_campaign_sha256": campaign_sha256,
            "report_manifest_sha256": mass.get("manifest_sha256"),
            "reported_passed": mass.get("passed"),
            "campaign_complete": mass.get("campaign_complete"),
            "strict_trace_deterministic": mass.get(
                "strict_trace_deterministic"),
            "gpu_replay_contract": dict(replay_contract),
            "evaluation_stable": mass.get("evaluation_stable"),
            "promotion_ready": mass.get("promotion_ready"),
            "promotion_guard": dict(promotion),
            "unique_units": unique_units,
            "repeats": len(repeats),
            "exact_repeat_matrix": exact_repeat_matrix,
            "episodes_expected": reported_expected,
            "episodes_completed": completed,
            "episodes_failed": failed,
            "articulation_batches": articulation_batches,
            "batch_metrics": batch_metrics,
        }

    scale = mass
    reasons = []
    if scale.get("schema") != SCALE_SCHEMA:
        reasons.append("unsupported_scale_schema")
    if scale.get("success") is not True:
        reasons.append("scale_report_success_is_not_true")
    if scale.get("campaign_sha256") != campaign_sha256:
        reasons.append("scale_report_not_bound_to_exact_campaign")

    results = scale.get("results")
    rows = list(results) if isinstance(results, list) else []
    if not rows:
        reasons.append("scale_report_has_no_results")
    if not any(
            isinstance(row, Mapping) and int(row.get("nworld", 0)) >= 1000
            for row in rows):
        reasons.append("scale_report_has_no_N1000_run")
    if any(
            isinstance(row, Mapping)
            and (row.get("ok") is not True
                 or row.get("deterministic") is not True)
            for row in rows):
        reasons.append("strict_repeat_determinism_failed")

    candidates = scale.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        reasons.append("scale_report_has_no_candidate_bindings")
    if not reasons:
        # A throughput benchmark is not the normalized mass-evaluation result:
        # it still lacks complete-matrix ranking, top-T hashes, and 100% success.
        reasons.append("scale_benchmark_is_not_mass_evaluation_evidence")

    summaries = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        summaries.append({
            "nworld": row.get("nworld"),
            "ok": row.get("ok"),
            "episodes_expected": row.get("episodes_expected"),
            "episodes_completed": row.get("episodes_completed"),
            "episodes_failed": row.get("episodes_failed"),
            "median_episodes_per_second": row.get(
                "median_episodes_per_second"),
            "deterministic": row.get("deterministic"),
            "bitwise_deterministic": row.get("bitwise_deterministic"),
            "numeric_deterministic": row.get("numeric_deterministic"),
            "max_abs_trace_delta": row.get("max_abs_trace_delta"),
            "worst_repeat_delta": row.get("worst_repeat_delta"),
        })
    return {
        "accepted": False,
        "schema": scale.get("schema"),
        "reasons": reasons,
        "manager_campaign_sha256": campaign_sha256,
        "reported_success": scale.get("success"),
        "requested_scales": scale.get("requested_scales"),
        "repeats": scale.get("repeats"),
        "results": summaries,
    }


def build_manager_audit(
    evidence_dir: str | Path,
    *,
    source_root: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """Build the actual local Newton audit and stop on the first red gate."""
    root = (
        Path(source_root).expanduser().resolve()
        if source_root is not None
        else Path(__file__).resolve().parents[3]
    )
    evidence_root = Path(evidence_dir).expanduser().resolve()

    paths = {
        "scene_ingestion": evidence_root / "scene_ingestion.json",
        "gravity_contact": evidence_root / "gravity_contact.json",
        "curobo_actuation": (
            evidence_root / "evolved_policy_contact_gate_v8.json"),
        "mass_attempt": (
            evidence_root
            / "mujoco_warp_evolution_campaign_1002_typed_policy_v015.json"),
        "perception_observation": evidence_root / "sweep_oracle_perception.json",
        "authored_plan": evidence_root / "sweep_curobo_plan_result.json",
        "contact_policy": evidence_root / "evolved_sweep_policy_v3.json",
        "contact_request": (
            evidence_root / "evolved_sweep_curobo_request_v3.json"),
        "contact_plan": (
            evidence_root / "evolved_sweep_curobo_result_v3.json"),
    }
    artifacts = {
        name: _load_json(path, name) for name, path in paths.items()
    }

    source_paths = {
        "robotics_program": (
            root / "remoroo_brain/seeds/robotics/robotics_program.md"),
        "proof_ladder": (
            root / "remoroo_studio/server/task_engine/proof_ladder.py"),
        "proof_audit": (
            root / "remoroo_studio/server/task_engine/proof_audit.py"),
        "perception_program": (
            root / "sibling_worker/proofs/perception_sweep.py"),
        "planning_program": root / "sibling_worker/proofs/curobo_plan.py",
        "fitness_authoring": (
            root / "sibling_worker/proofs/evolution_campaign.py"),
        "typed_policy_contact": (
            root / "sibling_worker/proofs/typed_policy_contact.py"),
        "vector_executor": (
            root / "sibling_worker/proofs/mujoco_warp_batch.py"),
        "warp_kernels": (
            root / "sibling_worker/proofs/mujoco_warp_kernels.py"),
        "campaign_evaluator": (
            root / "sibling_worker/proofs/campaign.py"),
        "temporal_evaluator": (
            root / "sibling_worker/proofs/temporal.py"),
        "scene_contract": root / "sibling_worker/scene_contract.py",
        "physics_backend": (
            root / "sibling_worker/backends/mujoco_physics.py"),
    }
    for name, path in source_paths.items():
        if not path.is_file():
            raise ProofAuditError(f"{name} source does not exist: {path}")

    scene = artifacts["scene_ingestion"]
    perception = artifacts["perception_observation"]
    authored_plan = artifacts["authored_plan"]
    contact_policy = artifacts["contact_policy"]
    contact_request = artifacts["contact_request"]
    contact_plan = artifacts["contact_plan"]
    contact_gate = artifacts["curobo_actuation"]

    perception_identity = canonical_sha256({
        key: perception[key] for key in (
            "task_sentence", "support", "entities", "perception_provenance")
    })
    _require_equal(
        _nested(authored_plan, "task", "perception_sha256"),
        perception_identity,
        "authored cuRobo plan is not bound to the typed perception artifact",
    )
    _require_equal(
        _nested(authored_plan, "provenance", "cell_artifacts",
                "cell_yaml", "sha256"),
        scene["cell_yaml_sha256"],
        "authored cuRobo plan is for a different cell.yaml",
    )
    _require_equal(
        _nested(authored_plan, "provenance", "cell_artifacts",
                "robot_urdf", "sha256"),
        scene["urdf_sha256"],
        "authored cuRobo plan is for a different robot.urdf",
    )
    planner_source_sha256 = file_sha256(source_paths["planning_program"])
    _require_equal(
        _nested(authored_plan, "provenance", "cell_artifacts",
                "proof_module", "sha256"),
        planner_source_sha256,
        "cuRobo proof module differs from the audited planning source",
    )
    _require_equal(
        _nested(authored_plan, "provenance", "planner_world",
                "generated_task_assets_included"),
        False,
        "cuRobo planner was not blind to generated task assets",
    )

    # Regenerate the Cartesian request from typed perception plus dimensionless
    # genes.  A manually authored waypoint list cannot pass this equality check.
    from sibling_worker.proofs.perception_sweep import (
        build_curobo_plan_request,
    )
    mutated_perception = dict(perception)
    mutated_perception["policy"] = dict(contact_policy)
    regenerated_contact_request = build_curobo_plan_request(
        mutated_perception)
    _require_equal(
        contact_request,
        regenerated_contact_request,
        "contact request is not the typed program output for the bound policy",
    )
    _require_equal(
        _nested(contact_request, "task", "policy"),
        contact_policy,
        "contact request contains different policy genes",
    )
    _require_equal(
        _nested(contact_request, "task", "perception_sha256"),
        perception_identity,
        "contact request is not bound to the typed perception artifact",
    )
    _require_equal(
        contact_plan.get("task"),
        contact_request.get("task"),
        "contact plan is for a different typed-program request",
    )
    _require_equal(
        _nested(contact_plan, "provenance", "request_sha256"),
        canonical_sha256(contact_request),
        "contact planner provenance does not bind the exact request",
    )

    # Bind the accepted contact gate to that exact blind cuRobo result.
    from sibling_worker.backends.mujoco_physics import (
        validate_trajectory_payload,
    )
    from sibling_worker.proofs.evolution_campaign import (
        derive_typed_policy_execution,
    )
    execution = derive_typed_policy_execution(
        contact_policy,
        _nested(contact_plan, "trajectory"),
        planner_trajectory_sha256=_nested(
            contact_plan, "provenance", "trajectory_sha256"),
    )
    executed = validate_trajectory_payload(execution["trajectory"])
    _require_equal(
        contact_gate["trajectory_sha256"],
        executed["sha256"],
        "contact evidence executed a different trajectory",
    )
    _require_equal(
        _nested(contact_plan, "provenance", "cell_artifacts",
                "proof_module", "sha256"),
        planner_source_sha256,
        "contact plan proof module differs from the audited planning source",
    )
    _require_equal(
        _nested(contact_plan, "provenance", "cell_artifacts",
                "cell_yaml", "sha256"),
        scene["cell_yaml_sha256"],
        "contact plan is for a different cell.yaml",
    )
    _require_equal(
        _nested(contact_plan, "provenance", "cell_artifacts",
                "robot_urdf", "sha256"),
        scene["urdf_sha256"],
        "contact plan is for a different robot.urdf",
    )
    _require_equal(
        _nested(contact_plan, "provenance", "planner_world",
                "generated_task_assets_included"),
        False,
        "contact planner was not blind to generated task assets",
    )
    _require_equal(
        contact_gate.get("schema"),
        "remoroo.typed-policy-contact-proof/v2",
        "contact gate has an unsupported schema",
    )
    contact_checks = _nested(contact_gate, "binding_checks")
    required_contact_checks = {
        "regenerated_request_matches",
        "planner_trajectory_matches_declared_hash",
        "executed_trajectory_matches_campaign_candidate",
        "executed_control_matches_campaign_candidate",
        "urdf_mimic_constraints_preserved",
        "curobo_mujoco_fk_parity",
        "zero_runtime_state_writes",
    }
    if (
        not isinstance(contact_checks, Mapping)
        or not required_contact_checks.issubset(contact_checks)
        or not all(
            contact_checks[name] is True
            for name in required_contact_checks
        )
    ):
        raise ProofAuditError(
            "contact gate did not pass every exact binding/state-write check")

    # Use the task-authored, signed robustness formula itself as the frozen
    # fitness identity.  Importing this builder does not load CUDA or execute sim.
    from sibling_worker.proofs.evolution_campaign import build_sweep_fitness
    fitness, fitness_derivation = build_sweep_fitness(
        perception, authored_plan)

    source_hashes = {
        name: file_sha256(path) for name, path in source_paths.items()
    }
    mass = artifacts["mass_attempt"]
    if mass.get("schema") != EVOLUTION_SCHEMA:
        raise ProofAuditError(
            "manager audit requires the exact typed-program evolution report")
    mass_manifest = _nested(mass, "manifest")
    mass_provenance = _nested(mass, "provenance")
    _require_equal(
        mass.get("manifest_sha256"),
        canonical_sha256(mass_manifest),
        "mass report manifest hash is invalid",
    )
    _require_equal(
        mass_manifest.get("cell_sha256"),
        scene["cell_yaml_sha256"],
        "mass report is for a different cell.yaml",
    )
    _require_equal(
        mass_manifest.get("urdf_sha256"),
        scene["urdf_sha256"],
        "mass report is for a different robot.urdf",
    )
    _require_equal(
        mass_manifest.get("fitness_sha256"),
        fitness.sha256,
        "mass report uses a different frozen fitness",
    )
    _require_equal(
        mass_provenance.get("scene_fingerprint"),
        scene["scene_fingerprint"],
        "mass report is for a different assembled scene",
    )
    for report_field, source_name in (
        ("evolution_campaign_source_sha256", "fitness_authoring"),
        ("mujoco_warp_batch_source_sha256", "vector_executor"),
        ("mujoco_warp_kernels_source_sha256", "warp_kernels"),
        ("campaign_evaluator_source_sha256", "campaign_evaluator"),
        ("temporal_evaluator_source_sha256", "temporal_evaluator"),
        ("scene_contract_source_sha256", "scene_contract"),
        ("mujoco_physics_source_sha256", "physics_backend"),
        ("perception_program_source_sha256", "perception_program"),
        ("planning_program_source_sha256", "planning_program"),
    ):
        _require_equal(
            mass_provenance.get(report_field),
            source_hashes[source_name],
            f"mass report {report_field} differs from audited source",
        )
    _require_equal(
        mass_provenance.get("perception_file_sha256"),
        file_sha256(paths["perception_observation"]),
        "mass report used a different perception artifact",
    )
    _require_equal(
        mass_provenance.get("plan_result_file_sha256"),
        file_sha256(paths["authored_plan"]),
        "mass report used a different authored plan",
    )

    typed_binding = _nested(
        mass, "candidate_set", "bindings", "typed_policy_candidates",
        "evolved_policy_sweep",
    )
    _require_equal(
        typed_binding.get("typed_perception_program_output"),
        True,
        "mass candidate is not declared as typed program output",
    )
    _require_equal(
        typed_binding.get("regenerated_request_matches"),
        True,
        "mass worker did not regenerate the typed candidate request",
    )
    _require_equal(
        typed_binding.get("policy"),
        contact_policy,
        "mass candidate used different policy genes",
    )
    _require_equal(
        typed_binding.get("request_file_sha256"),
        file_sha256(paths["contact_request"]),
        "mass candidate used a different typed request",
    )
    _require_equal(
        typed_binding.get("result_file_sha256"),
        file_sha256(paths["contact_plan"]),
        "mass candidate used a different cuRobo result",
    )
    _require_equal(
        typed_binding.get("trajectory_sha256"),
        _nested(contact_plan, "provenance", "trajectory_sha256"),
        "mass candidate used a different planner trajectory",
    )
    _require_equal(
        typed_binding.get("executed_trajectory_sha256"),
        executed["sha256"],
        "mass candidate used a different retimed execution trajectory",
    )
    _require_equal(
        typed_binding.get("control"),
        execution["control"],
        "mass candidate used different torque-controller genes",
    )
    _require_equal(
        _nested(contact_gate, "candidate", "program_sha256"),
        typed_binding.get("program_sha256"),
        "contact and mass evidence bind different typed programs",
    )
    _require_equal(
        _nested(contact_gate, "candidate", "trajectory_sha256"),
        typed_binding.get("executed_trajectory_sha256"),
        "contact and mass evidence executed different trajectories",
    )
    _require_equal(
        _nested(contact_gate, "typed_program_binding"),
        typed_binding,
        "contact and mass evidence carry different program bindings",
    )
    contact_artifacts = _nested(contact_gate, "artifacts")
    for gate_field, expected in (
        ("cell_yaml_sha256", scene["cell_yaml_sha256"]),
        ("urdf_sha256", scene["urdf_sha256"]),
        ("scene_fingerprint", scene["scene_fingerprint"]),
        ("typed_request_file_sha256",
         file_sha256(paths["contact_request"])),
        ("typed_result_file_sha256",
         file_sha256(paths["contact_plan"])),
        ("policy_file_sha256", file_sha256(paths["contact_policy"])),
        ("perception_program_source_sha256",
         source_hashes["perception_program"]),
        ("planning_program_source_sha256",
         source_hashes["planning_program"]),
        ("evolution_campaign_source_sha256",
         source_hashes["fitness_authoring"]),
        ("typed_policy_contact_source_sha256",
         source_hashes["typed_policy_contact"]),
        ("mujoco_physics_source_sha256",
         source_hashes["physics_backend"]),
    ):
        _require_equal(
            contact_artifacts.get(gate_field),
            expected,
            f"contact gate {gate_field} differs from audited evidence",
        )

    mass_candidates = list(mass_manifest.get("candidates") or [])
    mass_worlds = list(mass_manifest.get("worlds") or [])
    mass_seeds = list(mass_manifest.get("seeds") or [])
    mass_repeats = list(mass.get("campaign_repeats") or [])
    unique_units = (
        len(mass_candidates) * len(mass_worlds) * len(mass_seeds))
    if unique_units < 1000:
        raise ProofAuditError(
            "mass report is below the N=1000 unique-unit floor")
    if len(mass_repeats) != 2:
        raise ProofAuditError(
            "mass report does not contain the required two repeats")
    scene_ids = [
        str(_nested(world, "world_id")) for world in mass_worlds]
    manifest = build_campaign_manifest(
        task=str(perception["task_sentence"]),
        cell_yaml_sha256=str(scene["cell_yaml_sha256"]),
        urdf_sha256=str(scene["urdf_sha256"]),
        world_sha256=[str(scene["world_sha256"])],
        fitness_sha256=fitness.sha256,
        perception_sha256=source_hashes["perception_program"],
        planner_sha256=source_hashes["planning_program"],
        scenes=scene_ids,
        seeds=[int(seed) for seed in mass_seeds],
        n=unique_units,
        top_t=2,
        world_attempts=len(mass_repeats),
    )
    ladder = new_ladder(manifest)

    for gate, backend in (
        ("scene_ingestion", "mujoco"),
        ("gravity_contact", "mujoco"),
        ("curobo_actuation", "newton_mujoco_warp"),
    ):
        record_gate(
            ladder,
            gate=gate,
            backend=backend,
            evidence_path=str(paths[gate]),
        )

    mass_decision = _mass_rejection(
        mass,
        campaign_sha256=manifest["campaign_sha256"],
    )
    record_tool_event(
        ladder,
        tool="task_sibling:evolve",
        request={
            "campaign_sha256": manifest["campaign_sha256"],
            "N": manifest["tunables"]["N"],
            "T": manifest["tunables"]["T"],
            "W": manifest["tunables"]["W"],
            "backend": manifest["backend_family"],
            "report_manifest_sha256": mass["manifest_sha256"],
        },
        outcome=f"rejected:{mass_decision['reasons'][0]}",
        evidence_path=str(paths["mass_attempt"]),
    )

    ladder_report = report(ladder)
    action = next_action(ladder)
    if (action.get("gate") != "mass_evaluation"
            or ladder_report["promotion_ready"] is not False):
        raise ProofAuditError(
            "manager did not fail closed at the red mass-evaluation gate")

    audit = {
        "schema": AUDIT_SCHEMA,
        "task": manifest["task"],
        "source_bindings": {
            name: {
                "path": str(path),
                "sha256": source_hashes[name],
            }
            for name, path in source_paths.items()
        },
        "artifact_bindings": {
            name: {
                "path": str(path),
                "sha256": file_sha256(path),
            }
            for name, path in paths.items()
        },
        "program_bindings": {
            "typed_perception_output_sha256": perception_identity,
            "planner_source_sha256": planner_source_sha256,
            "authored_plan_trajectory_sha256": _nested(
                authored_plan, "provenance", "trajectory_sha256"),
            "contact_policy_sha256": canonical_sha256(contact_policy),
            "contact_request_sha256": canonical_sha256(contact_request),
            "contact_result_trajectory_sha256": _nested(
                contact_plan, "provenance", "trajectory_sha256"),
            "executed_contact_trajectory_sha256": executed["sha256"],
            "contact_candidate_program_sha256": typed_binding[
                "program_sha256"],
            "contact_candidate_class": (
                "typed_dimensionless_policy_and_execution_mutation"),
            "contact_candidate_control": dict(execution["control"]),
            "contact_candidate_trajectory_time_scale": execution[
                "trajectory_time_scale"],
            "contact_candidate_typed_output": True,
            "contact_candidate_mass_graduated": False,
        },
        "frozen_fitness": fitness.to_dict(),
        "fitness_derivation": fitness_derivation,
        "manifest": manifest,
        "ladder": ladder,
        "mass_attempt": mass_decision,
        "manager_decision": {
            "status": "blocked_by_evidence",
            "completed_gates": ladder_report["completed_gates"],
            "promotion_ready": ladder_report["promotion_ready"],
            "next_action": action,
            "no_false_promotion": (
                ladder_report["promotion_ready"] is False
                and action.get("gate") == "mass_evaluation"
            ),
            "hard_rule": (
                "no real execution: exact campaign has no accepted N>=1000 "
                "complete, stable-outcome, 100%-success top-T mass report"),
        },
        "verification": verify_ladder(ladder),
        "report": ladder_report,
    }
    audit["audit_sha256"] = canonical_sha256(audit)
    return audit


def verify_manager_audit(audit: Mapping[str, Any]) -> Dict[str, Any]:
    """Recheck the audit hash, its evidence files, and its fail-closed decision."""
    expected_hash = canonical_sha256({
        key: value for key, value in audit.items() if key != "audit_sha256"
    })
    failures = []
    if audit.get("schema") != AUDIT_SCHEMA:
        failures.append("unsupported audit schema")
    if audit.get("audit_sha256") != expected_hash:
        failures.append("audit content hash is invalid")
    for section in ("source_bindings", "artifact_bindings"):
        bindings = audit.get(section)
        if not isinstance(bindings, Mapping):
            failures.append(f"audit {section} are missing")
            continue
        for name, binding in bindings.items():
            if not isinstance(binding, Mapping):
                failures.append(f"{section}.{name} is malformed")
                continue
            path = Path(str(binding.get("path")))
            if not path.is_file():
                failures.append(f"{section}.{name} file is missing")
            elif file_sha256(path) != str(binding.get("sha256")):
                failures.append(f"{section}.{name} file changed")
    ladder = audit.get("ladder")
    if not isinstance(ladder, Mapping):
        failures.append("audit ladder is missing")
        ladder_status = {"valid": False, "promotion_ready": False}
        action = {}
    else:
        ladder_status = verify_ladder(ladder)
        action = next_action(ladder)
        expected_incomplete = {
            "mass_evaluation: missing",
            "agent_management: missing",
        }
        unexpected = [
            failure for failure in ladder_status.get("failures", [])
            if failure not in expected_incomplete
        ]
        if unexpected:
            failures.extend(
                f"ladder: {failure}" for failure in unexpected)
        if action.get("gate") != "mass_evaluation":
            failures.append("audit does not stop at mass_evaluation")
        if ladder_status.get("promotion_ready") is not False:
            failures.append("audit incorrectly permits promotion")
    decision = audit.get("manager_decision")
    if (not isinstance(decision, Mapping)
            or decision.get("no_false_promotion") is not True):
        failures.append("audit lacks a fail-closed manager decision")
    return {
        "valid": not failures,
        "failures": failures,
        "ladder": ladder_status,
        "next_action": action,
        "audit_sha256": expected_hash,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the local evidence-bound Newton manager audit")
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--source-root")
    parser.add_argument("--output")
    parser.add_argument("--verify", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    audit = build_manager_audit(
        args.evidence_dir, source_root=args.source_root)
    verification = verify_manager_audit(audit)
    if args.verify and not verification["valid"]:
        raise SystemExit(
            "audit verification failed: "
            + "; ".join(verification["failures"]))
    text = json.dumps(audit, indent=2, sort_keys=True) + "\n"
    if args.output:
        destination = Path(args.output).expanduser().resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(text, encoding="utf-8")
    print(json.dumps({
        "audit_sha256": audit["audit_sha256"],
        "promotion_ready": audit["manager_decision"]["promotion_ready"],
        "next_action": audit["manager_decision"]["next_action"],
        "mass_rejection_reasons": audit["mass_attempt"]["reasons"],
        "verified": verification["valid"],
        "output": str(Path(args.output).expanduser().resolve())
        if args.output else None,
    }, indent=2, sort_keys=True))
    return 0 if verification["valid"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
