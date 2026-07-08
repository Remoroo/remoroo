"""VLA runtime (COMP-25, OQ-B1) — the pi0.5/openpi integration seam plus the FakeVLA used
everywhere off-GPU. runtime.policy(instruction) -> (obs -> action|None) in the v1 action
dialect the ActionAdapter maps onto THIS cell; every action still passes the GuardedExecutor.
Weights load lazily on the GPU cell only; nothing here imports torch in CI.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional


class FakeVLA:
    """Scriptable policy for CI and dry runs: walks toward a named entity then grips."""

    def __init__(self, *, approach_steps: int = 3) -> None:
        self.approach_steps = approach_steps

    def policy(self, instruction: str) -> Callable[[Any], Optional[Dict[str, Any]]]:
        state = {"i": 0}

        def act(obs: Any) -> Optional[Dict[str, Any]]:
            i = state["i"]
            state["i"] += 1
            if i < self.approach_steps:
                return {"tcp": "arm_a", "delta_xyz": [0.02, 0.0, -0.02]}
            if i == self.approach_steps:
                return {"tcp": "arm_a", "gripper": 0.015}
            return None
        return act


class OpenPiRuntime:
    """pi0.5 via openpi (Apache-2.0). Lazy-loaded; the checkpoint id and the per-arm mapping
    are deploy-time config. The policy consumes the SceneState header + camera frame refs the
    Env provides and emits v1-dialect actions (delta_xyz / gripper) chunk by chunk."""

    def __init__(self, *, checkpoint: str = "pi05_base", device: str = "cuda",
                 tcp: str = "arm_a", max_chunk: int = 8) -> None:
        self.checkpoint = checkpoint
        self.device = device
        self.tcp = tcp
        self.max_chunk = max_chunk
        self._policy = None

    def _load(self) -> None:
        if self._policy is not None:
            return
        from openpi.policies import policy_config       # lazy: GPU cell only
        from openpi.training import config as opi_config
        cfg = opi_config.get_config(self.checkpoint)
        self._policy = policy_config.create_trained_policy(cfg, self.checkpoint)

    def policy(self, instruction: str) -> Callable[[Any], Optional[Dict[str, Any]]]:
        self._load()
        pending: List[Dict[str, Any]] = []

        def act(obs: Any) -> Optional[Dict[str, Any]]:
            if pending:
                return pending.pop(0)
            example = {"prompt": instruction, "observation": obs}
            chunk = self._policy.infer(example)["actions"][: self.max_chunk]
            for a in chunk:
                a = [float(v) for v in a]
                pending.append({"tcp": self.tcp, "delta_xyz": a[:3]})
                if len(a) > 6 and a[6] < 0.5:            # gripper channel convention
                    pending.append({"tcp": self.tcp, "gripper": 0.015})
            return pending.pop(0) if pending else None
        return act


def finetune_job_payload(*, task_slug: str, dataset_dir: str,
                         base_checkpoint: str = "pi05_base",
                         steps: int = 2000) -> Dict[str, Any]:
    """The jobs.py (IFACE-10) payload for the recurring RECAP-style fine-tune on the GPU box:
    advantage-conditioned on the verifier-labeled episodes (good AND bad actions train it)."""
    return {"kind": "vla_finetune", "task": task_slug, "dataset_dir": dataset_dir,
            "base_checkpoint": base_checkpoint, "steps": steps,
            "recipe": "advantage_conditioned",
            "labels": "verifier verdicts (versioned; re-judged history allowed)"}
