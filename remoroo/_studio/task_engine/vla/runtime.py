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


class LingBotRuntime:
    """LingBot-VLA 2.0 (Robbyant/Ant, Apache-2.0) — the PRIMARY VLA (decision 2026-07-09):
    beats pi0.5 on bimanual benchmarks, PyTorch (no second framework on the rig),
    post-trains on LeRobot v2 (exactly what export.py emits), HF-pinned weights.

    Integration is the model's OWN policy server, started once on the rig in its own
    venv (`remoroo vla start`; no model imports in the edge):

        python -m deploy.lingbot_vla_v2_policy --model_path <ckpt> --use_compile --port 8791

    WIRE (mirrored from the repo's deploy/ client, 2026-07-10): WebSocket + msgpack-numpy
    (wire.py), observation = {"image": {slot: uint8 HxWx3}, "state": float32[state_dim],
    "prompt": [str]} — built by packer.py from the cell's authored vla_profile.yaml.

    PIN: repo robbyant/lingbot-vla-v2-6b @ revision
    11c703bf6a5c1f45b3b69168482da11fdbba53d7 (config + 6 safetensors shards + bundled
    depth/dino_video teachers). ~130 ms/call on a 4090-class GPU, 10 denoising steps.

    DECODE: with a profile, returned action rows are unpacked by the profile's action
    layout (end.position -> delta_xyz per chain, effector.position -> gripper). Without
    one (zero-shot, pre-profile), the documented 55-dim canonical layout applies
    (dims 0-13 arm joints, 14-27 EE poses [7/arm], 28-29 grippers, ...)."""

    PIN = {"repo_id": "robbyant/lingbot-vla-v2-6b",
           "revision": "11c703bf6a5c1f45b3b69168482da11fdbba53d7"}

    def __init__(self, *, server_url: str = "ws://127.0.0.1:8791",
                 profile: Optional[Any] = None,
                 tcps: Optional[List[str]] = None, max_chunk: int = 8,
                 decode_fn: Optional[Callable] = None,
                 client: Optional[Any] = None) -> None:
        self.server_url = server_url.rstrip("/")
        self.profile = profile
        self.tcps = tcps or ["arm_a", "arm_b"]
        self.max_chunk = max_chunk
        self.decode_fn = decode_fn or (self._decode_profile if profile is not None
                                       else self._decode_canonical)
        self._client = client                        # injectable (tests); lazy otherwise

    def _client_or_connect(self) -> Any:
        if self._client is None:
            from .wire import PolicyClient

            self._client = PolicyClient(self.server_url)
        return self._client

    def prove(self, obs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Exchange REAL bytes with the server at load time (audit 2026-07-13: "loaded"
        was a constructor; the wire had never been exercised). With an observation:
        one full inference round-trip (proven: "inference", + latency and the response
        row shape). Without: connect + the server's hello frame (proven: "handshake").
        Failure returns the server's words, stated — never a silent handle."""
        import time as _time

        t0 = _time.time()
        try:
            client = self._client_or_connect()
            client._ensure()
            hello = sorted((client.metadata or {}).keys())
            if obs is None:
                return {"proven": "handshake", "latency_ms": round((_time.time() - t0) * 1e3),
                        "hello_keys": hello,
                        "note": "no observation packer (no vla_profile) — wire connects; "
                                "inference unproven until a profile exists"}
            obs = dict(obs)
            obs.setdefault("task", "remoroo wire proof")      # pad_and_concat needs it
            obs.setdefault("prompt", [obs["task"]])
            resp = client.infer(obs)
            rows = self._rows_of(resp)
            return {"proven": "inference", "latency_ms": round((_time.time() - t0) * 1e3),
                    "hello_keys": hello, "action_rows": len(rows),
                    "action_dim": (len(rows[0]) if rows else 0)}
        except Exception as e:                       # noqa: BLE001 - the server's words
            msg = str(e)
            refused = isinstance(e, ConnectionRefusedError) or "refused" in msg.lower()
            server_side = "policy server error" in msg    # OUR wire worked; THEIR code crashed
            if refused:
                note = ("the server is DOWN (connection refused — NOT loading; a loading "
                        "server accepts and stalls): run `remoroo vla status` and "
                        "`remoroo vla logs` via bash. A crashed server restarts with "
                        "`remoroo vla restart`; an import/env crash (libcudnn etc.) means "
                        ".remoroo/vla.yaml env_file/env is wrong — fix the declaration, "
                        "restart, vla_load again")
            elif server_side:
                note = ("THE WIRE WORKS — the server received and parsed the observation "
                        "and crashed inside ITS OWN policy code. That is an INCOMPLETE "
                        "SERVER INSTALL/CONFIG, and integration configs are YOURS to fix "
                        "(read_seed robotics/vla_serving — the venv is the customer's, "
                        "the configs are yours): read the traceback, inspect the server's "
                        "config files next to the checkpoint (cli yaml / robot config / "
                        "norm stats — e.g. feature_transform None means the policy "
                        "loaded weights WITHOUT its feature/robot config), fix, "
                        "`remoroo vla restart`, vla_load again. Do NOT shrug this off "
                        "as a rig problem")
            else:
                note = ("wire proof FAILED — the server may still be loading (retry "
                        "vla_load in minutes) or the profile/config disagrees with the "
                        "server; nothing is loaded")
            return {"proven": False, "error": f"{type(e).__name__}: {e}", "note": note}

    # --- decoders ---------------------------------------------------------------------
    def _decode_canonical(self, step: List[float],
                          prev: Optional[List[float]]) -> List[Dict[str, Any]]:
        """One canonical action step -> v1-dialect actions. EE-pose dims are absolute
        targets; deltas derive against the previous step (first step: skipped — the
        GuardedExecutor's clamps bound every move regardless)."""
        out: List[Dict[str, Any]] = []
        for i, tcp in enumerate(self.tcps[:2]):
            base = 14 + 7 * i                            # EE pose block, 7 per arm
            xyz = step[base:base + 3]
            if prev is not None:
                delta = [xyz[k] - prev[base + k] for k in range(3)]
                if any(abs(d) > 1e-6 for d in delta):
                    out.append({"tcp": tcp, "delta_xyz": delta})
            if len(step) > 28 + i and step[28 + i] < 0.5:  # normalized close signal
                out.append({"tcp": tcp, "gripper": 0.012})
        return out

    def _decode_profile(self, step: List[float],
                        prev: Optional[List[float]]) -> List[Dict[str, Any]]:
        """Profile-driven unpack: the SAME layout the server-side configs were generated
        from. Live execution consumes end.position (EE absolute -> delta) and
        effector.position (close signal) groups; arm.position (joint-space) rows train
        the model but are not executed — our dialect is EE-space, always clamped."""
        p = self.profile
        out: List[Dict[str, Any]] = []
        for s in p.action_slices("end.position"):
            xyz = step[s.start:s.start + 3]
            if prev is not None:
                delta = [xyz[k] - prev[s.start + k] for k in range(3)]
                if any(abs(d) > 1e-6 for d in delta):
                    out.append({"tcp": s.chain, "delta_xyz": delta})
        for s in p.action_slices("effector.position"):
            if step[s.start] < p.close_threshold:
                out.append({"tcp": s.chain, "gripper": p.gripper_close_width})
        return out

    # --- response -> flat rows ----------------------------------------------------------
    def _rows_of(self, resp: Dict[str, Any]) -> List[List[float]]:
        import numpy as np

        for flat_key in ("action", "actions"):       # the REAL server returns "action":
            if flat_key in resp:                     # unapply's org key = the flat vector
                arr = np.asarray(resp[flat_key], dtype=float)   # [chunk, action_dim],
                return [list(r) for r in               # already in the PROFILE's layout
                        (arr if arr.ndim > 1 else arr.reshape(1, -1))]
        if self.profile is not None:                 # per-group keys ("action.<group>")
            chunks = {}
            for s in self.profile.actions:
                key = f"action.{s.group}"
                if key in resp:
                    chunks[s.group] = np.asarray(resp[key], dtype=float)
            if chunks:
                n = min(int(c.shape[0]) if c.ndim > 1 else 1 for c in chunks.values())
                rows = np.zeros((n, self.profile.action_dim), dtype=float)
                for s in self.profile.actions:
                    c = chunks.get(s.group)
                    if c is None:
                        continue
                    c2 = c if c.ndim > 1 else c.reshape(1, -1)
                    off = sum(x.dims for x in self.profile.action_slices(s.group)
                              if x.start < s.start)
                    rows[:, s.start:s.end] = c2[:n, off:off + s.dims]
                return [list(r) for r in rows]
        raise RuntimeError(f"unrecognized policy response keys: {sorted(resp)}")

    def policy(self, instruction: str) -> Callable[[Any], Optional[Dict[str, Any]]]:
        pending: List[Dict[str, Any]] = []
        prev_step: List[Optional[List[float]]] = [None]

        def act(obs: Any) -> Optional[Dict[str, Any]]:
            if pending:
                return pending.pop(0)
            payload = dict(obs) if isinstance(obs, dict) else {"observation": obs}
            payload["prompt"] = [instruction]
            payload["task"] = instruction        # pad_and_concat reads item["task"]
            resp = self._client_or_connect().infer(payload)
            for raw in self._rows_of(resp)[: self.max_chunk]:
                step = [float(v) for v in raw]
                pending.extend(self.decode_fn(step, prev_step[0]))
                prev_step[0] = step
            return pending.pop(0) if pending else None
        return act


def finetune_job_payload(*, task_slug: str, dataset_dir: str,
                         base_checkpoint: str = "lingbot-vla-v2-6b",
                         steps: int = 2000) -> Dict[str, Any]:
    """The jobs.py (IFACE-10) payload for the recurring RECAP-style fine-tune on the GPU box:
    advantage-conditioned on the verifier-labeled episodes (good AND bad actions train it)."""
    return {"kind": "vla_finetune", "task": task_slug, "dataset_dir": dataset_dir,
            "base_checkpoint": base_checkpoint, "steps": steps,
            "recipe": "advantage_conditioned",
            "labels": "verifier verdicts (versioned; re-judged history allowed)"}
