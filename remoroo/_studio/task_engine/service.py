"""TaskService (COMP-32) — the JSON-verb dispatch behind /edge/task/<verb>, mirroring the
calib_engine service pattern: edge_real routes here; this class is CI-tested off-robot. Verbs
that touch the shared bridge are declared in BRIDGE_VERBS so edge_real serializes them under
its bridge lock (the proven calib discipline).

The live trial verb runs the AUTHORED cell task package: remoroo_cell.task_<slug> must expose
build_env(shared) -> GenericEnv and actor(env, ctx). Off-robot (no cell package) the verb
returns a stated error, never a crash.
"""
from __future__ import annotations

import importlib
import inspect
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .params import Knobs
from .perceive.bank import RegressionBank
from .record import TrialStore, replay_score
from .reset.reversibility import ReversibilityTable
from .run_trial import TrialBudget, run_trial
from .supervisor import Supervisor, SupervisorConfig

BRIDGE_VERBS = {"trial_run", "vla_rollout", "ritual_trial"}   # edge_real wraps these in its _bridge_lock


class TaskService:
    def __init__(self, *, data_dir: str = ".remoroo/task",
                 env_builder: Optional[Callable[[str], Any]] = None,
                 bridge: Optional[Any] = None,
                 stack_provider: Optional[Callable[[], Any]] = None) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.bank = RegressionBank.load(str(self.data_dir / "bank.jsonl"))
        self.reversibility = ReversibilityTable.load(str(self.data_dir / "reversibility.json"))
        self.supervisor: Optional[Supervisor] = None
        self._bridge = bridge
        self._stack_provider = stack_provider
        self._env_builder = env_builder or self._default_env_builder
        self._stores: Dict[str, TrialStore] = {}

    # ---- plumbing --------------------------------------------------------------------
    def _store(self, task_slug: str) -> TrialStore:
        if task_slug not in self._stores:
            self._stores[task_slug] = TrialStore(str(self.data_dir / "trials" / task_slug))
        return self._stores[task_slug]

    def _shared(self) -> Dict[str, Any]:
        """THE handoff to the authored cell package: build_env(shared) receives the LIVE
        surfaces here — the Bridge and the MotionStack — plus the task data dir. This is
        the contract; the package never reaches into the edge for accessors."""
        stack = None
        if self._stack_provider is not None:
            stack = self._stack_provider()             # may build/warm: that's the point
        return {"bridge": self._bridge, "stack": stack,
                "data_dir": str(self.data_dir)}

    def _default_env_builder(self, task_slug: str) -> Any:
        # HOT RELOAD: every trial imports the authored package FRESH, so the agent's
        # edit -> task_trial cycle needs NO edge restart (a restart also costs the
        # cuRobo warmup). Only the tiny task package reloads; the Bridge
        # (primitives.py) holds live device handles and still needs a real restart.
        pkg = f"remoroo_cell.task_{task_slug}"
        for name in [m for m in list(sys.modules)
                     if m == pkg or m.startswith(pkg + ".")]:
            del sys.modules[name]
        # The package is AUTHORED mid-session (after this process started): without this,
        # the FileFinder's directory cache can miss brand-new files entirely.
        importlib.invalidate_caches()
        mod = importlib.import_module(pkg)
        build = mod.build_env
        # build_env(shared) is the contract; a zero-arg build_env stays supported.
        takes_shared = any(
            p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
            for p in inspect.signature(build).parameters.values())
        env = build(self._shared()) if takes_shared else build()
        return env, getattr(mod, "actor")

    # ---- dispatch --------------------------------------------------------------------
    def handle(self, verb: str, body: Dict[str, Any]) -> Dict[str, Any]:
        try:
            fn = getattr(self, f"v_{verb}", None)
            if fn is None:
                return {"error": f"unknown task verb {verb!r}"}
            return fn(body or {})
        except Exception as e:                     # noqa: BLE001 - stated, never a dead socket
            return {"error": f"{type(e).__name__}: {e}"}

    # ---- verbs -----------------------------------------------------------------------
    def v_status(self, body: dict) -> dict:
        return {
            "service": "task", "data_dir": str(self.data_dir),
            "supervisor": self.supervisor.status() if self.supervisor else {"state": "off"},
            "bank_cases": len(self.bank.cases),
            "reversibility": self.reversibility.to_dict(),
        }

    def v_supervisor_start(self, body: dict) -> dict:
        if self.supervisor is None:
            cfg = SupervisorConfig(**(body.get("config") or {}))
            bridge = self._bridge
            if bridge is None:
                return {"error": "no bridge wired; supervisor needs the cell"}
            self.supervisor = Supervisor(bridge, cfg)
            self.supervisor.start()
        return {"ok": True, "status": self.supervisor.status()}

    def v_supervisor_status(self, body: dict) -> dict:
        return self.supervisor.status() if self.supervisor else {"state": "off"}

    def v_supervisor_stop(self, body: dict) -> dict:
        if self.supervisor:
            self.supervisor.shutdown()
            self.supervisor = None
        return {"ok": True}

    def v_reversibility_get(self, body: dict) -> dict:
        return {"table": self.reversibility.to_dict()}

    def v_reversibility_update(self, body: dict) -> dict:
        self.reversibility.update(body["action_class"], bool(body["undo_ok"]))
        self.reversibility.save(str(self.data_dir / "reversibility.json"))
        return {"ok": True, "score": self.reversibility.score(body["action_class"])}

    def v_bank_stats(self, body: dict) -> dict:
        by = {}
        for c in self.bank.cases:
            by[c.source] = by.get(c.source, 0) + 1
        return {"cases": len(self.bank.cases), "by_source": by}

    def v_bank_label_grasp(self, body: dict) -> dict:
        case = self.bank.label_from_grasp(predicted_xyz=body["xyz"], held=bool(body["held"]),
                                          closed_width=float(body.get("closed_width", 0.0)),
                                          frame_ref=body.get("frame_ref"))
        self.bank.save(str(self.data_dir / "bank.jsonl"))
        return {"ok": True, "case_id": case.case_id}

    def v_trial_ids(self, body: dict) -> dict:
        return {"trial_ids": self._store(body["task_slug"]).all_ids()}

    def v_trial_get(self, body: dict) -> dict:
        rec = self._store(body["task_slug"]).load(body["trial_id"])
        return {"record": rec.to_dict()}

    def v_trial_run(self, body: dict) -> dict:
        """Run one trial of the AUTHORED task package on this cell (the M0 smoke path)."""
        slug = body["task_slug"]
        try:
            env, actor = self._env_builder(slug)
        except Exception as e:                     # noqa: BLE001
            return {"error": (f"cell task package not available for {slug!r}: "
                              f"{type(e).__name__}: {e}")}
        rec = run_trial(env, actor, task_slug=slug,
                        program_ref=body.get("program_ref", f"{slug}@cell"),
                        knobs=Knobs(body.get("knobs") or {}),
                        seed=int(body.get("seed", 0)),
                        budget=TrialBudget(**(body.get("budget") or {})),
                        store=self._store(slug),
                        skip_reset=bool(body.get("skip_reset", False)))
        return {"trial_id": rec.trial_id, "outcome": rec.outcome, "verdict": rec.verdict,
                "knobs": rec.knobs}

    def v_trial_replay(self, body: dict) -> dict:
        """Re-judge a stored trial from its evidence (the DEC-05 re-basing path)."""
        rec = self._store(body["task_slug"]).load(body["trial_id"])
        env, _actor = self._env_builder(body["task_slug"])
        return {"trial_id": rec.trial_id, "old": rec.verdict,
                "new": replay_score(rec, env.judge_fn)}

    # ---- scene record + phase spine + ritual (Task Engine v2, M0) ----------------------
    def _record(self, task_slug: str):
        from .scene_record.store import SceneRecord
        return SceneRecord.load(str(self.data_dir / "record" / f"{task_slug}.json"),
                                task_slug)

    def _ledger(self, task_slug: str):
        from .spine import PhaseLedger
        return PhaseLedger.load(str(self.data_dir / "state" / f"{task_slug}.json"),
                                task_slug)

    def v_record_submit(self, body: dict) -> dict:
        """The agent's perception submits a scene WITH capture provenance; the engine
        merges and GRADES (two looks or a touch; arm-correlation bounce; calibration
        attribution). Returns the grade — the agent iterates until it passes."""
        rec = self._record(body["task_slug"])
        return rec.submit(body.get("entities") or [], body.get("provenance") or {})

    def v_record_get(self, body: dict) -> dict:
        return self._record(body["task_slug"]).to_dict()

    def v_record_confirm(self, body: dict) -> dict:
        """Physical proof from a probe/trial: touch|lift|trial. Also narrows ranges."""
        rec = self._record(body["task_slug"])
        rec.confirm_touch(body["object_id"], kind=body.get("kind", "touch"),
                          source=body.get("source", "probe"))
        for n in body.get("narrow") or []:
            rec.narrow(body["object_id"], n["param"], float(n["lo"]), float(n["hi"]))
        return {"ok": True,
                "proof_level": rec.objects[body["object_id"]].proof_level()}

    def v_phase_status(self, body: dict) -> dict:
        return self._ledger(body["task_slug"]).status()

    def v_phase_start(self, body: dict) -> dict:
        """Startup: capture RUN HOME from the live stack (COMP-09) + instrument notes."""
        led = self._ledger(body["task_slug"])
        joints = body.get("joints")                    # tests/sim may inject
        if joints is None:
            stack = self._stack_provider() if self._stack_provider else None
            reader = (getattr(stack, "joint_positions", None)
                      or getattr(self._bridge, "joint_state", None)
                      or getattr(stack, "_seed_positions", None))  # the real MotionStack's live config
            joints = dict(reader()) if callable(reader) else {}
        if not joints:
            return {"error": "no joints readable for RUN HOME — startup can't complete"}
        led.set_run_home(joints)
        led.instruments = body.get("instruments") or led.instruments
        led.save()
        return {"ok": True, "run_home_joints": len(joints)}

    def v_phase_advance(self, body: dict) -> dict:
        return self._ledger(body["task_slug"]).advance(body["to"],
                                                       body.get("evidence") or {})

    def v_phase_regress(self, body: dict) -> dict:
        return self._ledger(body["task_slug"]).regress(body["to"],
                                                       body.get("reason", ""))

    def v_ritual_trial(self, body: dict) -> dict:
        """A real trial under the ritual (home, drift check, act, home, look, compare).
        This is how the search's promoted candidates touch hardware; the ledger must be
        in the 'real' phase (the flow, not a guard: hardware is this phase's tool)."""
        slug = body["task_slug"]
        led = self._ledger(slug)
        if led.phase != "real":
            return {"error": f"ritual trials belong to the 'real' phase; ledger says "
                             f"{led.phase!r} — the spine advances on evidence"}
        try:
            env, actor = self._env_builder(slug)
        except Exception as e:                     # noqa: BLE001
            return {"error": f"cell task package not available: "
                             f"{type(e).__name__}: {e}"}
        from .ritual import run_ritual_trial
        out = run_ritual_trial(
            env, actor, task_slug=slug,
            program_ref=body.get("program_ref", f"{slug}@cell"),
            record=self._record(slug), run_home=led.run_home,
            sim_prediction=body.get("sim_prediction"),
            knobs=Knobs(body.get("knobs") or {}), seed=int(body.get("seed", 0)),
            budget=TrialBudget(**(body.get("budget") or {})),
            store=self._store(slug))
        rec = out["record"]
        res = {"ritual": out["ritual"]}
        if rec is not None:
            from .expect import summarize
            res.update(trial_id=rec.trial_id, outcome=rec.outcome,
                       verdict=rec.verdict,
                       expectations=summarize(rec.trace))
        return res

    # ---- ground truth + replay + probes + scout (Task Engine v2, M1) --------------------
    def _library(self):
        from .perceive.groundtruth import GroundTruthLibrary
        return GroundTruthLibrary.load(str(self.data_dir / "groundtruth"))

    def v_gt_stats(self, body: dict) -> dict:
        lib = self._library()
        return {"cases": len(lib.cases),
                "by_provenance": {p: sum(1 for c in lib.cases
                                         if c.provenance.startswith(p))
                                  for p in {"probe", "trial"}}}

    def v_replay_score(self, body: dict) -> dict:
        """Score a perception program against every physically confirmed scene this
        cell has ever recorded (IFACE-04) — the free fitness; no robot time."""
        from .perceive.groundtruth import score_perception
        lib = self._library()
        if not lib.cases:
            return {"error": "ground-truth library is empty — probe first (every "
                             "touch/lift writes a case), then replay means something"}
        return score_perception(body["program_src"], lib)

    def v_probe_run(self, body: dict) -> dict:
        """One probe on a RECORDED object; success writes proof + ground-truth case +
        narrowed ranges in one call (rule 5). Bridge-locked (real motion)."""
        slug = body["task_slug"]
        try:
            env, _actor = self._env_builder(slug)
        except Exception as e:                     # noqa: BLE001
            return {"error": f"cell task package not available: "
                             f"{type(e).__name__}: {e}"}
        frame = None
        if body.get("camera"):
            from .perceive.operators import PerceptionOps
            frame = PerceptionOps(None, None, env.bridge).capture(body["camera"])
        from .probe_record import probe_and_record
        from .ritual import _goto_home
        led = self._ledger(slug)
        _goto_home(env, led.run_home)              # probe starts from RUN HOME ...
        out = probe_and_record(
            body.get("kind", "touch"), env.new_ctx(), body.get("tcp", ""),
            record=self._record(slug), object_id=body["object_id"],
            library=self._library(), frame=frame,
            narrow=body.get("narrow"), params=body.get("params"))
        _goto_home(env, led.run_home)              # ... and ends there: cameras clear
        return out

    def v_scout_run(self, body: dict) -> dict:
        """The VLA scout (COMP-12): a guarded rollout that SWEEPS while saving
        keyframes for the agent's perception to process. Each keyframe carries a
        distinct viewpoint tag, so submissions from these frames satisfy the two-look
        rule. No VLA loaded -> stated fallback (PROB-12): use a look tour instead."""
        runtime = getattr(self, "_vla", None)
        if runtime is None:
            return {"error": "no VLA runtime loaded (vla_load first); fallback: plan a "
                             "look tour with atoms.look over 3-4 viewpoints and submit "
                             "each view — same two-look effect, slower"}
        slug = body["task_slug"]
        try:
            env, _actor = self._env_builder(slug)
        except Exception as e:                     # noqa: BLE001
            return {"error": f"cell task package not available: "
                             f"{type(e).__name__}: {e}"}
        camera = body.get("camera", "wrist")
        every = int(body.get("keyframe_every", 3))
        out_dir = self.data_dir / "scout" / slug
        out_dir.mkdir(parents=True, exist_ok=True)
        from .perceive.operators import PerceptionOps
        ops = PerceptionOps(None, None, env.bridge)
        frames: list = []

        base_observe = (getattr(self, "_vla_observe", None) or env.observe)
        steps = [0]

        def observe_and_tap():
            obs = base_observe()
            if steps[0] % every == 0:
                f = ops.capture(camera)
                k = len(frames)
                ref = str(out_dir / f"kf_{k:03d}.json")
                import json as _json
                Path(ref).write_text(_json.dumps(
                    {"camera": camera, "t": f.get("t_capture"),
                     "viewpoint": f"scout_{k:03d}"}, default=str))
                frames.append({"ref": ref, "viewpoint": f"scout_{k:03d}"})
            steps[0] += 1
            return obs

        from .vla.skill import make_vla_skill
        vla = make_vla_skill(runtime, observe_of=lambda _env: observe_and_tap)
        ctx = env.new_ctx()
        r = vla(env, ctx, body.get("instruction",
                                   "find the objects and record their poses"),
                max_steps=int(body.get("max_steps", 20)))
        return {"ok": r.ok, "stopped_by": r.evidence.get("stopped_by"),
                "keyframes": frames,
                "note": "run your perception on each keyframe and record_submit with "
                        "its viewpoint tag — that satisfies the two-look rule"}

    def v_audit_bundle(self, body: dict) -> dict:
        """The cert's audit bundle (ART-06): record + ledger + per-trial expectation
        stats into .remoroo/task/audit/<slug>/bundle.json — what makes the
        certificate a legible artifact."""
        import json as _json
        slug = body["task_slug"]
        from .expect import summarize
        trials = []
        store = self._store(slug)
        for tid in store.all_ids()[-50:]:
            rec = store.load(tid)
            trials.append({"trial_id": tid, "outcome": rec.outcome,
                           "verdict": rec.verdict,
                           "expectations": summarize(rec.trace)})
        bundle = {"task_slug": slug,
                  "record": self._record(slug).to_dict(),
                  "ledger": self._ledger(slug).status(),
                  "trials": trials}
        out = self.data_dir / "audit" / slug / "bundle.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(_json.dumps(bundle, indent=1, default=str), encoding="utf-8")
        return {"ok": True, "path": str(out), "trials": len(trials)}

    def v_look_plan(self, body: dict) -> dict:
        """Ranked next-viewpoint proposals from the record's open questions (COMP-14):
        look requests first, then unknown volumes by size, then single-look objects."""
        from .scene_record.lookplan import propose
        return {"proposals": propose(self._record(body["task_slug"]))}

    def v_toolbox(self, body: dict) -> dict:
        from .perceive.loaders import toolbox_catalog
        return toolbox_catalog()

    # ---- perception models (install ONCE per cell; never inside a task run) -----------
    def v_models_status(self, body: dict) -> dict:
        from .perceive.loaders import models_status
        return {"weights_dir": str(self.data_dir / "weights"),
                "models": models_status(str(self.data_dir / "weights"))}

    def v_models_install(self, body: dict) -> dict:
        """Fetch + hash-verify the pinned checkpoints (GBs; minutes on first run).
        Idempotent — `remoroo models install` calls this at setup time so the agent
        never spends tokens discovering or fetching weights."""
        from .perceive.loaders import models_install
        return {"weights_dir": str(self.data_dir / "weights"),
                "models": models_install(str(self.data_dir / "weights"))}

    # ---- VLA verbs (ENG 8.3): the cold-start policy as smart probe / whole stage -------
    def v_vla_load(self, body: dict) -> dict:
        """Load a policy runtime on this cell. 'lingbot' (PRIMARY: LingBot-VLA 2.0 via
        its local policy server) | 'openpi' (pi0.5, evaluation) | 'fake' (CI/dry-run)."""
        kind = body.get("runtime", "fake")
        if kind == "fake":
            from .vla.runtime import FakeVLA
            self._vla = FakeVLA(approach_steps=int(body.get("approach_steps", 3)))
        elif kind == "lingbot":
            from .vla.runtime import LingBotRuntime
            profile = self._vla_profile()
            self._vla = LingBotRuntime(
                server_url=body.get("server_url") or self._vla_server_url(),
                profile=profile, tcps=body.get("tcps"))
            self._vla_observe = None
            if profile is not None:                   # profile-driven observation packing
                from .vla.packer import make_observation_packer
                stack = self._stack_provider() if self._stack_provider else None
                self._vla_observe = make_observation_packer(
                    profile, bridge=self._bridge, stack=stack)
        elif kind == "openpi":
            from .vla.runtime import OpenPiRuntime
            self._vla = OpenPiRuntime(checkpoint=body.get("checkpoint", "pi05_base"),
                                      tcp=body.get("tcp", "arm_a"))
        else:
            return {"error": f"unknown runtime {kind!r} (fake | lingbot | openpi)"}
        self._vla_kind = kind
        return {"ok": True, "runtime": kind}

    def v_vla_status(self, body: dict) -> dict:
        kind = getattr(self, "_vla_kind", None)
        return {"loaded": kind is not None, "runtime": kind}

    def _vla_profile(self):
        """The cell's authored embodiment profile (remoroo_cell/vla_profile.yaml), or
        None — the agent authors it; both wire sides derive from it."""
        from .vla.profile import load_profile
        return load_profile(str(self.data_dir.parent.parent
                                / "remoroo_cell" / "vla_profile.yaml"))

    def v_vla_apply_profile(self, body: dict) -> dict:
        """Generate LingBot's OWN config files (robot config + the cli yaml the policy
        server reads) from the authored profile — the agent never hand-writes LingBot's
        format. Paths come from .remoroo/vla.yaml; restart the server after (`remoroo
        vla restart`) so it loads them."""
        profile = self._vla_profile()
        if profile is None:
            return {"error": "no remoroo_cell/vla_profile.yaml on this cell — author it "
                             "first (the embodiment wiring: cameras/state/actions)"}
        try:
            import yaml
            cfg = yaml.safe_load((self.data_dir.parent / "vla.yaml").read_text()) or {}
        except Exception as e:                        # noqa: BLE001
            return {"error": f"no readable .remoroo/vla.yaml ({type(e).__name__}: {e}) — "
                             "the rig must declare the policy server first"}
        workdir, checkpoint = cfg.get("workdir"), cfg.get("checkpoint")
        if not workdir or not checkpoint:
            return {"error": "vla.yaml needs workdir + checkpoint to place the configs"}
        from .vla.profile import write_lingbot_configs
        norm_stats = str(self.data_dir / "vla" / "norm_stats.json")   # finetune job output
        out = write_lingbot_configs(
            profile, workdir=workdir, checkpoint=checkpoint,
            qwen_path=cfg.get("qwen_path") or str(Path(checkpoint).parent
                                                  / "Qwen3-VL-4B-Instruct"),
            norm_stats_path=norm_stats)
        # the config DICTS ride along: task_vla_finetune ships them to the trainer box
        return {"ok": True, "robo_name": profile.robo_name,
                "state_dim": profile.state_dim, "action_dim": profile.action_dim,
                "norm_stats_expected_at": norm_stats, **out}

    def _vla_server_url(self) -> str:
        """The rig's declared policy server: .remoroo/vla.yaml (the SAME file
        `remoroo vla` starts the process from), so the service manager and this
        client can never disagree about the port."""
        default = "http://127.0.0.1:8791"
        cfg = self.data_dir.parent / "vla.yaml"           # data_dir = .remoroo/task
        try:
            import yaml
            port = (yaml.safe_load(cfg.read_text()) or {}).get("port")
            return f"http://127.0.0.1:{int(port)}" if port else default
        except Exception:                                 # noqa: BLE001 - default is fine
            return default

    def v_vla_rollout(self, body: dict) -> dict:
        """One guarded VLA episode as a FULL trial: reset -> observe -> policy (every
        action through GuardedExecutor clamps + envelope) -> judge -> record. The record
        is judge-labeled probe footage — day-zero demonstrations and finetune data."""
        runtime = getattr(self, "_vla", None)
        if runtime is None:
            return {"error": "no VLA runtime loaded; call vla_load first"}
        slug = body["task_slug"]
        instruction = body.get("instruction") or slug.replace("_", " ")
        try:
            env, _actor = self._env_builder(slug)
        except Exception as e:                     # noqa: BLE001
            return {"error": (f"cell task package not available for {slug!r}: "
                              f"{type(e).__name__}: {e}")}
        from .vla.skill import make_vla_skill
        packer = getattr(self, "_vla_observe", None)  # profile packer beats env.observe
        vla = make_vla_skill(runtime,
                             observe_of=(lambda env: packer) if packer else None)
        max_steps = int(body.get("max_steps", 30))

        def actor(env_, ctx):
            vla(env_, ctx, instruction, max_steps=max_steps)

        rec = run_trial(env, actor, task_slug=slug,
                        program_ref=f"vla:{getattr(self, '_vla_kind', '?')}",
                        knobs=Knobs(body.get("knobs") or {}),
                        seed=int(body.get("seed", 0)),
                        budget=TrialBudget(**(body.get("budget") or {})),
                        store=self._store(slug),
                        skip_reset=bool(body.get("skip_reset", False)))
        return {"trial_id": rec.trial_id, "outcome": rec.outcome, "verdict": rec.verdict,
                "instruction": instruction, "runtime": getattr(self, "_vla_kind", "?")}
