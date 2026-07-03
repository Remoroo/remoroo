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
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .params import Knobs
from .perceive.bank import RegressionBank
from .record import TrialStore, replay_score
from .reset.reversibility import ReversibilityTable
from .run_trial import TrialBudget, run_trial
from .supervisor import Supervisor, SupervisorConfig

BRIDGE_VERBS = {"trial_run"}          # edge_real wraps these in its _bridge_lock


class TaskService:
    def __init__(self, *, data_dir: str = ".remoroo/task",
                 env_builder: Optional[Callable[[str], Any]] = None,
                 bridge: Optional[Any] = None) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.bank = RegressionBank.load(str(self.data_dir / "bank.jsonl"))
        self.reversibility = ReversibilityTable.load(str(self.data_dir / "reversibility.json"))
        self.supervisor: Optional[Supervisor] = None
        self._bridge = bridge
        self._env_builder = env_builder or self._default_env_builder
        self._stores: Dict[str, TrialStore] = {}

    # ---- plumbing --------------------------------------------------------------------
    def _store(self, task_slug: str) -> TrialStore:
        if task_slug not in self._stores:
            self._stores[task_slug] = TrialStore(str(self.data_dir / "trials" / task_slug))
        return self._stores[task_slug]

    def _default_env_builder(self, task_slug: str) -> Any:
        mod = importlib.import_module(f"remoroo_cell.task_{task_slug}")
        return mod.build_env(), getattr(mod, "actor")

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
