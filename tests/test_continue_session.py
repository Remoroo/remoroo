"""`remoroo run --continue-from`: a new run seeded with a finished run's conversation.

The one outcome these tests exist to prevent is a continue that silently cold-starts and
pays to redo the whole session: every refusal is loud, and the seeded checkpoint must pass
the brain's OWN restore (checked against remoroo_brain when it is importable).
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from remoroo.continue_session import (
    NOTE_HEADER,
    ContinueError,
    load_prior,
    restorable,
    seed,
    watch_for_restore,
    write_seed,
)

GOAL, METRIC = "@task_author resolved goal", "gates_report_failed"


def _prior(tmp_path: Path, run_id: str = "627b4eb5", **over) -> Path:
    doc = {
        "run_id": run_id,
        "goal": "old goal",
        "original_goal": "old goal",
        "metric": "old metric",
        "status": "completed",
        "protocol": {"terminal": True, "goal_met": False, "last_verdict": "partial",
                     "understood": True},
        "budget": {"actions_used": 13, "tokens_used": 505563},
        "history": [
            {"role": "system", "content": "You are Remoroo"},
            {"role": "user", "content": "## Task\nGoal: author the task"},
            {"role": "assistant", "content": "", "tool_calls": [{"id": "t1", "function": {
                "name": "done", "arguments": "{}"}}]},
            {"role": "tool", "content": "done", "tool_call_id": "t1", "name": "done"},
        ],
    }
    doc.update(over)
    p = tmp_path / ".remoroo" / "runs" / run_id / "checkpoint.json"
    p.parent.mkdir(parents=True)
    p.write_text(json.dumps(doc))
    return p


def test_seed_is_what_the_brain_restores(tmp_path):
    _prior(tmp_path)
    doc = seed(load_prior(tmp_path, "627b4eb5"), run_id="new00001", goal=GOAL, metric=METRIC,
               note="the probe is fixed; run prove")
    assert restorable(doc, goal=GOAL, metric=METRIC) is None
    assert doc["run_id"] == "new00001" and doc["continued_from"] == "627b4eb5"
    assert doc["original_goal"] == doc["goal"] == doc["active_task_goal"] == GOAL
    assert doc["protocol"]["terminal"] is False and "last_verdict" not in doc["protocol"]
    assert doc["protocol"]["understood"] is True           # earned progress is kept
    assert doc["history"][-1]["role"] == "user"
    assert doc["history"][-1]["content"].startswith(NOTE_HEADER)
    assert len(doc["history"]) == 5


def test_refusals_are_loud_never_a_cold_start(tmp_path):
    with pytest.raises(ContinueError, match="no checkpoint"):
        load_prior(tmp_path, "missing1")
    _prior(tmp_path, "emptyhis", history=[])
    with pytest.raises(ContinueError, match="no conversation"):
        load_prior(tmp_path, "emptyhis")
    with pytest.raises(ContinueError, match="needs a run id"):
        load_prior(tmp_path, "../etc")
    _prior(tmp_path)
    with pytest.raises(ContinueError, match="needs a note"):
        seed(load_prior(tmp_path, "627b4eb5"), run_id="n", goal=GOAL, metric=METRIC, note=" ")
    with pytest.raises(ContinueError, match="no resolved goal"):
        seed(load_prior(tmp_path, "627b4eb5"), run_id="n", goal="", metric=METRIC, note="x")


def test_restorable_names_each_brain_condition(tmp_path):
    _prior(tmp_path)
    doc = seed(load_prior(tmp_path, "627b4eb5"), run_id="n", goal=GOAL, metric=METRIC, note="x")
    assert "metric" in restorable(doc, goal=GOAL, metric="other")
    assert "goal" in restorable(doc, goal="other", metric=METRIC)
    assert "terminal" in restorable({**doc, "protocol": {"terminal": True}}, goal=GOAL,
                                    metric=METRIC)


def test_write_seed_lands_under_the_new_run(tmp_path):
    out = tmp_path / ".remoroo" / "runs" / "new00001"
    p = write_seed(out, {"run_id": "new00001", "history": [{"role": "user", "content": "x"}]})
    assert p == out / "checkpoint.json" and json.loads(p.read_text())["run_id"] == "new00001"


def test_watch_aborts_when_the_brain_starts_cold():
    aborted = []
    t = watch_for_restore(lambda: [{"kind": "status"}], aborted.append, timeout_s=0.3,
                          poll_s=0.05, log=lambda m: None)
    t.join(2)
    assert aborted == ["continue_not_restored"]


def test_watch_stands_down_on_run_resumed():
    aborted, seen = [], []

    def events():
        seen.append(1)
        return [{"kind": "run_resumed"}] if len(seen) > 2 else []

    t = watch_for_restore(events, aborted.append, timeout_s=5, poll_s=0.02, log=lambda m: None)
    t.join(3)
    assert not t.is_alive() and aborted == []


def test_the_brains_own_restore_accepts_the_seed(tmp_path):
    """Against remoroo_brain's AgentLoop itself when this environment can import it."""
    agent_loop = pytest.importorskip("remoroo_brain.v2.agent_loop")
    events_mod = pytest.importorskip("remoroo_brain.v2.events")
    _prior(tmp_path)
    doc = seed(load_prior(tmp_path, "627b4eb5"), run_id="new00001", goal=GOAL, metric=METRIC,
               note="the probe is fixed; run prove")

    class Store:
        def load_checkpoint(self):
            return json.loads(json.dumps(doc))

        def load_run_state(self):
            return None

    got = []
    loop = agent_loop.AgentLoop(config={"repo_root": str(tmp_path)}, transport=None,
                                run_id="new00001", event_callback=got.append)
    loop._persistence = Store()
    assert loop._load_and_restore_checkpoint(GOAL, METRIC) is True
    assert any(isinstance(e, events_mod.RunResumed) for e in got)
    assert loop.protocol.terminal is False
    assert loop._build_context_view()[-1].content.startswith(NOTE_HEADER)
