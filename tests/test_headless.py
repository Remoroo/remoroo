"""Tests for `remoroo_cli.remoroo.run_local._headless_step_loop` et al.

Stage 2 of the Try-Now plan (§5.5). These tests only exercise the pure
poll/handle/submit state machine; the CLI wrapper and
`prepare_local_worker_context` are covered by the existing TUI path
tests and smoke-run.
"""
from __future__ import annotations

import io
import json
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

import pytest

from remoroo.run_local import (
    HeadlessLoopOutcome,
    HeadlessLoopStats,
    _headless_step_loop,
)


# ── Fakes mirroring the real ExecutionRequest / ExecutionResult shapes ──

@dataclass
class FakeStep:
    type: str
    payload: Optional[dict] = None


@dataclass
class FakeResult:
    data: Any = None


class FakePoller:
    def __init__(self, steps: List[Tuple[Optional[FakeStep], Any, Any]]):
        self.steps = list(steps)
        self.calls: List[Tuple[float, str]] = []

    def __call__(self, timeout: float, run_id: str):
        self.calls.append((timeout, run_id))
        if not self.steps:
            return None, None, None
        return self.steps.pop(0)


class FakeHandler:
    def __init__(self, results: Optional[List[FakeResult]] = None):
        self.results = list(results) if results else []
        self.calls: List[FakeStep] = []

    def __call__(self, step: FakeStep) -> FakeResult:
        self.calls.append(step)
        if self.results:
            r = self.results.pop(0)
            if isinstance(r, Exception):
                raise r
            return r
        return FakeResult(data={"echo": step.type})


class FakeSubmitter:
    def __init__(self, fail_first: int = 0):
        self.fail_first = fail_first
        self.calls: List[FakeResult] = []

    def __call__(self, result: FakeResult) -> None:
        self.calls.append(result)
        if self.fail_first > 0:
            self.fail_first -= 1
            raise RuntimeError("submit boom")


# ── Happy path ───────────────────────────────────────────────────

def test_loop_returns_outcome_on_workflow_complete():
    terminal = FakeStep(
        type="workflow_complete",
        payload={"outcome": "SUCCESS", "success": True},
    )
    poller = FakePoller([(terminal, None, None)])
    handler = FakeHandler()
    submitter = FakeSubmitter()
    log = io.StringIO()

    outcome, stats = _headless_step_loop(
        run_id="r1",
        poll_fn=poller,
        handle_fn=handler,
        submit_fn=submitter,
        log_stream=log,
    )

    assert outcome.outcome == "SUCCESS"
    assert outcome.success is True
    assert outcome.partial_success is False
    assert outcome.step_type == "workflow_complete"
    # A terminal step is an announcement: never executed, never acked.
    # Acking it made the CP re-derive the run outcome from a failed
    # ExecutionResult and flip a SUCCESS run to FAILED/EXECUTION.
    assert stats.steps_handled == 0
    assert stats.polls == 1
    assert submitter.calls == []
    assert handler.calls == []


def test_loop_handles_non_terminal_steps_before_completion():
    steps = [
        (FakeStep("bash_exec"), None, None),
        (FakeStep("assistant_message"), None, None),
        (
            FakeStep(
                "workflow_complete",
                payload={"outcome": "PARTIAL_SUCCESS", "partial_success": True},
            ),
            None,
            None,
        ),
    ]
    poller = FakePoller(steps)
    handler = FakeHandler()
    submitter = FakeSubmitter()

    outcome, stats = _headless_step_loop(
        run_id="r1",
        poll_fn=poller,
        handle_fn=handler,
        submit_fn=submitter,
        log_stream=io.StringIO(),
    )

    assert outcome.outcome == "PARTIAL_SUCCESS"
    assert outcome.success is False
    assert outcome.partial_success is True
    assert stats.steps_handled == 2  # the terminal step is not work
    assert stats.last_step_type == "workflow_complete"


def test_loop_skips_none_steps():
    steps = [
        (None, None, None),
        (None, None, None),
        (FakeStep("workflow_complete", {"outcome": "SUCCESS", "success": True}), None, None),
    ]
    poller = FakePoller(steps)
    handler = FakeHandler()
    submitter = FakeSubmitter()

    outcome, stats = _headless_step_loop(
        run_id="r1",
        poll_fn=poller,
        handle_fn=handler,
        submit_fn=submitter,
        log_stream=io.StringIO(),
    )

    assert outcome.success is True
    assert stats.polls == 3
    assert stats.steps_handled == 0  # the terminal step is not handed to the worker


# ── Error-isolation paths ───────────────────────────────────────-

def test_loop_counts_poll_errors_and_keeps_going():
    class ExplodingPoller:
        def __init__(self):
            self.n = 0

        def __call__(self, timeout, run_id):
            self.n += 1
            if self.n == 1:
                raise RuntimeError("poll boom")
            return (
                FakeStep("workflow_complete", {"outcome": "SUCCESS", "success": True}),
                None,
                None,
            )

    poller = ExplodingPoller()
    outcome, stats = _headless_step_loop(
        run_id="r1",
        poll_fn=poller,
        handle_fn=FakeHandler(),
        submit_fn=FakeSubmitter(),
        log_stream=io.StringIO(),
    )
    assert stats.poll_errors == 1
    assert outcome.outcome == "SUCCESS"


def test_loop_counts_submit_failures_and_continues_to_terminal():
    steps = [
        (FakeStep("bash_exec"), None, None),
        (FakeStep("workflow_complete", {"outcome": "SUCCESS", "success": True}), None, None),
    ]
    submitter = FakeSubmitter(fail_first=1)
    outcome, stats = _headless_step_loop(
        run_id="r1",
        poll_fn=FakePoller(steps),
        handle_fn=FakeHandler(),
        submit_fn=submitter,
        log_stream=io.StringIO(),
    )
    assert stats.submit_failures == 1
    assert outcome.outcome == "SUCCESS"


def test_loop_exits_on_handler_exception_with_unknown():
    steps = [(FakeStep("bash_exec"), None, None)]
    handler = FakeHandler(results=[RuntimeError("handler boom")])  # type: ignore[list-item]
    outcome, stats = _headless_step_loop(
        run_id="r1",
        poll_fn=FakePoller(steps),
        handle_fn=handler,
        submit_fn=FakeSubmitter(),
        log_stream=io.StringIO(),
    )
    assert outcome.outcome == "UNKNOWN"
    assert outcome.success is False
    assert stats.steps_handled == 0


# ── Terminal-step taxonomy ──────────────────────────────────────

@pytest.mark.parametrize("t", ["workflow_complete", "workflow_error", "run_complete"])
def test_each_terminal_step_type_ends_loop(t):
    steps = [(FakeStep(t, {"outcome": "FAILED", "success": False}), None, None)]
    outcome, _stats = _headless_step_loop(
        run_id="r1",
        poll_fn=FakePoller(steps),
        handle_fn=FakeHandler(),
        submit_fn=FakeSubmitter(),
        log_stream=io.StringIO(),
    )
    assert outcome.step_type == t
    assert outcome.outcome == "FAILED"


def test_max_iterations_exits_even_without_terminal_step():
    # Keep feeding None forever; cap should protect the suite.
    class InfinitePoller:
        def __call__(self, timeout, run_id):
            return None, None, None

    outcome, stats = _headless_step_loop(
        run_id="r1",
        poll_fn=InfinitePoller(),
        handle_fn=FakeHandler(),
        submit_fn=FakeSubmitter(),
        log_stream=io.StringIO(),
        max_iterations=4,
    )
    assert outcome.outcome == "UNKNOWN"
    assert stats.polls == 4  # loop polls exactly `max_iterations` times, then exits


def test_terminal_step_is_not_executed_or_acked():
    """Regression: run be6f3cfc finished SUCCESS but was stored FAILED.

    The loop handed `workflow_complete` to the worker, which has no handler
    for it and acked `success=False`; the CP then re-derived the run outcome
    from that ack and overwrote the SUCCESS it had already written.
    """
    payload = {"outcome": "SUCCESS", "success": True, "metrics": {"pass": 1.0}}
    handler = FakeHandler()
    submitter = FakeSubmitter()
    log = io.StringIO()

    outcome, stats = _headless_step_loop(
        run_id="be6f3cfc",
        poll_fn=FakePoller([(FakeStep("workflow_complete", payload), None, None)]),
        handle_fn=handler,
        submit_fn=submitter,
        log_stream=log,
    )

    assert handler.calls == []
    assert submitter.calls == []
    assert stats.steps_handled == 0
    assert outcome.success is True
    # the brain's payload reaches the session summary (metrics render from it)
    assert outcome.final_result == payload
    events = [json.loads(ln)["event"] for ln in log.getvalue().splitlines() if ln]
    assert "headless.terminal_step" in events


# ── Logging ─────────────────────────────────────────────────────

def test_log_lines_are_json_and_tagged_per_step():
    steps = [
        (FakeStep("bash_exec"), None, None),
        (FakeStep("workflow_complete", {"outcome": "SUCCESS", "success": True}), None, None),
    ]
    log = io.StringIO()
    _headless_step_loop(
        run_id="r1",
        poll_fn=FakePoller(steps),
        handle_fn=FakeHandler(),
        submit_fn=FakeSubmitter(),
        log_stream=log,
    )
    lines = [ln for ln in log.getvalue().splitlines() if ln]
    parsed = [json.loads(ln) for ln in lines]
    events = [r["event"] for r in parsed]
    assert "headless.step_handled" in events
    # Every record has component + ts
    for r in parsed:
        assert r["component"] == "remoroo.headless"
        assert "ts" in r
        assert r["run_id"] == "r1"


def test_logs_poll_error_without_raising():
    class ExplodingPoller:
        def __init__(self):
            self.n = 0

        def __call__(self, timeout, run_id):
            self.n += 1
            if self.n == 1:
                raise ValueError("nope")
            return (FakeStep("workflow_complete", {"outcome": "SUCCESS", "success": True}), None, None)

    log = io.StringIO()
    _headless_step_loop(
        run_id="r1",
        poll_fn=ExplodingPoller(),
        handle_fn=FakeHandler(),
        submit_fn=FakeSubmitter(),
        log_stream=log,
    )
    text = log.getvalue()
    assert "headless.poll_error" in text
    assert "ValueError" in text


def test_exit_code_for_result_matches_loop_outcome():
    from remoroo.tui_launch_config import exit_code_for_result

    assert exit_code_for_result(True, False) == 0
    assert exit_code_for_result(False, True) == 2
    assert exit_code_for_result(False, False) == 1
