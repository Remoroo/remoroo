"""Continue a finished agent session: a NEW run whose conversation is the old one's.

A finished run cannot be reopened -- its brain thread has exited, its transport is gone
and its billing is settled. What survives is the checkpoint the brain wrote on THIS machine
(`.remoroo/runs/<id>/checkpoint.json`): the whole conversation, every file read and tool
result, the run state. The server keeps no message history at all, so this file is the only
copy.

The brain already restores a conversation from that file at the start of every run
(remoroo_brain/v2/agent_loop.py::_load_and_restore_checkpoint), but only when the file sits
under the NEW run's id and its `metric` and `original_goal` equal the new run's -- otherwise
it silently starts cold. So continuing is: create the run, read back the goal and metrics the
server resolved for it, and write the prior checkpoint under the new id with those values,
the session marked not-finished, and the operator's note appended as the next user turn.

⚠ A continue that silently cold-starts is the one outcome worth refusing: it re-reads every
file and pays for the whole session again while looking like a continuation. Every check here
fails loudly, and `watch_for_restore` aborts the run if the brain does not report
`run_resumed`.
"""
from __future__ import annotations

import copy
import json
import threading
import time
from pathlib import Path
from typing import Any, Callable, Optional

#: The brain drops user messages that start with these from its context
#: (remoroo_brain/v2/compactor.py::_EPHEMERAL_PREFIXES) -- a note shaped like one would be
#: appended and then never seen.
_BRAIN_EPHEMERAL_PREFIXES = (
    "[Active Jobs]", "[Protocol]", "[Working Memory]", "[Run State]", "[Plan]",
    "[Recent Execution History]", "[Task Reminder]", "[PHASE:", "Budget check:",
    "BUDGET EXHAUSTED", "You have run the same command", "Hint: You haven't captured baseline",
    "Your last",
)

NOTE_HEADER = "[OPERATOR] This session is being CONTINUED."

#: The protocol flags a finished session carries that would end the continued one at once
#: (`terminal` makes the loop stop after its first tool batch).
_RESET_PROTOCOL = {"terminal": False, "goal_met": False}


class ContinueError(Exception):
    """A continue that cannot be made faithfully. Never downgraded to a cold start."""


def checkpoint_path(repo_path: Path, run_id: str) -> Path:
    return Path(repo_path) / ".remoroo" / "runs" / str(run_id) / "checkpoint.json"


def load_prior(repo_path: Path, run_id: str) -> dict[str, Any]:
    """The finished session's checkpoint, or ContinueError saying why it cannot be used."""
    rid = str(run_id or "").strip()
    if not rid or "/" in rid or rid.startswith("."):
        raise ContinueError(f"--continue-from needs a run id, got {run_id!r}")
    p = checkpoint_path(repo_path, rid)
    if not p.is_file():
        raise ContinueError(
            f"no checkpoint for run {rid} at {p}: the conversation to continue lives only in "
            "that file, and without it this would be a cold start")
    try:
        doc = json.loads(p.read_text())
    except (OSError, ValueError) as e:
        raise ContinueError(f"the checkpoint of run {rid} is unreadable ({e})") from e
    hist = doc.get("history") if isinstance(doc, dict) else None
    if not isinstance(hist, list) or not any(isinstance(m, dict) for m in hist):
        raise ContinueError(f"the checkpoint of run {rid} holds no conversation to continue")
    return doc


def note_message(note: str) -> dict[str, str]:
    """The operator's note as the next user turn, shaped so the brain keeps it in context."""
    text = (note or "").strip()
    if not text:
        raise ContinueError("a continue needs a note: what changed since the session "
                            "finished, and what to do now")
    content = f"{NOTE_HEADER}\n\n{text}"
    if content.startswith(_BRAIN_EPHEMERAL_PREFIXES):  # guarded by NOTE_HEADER, kept honest
        raise ContinueError("the note would be dropped by the brain's context filter")
    return {"role": "user", "content": content}


def seed(prior: dict[str, Any], *, run_id: str, goal: str, metric: str,
         note: str) -> dict[str, Any]:
    """The prior checkpoint, re-keyed so the brain restores it into run `run_id`.

    `goal` and `metric` must be the values the SERVER resolved for the new run (its
    `GET /runs/{id}`), because the brain compares them for equality before it restores."""
    if not goal:
        raise ContinueError("the new run has no resolved goal to restore against")
    doc = copy.deepcopy(prior)
    doc["run_id"] = str(run_id)
    doc["goal"] = goal
    doc["original_goal"] = goal
    doc["active_task_goal"] = goal
    doc["metric"] = metric
    doc["status"] = "running"
    doc["continued_from"] = str(prior.get("run_id") or "")
    proto = dict(doc.get("protocol") or {})
    proto.update(_RESET_PROTOCOL)
    proto.pop("last_verdict", None)
    doc["protocol"] = proto
    doc["history"] = [m for m in (doc.get("history") or []) if isinstance(m, dict)]
    doc["history"].append(note_message(note))
    return doc


def restorable(doc: dict[str, Any], *, goal: str, metric: str) -> Optional[str]:
    """The brain's own restore conditions, checked here first: None when it will restore,
    else the reason it would silently start cold."""
    if doc.get("metric") != metric:
        return f"metric {doc.get('metric')!r} != the run's {metric!r}"
    orig = doc.get("original_goal")
    if (orig if orig is not None else doc.get("goal")) != goal:
        return "the goal does not match the run's resolved goal"
    if not [m for m in doc.get("history") or [] if isinstance(m, dict)]:
        return "empty history"
    if (doc.get("protocol") or {}).get("terminal"):
        return "the session is still marked terminal"
    return None


def write_seed(run_output_dir: Path, doc: dict[str, Any]) -> Path:
    path = Path(run_output_dir) / "checkpoint.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(doc))
    tmp.replace(path)
    return path


def watch_for_restore(fetch_events: Callable[[], list[dict[str, Any]]],
                      abort: Callable[[str], None], *, timeout_s: float = 240.0,
                      poll_s: float = 5.0,
                      log: Callable[[str], None] = print) -> threading.Thread:
    """Abort the run unless the brain reports `run_resumed` within `timeout_s`.

    The brain restores (or declines to) before its first turn, so the event arrives within
    seconds of the worker's first heartbeat. Its absence means the brain started cold --
    the one outcome this whole path exists to prevent -- so the run is stopped rather than
    left to re-read everything at full price."""
    def run() -> None:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                events = fetch_events() or []
            except Exception:  # noqa: BLE001 -- a failed poll is retried, not a verdict
                events = []
            if any(isinstance(e, dict) and e.get("kind") == "run_resumed" for e in events):
                log("continue: the brain restored the earlier session")
                return
            time.sleep(poll_s)
        log(f"continue: the brain did not report run_resumed within {timeout_s:.0f}s -- it "
            "started cold; aborting the run instead of paying to redo the session")
        abort("continue_not_restored")

    t = threading.Thread(target=run, name="continue-restore-watch", daemon=True)
    t.start()
    return t
