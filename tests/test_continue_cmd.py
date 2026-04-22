"""Tests for `remoroo_cli.remoroo.continue_cmd` (Stage 2, §8.2).

Pure-logic tests; every side effect (network, git) is mocked via the
DI hooks on `run_continue`.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import pytest

from remoroo.continue_cmd import (
    ContinueConfig,
    ContinueError,
    ContinueResult,
    artefact_url_from_receipt,
    resolve_receipt_url,
    run_continue,
)


# ── Recorders ──────────────────────────────────────────────────

class FetchRecorder:
    def __init__(self, responses: Dict[str, bytes]):
        self.responses = dict(responses)
        self.calls: List[str] = []

    def __call__(self, url: str) -> bytes:
        self.calls.append(url)
        if url not in self.responses:
            raise ContinueError(f"unexpected URL: {url}", code=99)
        out = self.responses[url]
        if isinstance(out, Exception):
            raise out  # type: ignore[misc]
        return out


class CloneRecorder:
    def __init__(self, scaffold_files: Optional[Dict[str, str]] = None):
        self.calls: List[tuple] = []
        self.scaffold_files = scaffold_files or {"README.md": "stock\n"}

    def __call__(self, repo_url: str, commit_sha: str, dest: Path) -> None:
        self.calls.append((repo_url, commit_sha, dest))
        dest.mkdir(parents=True, exist_ok=True)
        for rel, content in self.scaffold_files.items():
            (dest / rel).write_text(content, encoding="utf-8")


class ApplyRecorder:
    def __init__(self, *, fail: bool = False):
        self.calls: List[tuple] = []
        self.fail = fail

    def __call__(self, repo_dir: Path, diff_bytes: bytes) -> None:
        self.calls.append((repo_dir, diff_bytes))
        if self.fail:
            raise ContinueError("diff rejected", code=2)


# ── Receipt helpers ───────────────────────────────────────────

def make_receipt(**overrides: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "schema": "remoroo.try_now.receipt.v1",
        "rid": "abc123",
        "run_id": "run-xyz",
        "env": "BipedalWalker",
        "baseline": {"reward": 121, "clip": "baselines/v1/clip.mp4"},
        "target": 160,
        "best": {"reward": 181, "clip": "clips/best.mp4"},
        "experiments": [],
        "final_diff": "diffs/final.diff",
        "repo_base_commit": "deadbeef1234",
        "worker": {"type": "mock"},
        "wall_seconds": 120.0,
        "created_at": "2026-04-20T00:00:00Z",
        "resume_token": "",
        "urls": {
            "final_diff": "https://r2.example.com/sessions/abc123/diffs/final.diff",
            "checkpoint": "https://r2.example.com/sessions/abc123/checkpoint.pt",
        },
    }
    base.update(overrides)
    return base


# ── resolve_receipt_url ──────────────────────────────────────

def test_resolve_receipt_url_accepts_public_r_url():
    rid, url = resolve_receipt_url("https://remoroo.com/r/abc123")
    assert rid == "abc123"
    assert url.endswith("/api/try/receipts/abc123")


def test_resolve_receipt_url_accepts_api_url():
    rid, url = resolve_receipt_url("https://remoroo.com/api/try/receipts/xyz789")
    assert rid == "xyz789"
    assert "xyz789" in url


def test_resolve_receipt_url_accepts_bare_id():
    rid, url = resolve_receipt_url("abc1234567")
    assert rid == "abc1234567"
    assert "abc1234567" in url


def test_resolve_receipt_url_handles_query_string_and_fragment():
    rid, _ = resolve_receipt_url("https://remoroo.com/r/abc123?ref=twitter#watch")
    assert rid == "abc123"


def test_resolve_receipt_url_rejects_empty_and_garbage():
    with pytest.raises(ContinueError):
        resolve_receipt_url("")
    with pytest.raises(ContinueError):
        resolve_receipt_url("./local/path")
    with pytest.raises(ContinueError):
        resolve_receipt_url("@@@!!")


def test_resolve_receipt_url_uses_custom_template():
    rid, url = resolve_receipt_url(
        "abc1234567",
        receipt_url_template="https://cp.test/r/{rid}.json",
    )
    assert url == "https://cp.test/r/abc1234567.json"


# ── artefact_url_from_receipt ────────────────────────────────

def test_artefact_url_prefers_urls_block():
    r = make_receipt()
    assert artefact_url_from_receipt(r, kind="final_diff") == r["urls"]["final_diff"]
    assert artefact_url_from_receipt(r, kind="checkpoint") == r["urls"]["checkpoint"]


def test_artefact_url_falls_back_to_base_plus_relative():
    r = make_receipt()
    del r["urls"]
    r["artefact_base_url"] = "https://r2.example.com/sessions/abc123"
    assert (
        artefact_url_from_receipt(r, kind="final_diff")
        == "https://r2.example.com/sessions/abc123/diffs/final.diff"
    )


def test_artefact_url_none_when_no_source():
    r = make_receipt()
    del r["urls"]
    assert artefact_url_from_receipt(r, kind="checkpoint") is None


# ── run_continue: happy paths ────────────────────────────────

def test_run_continue_writes_clone_diff_and_program_md(tmp_path):
    receipt = make_receipt()
    diff_bytes = b"--- a/train.py\n+++ b/train.py\n@@ -1,1 +1,1 @@\n-old\n+new\n"
    fetch = FetchRecorder(
        {
            "https://remoroo.com/api/try/receipts/abc123": json.dumps(receipt).encode(),
            receipt["urls"]["final_diff"]: diff_bytes,
        }
    )
    clone = CloneRecorder()
    apply_ = ApplyRecorder()

    dest = tmp_path / "landed"
    cfg = ContinueConfig(input_url="https://remoroo.com/r/abc123", dest_dir=dest)
    result = run_continue(
        cfg,
        fetch_bytes_fn=fetch,
        clone_fn=clone,
        apply_fn=apply_,
    )
    assert isinstance(result, ContinueResult)
    assert result.rid == "abc123"
    assert result.env == "BipedalWalker"
    assert result.clone_dir == dest
    assert result.wrote_checkpoint is False
    assert len(result.warnings) == 0

    # clone called with the right URL + commit
    assert len(clone.calls) == 1
    repo_url, commit, called_dest = clone.calls[0]
    assert "bipedalwalker" in repo_url.lower()
    assert commit == "deadbeef1234"
    assert called_dest == dest

    # apply called with the diff bytes
    assert len(apply_.calls) == 1
    assert apply_.calls[0][1] == diff_bytes

    # program.md lives in the clone
    pm = (dest / "program.md").read_text(encoding="utf-8")
    assert "BipedalWalker" in pm
    assert "mean_reward" in pm


def test_run_continue_with_checkpoint_writes_file(tmp_path):
    receipt = make_receipt()
    diff_bytes = b"--- a/x\n+++ b/x\n"
    ck_bytes = b"\x80\x02torch_blob"
    fetch = FetchRecorder({
        "https://remoroo.com/api/try/receipts/abc123": json.dumps(receipt).encode(),
        receipt["urls"]["final_diff"]: diff_bytes,
        receipt["urls"]["checkpoint"]: ck_bytes,
    })
    dest = tmp_path / "landed"
    cfg = ContinueConfig(
        input_url="https://remoroo.com/r/abc123",
        dest_dir=dest,
        with_checkpoint=True,
    )
    result = run_continue(
        cfg,
        fetch_bytes_fn=fetch,
        clone_fn=CloneRecorder(),
        apply_fn=ApplyRecorder(),
    )
    assert result.wrote_checkpoint is True
    ck_path = dest / "checkpoint.pt"
    assert ck_path.exists()
    assert ck_path.read_bytes() == ck_bytes
    # No partial tmp left behind.
    assert not (dest / "checkpoint.pt.partial").exists()


def test_run_continue_with_checkpoint_warns_when_url_missing(tmp_path):
    receipt = make_receipt()
    receipt["urls"] = {"final_diff": receipt["urls"]["final_diff"]}
    diff_bytes = b"--- a/x\n+++ b/x\n"
    fetch = FetchRecorder({
        "https://remoroo.com/api/try/receipts/abc123": json.dumps(receipt).encode(),
        receipt["urls"]["final_diff"]: diff_bytes,
    })
    dest = tmp_path / "landed"
    cfg = ContinueConfig(
        input_url="https://remoroo.com/r/abc123",
        dest_dir=dest,
        with_checkpoint=True,
    )
    result = run_continue(
        cfg,
        fetch_bytes_fn=fetch,
        clone_fn=CloneRecorder(),
        apply_fn=ApplyRecorder(),
    )
    assert result.wrote_checkpoint is False
    assert any("checkpoint" in w.lower() for w in result.warnings)
    assert not (dest / "checkpoint.pt").exists()


# ── run_continue: error surfaces ──────────────────────────────

def test_run_continue_fails_on_malformed_receipt(tmp_path):
    fetch = FetchRecorder({
        "https://remoroo.com/api/try/receipts/abc123": b"not json",
    })
    cfg = ContinueConfig(
        input_url="https://remoroo.com/r/abc123",
        dest_dir=tmp_path / "landed",
    )
    with pytest.raises(ContinueError) as exc:
        run_continue(
            cfg,
            fetch_bytes_fn=fetch,
            clone_fn=CloneRecorder(),
            apply_fn=ApplyRecorder(),
        )
    assert "JSON" in exc.value.message


def test_run_continue_fails_on_missing_env(tmp_path):
    r = make_receipt(env="")
    fetch = FetchRecorder({
        "https://remoroo.com/api/try/receipts/abc123": json.dumps(r).encode(),
    })
    cfg = ContinueConfig(
        input_url="https://remoroo.com/r/abc123",
        dest_dir=tmp_path / "landed",
    )
    with pytest.raises(ContinueError):
        run_continue(
            cfg,
            fetch_bytes_fn=fetch,
            clone_fn=CloneRecorder(),
            apply_fn=ApplyRecorder(),
        )


def test_run_continue_fails_on_missing_base_commit(tmp_path):
    r = make_receipt(repo_base_commit="")
    fetch = FetchRecorder({
        "https://remoroo.com/api/try/receipts/abc123": json.dumps(r).encode(),
    })
    cfg = ContinueConfig(
        input_url="https://remoroo.com/r/abc123",
        dest_dir=tmp_path / "landed",
    )
    with pytest.raises(ContinueError):
        run_continue(
            cfg,
            fetch_bytes_fn=fetch,
            clone_fn=CloneRecorder(),
            apply_fn=ApplyRecorder(),
        )


def test_run_continue_fails_on_missing_final_diff_url(tmp_path):
    r = make_receipt()
    del r["urls"]  # no urls block and no artefact_base_url
    fetch = FetchRecorder({
        "https://remoroo.com/api/try/receipts/abc123": json.dumps(r).encode(),
    })
    cfg = ContinueConfig(
        input_url="https://remoroo.com/r/abc123",
        dest_dir=tmp_path / "landed",
    )
    with pytest.raises(ContinueError) as exc:
        run_continue(
            cfg,
            fetch_bytes_fn=fetch,
            clone_fn=CloneRecorder(),
            apply_fn=ApplyRecorder(),
        )
    assert "final_diff" in exc.value.message


def test_run_continue_rejects_nonempty_dest(tmp_path):
    r = make_receipt()
    diff_bytes = b"--- a/x\n+++ b/x\n"
    fetch = FetchRecorder({
        "https://remoroo.com/api/try/receipts/abc123": json.dumps(r).encode(),
        r["urls"]["final_diff"]: diff_bytes,
    })
    dest = tmp_path / "landed"
    dest.mkdir()
    (dest / "junk.txt").write_text("x")

    cfg = ContinueConfig(
        input_url="https://remoroo.com/r/abc123",
        dest_dir=dest,
    )
    with pytest.raises(ContinueError) as exc:
        run_continue(
            cfg,
            fetch_bytes_fn=fetch,
            clone_fn=CloneRecorder(),
            apply_fn=ApplyRecorder(),
        )
    assert "not empty" in exc.value.message


def test_run_continue_propagates_apply_failure_with_code_2(tmp_path):
    r = make_receipt()
    diff_bytes = b"--- a/x\n+++ b/x\n"
    fetch = FetchRecorder({
        "https://remoroo.com/api/try/receipts/abc123": json.dumps(r).encode(),
        r["urls"]["final_diff"]: diff_bytes,
    })
    cfg = ContinueConfig(
        input_url="https://remoroo.com/r/abc123",
        dest_dir=tmp_path / "landed",
    )
    with pytest.raises(ContinueError) as exc:
        run_continue(
            cfg,
            fetch_bytes_fn=fetch,
            clone_fn=CloneRecorder(),
            apply_fn=ApplyRecorder(fail=True),
        )
    assert exc.value.code == 2


def test_run_continue_rejects_empty_diff(tmp_path):
    r = make_receipt()
    fetch = FetchRecorder({
        "https://remoroo.com/api/try/receipts/abc123": json.dumps(r).encode(),
        r["urls"]["final_diff"]: b"",
    })
    cfg = ContinueConfig(
        input_url="https://remoroo.com/r/abc123",
        dest_dir=tmp_path / "landed",
    )
    with pytest.raises(ContinueError) as exc:
        run_continue(
            cfg,
            fetch_bytes_fn=fetch,
            clone_fn=CloneRecorder(),
            apply_fn=ApplyRecorder(),
        )
    assert "empty" in exc.value.message.lower()


# ── end-to-end with real git apply on a scratch repo ──────────

def test_default_apply_fn_applies_to_real_git_repo(tmp_path):
    """Exercises the real `git apply` to catch shell/encoding regressions."""
    from remoroo.continue_cmd import default_apply_fn

    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "t@t"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=repo, check=True)
    (repo / "a.txt").write_text("old\n")
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "init"], cwd=repo, check=True)

    diff = (
        b"diff --git a/a.txt b/a.txt\n"
        b"--- a/a.txt\n"
        b"+++ b/a.txt\n"
        b"@@ -1 +1 @@\n"
        b"-old\n"
        b"+new\n"
    )
    default_apply_fn(repo, diff)
    assert (repo / "a.txt").read_text() == "new\n"


def test_default_apply_fn_raises_on_reject(tmp_path):
    from remoroo.continue_cmd import default_apply_fn

    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "t@t"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=repo, check=True)
    (repo / "a.txt").write_text("wrong content\n")
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "init"], cwd=repo, check=True)

    diff = (
        b"diff --git a/a.txt b/a.txt\n"
        b"--- a/a.txt\n"
        b"+++ b/a.txt\n"
        b"@@ -1 +1 @@\n"
        b"-old\n"
        b"+new\n"
    )
    with pytest.raises(ContinueError) as exc:
        default_apply_fn(repo, diff)
    assert exc.value.code == 2
