"""End-to-end tests: ``LocalWorker.handle_request`` honours ``.remorooignore``.

Each test creates a small repo with a blocklist, instantiates ``LocalWorker``
in venv mode (no docker, no real subprocess for most cases), and dispatches
a single ``ExecutionRequest`` to verify the structured-tool gate.

Bash kernel sandboxing is *not* exercised here — that requires a working
sandbox-exec / bwrap / firejail and is platform-dependent. We only assert
that the literal pre-flight refuses commands that name a blocked path.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from remoroo.engine.local_worker import LocalWorker  # noqa: E402
from remoroo_core.protocol import ExecutionRequest  # noqa: E402


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """Mini repo with a blocklist that hides ``.env`` and ``secrets/``."""
    (tmp_path / ".env").write_text("SECRET=hunter2\n")
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "main.py").write_text("print('hi')\n")
    (tmp_path / "secrets").mkdir()
    (tmp_path / "secrets" / "api.pem").write_text("-----BEGIN-----\n")
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "train.csv").write_text("a,b\n1,2\n")
    (tmp_path / ".remorooignore").write_text(".env\nsecrets/\n")
    return tmp_path


@pytest.fixture
def worker(repo: Path, tmp_path_factory: pytest.TempPathFactory) -> LocalWorker:
    """LocalWorker in venv/in-place mode pointing at the fixture repo."""
    artifact = tmp_path_factory.mktemp("artifact")
    return LocalWorker(
        repo_root=str(repo),
        artifact_dir=str(artifact),
        engine="venv",
        in_place=True,
    )


def _send(worker: LocalWorker, type_: str, **payload: Any):
    req = ExecutionRequest(type=type_, payload=payload)
    return worker.handle_request(req)


# ── Read family ─────────────────────────────────────────────────────────────


def test_read_file_blocked_returns_not_found(worker: LocalWorker) -> None:
    res = _send(worker, "read_file", path=".env")
    assert res.success is True
    assert res.data == {"exists": False}


def test_read_file_unblocked_works(worker: LocalWorker) -> None:
    res = _send(worker, "read_file", path="src/main.py")
    assert res.success is True
    assert res.data.get("content", "").strip() == "print('hi')"


def test_file_exists_blocked(worker: LocalWorker) -> None:
    res = _send(worker, "file_exists", path="secrets/api.pem")
    assert res.success is True
    assert res.data == {"exists": False}


def test_file_exists_unblocked(worker: LocalWorker) -> None:
    res = _send(worker, "file_exists", path="src/main.py")
    assert res.success is True
    assert res.data == {"exists": True}


def test_is_data_file_blocked(worker: LocalWorker) -> None:
    res = _send(worker, "is_data_file", path=".env")
    assert res.success is True
    assert res.data == {"is_data_file": False}


# ── Listing family ──────────────────────────────────────────────────────────


def test_list_files_drops_blocked(worker: LocalWorker) -> None:
    res = _send(worker, "list_files", path=".")
    assert res.success is True
    names = {f["name"] for f in res.data["files"]}
    assert ".env" not in names
    assert "secrets" not in names
    assert "src" in names


def test_snapshot_files_drops_blocked(worker: LocalWorker) -> None:
    res = _send(worker, "snapshot_files")
    assert res.success is True
    paths = {f["path"] for f in res.data["files"]}
    # secrets/api.pem is non-source, so without filtering it would appear;
    # data/train.csv is non-source too and is *not* blocked, so it must remain.
    assert all(not p.startswith("secrets") for p in paths)
    assert any(p.startswith("data") for p in paths)


def test_list_repo_files_drops_blocked(worker: LocalWorker) -> None:
    res = _send(worker, "list_repo_files", path=".", max_depth=3)
    assert res.success is True
    tree = res.data["tree"]
    assert "secrets" not in tree
    assert "src" in tree


# ── Write family ────────────────────────────────────────────────────────────


def test_write_file_blocked(worker: LocalWorker) -> None:
    res = _send(worker, "write_file", path=".env", content="OOPS=1\n", target_scope="current")
    assert res.success is False
    assert res.data and res.data.get("blocked") is True


def test_write_file_unblocked(worker: LocalWorker, repo: Path) -> None:
    res = _send(worker, "write_file", path="notes.md", content="hi\n", target_scope="current")
    assert res.success is True
    assert (repo / "notes.md").read_text() == "hi\n"


def test_apply_patch_refuses_blocked_target(worker: LocalWorker) -> None:
    patch_proposal: Dict[str, Any] = {
        "patch_id": "x",
        "edits": [
            {"path": ".env", "kind": "replace", "old": "", "new": "EVIL=1\n"}
        ],
    }
    res = _send(worker, "apply_patch", patch_proposal=patch_proposal)
    assert res.success is False
    assert res.data and res.data.get("blocked") is True
    assert ".env" in res.data.get("blocked_paths", [])


# ── Bash literal pre-flight ─────────────────────────────────────────────────


def test_execute_command_refuses_blocked_path_in_command(worker: LocalWorker) -> None:
    res = _send(worker, "execute_command", command="cat .env")
    assert res.success is False
    assert res.data and res.data.get("blocked") is True
    assert res.data.get("pattern") == ".env"


def test_execute_command_allows_safe_command(worker: LocalWorker) -> None:
    # Run a no-op-ish shell command that doesn't touch blocked paths.
    # On macOS / Linux this should still execute (host engine, in_place).
    res = _send(worker, "execute_command", command="echo hello")
    # We don't strictly require success (sandbox plumbing varies), but the
    # gate must NOT mark it blocked.
    assert not (res.data or {}).get("blocked")


def test_first_run_auto_seeds_remorooignore(tmp_path: Path) -> None:
    """Worker startup creates .remorooignore with the recommended defaults
    when it's missing, so customers get day-one protection without reading docs.
    """
    artifact = tmp_path / "art"
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "src").mkdir()
    (repo / "src" / "main.py").write_text("x = 1\n")

    assert not (repo / ".remorooignore").exists()
    LocalWorker(
        repo_root=str(repo),
        artifact_dir=str(artifact),
        engine="venv",
        in_place=True,
    )
    assert (repo / ".remorooignore").exists()
    body = (repo / ".remorooignore").read_text()
    # Must seed with the secret-hiding defaults.
    for needed in (".env", "*.pem", "secrets/"):
        assert needed in body


def test_empty_remorooignore_disables_policy(tmp_path: Path) -> None:
    """Empty .remorooignore is the customer's explicit opt-out signal."""
    artifact = tmp_path / "art"
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "src").mkdir()
    (repo / "src" / "main.py").write_text("x = 1\n")
    (repo / ".remorooignore").write_text("")  # opt out

    w = LocalWorker(
        repo_root=str(repo),
        artifact_dir=str(artifact),
        engine="venv",
        in_place=True,
    )
    assert not w.access_policy.has_patterns()
    res = _send(w, "execute_command", command="cat .env")
    assert not (res.data or {}).get("blocked")


def test_run_command_async_refuses_blocked(worker: LocalWorker) -> None:
    res = _send(worker, "run_command_async", command="grep -r SECRET secrets/")
    assert res.success is False
    assert res.data and res.data.get("blocked") is True


def test_v2_exec_start_refuses_blocked(worker: LocalWorker) -> None:
    res = _send(
        worker,
        "v2_exec_start",
        command="python -c \"open('.env').read()\"",
        env={},
        metadata={},
    )
    assert res.success is False
    assert res.data and res.data.get("blocked") is True


def test_execute_plan_refuses_blocked_step(worker: LocalWorker) -> None:
    plan = {"setup": ["echo ok"], "main": ["cat secrets/api.pem"]}
    res = _send(worker, "execute_plan", command_plan=plan)
    assert res.success is False
    assert res.data and res.data.get("blocked") is True
