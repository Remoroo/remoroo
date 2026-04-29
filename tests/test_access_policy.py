"""Unit tests for ``remoroo.engine.access_policy``.

These tests exercise the policy layer in isolation. End-to-end LocalWorker
gating is covered by ``test_access_policy_e2e.py``.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Make the package importable when running from the repo root with bare pytest.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from remoroo.engine.access_policy import (  # noqa: E402  (after sys.path tweak)
    AccessPolicy,
    IGNORE_FILENAME,
    RECOMMENDED_DEFAULT_PATTERNS,
    append_to_ignore_file,
    build_sandbox_exec_profile,
    read_ignore_file,
    remove_from_ignore_file,
)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """Mini repo with a few real files we can feed to the policy."""
    (tmp_path / ".env").write_text("SECRET=1\n")
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "main.py").write_text("print('ok')\n")
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "train.csv").write_text("a,b\n1,2\n")
    (tmp_path / "secrets").mkdir()
    (tmp_path / "secrets" / "api.pem").write_text("-----BEGIN-----\n")
    return tmp_path


def write_ignore(repo_path: Path, *patterns: str) -> None:
    (repo_path / IGNORE_FILENAME).write_text("\n".join(patterns) + "\n")


# ── load() ──────────────────────────────────────────────────────────────────


def test_load_missing_file_yields_empty_policy(tmp_path: Path) -> None:
    p = AccessPolicy.load(str(tmp_path))
    assert not p.has_patterns()
    assert p.absolute_subpaths() == []
    assert p.host_bash_wrapper() is None


def test_load_strips_comments_and_blanks(repo: Path) -> None:
    write_ignore(repo, "# header", "", ".env", "  # indented comment", "secrets/")
    p = AccessPolicy.load(str(repo))
    assert p.patterns == (".env", "secrets/")


def test_load_tolerates_unreadable_file(repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_ignore(repo, ".env")
    real_open = open

    def boom(*args, **kwargs):  # noqa: ANN001 (signature mirrors built-in)
        if args and isinstance(args[0], str) and IGNORE_FILENAME in args[0]:
            raise OSError("nope")
        return real_open(*args, **kwargs)

    monkeypatch.setattr("builtins.open", boom)
    p = AccessPolicy.load(str(repo))
    assert not p.has_patterns()


# ── is_blocked() ────────────────────────────────────────────────────────────


def test_is_blocked_dir_pattern_matches_descendants(repo: Path) -> None:
    write_ignore(repo, "secrets/")
    p = AccessPolicy.load(str(repo))
    assert p.is_blocked("secrets")
    assert p.is_blocked("secrets/api.pem")
    assert p.is_blocked("secrets/nested/deeper.txt")
    assert not p.is_blocked("src/main.py")


def test_is_blocked_glob_matches_basename(repo: Path) -> None:
    write_ignore(repo, "*.pem")
    p = AccessPolicy.load(str(repo))
    assert p.is_blocked("secrets/api.pem")
    assert p.is_blocked("anywhere/else/here.pem")
    assert not p.is_blocked("secrets/api.txt")


def test_is_blocked_anchored_pattern_only_matches_root(repo: Path) -> None:
    (repo / "src" / "data").mkdir()
    (repo / "src" / "data" / "x.csv").write_text("x\n")
    write_ignore(repo, "/data/")
    p = AccessPolicy.load(str(repo))
    assert p.is_blocked("data/train.csv")
    assert not p.is_blocked("src/data/x.csv")


def test_is_blocked_resolves_symlinks(repo: Path) -> None:
    write_ignore(repo, "secrets/")
    link = repo / "shortcut"
    try:
        link.symlink_to(repo / "secrets")
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")
    p = AccessPolicy.load(str(repo))
    assert p.is_blocked("shortcut/api.pem")


def test_is_blocked_returns_false_for_outside_repo(repo: Path, tmp_path: Path) -> None:
    write_ignore(repo, "secrets/")
    p = AccessPolicy.load(str(repo))
    outside = tmp_path / "elsewhere.txt"
    assert not p.is_blocked(str(outside))


def test_empty_policy_blocks_nothing(repo: Path) -> None:
    p = AccessPolicy.load(str(repo))
    assert not p.is_blocked(".env")
    assert not p.is_blocked("secrets/api.pem")


# ── filter_paths() ──────────────────────────────────────────────────────────


def test_filter_paths_preserves_order(repo: Path) -> None:
    write_ignore(repo, ".env", "secrets/")
    p = AccessPolicy.load(str(repo))
    assert p.filter_paths(["src/a.py", ".env", "data/x.csv", "secrets/k"]) == [
        "src/a.py",
        "data/x.csv",
    ]


# ── absolute_subpaths() ─────────────────────────────────────────────────────


def test_absolute_subpaths_returns_only_existing(repo: Path) -> None:
    write_ignore(repo, ".env", "secrets/", "*.nonexistent")
    p = AccessPolicy.load(str(repo))
    paths = p.absolute_subpaths()
    assert os.path.join(str(repo), ".env") in paths
    assert os.path.join(str(repo), "secrets") in paths
    assert all(os.path.exists(pp) for pp in paths)


# ── literal_match_in_command() ──────────────────────────────────────────────


def test_literal_match_finds_blocked_path(repo: Path) -> None:
    write_ignore(repo, ".env", "secrets/")
    p = AccessPolicy.load(str(repo))
    assert p.literal_match_in_command("cat .env") == ".env"
    assert p.literal_match_in_command("ls secrets/foo") == "secrets/"


def test_literal_match_avoids_substring_collision(repo: Path) -> None:
    """Should not match 'data' inside 'datadog' / 'database'."""
    write_ignore(repo, "data/")
    p = AccessPolicy.load(str(repo))
    assert p.literal_match_in_command("pip install datadog") is None
    assert p.literal_match_in_command("psql -h database.host") is None
    assert p.literal_match_in_command("ls data/train.csv") == "data/"


def test_literal_match_glob_pattern(repo: Path) -> None:
    write_ignore(repo, "*.pem")
    p = AccessPolicy.load(str(repo))
    assert p.literal_match_in_command("openssl x509 -in api.pem -noout") == "*.pem"
    assert p.literal_match_in_command("echo hello") is None


def test_literal_match_returns_none_for_empty_policy(repo: Path) -> None:
    p = AccessPolicy.load(str(repo))
    assert p.literal_match_in_command("cat .env") is None


# ── host_bash_wrapper() / sandbox profile ───────────────────────────────────


def test_host_bash_wrapper_none_without_patterns(tmp_path: Path) -> None:
    p = AccessPolicy.load(str(tmp_path))
    assert p.host_bash_wrapper() is None


def test_host_bash_wrapper_none_when_paths_dont_exist(repo: Path) -> None:
    write_ignore(repo, "future-path-that-does-not-exist/")
    p = AccessPolicy.load(str(repo))
    assert p.host_bash_wrapper() is None


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS only sandbox-exec test")
def test_host_bash_wrapper_macos(repo: Path) -> None:
    write_ignore(repo, ".env")
    p = AccessPolicy.load(str(repo))
    wrap = p.host_bash_wrapper()
    assert wrap is not None
    wrapped = wrap("echo hi")
    assert wrapped.startswith("sandbox-exec")
    assert "echo hi" in wrapped


def test_build_sandbox_exec_profile_is_well_formed(repo: Path) -> None:
    blocked = [
        os.path.join(str(repo), ".env"),
        os.path.join(str(repo), "secrets"),
    ]
    profile = build_sandbox_exec_profile(str(repo), blocked)
    assert profile.startswith("(version 1)")
    assert "(allow default)" in profile
    assert "(deny" in profile
    assert ".env" in profile
    assert "secrets" in profile


# ── ignore-file mutation helpers ────────────────────────────────────────────


def test_append_creates_file_with_recommended_defaults(tmp_path: Path) -> None:
    added, already = append_to_ignore_file(str(tmp_path), ["data/"])
    assert "data/" in added
    assert already == []
    body = (tmp_path / IGNORE_FILENAME).read_text()
    # Recommended defaults are written on first creation.
    for default_pat in RECOMMENDED_DEFAULT_PATTERNS:
        assert default_pat in body


def test_append_is_idempotent(tmp_path: Path) -> None:
    append_to_ignore_file(str(tmp_path), ["data/"])
    added, already = append_to_ignore_file(str(tmp_path), ["data/", "logs/"])
    assert "data/" in already
    assert "logs/" in added


def test_remove_from_ignore_file(tmp_path: Path) -> None:
    append_to_ignore_file(str(tmp_path), ["data/", "logs/"])
    assert remove_from_ignore_file(str(tmp_path), "data/") is True
    assert remove_from_ignore_file(str(tmp_path), "data/") is False
    assert "data/" not in read_ignore_file(str(tmp_path))
    assert "logs/" in read_ignore_file(str(tmp_path))
