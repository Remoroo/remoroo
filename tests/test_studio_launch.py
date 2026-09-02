from pathlib import Path

from remoroo import studio_launch


def _studio(root: Path, *, bundled: bool) -> studio_launch.Studio:
    return studio_launch.Studio(
        dist=root / "dist",
        server_py=root / "studio_server.py",
        edge_py=root / "edge_real.py",
        build_dir=None if bundled else root,
    )


def test_find_studio_prefers_repo_source(monkeypatch, tmp_path):
    repo = _studio(tmp_path / "repo", bundled=False)
    bundled = _studio(tmp_path / "package", bundled=True)
    monkeypatch.setattr(studio_launch, "_repo", lambda: repo)
    monkeypatch.setattr(studio_launch, "_bundled", lambda: bundled)

    assert studio_launch.find_studio() == repo


def test_find_studio_falls_back_to_bundle_when_no_repo(monkeypatch, tmp_path):
    bundled = _studio(tmp_path / "package", bundled=True)
    monkeypatch.setattr(studio_launch, "_repo", lambda: None)
    monkeypatch.setattr(studio_launch, "_bundled", lambda: bundled)

    assert studio_launch.find_studio() == bundled
