from typing import Optional
from pathlib import Path


def resolve_repo_path(repo: Path) -> Path:
    """Resolve the repository path."""
    return repo.resolve()


def resolve_canonical_repo_root(repo_path: Path) -> Path:
    """If ``repo_path`` is inside ``<root>/.remoroo/runs/<run_id>``, return ``<root>``.

    Prevents nested ``.remoroo/runs`` when the user runs from inside a prior run output
    directory (e.g. after ``cd`` into ``.remoroo/runs/abc``).
    """
    p = repo_path.resolve()
    cur = p if p.is_dir() else p.parent
    for d in (cur, *cur.parents):
        try:
            if d.name == "runs" and d.parent.name == ".remoroo":
                return d.parent.parent.resolve()
        except (IndexError, ValueError):
            pass
    return p


def resolve_out_dir(out: Optional[Path], repo_path: Path) -> Path:
    """Resolve a default output / scratch parent directory.

    Per-run artifacts are stored under ``<repo>/.remoroo/runs/<run_id>`` (see
    ``prepare_local_worker_context``). This default keeps CLI fallbacks (e.g. cancelled
    wizard) under the same repo tree instead of ``~/.cache/remoroo/runs``.
    """
    if out:
        return out.resolve()
    root = resolve_canonical_repo_root(repo_path)
    return (root / ".remoroo" / "runs").resolve()

