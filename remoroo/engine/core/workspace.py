"""Workspace-wide venv placement + ignore list.

Single source of truth for:
  1. Where the CLI creates / looks for its managed virtual environments.
     These live OUTSIDE the user's repo so repo walks (grep, indexer,
     ripgrep, ls) never traverse megabytes of site-packages.
  2. Which directories CLI/brain tools should skip when walking the
     repo, regardless of whether the user's `.gitignore` covers them.

Kept in `remoroo_cli/remoroo/engine/core/` so both the CLI's local
worker and anything the brain imports through the execution contract
can reach the same constants without a circular dep.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Tuple


WORKSPACE_IGNORE_DIRS: Tuple[str, ...] = (
    # VCS
    ".git",
    ".hg",
    ".svn",
    # Python venvs (the whole reason this module exists)
    "venv",
    ".venv",
    "env",
    ".env-venv",
    # Python caches / build artefacts
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".nox",
    "dist",
    "build",
    "*.egg-info",
    # JS
    "node_modules",
    ".next",
    ".nuxt",
    ".turbo",
    # IDE / tooling
    ".idea",
    ".vscode",
    ".cursor",
    # Remoroo's own stuff
    ".remoroo_venvs",
    ".remoroo_cache",
)
"""Directory names the CLI's repo-walking tools MUST skip.

Glob-style names; patterns containing `*` are interpreted as globs by
callers (ripgrep `-g !pattern`, fnmatch, etc). Names without `*` match
any directory with that basename at any depth.

Keep this list conservative — only entries that are unambiguously
"never source code" belong here. Anything a user might legitimately
want to grep stays off the list.
"""


def rg_ignore_globs() -> Tuple[str, ...]:
    """Return the ignore list formatted as ripgrep `-g` arguments.

    Each entry becomes a pair ``("-g", "!<pattern>/")`` so the caller
    can splat it straight into an `rg` argv.
    """
    args: list[str] = []
    for name in WORKSPACE_IGNORE_DIRS:
        pattern = name if "*" in name else f"{name}/"
        args.append("-g")
        args.append(f"!{pattern}")
    return tuple(args)


# ── Managed-venv location ───────────────────────────────────────────
#
# The CLI USED to create venvs at ``{repo_root}/venv``. That leaked
# hundreds of MB of site-packages into the user's repo, confused
# ripgrep (blew up LLM context when an agent grep'd the repo), and
# sometimes shadowed the user's own `.venv/` with a stale interpreter.
#
# The fix: the CLI's managed venvs live OUTSIDE the repo, keyed by
# the repo's absolute path so two checkouts of the same project get
# distinct envs. Users can override via REMOROO_VENV_ROOT for CI or
# homedir-less containers.

_DEFAULT_ROOT_NAME = "remoroo/venvs"


def _default_cache_root() -> str:
    override = os.environ.get("REMOROO_VENV_ROOT")
    if override:
        return override
    xdg = os.environ.get("XDG_CACHE_HOME")
    base = xdg if xdg else os.path.join(os.path.expanduser("~"), ".cache")
    return os.path.join(base, _DEFAULT_ROOT_NAME)


def managed_venv_path(repo_root: str) -> str:
    """Return the CLI-managed venv path for ``repo_root``.

    Path is deterministic: ``<cache_root>/<basename>-<sha1[:16]>/``.
    Including the basename keeps the directory human-scannable when
    users `ls` the cache; the hash guarantees uniqueness across
    multiple checkouts of the same-named repo.
    """
    abs_root = os.path.abspath(repo_root)
    key = hashlib.sha1(abs_root.encode("utf-8")).hexdigest()[:16]
    basename = os.path.basename(abs_root.rstrip(os.sep)) or "repo"
    return os.path.join(_default_cache_root(), f"{basename}-{key}")


def managed_venv_python(repo_root: str) -> str:
    """Return the absolute path to the managed venv's python executable.

    Does NOT check whether the venv exists — callers decide whether to
    create it or fall back.
    """
    vp = managed_venv_path(repo_root)
    if os.name == "nt":
        return os.path.join(vp, "Scripts", "python.exe")
    return os.path.join(vp, "bin", "python")


def is_in_repo_venv_dir(path: str) -> bool:
    """True if ``path`` is a plausible in-repo venv dir name.

    Used by VenvSandbox to decide whether to emit a "found a venv we
    don't own" warning. We key on the basename rather than contents
    because a busted/incomplete venv still has the directory shape.
    """
    name = os.path.basename(path.rstrip(os.sep))
    return name in {"venv", ".venv", "env"} and os.path.isdir(path)


# ── Recommended .gitignore entries ──────────────────────────────────
#
# The CLI appends these to the user's ``.gitignore`` on first run (see
# run_local.py). The list is intentionally conservative — only
# directories that are essentially universal build/cache bloat. IDE
# dirs (``.idea/``, ``.vscode/``) are left out because teams disagree
# on whether those belong in version control.

RECOMMENDED_GITIGNORE_ENTRIES: Tuple[str, ...] = (
    ".remoroo/",
    # Bootstrap / init artifacts (seed catalog staged by `remoroo init`).
    # Kept separate from .remoroo/ which holds run state.
    ".remoroo_init/",
    # Python venvs
    "venv/",
    ".venv/",
    "env/",
    # Python caches / build artefacts
    "__pycache__/",
    "*.pyc",
    "*.pyo",
    ".mypy_cache/",
    ".pytest_cache/",
    ".ruff_cache/",
    ".tox/",
    ".nox/",
    "*.egg-info/",
    # Node
    "node_modules/",
    # OS junk
    ".DS_Store",
    # experiment log needs to be ignored so that revert commits does not wipe it out
    "results.tsv",
    "Thumbs.db",
)

GITIGNORE_BLOCK_HEADER = "# Remoroo CLI — recommended ignores"
"""Marker comment at the top of the block we append. Finding this
string in an existing .gitignore is how we detect that the block is
already present and skip re-append (idempotency)."""


def missing_gitignore_entries(existing: str) -> Tuple[str, ...]:
    """Return the subset of `RECOMMENDED_GITIGNORE_ENTRIES` not yet in
    ``existing`` (the full .gitignore text).

    Matching is line-based after stripping whitespace and trailing
    comments. Entries with a leading ``!`` (negations) or leading
    whitespace are treated as distinct from our candidates — if a
    user has ``!venv/`` we still consider ``venv/`` missing, because
    we only want to match exact positive entries.
    """
    present: set[str] = set()
    for raw in existing.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # Drop inline comments e.g. `venv/   # my ignore`
        if "#" in line:
            line = line.split("#", 1)[0].strip()
        present.add(line)
    return tuple(e for e in RECOMMENDED_GITIGNORE_ENTRIES if e not in present)
