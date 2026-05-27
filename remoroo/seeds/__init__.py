"""Bundled seed catalog (CLI package data).

Seeds are real ``program.md`` examples copied from existing Remoroo
autoresearch workflows (autoresearch/, RooDojo/). They ship with the CLI
pip package and are staged on the operator's machine — by the CLI — at
``remoroo init`` start. The brain has no role in serving or writing them:
file mechanics are CLI territory because the CLI is the side with
filesystem access to the operator's repo.

To add a new seed:
    1. Drop ``<name>.md`` into ``remoroo_cli/remoroo/seeds/<category>/``.
    2. Update ``cli_seed_categories`` on the relevant ``GoalAlias`` (see
       ``remoroo_brain/prompts/goal_aliases.py``).
    3. Bump the CLI version and republish — seeds are bundled, not
       fetched at runtime.
"""
from __future__ import annotations

from pathlib import Path

_SEEDS_ROOT = Path(__file__).parent


class SeedNotFoundError(FileNotFoundError):
    """Raised when ``load_seed`` cannot locate the requested template."""


def seeds_root() -> Path:
    """Return the absolute path of the bundled seed catalog."""
    return _SEEDS_ROOT


def load_seed(category: str, name: str) -> str:
    """Return the textual contents of a bundled seed template.

    Parameters
    ----------
    category:
        Subdirectory under ``remoroo_cli/remoroo/seeds/`` (e.g.
        ``"program_md"``).
    name:
        Bare template name without extension (e.g. ``"speech_asr"``).

    Raises
    ------
    SeedNotFoundError
        If neither ``<category>/<name>.md`` nor ``<category>/<name>``
        exists, or if either argument tries to escape the seed root.
    """
    if not category or "/" in category or ".." in category:
        raise SeedNotFoundError(f"Invalid seed category: {category!r}")
    if not name or "/" in name or ".." in name:
        raise SeedNotFoundError(f"Invalid seed name: {name!r}")

    base = _SEEDS_ROOT / category
    for path in (base / f"{name}.md", base / name):
        if path.is_file():
            return path.read_text(encoding="utf-8")
    raise SeedNotFoundError(f"Seed not found: {category}/{name}")


def list_seeds(category: str) -> list[str]:
    """Return the bare names of every seed in ``category`` (alphabetical).

    The catalog ``README.md`` is excluded from the listing so callers can
    iterate seeds without filtering.
    """
    base = _SEEDS_ROOT / category
    if not base.is_dir():
        return []
    return sorted(p.stem for p in base.glob("*.md") if p.name != "README.md")
