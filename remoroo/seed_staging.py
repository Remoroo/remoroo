"""Stage bundled seed catalogs onto the operator's machine.

The CLI is the side that owns filesystem access to the user's repo. When
``remoroo init`` (or any alias-driven run whose ``cli_seed_categories``
includes a category) starts, this module copies the relevant bundled
seeds from the CLI's pip package data into a hidden, gitignored
directory inside the repo:

    <repo>/.remoroo_init/seeds/<category>/<name>.md

The agent then reads these files via its normal ``read_file`` / ``bash``
tool calls — no transport gymnastics, no brain-side file mechanics. Each
run starts fresh: existing files at the destination are skipped (so we
never clobber an operator's hand-edited seed). The whole
``.remoroo_init/`` tree is added to ``.gitignore`` automatically by the
CLI's pre-flight gitignore step.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

from .seeds import list_seeds, load_seed, SeedNotFoundError

logger = logging.getLogger(__name__)


# Per the user's directive: stage under .remoroo_init/, not .remoroo/.
# Keeps init artifacts separate from the run-state directory.
SEED_STAGING_ROOT = ".remoroo_init/seeds"


@dataclass
class StageResult:
    requested: int
    staged: List[str]
    skipped: List[str]
    failed: List[str]


def stage_seed_categories(
    repo_path: Path,
    categories: Iterable[str],
) -> StageResult:
    """Copy every bundled seed in each category into the operator's repo.

    Parameters
    ----------
    repo_path:
        Absolute path to the operator's repo. The destination tree
        ``<repo_path>/.remoroo_init/seeds/`` is created if missing.
    categories:
        Iterable of seed category names (e.g. ``("program_md",)``).

    Returns
    -------
    StageResult
        Summary suitable for displaying in the launch banner.
    """
    repo_path = Path(repo_path).expanduser().resolve()
    requested = 0
    staged: List[str] = []
    skipped: List[str] = []
    failed: List[str] = []

    for category in categories:
        names = list_seeds(category)
        # The catalog README is staged too — it is the agent-facing
        # routing guide and must always be present alongside the seeds.
        all_names = list(names) + ["README"]
        requested += len(all_names)

        dest_dir = repo_path / SEED_STAGING_ROOT / category
        try:
            dest_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.error("seed staging: cannot create %s: %s", dest_dir, exc)
            failed.extend(f"{category}/{n}" for n in all_names)
            continue

        for name in all_names:
            dest = dest_dir / f"{name}.md"
            try:
                body = load_seed(category, name)
            except SeedNotFoundError as exc:
                logger.error("seed staging: %s/%s missing: %s", category, name, exc)
                failed.append(f"{category}/{name}")
                continue

            if dest.exists():
                # Never clobber an operator-edited seed.
                skipped.append(f"{category}/{name}")
                continue

            try:
                dest.write_text(body, encoding="utf-8")
            except OSError as exc:
                logger.error("seed staging: write failed %s: %s", dest, exc)
                failed.append(f"{category}/{name}")
                continue

            staged.append(f"{category}/{name}")

    return StageResult(
        requested=requested,
        staged=staged,
        skipped=skipped,
        failed=failed,
    )
