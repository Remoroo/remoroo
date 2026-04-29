"""``remoroo block / unblock / blocked`` — manage ``.remorooignore``.

Customer-facing surface for the access policy. All commands operate on the
``.remorooignore`` file at the repo root (resolved via the same logic as
``remoroo run`` so behaviour matches what the worker enforces at runtime).

See ``docs/access_policy_plan.md``.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import typer

from .engine.access_policy import (
    AccessPolicy,
    IGNORE_FILENAME,
    RECOMMENDED_DEFAULT_PATTERNS,
    append_to_ignore_file,
    ignore_file_path,
    read_ignore_file,
    remove_from_ignore_file,
)


# ── Suspicious-pattern guard ─────────────────────────────────────────────────
# These would block essentially everything; almost certainly a typo.
_SUSPICIOUS_PATTERNS = {"*", "/", "**", "**/*", "."}


def _resolve_repo_root(repo: Optional[Path]) -> Path:
    """Return the repo root we should write ``.remorooignore`` into.

    Falls back to ``Path.cwd()`` so calling ``remoroo block`` from anywhere
    inside the repo works the way customers expect.
    """
    if repo is None:
        return Path.cwd().resolve()
    return repo.resolve()


# ── block ───────────────────────────────────────────────────────────────────


def block(
    patterns: List[str] = typer.Argument(
        ...,
        help="One or more gitignore-style patterns to hide from the agent (e.g. 'data/' '*.pem').",
    ),
    repo: Optional[Path] = typer.Option(
        None,
        "--repo",
        "-r",
        help="Repo root (defaults to the current directory).",
        file_okay=False,
        dir_okay=True,
        exists=True,
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip confirmation when adding suspicious patterns like '*' or '/'.",
    ),
) -> None:
    """Hide one or more paths from the Remoroo agent.

    Appends the patterns to ``.remorooignore`` at the repo root. The file is
    created (with sensible defaults) on first invocation. Patterns use
    gitignore syntax — directory entries should end with ``/``.
    """
    repo_root = _resolve_repo_root(repo)
    cleaned: List[str] = []
    for raw in patterns:
        pat = raw.strip()
        if not pat:
            continue
        if pat in _SUSPICIOUS_PATTERNS and not yes:
            typer.secho(
                f"⚠️  Pattern '{pat}' would block essentially everything. "
                "Re-run with --yes if you really mean this.",
                fg=typer.colors.YELLOW,
            )
            raise typer.Exit(code=1)
        cleaned.append(pat)
    if not cleaned:
        typer.secho("No patterns provided.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    is_first_write = not Path(ignore_file_path(str(repo_root))).is_file()
    added, already = append_to_ignore_file(str(repo_root), cleaned)

    target = ignore_file_path(str(repo_root))
    rel_target = _try_relpath(target, repo_root)

    if is_first_write:
        typer.secho(f"📝 Created {rel_target}", fg=typer.colors.GREEN)
        defaults_added = [p for p in added if p in RECOMMENDED_DEFAULT_PATTERNS]
        if defaults_added:
            typer.echo(f"   Seeded with {len(defaults_added)} recommended default(s):")
            for p in defaults_added:
                typer.echo(f"     • {p}")

    user_added = [p for p in added if p not in RECOMMENDED_DEFAULT_PATTERNS]
    if user_added:
        verb = "Added" if not is_first_write else "Plus you blocked"
        typer.secho(f"🔒 {verb}:", fg=typer.colors.GREEN)
        for p in user_added:
            typer.echo(f"     • {p}")
    if already:
        typer.echo("   (already present, skipped):")
        for p in already:
            typer.echo(f"     • {p}")

    typer.echo("")
    typer.echo(
        "These paths are now hidden from the agent on every `remoroo run`. "
        "Inspect with `remoroo blocked`."
    )


# ── unblock ─────────────────────────────────────────────────────────────────


def unblock(
    pattern: str = typer.Argument(..., help="Exact pattern to remove from .remorooignore."),
    repo: Optional[Path] = typer.Option(
        None,
        "--repo",
        "-r",
        help="Repo root (defaults to the current directory).",
        file_okay=False,
        dir_okay=True,
        exists=True,
    ),
) -> None:
    """Remove a pattern from ``.remorooignore``.

    Pattern must match exactly (whitespace-trimmed) what's on a line in the
    file. Use ``remoroo blocked`` to see the current list.
    """
    repo_root = _resolve_repo_root(repo)
    if not Path(ignore_file_path(str(repo_root))).is_file():
        typer.secho("No .remorooignore in this repo — nothing to remove.", fg=typer.colors.YELLOW)
        raise typer.Exit(code=1)
    removed = remove_from_ignore_file(str(repo_root), pattern)
    if not removed:
        typer.secho(
            f"Pattern '{pattern}' not found in .remorooignore. "
            "Run `remoroo blocked` to see the active list.",
            fg=typer.colors.YELLOW,
        )
        raise typer.Exit(code=1)
    typer.secho(f"🔓 Unblocked: {pattern}", fg=typer.colors.GREEN)


# ── blocked ─────────────────────────────────────────────────────────────────


def blocked(
    repo: Optional[Path] = typer.Option(
        None,
        "--repo",
        "-r",
        help="Repo root (defaults to the current directory).",
        file_okay=False,
        dir_okay=True,
        exists=True,
    ),
) -> None:
    """Show the active blocklist and which sandbox engine is in use."""
    repo_root = _resolve_repo_root(repo)
    target = ignore_file_path(str(repo_root))
    rel_target = _try_relpath(target, repo_root)

    patterns = read_ignore_file(str(repo_root))
    policy = AccessPolicy.load(str(repo_root))

    if not patterns:
        typer.secho(
            f"No {IGNORE_FILENAME} in {repo_root}. The agent can see every file.",
            fg=typer.colors.YELLOW,
        )
        typer.echo("Run `remoroo block <pattern>` to start hiding files.")
        return

    typer.secho(f"📋 {rel_target} ({len(patterns)} pattern(s))", fg=typer.colors.GREEN)
    for p in patterns:
        marker = "  default " if p in RECOMMENDED_DEFAULT_PATTERNS else "          "
        typer.echo(f"{marker}{p}")

    typer.echo("")
    typer.echo(f"Sandbox engine for bash: {policy.describe_engine()}")
    expanded = policy.absolute_subpaths()
    if expanded:
        typer.echo(f"Currently masking {len(expanded)} concrete path(s) on disk.")


# ── helpers ─────────────────────────────────────────────────────────────────


def _try_relpath(target: str, base: Path) -> str:
    """Return ``target`` relative to ``base`` if possible, else absolute."""
    try:
        return str(Path(target).resolve().relative_to(base))
    except ValueError:
        return target
