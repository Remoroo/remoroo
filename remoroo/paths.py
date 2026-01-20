from typing import Optional
from pathlib import Path
import os
import typer

def resolve_repo_path(repo: Path) -> Path:
    """Resolve the repository path."""
    return repo.resolve()

def resolve_out_dir(out: Optional[Path], repo_path: Path) -> Path:
    """Resolve the output directory. Defaults to ~/.cache/remoroo/runs to avoid polluting the target repo."""
    if out:
        return out.resolve()
    
    # CRITICAL: Do NOT default to repo_path/runs - this pollutes the target repository!
    # Default to ~/.cache/remoroo/runs/<repo_name> for isolation
    cache_dir = Path.home() / ".cache" / "remoroo" / "runs"
    repo_name = repo_path.resolve().name
    return cache_dir / repo_name

