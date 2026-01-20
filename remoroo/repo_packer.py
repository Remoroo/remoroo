import os
import zipfile
import subprocess
from pathlib import Path
from typing import List, Optional, Set

def get_git_files(repo_path: Path) -> Optional[List[str]]:
    """
    Get list of files tracked by git + untracked files (respecting .gitignore).
    Returns None if git is not available or not a git repo.
    """
    try:
        # Check if it's a git repo
        if not (repo_path / ".git").exists():
            return None
            
        # Get tracked files
        tracked = subprocess.check_output(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
            cwd=str(repo_path),
            stderr=subprocess.DEVNULL
        ).decode("utf-8").splitlines()
        
        return [f for f in tracked if f.strip()]
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None

def pack_repo(repo_path: Path, output_path: Optional[Path] = None) -> Path:
    """
    Pack the repository at repo_path into a zip file.
    Respects .gitignore if possible, otherwise excludes common junk.
    """
    repo_path = repo_path.resolve()
    
    if output_path is None:
        import tempfile
        fd, output_path_str = tempfile.mkstemp(suffix=".zip", prefix="remoroo_context_")
        os.close(fd)
        output_path = Path(output_path_str)
        
    # Try using git ls-files first (fastest and most accurate)
    files_to_pack = get_git_files(repo_path)
    
    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
        if files_to_pack:
            # Git strategy
            for rel_path in files_to_pack:
                abs_path = repo_path / rel_path
                if abs_path.is_file():
                    zf.write(abs_path, rel_path)
        else:
            # Fallback strategy: Walk and exclude junk
            # Simple junk list
            EXCLUDES = {
                ".git", "node_modules", "venv", "__pycache__", 
                ".DS_Store", ".idea", ".vscode", "dist", "build",
                "remoroo_venvs"
            }
            
            for root, dirs, files in os.walk(repo_path):
                # Modify dirs in-place to skip excluded directories
                dirs[:] = [d for d in dirs if d not in EXCLUDES]
                
                for file in files:
                    if file in EXCLUDES or file.endswith(".pyc"):
                        continue
                        
                    abs_file = Path(root) / file
                    rel_file = abs_file.relative_to(repo_path)
                    
                    zf.write(abs_file, rel_file)
                    
    return output_path
