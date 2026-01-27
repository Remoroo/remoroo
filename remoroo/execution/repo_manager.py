"""
Repository management utilities for working with repository copies and diffs.
"""
from __future__ import annotations
import os
import shutil
import tempfile
import subprocess
from pathlib import Path
from typing import Optional, Tuple


import zipfile
import requests

IGNORED_PATTERNS = {
    '.remoroo', '.git', 'venv', '.venv', 'env', '.env', 
    '__pycache__', '.DS_Store', 'node_modules', '.remoroo_venvs',
    'artifacts', 'runs'
}

def create_working_copy(original_repo: str) -> str:
    """
    Create a temporary working copy of the repository.
    Supports local paths and HTTP(S) Zip URLs.
    """
    
    # Handle URL (Stage 6.5)
    if original_repo.startswith("http://") or original_repo.startswith("https://"):
        return create_working_copy_from_url(original_repo)

    original_path = Path(original_repo).resolve()
    if not original_path.is_dir():
        raise ValueError(f"Repository path does not exist: {original_repo}")
    
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp(prefix="remoroo_working_")
    working_repo = os.path.join(temp_dir, "repo")
    
    # Copy the entire repository, but exclude ignored dirs
    def ignore_patterns(dirname, names):
        ignored = []
        for name in names:
            if name in IGNORED_PATTERNS:
                ignored.append(name)
        return set(ignored)
    
    shutil.copytree(str(original_path), working_repo, symlinks=False, ignore=ignore_patterns)
    
    return working_repo

def create_working_copy_from_url(url: str) -> str:
    """Download and extract a zip repository from URL."""
    try:
        temp_dir = tempfile.mkdtemp(prefix="remoroo_working_")
        zip_path = os.path.join(temp_dir, "repo.zip")
        working_repo = os.path.join(temp_dir, "repo")
        
        # Download
        print(f"⬇️  Downloading repo from {url}...")
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                
        # Extract
        print(f"📦 Extracting to {working_repo}...")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(working_repo)
            
        # Cleanup zip
        os.unlink(zip_path)
        
        return working_repo
    except Exception as e:
        # Cleanup on failure
        if 'temp_dir' in locals() and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
        raise RuntimeError(f"Failed to hydrate repo from URL: {e}")


def generate_diff(original_repo: str, working_repo: str, files: Optional[list[str]] = None) -> str:
    """
    Generate a unified diff between original and working repository.
    
    Args:
        original_repo: Path to the original repository
        working_repo: Path to the working copy
        files: Optional list of relative file paths to include in the diff.
               If None, diffs the entire directory.
        
    Returns:
        Unified diff string
    """
    original_path = Path(original_repo).resolve()
    working_path = Path(working_repo).resolve()
    
    # If a specific list of files is provided, generate diff for each and concatenate
    if files:
        diff_outputs = []
        for rel_path in files:
            orig_file = original_path / rel_path
            work_file = working_path / rel_path
            
            # For new files, diff against /dev/null
            diff_orig = str(orig_file) if orig_file.exists() else os.devnull
            diff_work = str(work_file) if work_file.exists() else os.devnull
            
            if not os.path.exists(diff_work) and not os.path.exists(diff_orig):
                continue

            # Use git diff --no-index for each file pair
            try:
                # Note: git diff --no-index returns 1 if there are differences
                result = subprocess.run(
                    ["git", "diff", "--no-index", diff_orig, diff_work],
                    capture_output=True,
                    text=True
                )
                if result.stdout:
                    # Clean up the paths in the diff header to be relative to repo root
                    lines = result.stdout.splitlines()
                    clean_lines = []
                    for line in lines:
                        if line.startswith("--- "):
                            if diff_orig == os.devnull:
                                clean_lines.append("--- /dev/null")
                            else:
                                clean_lines.append(f"--- a/{rel_path}")
                        elif line.startswith("+++ "):
                            if diff_work == os.devnull:
                                clean_lines.append("+++ /dev/null")
                            else:
                                clean_lines.append(f"+++ b/{rel_path}")
                        elif line.startswith("diff --git"):
                            clean_lines.append(f"diff --git a/{rel_path} b/{rel_path}")
                        elif line.startswith("+"):
                            # Strip trailing whitespace from added lines to avoid git apply warnings
                            clean_lines.append(line.rstrip())
                        else:
                            clean_lines.append(line)
                    diff_outputs.append("\n".join(clean_lines))
            except (FileNotFoundError, subprocess.SubprocessError):
                continue
        return "\n".join(diff_outputs) + "\n" if diff_outputs else ""

    # Use git diff if available, otherwise use diff command
    try:
        # Try git diff first (more reliable)
        result = subprocess.run(
            ["git", "diff", "--no-index", str(original_path), str(working_path)],
            capture_output=True,
            text=True,
            cwd=str(original_path.parent)
        )
        if result.returncode == 0 or result.returncode == 1:  # 1 means differences found
            # Post-process to strip trailing whitespace from added lines
            lines = result.stdout.splitlines()
            clean_lines = [line.rstrip() if line.startswith("+") else line for line in lines]
            return "\n".join(clean_lines) + "\n" if clean_lines else ""
    except (FileNotFoundError, subprocess.SubprocessError):
        pass
    
    # Fallback to system diff command
    try:
        result = subprocess.run(
            ["diff", "-urN", str(original_path), str(working_path)],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            return ""  # No differences
        elif result.returncode == 1:
            # Post-process to strip trailing whitespace from added lines
            lines = result.stdout.splitlines()
            clean_lines = [line.rstrip() if line.startswith("+") else line for line in lines]
            return "\n".join(clean_lines) + "\n" if clean_lines else ""
        else:
            return f"Error generating diff: {result.stderr}"
    except (FileNotFoundError, subprocess.SubprocessError) as e:
        return f"Error generating diff: {str(e)}"


def get_modified_files(original_repo: str, working_repo: str) -> list[str]:
    """
    Get list of modified files between original and working repository.
    
    Args:
        original_repo: Path to the original repository
        working_repo: Path to the working copy
        
    Returns:
        List of relative file paths that were modified
    """
    original_path = Path(original_repo).resolve()
    working_path = Path(working_repo).resolve()
    
    modified_files = []
    
    # Walk through working repo and compare files
    for root, dirs, files in os.walk(working_path):
        # Skip ignored directories
        dirs[:] = [d for d in dirs if d not in IGNORED_PATTERNS]
        
        for file in files:
            # Skip ignored files
            if file in IGNORED_PATTERNS:
                continue
                
            rel_path = os.path.relpath(os.path.join(root, file), working_path)
            original_file = original_path / rel_path
            working_file = working_path / rel_path
            
            if not original_file.exists():
                # New file
                modified_files.append(rel_path)
            elif working_file.exists() and original_file.exists():
                # Compare file contents
                try:
                    with open(original_file, 'rb') as f1, open(working_file, 'rb') as f2:
                        if f1.read() != f2.read():
                            modified_files.append(rel_path)
                except (OSError, IOError):
                    pass
    
    # Check for deleted files
    for root, dirs, files in os.walk(original_path):
        dirs[:] = [d for d in dirs if d not in IGNORED_PATTERNS]
        
        for file in files:
            if file in IGNORED_PATTERNS:
                continue
                
            rel_path = os.path.relpath(os.path.join(root, file), original_path)
            original_file = original_path / rel_path
            working_file = working_path / rel_path
            
            if original_file.exists() and not working_file.exists():
                modified_files.append(rel_path)
    
    return sorted(set(modified_files))


def apply_changes(original_repo: str, working_repo: str) -> None:
    """
    Apply changes from working copy back to original repository.
    
    Args:
        original_repo: Path to the original repository
        working_repo: Path to the working copy
    """
    original_path = Path(original_repo).resolve()
    working_path = Path(working_repo).resolve()
    
    modified_files = get_modified_files(original_repo, working_repo)
    
    for rel_path in modified_files:
        original_file = original_path / rel_path
        working_file = working_path / rel_path
        
        if working_file.exists():
            # File was modified or created
            original_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(working_file, original_file)
        elif original_file.exists():
            # File was deleted
            original_file.unlink()
            # Remove empty parent directories if needed
            try:
                original_file.parent.rmdir()
            except OSError:
                pass  # Directory not empty or doesn't exist


def cleanup_working_copy(working_repo: str) -> None:
    """
    Clean up the temporary working copy directory.
    
    Args:
        working_repo: Path to the working copy
    """
    working_path = Path(working_repo)
    if working_path.exists():
        # Get the parent temp directory
        temp_dir = working_path.parent
        if temp_dir.exists() and temp_dir.name.startswith("remoroo_working_"):
            shutil.rmtree(temp_dir, ignore_errors=True)
        else:
            # Just remove the working repo if parent doesn't match pattern
            shutil.rmtree(working_path, ignore_errors=True)


def ensure_remoroo_dir(repo_root: str) -> str:
    """
    Create .remoroo/ directory if it doesn't exist.
    
    Args:
        repo_root: Path to repository root
        
    Returns:
        Path to .remoroo/ directory
    """
    remoroo_dir = os.path.join(repo_root, ".remoroo")
    os.makedirs(remoroo_dir, exist_ok=True)
    return remoroo_dir


def get_index_path(repo_root: str) -> str:
    """
    Get path to repo_index.json in .remoroo/.
    
    Args:
        repo_root: Path to repository root
        
    Returns:
        Path to repo_index.json
    """
    remoroo_dir = ensure_remoroo_dir(repo_root)
    return os.path.join(remoroo_dir, "repo_index.json")


def get_index_meta_path(repo_root: str) -> str:
    """
    Get path to repo_index.meta.json in .remoroo/.
    
    Args:
        repo_root: Path to repository root
        
    Returns:
        Path to repo_index.meta.json
    """
    remoroo_dir = ensure_remoroo_dir(repo_root)
    return os.path.join(remoroo_dir, "repo_index.meta.json")

