import os
import json
import time
from typing import List, Dict, Any, Set, Optional
from .configs import DEFAULT_EXCLUDED_DIRS, DEFAULT_DENY_PATHS

ALWAYS_DATA_FOLDERS = {"data", "dataset", "datasets", "artifacts", "models", "checkpoints", "outputs"}

def is_data_folder(path: str, repo_root: str, allowed_data_folders: List[str]) -> bool:
    """Check if a path is within a data folder (including always-on detection)."""
    abs_path = os.path.abspath(path)
    abs_repo = os.path.abspath(repo_root)
    repo_name = os.path.basename(abs_repo)
    
    # Check always-on data folders first
    try:
        rel_path = os.path.relpath(abs_path, abs_repo)
    except ValueError:
        return False # Path is on different drive
        
    path_parts = rel_path.split(os.sep)
    for part in path_parts:
        if part in ALWAYS_DATA_FOLDERS:
            return True
    
    # Check allowed_data_folders
    for allowed in allowed_data_folders:
        # Normalize: strip repo name prefix if present
        normalized = allowed
        if allowed.startswith(repo_name + "/") or allowed.startswith(repo_name + os.sep):
            normalized = allowed[len(repo_name)+1:]
        
        # Resolve allowed path relative to repo_root
        if os.path.isabs(normalized):
            abs_allowed = os.path.abspath(normalized)
        else:
            abs_allowed = os.path.abspath(os.path.join(repo_root, normalized))
        
        # Check if path is within the allowed data folder
        try:
            rel = os.path.relpath(abs_path, abs_allowed)
            if not rel.startswith(".."):
                return True
        except ValueError:
            pass
    
    return False

def is_data_file(file_path: str, repo_root: str) -> bool:
    """Detect if a file is a data file."""
    # Check file extension
    data_extensions = {'.csv', '.tsv', '.h5', '.hdf5', '.pkl', '.pickle', '.npz', '.npy', 
                      '.parquet', '.feather', '.arrow', '.xlsx', '.xls'}
    ext = os.path.splitext(file_path)[1].lower()
    if ext in data_extensions:
        return True
    
    # Check if file is in a data folder
    abs_path = os.path.join(repo_root, file_path)
    if is_data_folder(abs_path, repo_root, []):
        return True
    
    return False

def get_repo_max_mtime(repo_root: str) -> float:
    """Get maximum modification time of all files in repo."""
    max_mtime = 0
    try:
        for root, dirs, files in os.walk(repo_root):
            # Skip excluded directories
            dirs[:] = [d for d in dirs if d not in DEFAULT_EXCLUDED_DIRS]
            for file in files:
                if file.startswith('.') or file.startswith('__'):
                    continue
                file_path = os.path.join(root, file)
                try:
                    mtime = os.path.getmtime(file_path)
                    max_mtime = max(max_mtime, mtime)
                except OSError:
                    pass
    except OSError:
        pass
    return max_mtime

def scan_repository(repo_root: str, artifact_dir: str, force_refresh: bool = False) -> Dict[str, Any]:
    """Get informational listing of existing files in the repository with caching."""
    # Simplified for CLI engine (no caching logic complexity if not needed, but keep for now)
    cache_path = os.path.join(artifact_dir, "repo_structure_cache.json")
    
    # Check cache if not forcing refresh
    if not force_refresh and os.path.exists(cache_path):
        try:
            with open(cache_path, 'r') as f:
                cache = json.load(f)
            cache_time = cache.get("timestamp", 0)
            repo_mtime = get_repo_max_mtime(repo_root)
            
            # If repo hasn't changed, return cached
            if repo_mtime <= cache_time:
                return cache["structure"]
        except (json.JSONDecodeError, KeyError, OSError):
            pass
    
    # Perform scan
    files_list = []
    
    try:
        for root, dirs, files in os.walk(repo_root):
            # Skip excluded directories
            dirs[:] = [d for d in dirs if d not in DEFAULT_EXCLUDED_DIRS]
            
            # Check for allowed data folders filter? 
            # In scan_repository we just list everything usually, filtering happens later.
            
            for file in files:
                if file.startswith('.') and file != ".gitignore":
                     continue
                     
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, repo_root)
                
                # Check deny paths
                skip = False
                for denied in DEFAULT_DENY_PATHS:
                    if denied in rel_path:
                        skip = True
                        break
                if skip:
                    continue
                
                if is_data_file(rel_path, repo_root):
                    files_list.append({"path": rel_path, "type": "data"})
                else:
                    files_list.append({"path": rel_path, "type": "code"})
                    
    except OSError:
        pass
        
    structure = {"files": files_list}
    
    # Save cache
    try:
        os.makedirs(artifact_dir, exist_ok=True)
        with open(cache_path, 'w') as f:
            json.dump({
                "timestamp": time.time(),
                "structure": structure
            }, f)
    except OSError:
        pass
        
    return structure
