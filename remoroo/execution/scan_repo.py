"""Repository scanning for architecture diagram generation."""

import os
import json
import hashlib
import re
from typing import Dict, Any, List, Set, Optional
from pathlib import Path

# Import from centralized config
from ..execution.configs import DEFAULT_EXCLUDED_DIRS, DEFAULT_EXCLUDED_FILES

# Code file extensions
CODE_EXTENSIONS = {'.py', '.js', '.ts', '.go', '.rs', '.java', '.cpp', '.c', '.h', '.hpp', '.rb', '.php'}

# Entry point patterns
ENTRYPOINT_PATTERNS = [
    r'main\.py$',
    r'__main__\.py$',
    r'cli\.py$',
    r'app\.py$',
    r'server\.py$',
    r'index\.(js|ts)$',
    r'main\.(go|rs|java|cpp)$'
]


def extract_imports_python(content: str) -> List[str]:
    """Extract import statements from Python code."""
    imports = []
    # Match: import X, from Y import Z
    patterns = [
        r'^import\s+([^\s,]+)',
        r'^from\s+([^\s]+)\s+import',
    ]
    for line in content.split('\n'):
        for pattern in patterns:
            match = re.match(pattern, line.strip())
            if match:
                imports.append(match.group(1))
    return imports


def extract_imports_javascript(content: str) -> List[str]:
    """Extract import statements from JavaScript/TypeScript code."""
    imports = []
    # Match: import X from 'Y', import {A, B} from 'Y', require('Y')
    patterns = [
        r"import\s+.*?\s+from\s+['\"]([^'\"]+)['\"]",
        r"require\(['\"]([^'\"]+)['\"]\)",
    ]
    for line in content.split('\n'):
        for pattern in patterns:
            matches = re.findall(pattern, line)
            imports.extend(matches)
    return imports


def extract_imports_go(content: str) -> List[str]:
    """Extract import statements from Go code."""
    imports = []
    # Match: import "package" or import ("package1", "package2")
    pattern = r'import\s+(?:\([^)]+\)|["\']([^"\']+)["\'])'
    matches = re.findall(pattern, content, re.MULTILINE)
    imports.extend(matches)
    return imports


def extract_exports_python(content: str) -> List[str]:
    """Extract exported symbols from Python code (best-effort)."""
    exports = []
    # Look for class and function definitions at module level
    patterns = [
        r'^class\s+(\w+)',
        r'^def\s+(\w+)\s*\(',
    ]
    for line in content.split('\n'):
        # Skip if indented (not at module level)
        if line and not line[0].isspace():
            for pattern in patterns:
                match = re.match(pattern, line.strip())
                if match:
                    exports.append(match.group(1))
    return exports


def is_entrypoint(file_path: str) -> bool:
    """Check if file matches entrypoint patterns."""
    for pattern in ENTRYPOINT_PATTERNS:
        if re.search(pattern, file_path, re.IGNORECASE):
            return True
    return False


def scan_repository(repo_root: str, cache_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Scan repository and create RepoIndex.json.
    
    Args:
        repo_root: Root directory of repository
        cache_path: Optional path to cache file
        
    Returns:
        RepoIndex dictionary with file metadata
    """
    repo_index = {
        "repo_root": repo_root,
        "files": [],
        "entrypoints": [],
        "languages": set(),
        "total_files": 0,
        "total_size_bytes": 0
    }
    
    # Check cache if provided
    if cache_path and os.path.exists(cache_path):
        try:
            with open(cache_path, 'r') as f:
                cached = json.load(f)
            # Validate cache is for same repo
            if cached.get("repo_root") == repo_root:
                # Check if repo has changed (compare max mtime)
                cached_mtime = cached.get("scan_timestamp", 0)
                current_mtime = _get_repo_max_mtime(repo_root)
                if current_mtime <= cached_mtime:
                    return cached
        except (json.JSONDecodeError, KeyError):
            pass  # Cache invalid, rebuild
    
    # Scan repository
    for root, dirs, files in os.walk(repo_root):
        # Filter excluded directories
        dirs[:] = [d for d in dirs if d not in DEFAULT_EXCLUDED_DIRS and not d.startswith('.')]
        
        for filename in files:
            # Skip hidden files and common artifacts
            if filename.startswith('.') or filename.startswith('__') or filename in DEFAULT_EXCLUDED_FILES:
                continue
            
            file_path = os.path.join(root, filename)
            rel_path = os.path.relpath(file_path, repo_root)
            
            try:
                stat = os.stat(file_path)
                size = stat.st_size
                mtime = stat.st_mtime
                
                ext = os.path.splitext(filename)[1].lower()
                
                # Only process code files
                if ext not in CODE_EXTENSIONS:
                    continue
                
                # Detect language
                lang = _detect_language(ext, filename)
                if lang:
                    repo_index["languages"].add(lang)
                
                # Read file for analysis
                imports = []
                exports = []
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    
                    # Extract imports/exports based on language
                    if lang == 'python':
                        imports = extract_imports_python(content)
                        exports = extract_exports_python(content)
                    elif lang in ['javascript', 'typescript']:
                        imports = extract_imports_javascript(content)
                    elif lang == 'go':
                        imports = extract_imports_go(content)
                except (UnicodeDecodeError, OSError):
                    pass  # Skip if can't read
                
                file_entry = {
                    "path": rel_path,
                    "size_bytes": size,
                    "mtime": mtime,
                    "language": lang,
                    "imports": imports,
                    "exports": exports,
                    "is_entrypoint": is_entrypoint(rel_path)
                }
                
                repo_index["files"].append(file_entry)
                repo_index["total_files"] += 1
                repo_index["total_size_bytes"] += size
                
                if file_entry["is_entrypoint"]:
                    repo_index["entrypoints"].append(rel_path)
                    
            except OSError:
                continue  # Skip if can't access
    
    # Convert set to list for JSON serialization
    repo_index["languages"] = list(repo_index["languages"])
    repo_index["scan_timestamp"] = os.path.getmtime(repo_root) if os.path.exists(repo_root) else 0
    
    # Save cache if provided
    if cache_path:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, 'w') as f:
            json.dump(repo_index, f, indent=2)
    
    return repo_index


def _detect_language(ext: str, filename: str) -> Optional[str]:
    """Detect programming language from extension and filename."""
    lang_map = {
        '.py': 'python',
        '.js': 'javascript',
        '.ts': 'typescript',
        '.go': 'go',
        '.rs': 'rust',
        '.java': 'java',
        '.cpp': 'cpp',
        '.c': 'c',
        '.h': 'c',
        '.hpp': 'cpp',
        '.rb': 'ruby',
        '.php': 'php'
    }
    return lang_map.get(ext)


def _get_repo_max_mtime(repo_root: str) -> float:
    """Get maximum modification time of all files in repo."""
    max_mtime = 0
    try:
        for root, dirs, files in os.walk(repo_root):
            dirs[:] = [d for d in dirs if d not in DEFAULT_EXCLUDED_DIRS and not d.startswith('.')]
            for file in files:
                if file.startswith('.') or file.startswith('__') or file in DEFAULT_EXCLUDED_FILES:
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

