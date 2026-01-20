"""Generic import path diagnosis for ModuleNotFoundError."""

import os
import re
import subprocess
from typing import Dict, Any, Optional, List


def diagnose_import_error(
    error_message: str,
    repo_root: str,
    venv_python: Optional[str] = None
) -> Dict[str, Any]:
    """
    Diagnose if ModuleNotFoundError is due to:
    1. Package not installed (needs pip install)
    2. Wrong import path (package installed but wrong path used)
    
    Returns generic diagnosis without hardcoding module names.
    """
    # Extract module name from error message generically
    match = re.search(r"No module named ['\"]([^'\"]+)['\"]", error_message, re.IGNORECASE)
    if not match:
        return {
            "diagnosis": "unknown",
            "error_type": "unparseable"
        }
    
    wrong_import_path = match.group(1)
    
    # Try to determine if package is installed
    python_cmd = venv_python or "python"
    
    # Strategy 1: Check if it's a "src." prefix issue
    # If wrong_path is "src.package.module", try "package.module"
    suggested_paths = []
    if wrong_import_path.startswith("src."):
        without_src = wrong_import_path[4:]  # Remove "src." prefix
        suggested_paths.append(without_src)
    
    # Strategy 2: Check if first component is wrong
    # If wrong_path is "src.package.module", try "package.module"
    parts = wrong_import_path.split(".")
    if len(parts) > 1:
        # Try without first component
        suggested_paths.append(".".join(parts[1:]))
    
    # Strategy 3: Check if package root is installed
    root_package = parts[0] if parts else wrong_import_path
    if root_package.startswith("src."):
        root_package = root_package[4:]
    
    # Test if root package is importable (safely using importlib)
    test_script = f"""
import importlib
import sys

package_name = '{root_package}'
try:
    module = importlib.import_module(package_name)
    print("INSTALLED")
    if hasattr(module, '__file__'):
        print(module.__file__)
    else:
        print("NO_FILE")
except ImportError:
    print("NOT_INSTALLED")
except Exception as e:
    print(f"ERROR: {{e}}")
"""
    
    is_installed = False
    try:
        result = subprocess.run(
            [python_cmd, "-c", test_script],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=5
        )
        is_installed = "INSTALLED" in result.stdout
    except Exception:
        is_installed = False
    
    # If installed, try suggested paths
    working_path = None
    if is_installed:
        for suggested_path in suggested_paths:
            # Safely test import using importlib
            test_script = f"""
import importlib
import sys

module_path = '{suggested_path}'
try:
    # Try importing the module
    module = importlib.import_module(module_path)
    print("WORKS")
except ImportError as e:
    print(f"FAILS: {{e}}")
except Exception as e:
    print(f"ERROR: {{e}}")
"""
            try:
                result = subprocess.run(
                    [python_cmd, "-c", test_script],
                    cwd=repo_root,
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if "WORKS" in result.stdout:
                    working_path = suggested_path
                    break
            except Exception:
                continue
    
    # Build generic diagnosis
    diagnosis = {
        "error_type": "import_path_mismatch" if is_installed else "package_not_installed",
        "wrong_import_path": wrong_import_path,
        "package_installed": is_installed,
        "root_package": root_package,
        "suggested_import_paths": suggested_paths,
        "working_import_path": working_path
    }
    
    if is_installed and working_path:
        diagnosis["diagnosis"] = "package_installed_wrong_path"
        diagnosis["message"] = (
            f"Package '{root_package}' is installed, but validation script uses wrong import path. "
            f"Wrong: '{wrong_import_path}', Try: '{working_path}'"
        )
    elif is_installed:
        diagnosis["diagnosis"] = "package_installed_path_unknown"
        diagnosis["message"] = (
            f"Package '{root_package}' is installed, but correct import path is unclear. "
            f"Wrong path used: '{wrong_import_path}'. Check existing code in repo for correct import patterns."
        )
    else:
        diagnosis["diagnosis"] = "package_not_installed"
        diagnosis["message"] = (
            f"Package '{root_package}' is not installed. This may require 'pip install' or fixing requirements.txt."
        )
    
    return diagnosis


def extract_import_patterns_from_repo(
    repo_root: str,
    max_files: int = 20
) -> Dict[str, Any]:
    """
    Extract common import patterns from existing Python files in repo.
    Returns generic patterns without hardcoding specific module names.
    """
    from .scan_repo import extract_imports_python
    
    patterns = {
        "import_patterns": [],
        "common_prefixes": [],
        "package_structure": "unknown",
        "common_import_prefixes": [],  # Generic list of common prefixes (e.g., ["src", "lib", "packages"])
        "has_relative_imports": False
    }
    
    # Scan Python files
    python_files = []
    for root, dirs, files in os.walk(repo_root):
        # Skip hidden dirs and common exclusions
        dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['__pycache__', 'node_modules', '.remoroo_venvs', 'artifacts']]
        
        for file in files:
            if file.endswith('.py') and not file.startswith('validate'):
                file_path = os.path.join(root, file)
                # Skip validation scripts and test files for pattern extraction
                rel_path = os.path.relpath(file_path, repo_root)
                if 'validate' not in rel_path.lower() and 'test' not in rel_path.lower():
                    python_files.append(file_path)
                    if len(python_files) >= max_files:
                        break
        
        if len(python_files) >= max_files:
            break
    
    # Extract imports from files
    all_imports = []
    for file_path in python_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                imports = extract_imports_python(content)
                all_imports.extend(imports)
        except Exception:
            continue
    
    # Analyze patterns
    if all_imports:
        # Check for relative imports
        patterns["has_relative_imports"] = any(imp.startswith(".") for imp in all_imports)
        
        # Group by prefix (first component of import path)
        prefix_counts = {}
        for imp in all_imports:
            # Skip relative imports and stdlib
            if imp.startswith("."):
                continue
            parts = imp.split('.')
            if parts:
                prefix = parts[0]
                prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1
        
        # Get most common prefixes (excluding stdlib)
        stdlib_prefixes = {'os', 'sys', 'json', 're', 'time', 'datetime', 'typing', 'pathlib', 'collections', 'itertools', 'functools', 'abc', 'dataclasses', 'enum'}
        common_prefixes = [
            prefix for prefix, count in sorted(prefix_counts.items(), key=lambda x: -x[1])
            if prefix not in stdlib_prefixes and count >= 2
        ][:5]
        
        patterns["common_prefixes"] = common_prefixes
        patterns["common_import_prefixes"] = common_prefixes  # Alias for clarity
        
        # Get sample of unique imports (excluding stdlib)
        unique_imports = [
            imp for imp in set(all_imports)
            if not any(imp.startswith(stdlib) for stdlib in stdlib_prefixes)
            and not imp.startswith(".")
        ][:10]
        patterns["import_patterns"] = unique_imports
        
        # Detect package structure generically
        # If most imports start with a common prefix (like "src", "lib", etc.), it's a prefixed layout
        if common_prefixes and len(common_prefixes) > 0:
            top_prefix = common_prefixes[0]
            prefix_usage_count = sum(1 for imp in all_imports if imp.startswith(f"{top_prefix}."))
            total_non_stdlib = sum(1 for imp in all_imports if not any(imp.startswith(stdlib) for stdlib in stdlib_prefixes) and not imp.startswith("."))
            if total_non_stdlib > 0 and prefix_usage_count / total_non_stdlib > 0.3:  # >30% use this prefix
                patterns["package_structure"] = f"prefixed_layout"  # Generic, not "src_layout"
            else:
                patterns["package_structure"] = "flat_layout"
        elif any('.' in imp and not imp.startswith(("test", "tests", "conftest")) for imp in all_imports):
            patterns["package_structure"] = "flat_layout"
        else:
            patterns["package_structure"] = "mixed"
    
    return patterns

