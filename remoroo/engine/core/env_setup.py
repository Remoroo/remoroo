from __future__ import annotations
import os
import sys
import subprocess
import hashlib
import json
import shutil
import time
import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from .executor import run_command_with_timeout

@dataclass
class EnvSetupResult:
    """Result of environment setup process."""
    success: bool
    diagnosis: str
    commands_run: List[str] = field(default_factory=list)
    setup_duration_s: float = 0.0
    error_message: Optional[str] = None
    venv_python: Optional[str] = None

def detect_requirements_files(repo_root: str) -> List[str]:
    """Detect requirements files in the repository."""
    requirements_files = []
    common_names = [
        "requirements.txt",
        "requirements-dev.txt",
        "pyproject.toml",
        "setup.py",
        "Pipfile",
        "environment.yml",
        "conda.yml"
    ]
    
    for name in common_names:
        path = os.path.join(repo_root, name)
        if os.path.exists(path):
            requirements_files.append(name)
    
    return requirements_files

def _extract_package_name_from_pyproject(repo_root: str) -> Optional[str]:
    """Extract package name from pyproject.toml."""
    try:
        pyproject_path = os.path.join(repo_root, "pyproject.toml")
        if not os.path.exists(pyproject_path):
            return None
        
        with open(pyproject_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        match = re.search(r'\[project\]\s+name\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
        if match:
            return match.group(1)
        
        match = re.search(r'\[tool\.poetry\]\s+name\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
        if match:
            return match.group(1)
        
        match = re.search(r'\[tool\.flit\.metadata\]\s+module\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
        if match:
            return match.group(1)
    except Exception:
        pass
    return None

def _extract_package_name_from_setup(repo_root: str) -> Optional[str]:
    """Extract package name from setup.py."""
    try:
        setup_path = os.path.join(repo_root, "setup.py")
        if not os.path.exists(setup_path):
            return None
        
        with open(setup_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        patterns = [
            r'name\s*=\s*["\']([^"\']+)["\']',
            r'["\']name["\']\s*:\s*["\']([^"\']+)["\']',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                return match.group(1)
    except Exception:
        pass
    return None

def detect_needs_editable_install(repo_root: str) -> Tuple[bool, str]:
    """Detect if a repo needs 'pip install -e .' to be importable."""
    is_src_layout = os.path.isdir(os.path.join(repo_root, "src"))
    has_setup_py = os.path.exists(os.path.join(repo_root, "setup.py"))
    has_pyproject = os.path.exists(os.path.join(repo_root, "pyproject.toml"))
    
    package_name = _extract_package_name_from_pyproject(repo_root) or _extract_package_name_from_setup(repo_root)
    
    if not package_name:
        for item in os.listdir(repo_root):
            if os.path.isdir(os.path.join(repo_root, item)) and not item.startswith(".") and not item.startswith("_"):
                if os.path.exists(os.path.join(repo_root, item, "__init__.py")):
                     package_name = item
                     break
        if is_src_layout and not package_name:
            src_dir = os.path.join(repo_root, "src")
            for item in os.listdir(src_dir):
                 if os.path.isdir(os.path.join(src_dir, item)) and os.path.exists(os.path.join(src_dir, item, "__init__.py")):
                     package_name = item
                     break
    
    if has_setup_py or has_pyproject:
        return True, package_name or "unknown_package"
    
    return False, package_name or "unknown_package"

def rewrite_toolchain_commands(commands: List[str], venv_path: str) -> List[str]:
    """Rewrite commands to use venv python/pip."""
    rewritten = []
    
    if sys.platform == "win32":
        python_executable = os.path.join(venv_path, "Scripts", "python.exe")
        pip_executable = os.path.join(venv_path, "Scripts", "pip.exe")
    else:
        python_executable = os.path.join(venv_path, "bin", "python")
        pip_executable = os.path.join(venv_path, "bin", "pip")
    
    if not os.path.exists(python_executable):
        return commands
        
    for cmd in commands:
        parts = cmd.split()
        if not parts:
            rewritten.append(cmd)
            continue
            
        if parts[0] == "python" or parts[0] == "python3":
            parts[0] = python_executable
            rewritten.append(" ".join(parts))
        elif parts[0] == "pip" or parts[0] == "pip3":
            parts[0] = pip_executable
            rewritten.append(" ".join(parts))
        elif parts[0] == "pytest":
             # prefer python -m pytest
            parts[0] = python_executable
            parts.insert(1, "-m")
            parts.insert(2, "pytest")
            rewritten.append(" ".join(parts))
        else:
            rewritten.append(cmd)
            
    return rewritten

def execute_env_setup(
    repo_root: str,
    install_commands: Optional[List[str]] = None,
    timeout_s: float = 300,
    use_venv: bool = True,
    always_create_venv: bool = False,
    skip_auto_install: bool = False
) -> EnvSetupResult:
    """
    Execute environment setup, optionally creating a venv.
    """
    start_time = time.time()
    commands_run = []
    
    venv_path = os.path.join(repo_root, "venv")
    is_win = sys.platform == "win32"
    venv_python = os.path.join(venv_path, "Scripts", "python.exe") if is_win else os.path.join(venv_path, "bin", "python")
    venv_pip = os.path.join(venv_path, "Scripts", "pip.exe") if is_win else os.path.join(venv_path, "bin", "pip")
    
    if use_venv:
        if not os.path.exists(venv_path) or always_create_venv:
            print(f"Creating venv at {venv_path}")
            try:
                subprocess.check_call([sys.executable, "-m", "venv", venv_path])
                commands_run.append(f"{sys.executable} -m venv venv")
            except subprocess.CalledProcessError as e:
                return EnvSetupResult(
                    success=False,
                    diagnosis="Failed to create venv.",
                    commands_run=commands_run,
                    setup_duration_s=time.time() - start_time,
                    error_message=str(e)
                )
        else:
            print(f"Using existing venv at {venv_path}")
    
    python_cmd = venv_python if use_venv and os.path.exists(venv_python) else sys.executable
    pip_cmd = venv_pip if use_venv and os.path.exists(venv_pip) else "pip"
    
    actual_commands = []
    if install_commands:
        if use_venv:
            actual_commands = rewrite_toolchain_commands(install_commands, venv_path)
        else:
            actual_commands = install_commands
    elif not skip_auto_install:
        # Detect requirements
        req_files = detect_requirements_files(repo_root)
        if "requirements.txt" in req_files:
            actual_commands.append(f"{pip_cmd} install -r requirements.txt")
        elif "setup.py" in req_files:
            actual_commands.append(f"{pip_cmd} install -e .")
        elif "pyproject.toml" in req_files:
            actual_commands.append(f"{pip_cmd} install .")
    
    # Run commands
    for cmd in actual_commands:
        res = run_command_with_timeout(
            cmd,
            repo_root,
            timeout_s=timeout_s,
            show_progress=True
        )
        commands_run.append(cmd)
        if res["exit_code"] != 0:
            return EnvSetupResult(
                success=False,
                diagnosis=f"Failed to run setup command: {cmd}",
                commands_run=commands_run,
                setup_duration_s=time.time() - start_time,
                error_message=res.get("stderr", "")
            )
            
    return EnvSetupResult(
        success=True,
        diagnosis="Environment setup completed successfully.",
        commands_run=commands_run,
        setup_duration_s=time.time() - start_time,
        venv_python=python_cmd if use_venv else None
    )
