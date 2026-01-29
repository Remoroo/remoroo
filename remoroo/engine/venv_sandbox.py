import os
import sys
import subprocess
from typing import List, Dict, Union, Optional
from pathlib import Path
from .utils.system_interface import SystemInterface, RealSystem

class VenvSandbox:
    """
    Sandboxing strategy for local execution within a virtual environment.
    Ensures commands run using the repository's venv python/pip.
    """
    def __init__(self, repo_root: str, system: Optional[SystemInterface] = None):
        self.system = system or RealSystem()
        self.repo_root = self.system.fs.abspath(repo_root) if hasattr(self.system.fs, 'abspath') else os.path.abspath(repo_root)
        self.venv_path = os.path.join(self.repo_root, "venv")
        self.is_win = sys.platform == "win32"
        self._resolve_paths()

    def _resolve_paths(self):
        """Resolve paths to python and pip within the venv."""
        if self.is_win:
            self.bin_dir = os.path.join(self.venv_path, "Scripts")
            self.python_exe = os.path.join(self.bin_dir, "python.exe")
        else:
            self.bin_dir = os.path.join(self.venv_path, "bin")
            self.python_exe = os.path.join(self.bin_dir, "python")

    def _rewrite_command(self, cmd: Union[str, List[str]]) -> Union[str, List[str]]:
        """
        Rewrite command to use venv executable.
        If cmd is a string, we leave it alone (it will run in shell with VIRTUAL_ENV set).
        If cmd is a list, we replace 'python'/'pip' with absolute paths.
        """
        if isinstance(cmd, str):
            return cmd
        
        rewritten = list(cmd)
        if not rewritten:
            return rewritten
            
        exe = rewritten[0]
        if exe in ["python", "python3"]:
            # Only replace if the venv actually exists, otherwise fall back to system (with env set)
            if self.system.fs.exists(self.python_exe):
                rewritten[0] = self.python_exe
        elif exe in ["pip", "pip3"]:
            pip_exe = os.path.join(self.bin_dir, "pip.exe" if self.is_win else "pip")
            if self.system.fs.exists(pip_exe):
                rewritten[0] = pip_exe
        elif exe == "pytest":
             # Best practice: run as module
             if self.system.fs.exists(self.python_exe):
                 rewritten[0] = self.python_exe
                 rewritten.insert(1, "-m")
                 rewritten.insert(2, "pytest")

        return rewritten

    def exec_popen(self, 
                   cmd: Union[str, List[str]], 
                   env: Dict[str, str] = {}, 
                   workdir: Optional[str] = None,
                   **kwargs) -> subprocess.Popen:
        """
        Create a Popen object configured for the venv.
        Matches the DockerSandbox.exec_popen signature.
        """
        # 1. Prepare Environment
        # Start with system env, overlay provided env
        proc_env = os.environ.copy()
        proc_env.update(env)
        
        # Overlay Venv variables
        proc_env["VIRTUAL_ENV"] = self.venv_path
        
        # Prepend venv/bin to PATH
        current_path = proc_env.get("PATH", "")
        proc_env["PATH"] = f"{self.bin_dir}{os.pathsep}{current_path}"
        
        # Remove PYTHONHOME if set (standard venv behavior)
        proc_env.pop("PYTHONHOME", None)

        # 2. Rewrite Command (if list)
        final_cmd = self._rewrite_command(cmd)
        
        # 3. Determine CWD
        cwd = workdir if workdir else self.repo_root

        # 4. Spawn
        # Note: If cmd is string, shell=True is often implied/needed by caller logic in `local_worker`
        # But `exec_popen` standardizes on receiving what the caller wants.
        # DockerSandbox.exec_popen does NOT use shell=True by default for lists.
        # We adhere to subprocess defaults here.
        
        # Support kwargs pass-through (e.g. stdout/stderr pipes)
        # But ensure we control env and cwd
        start_new_session = kwargs.pop("start_new_session", True) # Default to true for signal groups
        
        if "cwd" in kwargs: cwd = kwargs.pop("cwd")
        if "env" in kwargs: kwargs.pop("env") # We built it manually
        
        # Platform specific preexec
        preexec_fn = None
        if not self.is_win and start_new_session:
            # Note: in v3 we use proc.spawn which handles group logic
            pass

        return self.system.proc.spawn(
            final_cmd,
            cwd=cwd,
            env=proc_env,
            start_new_session=start_new_session,
            **kwargs
        )
