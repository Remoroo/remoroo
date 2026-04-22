import os
import sys
import subprocess
from typing import List, Dict, Union, Optional, Any
from pathlib import Path
from .utils.system_interface import SystemInterface, RealSystem
from .core.workspace import managed_venv_path


class VenvSandbox:
    """
    Sandboxing strategy for local execution within a virtual environment.
    Ensures commands run using the repository's venv python/pip.

    Interface-compatible with DockerSandbox so LocalWorker can use either
    interchangeably via self.sandbox.
    """

    def __init__(self, repo_root: str, system: Optional[SystemInterface] = None):
        self.system = system or RealSystem()
        self.repo_root = self.system.fs.abspath(repo_root) if hasattr(self.system.fs, 'abspath') else os.path.abspath(repo_root)
        self.is_win = sys.platform == "win32"
        self.venv_path: Optional[str] = None
        self.bin_dir: Optional[str] = None
        self.python_exe: Optional[str] = None
        self.is_running = False
        self._detect_venv()

    @property
    def available(self) -> bool:
        """Always available — runs commands directly on host."""
        return True

    def _detect_venv(self):
        """Locate the venv to activate for this repo.

        Precedence (high → low):
          1. CLI-managed venv at ``workspace.managed_venv_path(repo_root)``
             — out-of-tree, keyed by the repo's absolute path.
          2. ``.venv/`` at repo root — a user-blessed convention; fine
             to activate because the user put it there deliberately.
          3. Nothing. Caller's environment wins; on Try-Now that's
             the shared ``/opt/try_now/.venv`` via ``PATH``.

        NOTE: A bare ``venv/`` (no dot) inside the repo is NOT picked
        up. Historically the CLI wrote its managed venv there, but
        that leaked megabytes of site-packages into users' repos and
        shadowed their real interpreter with a stale one. We now
        ignore such directories entirely and emit a one-line warning
        so operators know to delete the leftover.
        """
        managed = managed_venv_path(self.repo_root)
        if os.path.isdir(managed):
            self.venv_path = managed
            self._resolve_paths()
            return

        dotvenv = os.path.join(self.repo_root, ".venv")
        if os.path.isdir(dotvenv):
            self.venv_path = dotvenv
            self._resolve_paths()
            return

        bare = os.path.join(self.repo_root, "venv")
        if os.path.isdir(bare):
            print(
                f"⚠️  Found an in-repo 'venv/' at {bare} — ignoring it. "
                f"The CLI no longer creates venvs inside your repo. "
                f"If this was left by an older Remoroo version, delete "
                f"it: rm -rf {bare}"
            )

        # No usable venv. Point venv_path at the managed location so
        # a later env_setup call creates it there; has_venv() stays
        # False until that happens.
        self.venv_path = managed
        self._resolve_paths()

    def _resolve_paths(self):
        """Resolve paths to python and pip within the venv."""
        if self.is_win:
            self.bin_dir = os.path.join(self.venv_path, "Scripts")
            self.python_exe = os.path.join(self.bin_dir, "python.exe")
        else:
            self.bin_dir = os.path.join(self.venv_path, "bin")
            self.python_exe = os.path.join(self.bin_dir, "python")

    def has_venv(self) -> bool:
        """Return True if the detected venv directory actually exists."""
        return self.venv_path is not None and os.path.isdir(self.venv_path)

    # ── Lifecycle stubs (DockerSandbox compat) ──

    def start(self):
        """No-op for venv mode. Validates venv on first call."""
        if self.is_running:
            return
        if not self.has_venv():
            print(f"⚠️  No venv found at {self.repo_root} — commands will use system Python")
        self.is_running = True

    def stop(self):
        """No-op for venv mode."""
        self.is_running = False

    def commit_state(self, success: bool = True):
        """No-op for venv mode (packages persist naturally on disk)."""
        pass

    commit = commit_state

    def host_to_container(self, host_path: str) -> str:
        """Identity mapping — no container involved."""
        return host_path

    # ── Command execution ──

    def _rewrite_command(self, cmd: Union[str, List[str]]) -> Union[str, List[str]]:
        """
        Rewrite command to use venv executable.
        String commands are left alone (shell will use VIRTUAL_ENV/PATH from env).
        List commands get python/pip/pytest replaced with absolute venv paths.
        """
        if isinstance(cmd, str):
            return cmd

        rewritten = list(cmd)
        if not rewritten:
            return rewritten

        exe = rewritten[0]
        if exe in ["python", "python3"]:
            if self.python_exe and self.system.fs.exists(self.python_exe):
                rewritten[0] = self.python_exe
        elif exe in ["pip", "pip3"]:
            pip_exe = os.path.join(self.bin_dir, "pip.exe" if self.is_win else "pip")
            if self.system.fs.exists(pip_exe):
                rewritten[0] = pip_exe
        elif exe == "pytest":
            if self.python_exe and self.system.fs.exists(self.python_exe):
                rewritten[0] = self.python_exe
                rewritten.insert(1, "-m")
                rewritten.insert(2, "pytest")

        return rewritten

    def _build_env(self, env: Dict[str, str]) -> Dict[str, str]:
        """Build process env with venv activation layered on top of caller env."""
        proc_env = os.environ.copy()
        proc_env.update(env)

        if self.bin_dir and os.path.isdir(self.bin_dir):
            proc_env["VIRTUAL_ENV"] = self.venv_path
            current_path = proc_env.get("PATH", "")
            proc_env["PATH"] = f"{self.bin_dir}{os.pathsep}{current_path}"

        proc_env.pop("PYTHONHOME", None)
        return proc_env

    def exec_popen(self,
                   cmd: Union[str, List[str]],
                   env: Dict[str, str] = {},
                   workdir: Optional[str] = None,
                   **kwargs) -> subprocess.Popen:
        """
        Create a Popen object configured for the venv.
        Matches the DockerSandbox.exec_popen signature.
        """
        proc_env = self._build_env(env)
        final_cmd = self._rewrite_command(cmd)
        cwd = workdir if workdir else self.repo_root

        start_new_session = kwargs.pop("start_new_session", True)
        if "cwd" in kwargs:
            cwd = kwargs.pop("cwd")
        kwargs.pop("env", None)

        return self.system.proc.spawn(
            final_cmd,
            cwd=cwd,
            env=proc_env,
            start_new_session=start_new_session,
            **kwargs
        )

    def exec_run(self, cmd: Union[str, List[str]], env: Dict[str, str] = {},
                 workdir: Optional[str] = None, timeout: Optional[float] = None) -> Dict[str, Any]:
        """Blocking command execution matching DockerSandbox.exec_run signature."""
        import time as _time
        proc_env = self._build_env(env)
        final_cmd = self._rewrite_command(cmd)
        cwd = workdir if workdir else self.repo_root

        is_str = isinstance(final_cmd, str)
        start_time = _time.time()

        try:
            proc = subprocess.run(
                final_cmd,
                cwd=cwd,
                env=proc_env,
                shell=is_str,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout,
            )
            duration = _time.time() - start_time
            return {
                "cmd": cmd if isinstance(cmd, str) else " ".join(cmd),
                "exit_code": proc.returncode,
                "duration_s": duration,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
                "timed_out": False,
            }
        except subprocess.TimeoutExpired:
            return {
                "cmd": cmd if isinstance(cmd, str) else " ".join(cmd),
                "exit_code": -1,
                "duration_s": _time.time() - start_time,
                "stdout": "",
                "stderr": "Timeout",
                "timed_out": True,
            }

    # ── Stubs for Docker-only features ──

    def kill_process_by_command(self, command_pattern: str) -> bool:
        """Best-effort kill via pkill on host."""
        try:
            result = subprocess.run(
                ["pkill", "-9", "-f", command_pattern],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5,
            )
            return result.returncode == 0
        except Exception:
            return False

    def check_package_installed(self, package: str) -> bool:
        """Check if a Python package is installed in the venv."""
        pip_exe = os.path.join(self.bin_dir, "pip.exe" if self.is_win else "pip") if self.bin_dir else "pip"
        try:
            result = subprocess.run(
                [pip_exe, "show", package],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=5, env=self._build_env({}),
            )
            return result.returncode == 0
        except Exception:
            return False

    def filter_install_command(self, cmd: str) -> Optional[str]:
        """No filtering in venv mode — always run the command."""
        return cmd
