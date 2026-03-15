import subprocess
import os
import shlex
import sys
import time
import uuid
from typing import List, Dict, Any, Optional, Tuple, Union
from pathlib import Path
from .utils import configs


class DockerSandbox:
    def __init__(self, repo_path: str, artifact_dir: str, image_name: str = "remoroo-cli",
                 cache_env: bool = False, memory_limit: str = "8g", cpu_limit: str = "4"):
        self.repo_path = os.path.abspath(repo_path)
        self.artifact_dir = os.path.abspath(artifact_dir)
        self.image_name = image_name
        self.cache_env = cache_env
        self.memory_limit = memory_limit
        self.cpu_limit = cpu_limit
        self.container_name = f"remoroo-sandbox-{uuid.uuid4().hex[:8]}"
        self.is_running = False
        self.available = self.check_docker()
        if not self.available:
            print("⚠️  Docker not available. Sandbox disabled.")

    def host_to_container(self, host_path: str) -> str:
        """Map a host path to its container equivalent. Identity mapping for DockerSandbox."""
        return host_path

    def check_docker(self) -> bool:
        """Check if docker daemon is accessible."""
        try:
            subprocess.check_call(
                ["docker", "info"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    def check_image(self) -> bool:
        """Check if image exists."""
        try:
            subprocess.check_call(
                ["docker", "image", "inspect", self.image_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            return True
        except subprocess.CalledProcessError:
            return False

    def build_image_if_missing(self, context_path: str):
        """Build the worker image if it doesn't exist."""
        if not self.check_image():
            print(f"📦 Building sandbox image '{self.image_name}'...")

            current_file = Path(__file__).resolve()
            dockerfile_path = current_file.parent / "Dockerfile"

            if not dockerfile_path.exists():
                 print(f"⚠️  Warning: {dockerfile_path} not found. Cannot build sandbox.")
                 return

            build_context = dockerfile_path.parent
            subprocess.check_call(
                ["docker", "build", "-t", self.image_name, "-f", str(dockerfile_path), "."],
                cwd=str(build_context),
                stdout=sys.stdout,
                stderr=sys.stderr
            )

    def _is_venv_cross_platform(self, venv_dir: str) -> bool:
        """
        Check if a .venv was created on a different platform (e.g. macOS venv
        mounted into a Linux container). Returns True if the venv is incompatible
        and should be removed.
        """
        try:
            result = subprocess.run(
                ["docker", "exec", self.container_name, "/bin/sh", "-c",
                 f"test -f {shlex.quote(venv_dir)}/pyvenv.cfg && cat {shlex.quote(venv_dir)}/pyvenv.cfg"],
                timeout=5, capture_output=True, text=True,
            )
            if result.returncode != 0:
                return False

            cfg = result.stdout
            # If home points to a macOS path, it's cross-platform
            if "/Library/Frameworks/" in cfg or "/usr/local/Cellar/" in cfg:
                return True

            # Check the python binary — if it's a Mach-O binary in a Linux container it won't run
            python_path = f"{venv_dir}/bin/python"
            file_result = subprocess.run(
                ["docker", "exec", self.container_name, "file", python_path],
                timeout=5, capture_output=True, text=True,
            )
            if file_result.returncode == 0 and "Mach-O" in file_result.stdout:
                return True

            return False
        except Exception:
            return False

    def start(self):
        """Start the persistent sandbox container."""
        if self.is_running:
            return

        self.build_image_if_missing(os.path.dirname(self.repo_path) if os.path.isfile(self.repo_path) else self.repo_path)

        print(f"📦 Starting sandbox container '{self.container_name}'...")

        host_home = os.path.expanduser("~")
        host_cache = os.path.join(host_home, ".cache")

        cmd = [
            "docker", "run", "-d",
            "--name", self.container_name,
            "--memory", self.memory_limit,
            "--cpus", self.cpu_limit,
            "-v", f"{self.repo_path}:{self.repo_path}",
            "-v", f"{self.artifact_dir}:{self.artifact_dir}",
            "-v", f"{host_cache}:/root/.cache",
            "--workdir", self.repo_path,
            "--entrypoint", "sleep",
            self.image_name,
            "infinity"
        ]

        subprocess.check_call(cmd)
        self.is_running = True

        # Only purge venvs that have wrong-platform binaries (e.g. macOS venv in Linux).
        # Venvs created inside the container (Linux) are preserved.
        try:
            for venv_name in [".venv", "venv"]:
                venv_dir = os.path.join(self.repo_path, venv_name)
                if self._is_venv_cross_platform(venv_dir):
                    print(f"  🔄 Removing cross-platform {venv_name}/ (macOS binaries in Linux container)")
                    subprocess.run(
                        ["docker", "exec", self.container_name, "rm", "-rf", venv_dir],
                        timeout=10, capture_output=True,
                    )
                # Always safe to remove __pycache__
            subprocess.run(
                ["docker", "exec", self.container_name, "/bin/sh", "-c",
                 "find . -maxdepth 3 -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true"],
                timeout=10, capture_output=True,
            )
        except Exception as e:
            print(f"  ⚠️ venv cleanup check failed (non-fatal): {e}")

    def commit_state(self, success: bool = True):
        """
        Commit the current container state to the image for reuse in future runs.
        This enables environment caching — installed packages persist across runs.
        """
        if not self.is_running:
            print("ℹ️  Container not running, skipping commit")
            return

        if not success:
            print("⚠️  Run failed. Skipping Docker commit to avoid persisting bad state.")
            return

        if not self.cache_env:
            return

        try:
            commit_tag = f"{self.image_name}:latest"
            print(f"💾 Committing Docker container state to {commit_tag}...")

            subprocess.check_call(
                ["docker", "commit", self.container_name, commit_tag],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )

            print(f"✅ Docker environment cached for future runs")
        except subprocess.CalledProcessError as e:
            print(f"⚠️  Docker commit failed: {e}")
        except Exception as e:
            print(f"⚠️  Unexpected error during Docker commit: {e}")

    # Keep `commit` as an alias for backward compat
    commit = commit_state

    def stop(self):
        """Stop and remove the container."""
        if self.is_running:
            try:
                subprocess.run(
                    ["docker", "rm", "-f", self.container_name],
                    check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
            except Exception:
                pass
            self.is_running = False

    def kill_process_by_command(self, command_pattern: str) -> bool:
        """
        Kill processes inside the container matching the given command pattern.

        This is critical for cleaning up orphan processes that survive when the
        host-side `docker exec` wrapper is killed (SIGTERM/SIGKILL to docker exec
        does NOT propagate to the process inside the container).

        Returns True if any process was killed.
        """
        if not self.is_running:
            return False

        try:
            pgrep_result = subprocess.run(
                ["docker", "exec", self.container_name, "pgrep", "-f", command_pattern],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5
            )
            pids = pgrep_result.stdout.strip().split("\n") if pgrep_result.returncode == 0 else []
            pids = [p.strip() for p in pids if p.strip()]

            if not pids:
                return False

            print(f"🐳 [DockerSandbox] Killing {len(pids)} orphan process(es) inside container matching '{command_pattern}': PIDs {pids}")

            for pid in pids:
                try:
                    subprocess.run(
                        ["docker", "exec", self.container_name, "kill", "-9", pid],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5
                    )
                except Exception:
                    pass

            return True
        except Exception as e:
            print(f"⚠️  [DockerSandbox] Error killing container process: {e}")
            return False

    def check_package_installed(self, package: str) -> bool:
        """Check if a Python package is already installed in the container."""
        if not self.is_running:
            return False

        try:
            result = subprocess.run(
                ["docker", "exec", self.container_name, "pip", "show", package],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5
            )
            return result.returncode == 0
        except Exception:
            return False

    def filter_install_command(self, cmd: str) -> Optional[str]:
        """
        Filter pip install commands if cache_env is enabled.
        Returns None if all packages are already installed, otherwise returns filtered command.
        """
        if not self.cache_env:
            return cmd

        if not ("pip install" in cmd or "pip3 install" in cmd):
            return cmd

        if any(flag in cmd for flag in ["-r", "--requirement", "-e", "--editable", "."]):
            return cmd

        parts = cmd.split()
        try:
            install_idx = next(i for i, p in enumerate(parts) if p == "install")
            packages = parts[install_idx + 1:]

            packages_to_install = []
            for pkg in packages:
                pkg_name = pkg.split(">=")[0].split("==")[0].split("<")[0].split(">")[0].strip("'\"")
                if not self.check_package_installed(pkg_name):
                    packages_to_install.append(pkg)
                else:
                    print(f"  ✓ {pkg_name} already installed (skipping)")

            if not packages_to_install:
                print(f"  ℹ️  All packages already installed, skipping: {cmd}")
                return None

            filtered_cmd = " ".join(parts[:install_idx + 1] + packages_to_install)
            if filtered_cmd != cmd:
                print(f"  📦 Filtered install command: {filtered_cmd}")
            return filtered_cmd
        except (StopIteration, IndexError):
            return cmd

    @staticmethod
    def _encode_env_flag(key: str, value: str) -> str:
        """Encode a KEY=VALUE pair for docker exec -e, handling special characters."""
        return f"{key}={value}"

    def exec_popen(self, cmd: Union[str, List[str]], env: Dict[str, str] = {},
                   workdir: Optional[str] = None) -> subprocess.Popen:
        """Run a command via docker exec, returning a Popen object for streaming."""
        if not self.is_running:
            self.start()

        exec_cmd = ["docker", "exec", "-i"]

        if workdir:
            exec_cmd.extend(["-w", workdir])

        for k, v in env.items():
            exec_cmd.extend(["-e", self._encode_env_flag(k, v)])

        exec_cmd.append(self.container_name)
        if isinstance(cmd, str):
            exec_cmd.extend(["/bin/sh", "-c", cmd])
        else:
            exec_cmd.extend(cmd)

        return subprocess.Popen(
            exec_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

    def exec_run(self, cmd: Union[str, List[str]], env: Dict[str, str] = {},
                 workdir: Optional[str] = None, timeout: Optional[float] = None) -> Dict[str, Any]:
        """Run a command via docker exec (blocking). Returns a dict with exit_code, stdout, stderr."""
        if not self.is_running:
            self.start()

        exec_cmd = ["docker", "exec", "-i"]

        if workdir:
            exec_cmd.extend(["-w", workdir])

        for k, v in env.items():
            exec_cmd.extend(["-e", self._encode_env_flag(k, v)])

        exec_cmd.append(self.container_name)
        if isinstance(cmd, str):
            exec_cmd.extend(["/bin/sh", "-c", cmd])
        else:
            exec_cmd.extend(cmd)

        start_time = time.time()

        try:
            proc = subprocess.run(
                exec_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout
            )

            duration = time.time() - start_time
            return {
                "cmd": cmd if isinstance(cmd, str) else " ".join(cmd),
                "exit_code": proc.returncode,
                "duration_s": duration,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
                "timed_out": False
            }
        except subprocess.TimeoutExpired:
            return {
                "cmd": cmd if isinstance(cmd, str) else " ".join(cmd),
                "exit_code": -1,
                "duration_s": time.time() - start_time,
                "stdout": "",
                "stderr": "Timeout",
                "timed_out": True
            }
