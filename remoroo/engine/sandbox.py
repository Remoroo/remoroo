import subprocess
import os
import re
import shlex
import sys
import time
import uuid
from typing import List, Dict, Any, Optional, Tuple, Union
from pathlib import Path


# ---------------------------------------------------------------------------
# PyTorch CUDA wheel variants — ordered highest-first.
# Each entry: (min_cuda_major, min_cuda_minor, tag, torch_index_url)
# Only needs to track versions that PyTorch actually ships wheels for.
# ---------------------------------------------------------------------------
_PYTORCH_CUDA_VARIANTS: List[Tuple[int, int, str, str]] = [
    (12, 4, "cu124", "https://download.pytorch.org/whl/cu124"),
    (12, 1, "cu121", "https://download.pytorch.org/whl/cu121"),
    (11, 8, "cu118", "https://download.pytorch.org/whl/cu118"),
]

_CPU_TORCH_INDEX = "https://download.pytorch.org/whl/cpu"


# ---------------------------------------------------------------------------
# Docker daemon check (unchanged from before)
# ---------------------------------------------------------------------------

def check_docker_daemon() -> bool:
    """True if the ``docker`` CLI exists and the daemon responds (``docker info``)."""
    try:
        subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
            check=True,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return False


# ---------------------------------------------------------------------------
# GPU detection  (standalone — no brain imports, no transport)
# ---------------------------------------------------------------------------

def detect_host_gpu() -> Dict[str, Any]:
    """Probe the HOST for NVIDIA GPU hardware.

    Returns a dict with at least ``{"type": "none"}`` or
    ``{"type": "nvidia", "driver": str, "cuda_version": str, "devices": [...]}``.

    Respects:
      REMOROO_FORCE_CPU=1       → always returns ``{"type": "none"}``
      REMOROO_CUDA_VERSION=cuXX → skips detection, uses the given variant
    """
    if os.environ.get("REMOROO_FORCE_CPU", "").strip() == "1":
        return {"type": "none"}

    # macOS: Docker runs a Linux VM — no GPU passthrough regardless of host HW.
    if sys.platform == "darwin":
        return {"type": "none"}

    # Manual override — trust the user.
    override = os.environ.get("REMOROO_CUDA_VERSION", "").strip().lower()
    if override:
        for _, _, tag, index_url in _PYTORCH_CUDA_VARIANTS:
            if tag == override:
                return {
                    "type": "nvidia",
                    "driver": "unknown (override)",
                    "cuda_version": override,
                    "devices": [],
                    "_tag_override": tag,
                    "_torch_index_override": index_url,
                }
        print(f"  ⚠️  REMOROO_CUDA_VERSION={override} not recognized, auto-detecting...")

    try:
        result = subprocess.run(
            ["nvidia-smi"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return {"type": "none"}

        cuda_version: Optional[str] = None
        driver_version: Optional[str] = None
        for line in result.stdout.split("\n"):
            if "CUDA Version" in line:
                m = re.search(r"CUDA Version:\s*(\d+\.\d+)", line)
                if m:
                    cuda_version = m.group(1)
            if "Driver Version" in line:
                m = re.search(r"Driver Version:\s*([\d.]+)", line)
                if m:
                    driver_version = m.group(1)

        if not cuda_version:
            return {"type": "none"}

        devices: List[Dict[str, Any]] = []
        try:
            dev_result = subprocess.run(
                ["nvidia-smi", "--query-gpu=name,memory.total",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=10,
            )
            if dev_result.returncode == 0:
                for line in dev_result.stdout.strip().split("\n"):
                    parts = [p.strip() for p in line.split(",")]
                    if len(parts) >= 2:
                        devices.append({"name": parts[0], "memory_mb": float(parts[1])})
        except Exception:
            pass

        return {
            "type": "nvidia",
            "driver": driver_version or "unknown",
            "cuda_version": cuda_version,
            "devices": devices,
        }
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {"type": "none"}
    except Exception:
        return {"type": "none"}


def check_nvidia_docker_runtime() -> bool:
    """Return True if Docker has the NVIDIA runtime (nvidia-container-toolkit).

    Fast path: parse ``docker info`` for an ``nvidia`` runtime entry.
    This avoids pulling images and completes in <1 s.
    """
    try:
        result = subprocess.run(
            ["docker", "info", "--format", "{{json .Runtimes}}"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and "nvidia" in result.stdout.lower():
            return True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
    except Exception:
        pass

    # Fallback: scan full ``docker info`` text for nvidia runtime lines.
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            for line in result.stdout.split("\n"):
                low = line.lower()
                if "nvidia" in low and "runtime" in low:
                    return True
    except Exception:
        pass

    return False


def _resolve_cuda_variant(cuda_version_str: str) -> Tuple[str, str]:
    """Map a CUDA version string (e.g. ``'12.3'``) to ``(tag, torch_index_url)``.

    Falls back to ``("cpu", cpu_index)`` if the version is too old or unparseable.
    """
    try:
        parts = cuda_version_str.split(".")
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
    except (ValueError, IndexError):
        return "cpu", _CPU_TORCH_INDEX

    for min_major, min_minor, tag, index_url in _PYTORCH_CUDA_VARIANTS:
        if (major, minor) >= (min_major, min_minor):
            return tag, index_url

    return "cpu", _CPU_TORCH_INDEX


# ---------------------------------------------------------------------------
# Error helpers
# ---------------------------------------------------------------------------

DOCKER_SANDBOX_UNAVAILABLE_MSG = (
    "Docker sandbox is not available: the `docker` command was not found on PATH, "
    "or `docker info` failed (daemon not running). "
    "Install and start Docker, or run with `--engine venv` to use a local Python venv instead."
)


class DockerSandboxUnavailableError(RuntimeError):
    """Raised when Docker engine is selected but the CLI/daemon cannot run commands."""


# ---------------------------------------------------------------------------
# DockerSandbox
# ---------------------------------------------------------------------------

class DockerSandbox:
    def __init__(self, repo_path: str, artifact_dir: str, image_name: str = "remoroo-cli",
                 cache_env: bool = False):
        self.repo_path = os.path.abspath(repo_path)
        self.artifact_dir = os.path.abspath(artifact_dir)
        self._base_image_name = image_name
        self.cache_env = cache_env
        self.container_name = f"remoroo-sandbox-{uuid.uuid4().hex[:8]}"
        self.is_running = False
        self.available = self.check_docker()

        # --- GPU detection & image tag resolution ---
        self._gpu_info = detect_host_gpu()
        self._use_gpu = False
        self._resolved_tag = "cpu"
        self._build_args: Dict[str, str] = {}

        if self.available and self._gpu_info["type"] == "nvidia":
            # Check override first
            if "_tag_override" in self._gpu_info:
                tag = self._gpu_info["_tag_override"]
                torch_index = self._gpu_info["_torch_index_override"]
            else:
                tag, torch_index = _resolve_cuda_variant(self._gpu_info["cuda_version"])

            if tag != "cpu" and check_nvidia_docker_runtime():
                self._use_gpu = True
                self._resolved_tag = tag
                self._build_args = {"TORCH_INDEX": torch_index}
                devs = self._gpu_info.get("devices", [])
                dev_str = devs[0]["name"] if devs else "NVIDIA GPU"
                print(f"  🖥️  GPU detected: {dev_str} — using CUDA image (remoroo-cli:{tag})")
            elif tag != "cpu":
                devs = self._gpu_info.get("devices", [])
                dev_str = devs[0]["name"] if devs else "NVIDIA GPU"
                raise DockerSandboxUnavailableError(
                    f"NVIDIA GPU detected ({dev_str}) but nvidia-container-toolkit is not "
                    f"installed, so Docker cannot access the GPU.\n\n"
                    f"Either:\n"
                    f"  1. Install nvidia-container-toolkit:\n"
                    f"     https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html\n"
                    f"  2. Run with --engine venv to use the GPU directly (no Docker isolation)\n"
                    f"  3. Set REMOROO_FORCE_CPU=1 to run on CPU inside Docker"
                )

        self.image_name = f"{self._base_image_name}:{self._resolved_tag}"

    def host_to_container(self, host_path: str) -> str:
        """Map a host path to its container equivalent. Identity mapping for DockerSandbox."""
        return host_path

    def check_docker(self) -> bool:
        """Check if docker daemon is accessible."""
        return check_docker_daemon()

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
            cmd = [
                "docker", "build",
                "-t", self.image_name,
                "-f", str(dockerfile_path),
            ]
            for key, value in self._build_args.items():
                cmd.extend(["--build-arg", f"{key}={value}"])
            cmd.append(".")

            subprocess.check_call(
                cmd,
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
            if "/Library/Frameworks/" in cfg or "/usr/local/Cellar/" in cfg:
                return True

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

        if not self.available:
            raise DockerSandboxUnavailableError(DOCKER_SANDBOX_UNAVAILABLE_MSG)

        self.build_image_if_missing(os.path.dirname(self.repo_path) if os.path.isfile(self.repo_path) else self.repo_path)

        print(f"📦 Starting sandbox container '{self.container_name}'...")

        host_home = os.path.expanduser("~")
        host_cache = os.path.join(host_home, ".cache")

        # No --memory/--cpus: use Docker / host defaults so the same command works
        # on laptops, CI, Codespaces, rootless, etc.
        cmd: List[str] = ["docker", "run", "-d"]
        if self._use_gpu:
            cmd.extend(["--gpus", "all"])
        cmd.extend([
            "--name", self.container_name,
            "-v", f"{self.repo_path}:{self.repo_path}",
            "-v", f"{self.artifact_dir}:{self.artifact_dir}",
            "-v", f"{host_cache}:/root/.cache",
            "--workdir", self.repo_path,
            "--entrypoint", "sleep",
            self.image_name,
            "infinity",
        ])

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            err_txt = (result.stderr or result.stdout or "").strip()
            hint = (
                "You can run without a container using `remoroo run --engine venv` "
                "(host Python / project .venv)."
            )
            detail = f"{hint}\n\n{err_txt}" if err_txt else hint
            raise DockerSandboxUnavailableError(
                f"{DOCKER_SANDBOX_UNAVAILABLE_MSG}\n\n{detail}"
            )
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
            subprocess.run(
                ["docker", "exec", self.container_name, "/bin/sh", "-c",
                 "find . -maxdepth 3 -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true"],
                timeout=10, capture_output=True,
            )
        except Exception as e:
            print(f"  ⚠️ venv cleanup check failed (non-fatal): {e}")

        # Post-start GPU verification
        if self._use_gpu:
            self._verify_gpu_in_container()

    def _verify_gpu_in_container(self):
        """Quick probe: confirm torch sees CUDA inside the running container."""
        try:
            result = self.exec_run(
                ["python3", "-c", "import torch; print('cuda' if torch.cuda.is_available() else 'cpu')"],
                timeout=30,
            )
            detected = (result.get("stdout") or "").strip()
            if detected == "cuda":
                print("  ✅ GPU verified inside container (torch.cuda.is_available() == True)")
            else:
                print(
                    "  ⚠️  GPU expected but torch.cuda.is_available() returned False inside container.\n"
                    "     The run will proceed on CPU. Check nvidia-container-toolkit installation."
                )
        except Exception as e:
            print(f"  ⚠️  GPU verification probe failed (non-fatal): {e}")

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
            print(f"💾 Committing Docker container state to {self.image_name}...")

            subprocess.check_call(
                ["docker", "commit", self.container_name, self.image_name],
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
        if not self.available:
            raise DockerSandboxUnavailableError(DOCKER_SANDBOX_UNAVAILABLE_MSG)
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
        if not self.available:
            raise DockerSandboxUnavailableError(DOCKER_SANDBOX_UNAVAILABLE_MSG)
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
