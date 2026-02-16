import subprocess
import os
import sys
import time
import uuid
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from .utils import configs

class DockerSandbox:
    def __init__(self, repo_path: str, artifact_dir: str, image_name: str = "remoroo-cli", cache_env: bool = False):
        self.repo_path = os.path.abspath(repo_path)
        self.artifact_dir = os.path.abspath(artifact_dir)
        self.image_name = image_name
        self.cache_env = cache_env  # Enable environment caching
        self.container_name = f"remoroo-sandbox-{uuid.uuid4().hex[:8]}"
        self.is_running = False
        self.available = self.check_docker()
        if not self.available:
            print("⚠️  Docker not available. Sandbox disabled.")

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
            
            # Locate Dockerfile.worker relative to this module
            # remoroo_cli/remoroo/engine/sandbox.py -> root/Dockerfile.worker
            current_file = Path(__file__).resolve()
            
            # Specialized Dockerfile is always in the same directory as sandbox.py
            dockerfile_path = current_file.parent / "Dockerfile"
            
            if not dockerfile_path.exists():
                 print(f"⚠️  Warning: {dockerfile_path} not found. Cannot build sandbox.")
                 return

            # Use the directory containing Dockerfile as build context
            build_context = dockerfile_path.parent

            subprocess.check_call(
                ["docker", "build", "-t", self.image_name, "-f", str(dockerfile_path), "."],
                cwd=str(build_context),
                stdout=sys.stdout,
                stderr=sys.stderr
            )

    def start(self):
        """Start the persistent sandbox container."""
        if self.is_running:
            return

        self.build_image_if_missing(os.path.dirname(self.repo_path) if os.path.isfile(self.repo_path) else self.repo_path)

        print(f"📦 Starting sandbox container '{self.container_name}'...")
        
        # v13: Mirrored Mount (Zero-Mapping).
        # We mount the host repo_path to the exact same path inside the container.
        # This makes the filesystem layout identical between host and container.
        
        cmd = [
            "docker", "run", "-d", "--rm",
            "--name", self.container_name,
            "-v", f"{self.repo_path}:{self.repo_path}",
            "-v", f"{self.artifact_dir}:{self.artifact_dir}",
            # v14.1: Mirrored Mounts (Universal Path Unification).
            # We mirror both the repo and the artifact cache to their exact host paths.
            "--workdir", self.repo_path, 
            "--entrypoint", "sleep",
            self.image_name, 
            "infinity"
        ]
        
        subprocess.check_call(cmd)
        self.is_running = True
        
        # Fix permissions? In Docker usually root.
        # For now, we assume user mapping is not strict p0.

    def commit_state(self, success: bool = True):
        """
        Commit the current container state to the image for reuse in future runs.
        This enables environment caching - installed packages persist across runs.
        """
        if not self.is_running:
            print("ℹ️  Container not running, skipping commit")
            return
        
        if not success:
            print("⚠️  Run failed. Skipping Docker commit to avoid persisting bad state.")
            return
        
        if not self.cache_env:
            return  # Caching disabled, don't commit
        
        try:
            # Commit container state to the same image name (overwrite)
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

    def stop(self):
        """Stop and remove the container."""
        if self.is_running:
            try:
                subprocess.run(["docker", "kill", self.container_name], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except:
                pass
            self.is_running = False

    def commit(self, success: bool = True):
        """
        Commit container changes to image if run was successful.
        This persists installed packages for future runs.
        
        Args:
            success: Whether the run was successful. Only commits if True.
        """
        if not self.is_running:
            print("ℹ️  Container not running, skipping commit")
            return
        
        if not success:
            print("⚠️  Run failed. Skipping Docker commit to avoid persisting bad state.")
            return
        
        try:
            # Commit container state to the same image name
            commit_tag = f"{self.image_name}:latest"
            print(f"💾 Committing Docker container changes to {commit_tag}...")
            
            subprocess.check_call(
                ["docker", "commit", self.container_name, commit_tag],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            
            print(f"✅ Docker environment persisted for future runs")
        except subprocess.CalledProcessError as e:
            print(f"⚠️  Docker commit failed: {e}")
        except Exception as e:
            print(f"⚠️  Unexpected error during Docker commit: {e}")


    def kill_process_by_command(self, command_pattern: str) -> bool:
        """
        Kill processes inside the container matching the given command pattern.
        
        This is critical for cleaning up orphan processes that survive when the
        host-side `docker exec` wrapper is killed (SIGTERM/SIGKILL to docker exec
        does NOT propagate to the process inside the container).
        
        Uses pkill -9 -f to match by full command line.
        Returns True if any process was killed.
        """
        if not self.is_running:
            return False
        
        try:
            # First: find matching PIDs for logging
            pgrep_result = subprocess.run(
                ["docker", "exec", self.container_name, "pgrep", "-f", command_pattern],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5
            )
            pids = pgrep_result.stdout.strip().split("\n") if pgrep_result.returncode == 0 else []
            pids = [p.strip() for p in pids if p.strip()]
            
            if not pids:
                return False
            
            print(f"🐳 [DockerSandbox] Killing {len(pids)} orphan process(es) inside container matching '{command_pattern}': PIDs {pids}")
            
            # Kill each PID individually with -9 to ensure termination
            # Using individual kill instead of pkill to also kill the /bin/sh wrapper
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
        """
        Check if a Python package is already installed in the container.
        Returns True if installed, False otherwise.
        """
        if not self.is_running:
            return False
        
        try:
            # Use pip show to check if package exists
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
            return cmd  # No filtering if caching disabled
        
        if not ("pip install" in cmd or "pip3 install" in cmd):
            return cmd  # Not a pip install command
        
        # Skip special flags that we always want to run
        if any(flag in cmd for flag in ["-r", "--requirement", "-e", "--editable", "."]):
            return cmd  # Always run requirements.txt, editable installs
        
        # Extract package names from command
        parts = cmd.split()
        try:
            install_idx = next(i for i, p in enumerate(parts) if p == "install")
            packages = parts[install_idx + 1:]
            
            # Filter out packages that are already installed
            packages_to_install = []
            for pkg in packages:
                # Remove version specifiers for checking
                pkg_name = pkg.split(">=")[0].split("==")[0].split("<")[0].split(">")[0].strip("'\"")
                if not self.check_package_installed(pkg_name):
                    packages_to_install.append(pkg)
                else:
                    print(f"  ✓ {pkg_name} already installed (skipping)")
            
            if not packages_to_install:
                print(f"  ℹ️  All packages already installed, skipping: {cmd}")
                return None  # All packages installed, skip command
            
            # Reconstruct command with only missing packages
            filtered_cmd = " ".join(parts[:install_idx + 1] + packages_to_install)
            if filtered_cmd != cmd:
                print(f"  📦 Filtered install command: {filtered_cmd}")
            return filtered_cmd
        except (StopIteration, IndexError):
            return cmd  # Couldn't parse, run as-is

    def exec_popen(self, cmd: List[str], env: Dict[str, str] = {}, workdir: Optional[str] = None) -> subprocess.Popen:
        """
        Run a command via docker exec, returning a Popen object for streaming.
        """
        if not self.is_running:
            self.start()

        # Construct exec command
        exec_cmd = ["docker", "exec", "-i"]
        
        # Workdir
        if workdir:
            exec_cmd.extend(["-w", workdir])
        
        # Env
        for k, v in env.items():
            exec_cmd.extend(["-e", f"{k}={v}"])
            
        exec_cmd.append(self.container_name)
        if isinstance(cmd, str):
            # If shell=True behavior is needed, we should wrap in sh -c
            exec_cmd.extend(["/bin/sh", "-c", cmd])
        else:
            exec_cmd.extend(cmd)

        # print(f"🐳 [DockerSandbox] Executing: {' '.join(exec_cmd)}")
        return subprocess.Popen(
            exec_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

    def exec_run(self, cmd: List[str], env: Dict[str, str] = {}, workdir: Optional[str] = None, timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        Run a command via docker exec.
        Matches the return signature of executor.run_command_with_timeout mostly.
        """
        if not self.is_running:
            self.start()

        # Construct exec command
        exec_cmd = ["docker", "exec", "-i"]
        
        # Workdir
        if workdir:
            exec_cmd.extend(["-w", workdir])
        
        # Env
        for k, v in env.items():
            exec_cmd.extend(["-e", f"{k}={v}"])
            
        exec_cmd.append(self.container_name)
        exec_cmd.extend(cmd)

        start_time = time.time()
        
        try:
            # We use subprocess.run for simplicity for now, passing text=True
            # For streaming, we'd need Popen similar to executor.py
            # But let's wrap strictly.
            
            proc = subprocess.run(
                exec_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout
            )
            
            duration = time.time() - start_time
            return {
                "cmd": " ".join(cmd),
                "exit_code": proc.returncode,
                "duration_s": duration,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
                "timed_out": False
            }
        except subprocess.TimeoutExpired:
            return {
                "cmd": " ".join(cmd),
                "exit_code": -1,
                "duration_s": time.time() - start_time,
                "stdout": "",
                "stderr": "Timeout",
                "timed_out": True
            }
