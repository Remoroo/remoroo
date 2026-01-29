import subprocess
import os
import sys
import time
import uuid
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from .utils import configs

class DockerSandbox:
    def __init__(self, repo_path: str, artifact_dir: str, image_name: str = "remoroo-worker"):
        self.repo_path = os.path.abspath(repo_path)
        self.artifact_dir = os.path.abspath(artifact_dir)
        self.image_name = image_name
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
