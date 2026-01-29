import os
import time
import subprocess
import signal
import glob
from typing import Dict, List, Optional, Any, BinaryIO, TextIO, Union

class FileSystemInterface:
    def exists(self, path: str) -> bool: pass
    def remove(self, path: str) -> bool: pass
    def listdir(self, path: str) -> List[str]: pass
    def getmtime(self, path: str) -> float: pass
    def makedirs(self, path: str, exist_ok: bool = False): pass
    def open(self, path: str, mode: str = 'r') -> Any: pass
    def glob(self, pattern: str) -> List[str]: pass

class ProcessHandle:
    """Abstraction of subprocess.Popen"""
    @property
    def pid(self) -> int: pass
    @property
    def stdout(self) -> Optional[Union[BinaryIO, TextIO]]: pass
    @property
    def stderr(self) -> Optional[Union[BinaryIO, TextIO]]: pass
    def poll(self) -> Optional[int]: pass
    def wait(self, timeout: Optional[float] = None) -> int: pass
    def send_signal(self, sig: int): pass
    def terminate(self): pass
    def kill(self): pass

class ProcessInterface:
    def spawn(self, cmd: str, env: Dict[str, str], **kwargs) -> ProcessHandle: pass
    def kill_group(self, pid: int, sig: int): pass
    def get_pgid(self, pid: int) -> int: pass

class ClockInterface:
    def time(self) -> float: pass
    def sleep(self, seconds: float): pass

class SystemInterface:
    def __init__(self, fs: FileSystemInterface, proc: ProcessInterface, clock: ClockInterface):
        self.fs = fs
        self.proc = proc
        self.clock = clock

# --- Production Implementations ---

class RealFileSystem(FileSystemInterface):
    def exists(self, path: str) -> bool: return os.path.exists(path)
    def remove(self, path: str): os.remove(path)
    def listdir(self, path: str) -> List[str]: return os.listdir(path)
    def getmtime(self, path: str) -> float: return os.path.getmtime(path)
    def makedirs(self, path: str, exist_ok: bool = False): os.makedirs(path, exist_ok=exist_ok)
    def open(self, path: str, mode: str = 'r'): return open(path, mode)
    def glob(self, pattern: str) -> List[str]: return glob.glob(pattern)

class RealProcessHandle(ProcessHandle):
    def __init__(self, popen: subprocess.Popen):
        self._p = popen
    @property
    def pid(self) -> int: return self._p.pid
    @property
    def stdout(self): return self._p.stdout
    @property
    def stderr(self): return self._p.stderr
    @property
    def returncode(self): return self._p.returncode
    
    def poll(self): return self._p.poll()
    def wait(self, timeout=None): return self._p.wait(timeout=timeout)
    def send_signal(self, sig): self._p.send_signal(sig)
    def terminate(self): self._p.terminate()
    def kill(self): self._p.kill()

class RealProcess(ProcessInterface):
    def spawn(self, cmd, env, **kwargs):
        # Portable isolation: start_new_session=True ensures distinct PGID
        if "start_new_session" not in kwargs:
            kwargs["start_new_session"] = True
        p = subprocess.Popen(cmd, env=env, **kwargs)
        return RealProcessHandle(p)

    def kill_group(self, pid, sig):
        try:
            target_pgid = os.getpgid(pid)
            current_pgid = os.getpgrp()
            
            # SUPERVISOR SAFETY: Never kill our own group
            if target_pgid == current_pgid:
                print(f"⚠️ [System] Supervisor-Safety Guard: Target PID {pid} is in same PGID ({target_pgid}) as supervisor. Fallback: killing PID only.")
                os.kill(pid, sig)
                return

            os.killpg(target_pgid, sig)
        except OSError: pass

    def get_pgid(self, pid):
        return os.getpgid(pid)

class RealClock(ClockInterface):
    def time(self): return time.time()
    def sleep(self, seconds): time.sleep(seconds)

class RealSystem(SystemInterface):
    def __init__(self):
        super().__init__(RealFileSystem(), RealProcess(), RealClock())
