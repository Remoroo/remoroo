"""Persist cuRobo's runtime-compiled CUDA kernels (cubins) to DISK, across processes.

WHY: cuRobo's default `cuda_core` backend NVRTC-compiles every curobolib kernel at runtime and
caches the result in a PLAIN DICT (`CudaCoreKernelCache.compiled_kernels`) — in-memory only, by
design, because cuRobo assumes a long-lived process. Our workflow restarts the edge after every
authored-cell fix (`remoroo edge restart`), so every restart re-pays a full compiler pass —
minutes on a Jetson. cuRobo already computes a PERFECT disk key (SHA256 over kernel source bytes
+ kernel name + compile flags + `sm_<arch>`, `get_kernel_hash`); it just never writes the cubin
out. We monkeypatch `get_or_compile_kernel` with a disk-backed version: load `<key>.cubin` when
present, compile-and-save otherwise. The vendored cuRobo (embedded upstream repo) is NOT edited.

Everything is BEST-EFFORT: any failure (cuda.core API drift, unwritable cache dir, corrupt file,
stale cubin) falls back to cuRobo's own compile path — never worse than no cache. Source-content
hashing self-invalidates on a cuRobo upgrade; the arch in the key self-invalidates on a GPU swap.
Kill-switch: REMOROO_CUROBO_DISK_CACHE=0.

THE SUBTLETY — template name mangling: cuRobo compiles with
`prog.compile("cubin", name_expressions=(kernel_name,))`; `mod.get_kernel(kernel_name)` then
resolves the C++ name EXPRESSION (e.g. `reduce<float>`) to the mangled symbol via the mapping
NVRTC recorded in the Program. A cubin re-loaded from disk has no Program, so we persist that
mapping in a JSON sidecar and restore it onto the loaded ObjectCode (constructor kwarg where the
installed cuda.core supports it, else the private attribute). If restoration fails, get_kernel
raises, the load returns None, and we compile as before.

GPU-only by nature; this module itself imports cleanly off-GPU (install() just returns False).
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from .mtrace import stamp

_installed = False
_STATS = {"hits": 0, "misses": 0, "saves": 0, "load_errors": 0, "save_errors": 0,
          "hook_failures": 0, "capture_missed": 0, "no_code_bytes": 0}
_stamped_once: set = set()


def _once(tag: str, **kv) -> None:
    """Stamp a diagnostic ONCE per process — these fire per-kernel (dozens), and one line is
    enough to name the failure mode in the rig log without drowning it."""
    if tag not in _stamped_once:
        _stamped_once.add(tag)
        stamp(tag, **kv)


def stats() -> dict:
    """Counters since process start — stamped after a planner build so the rig log shows whether
    the cache is doing its job (all-hits = warm disk; all-misses = first run / stale sources)."""
    return dict(_STATS, installed=_installed)


def cache_dir() -> Path:
    """`<curobo cache>/cubins` (respects cuRobo's own configurable cache root when importable)."""
    try:
        from curobo._src import runtime
        base = Path(runtime.cache_dir)
    except Exception:  # noqa: BLE001 — cuRobo not importable (off-GPU) → the conventional default
        base = Path.home() / ".cache" / "curobo"
    return base / "cubins"


def _sidecar(key: str) -> Path:
    return cache_dir() / f"{key}.json"


def _cubin(key: str) -> Path:
    return cache_dir() / f"{key}.cubin"


def _symbol_map(mod) -> dict:
    """The name-expression → mangled-symbol mapping off a freshly compiled ObjectCode, wherever
    the installed cuda.core keeps it. Empty dict when not found (a plain extern-C kernel name
    still resolves without it). The VALUES are mangled symbol names that cuda.core keeps as
    BYTES (nvrtc returns bytes) — `str(bytes)` would write "b'_Z…'" garbage into the sidecar.
    That exact bug shipped in round 4 and produced `get_kernel` failing "expected bytes, str
    found" — the SAME error text as round 3's from_cubin path bug, from a DIFFERENT line, which
    misled the diagnosis a full release cycle. Decode properly."""
    for attr in ("_sym_map", "symbol_mapping", "_symbol_mapping"):
        m = getattr(mod, attr, None)
        if isinstance(m, dict) and m:
            return {str(k): (v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else str(v))
                    for k, v in m.items()}
    return {}


def _sym_bytes(sym: dict) -> dict:
    """Sidecar symbol values → the BYTES cuda.core's `get_kernel` hands to cuModuleGetFunction
    (str raises TypeError: expected bytes). Also REPAIRS sidecars written by the buggy
    str(bytes) encoder ("b'_Z…'" wrappers) so cubins already on disk load without a recompile."""
    out = {}
    for k, v in (sym or {}).items():
        s = str(v)
        if s.startswith("b'") and s.endswith("'"):
            s = s[2:-1]
        out[str(k)] = s.encode("utf-8")
    return out


def _module_bytes(mod) -> Optional[bytes]:
    """The compiled module's cubin bytes, wherever the installed cuda.core keeps them. First rig
    run showed saves=0 with NO error — the silent case this hunts: when no known attribute has
    bytes, a ONE-TIME stamp lists the module's real attributes so the next log names the fix."""
    for attr in ("code", "_module", "_code", "module"):
        v = getattr(mod, attr, None)
        if isinstance(v, (bytes, bytearray)) and v:
            return bytes(v)
    return None


def _save(mod, key: str, kernel_name: str) -> bool:
    """Write the compiled module's cubin + its symbol-mapping sidecar. Atomic (tmp+rename) so a
    crash mid-write never leaves a half cubin the next process would trust."""
    try:
        code = _module_bytes(mod)
        if code is None:                     # this cuda.core doesn't expose module bytes — no cache
            _STATS["no_code_bytes"] += 1
            _once("cubin disk cache: compiled module exposes no cubin bytes — cache stays read-only",
                  mod_type=type(mod).__name__,
                  attrs=",".join(a for a in dir(mod) if not a.startswith("__"))[:300])
            return False
        d = cache_dir()
        d.mkdir(parents=True, exist_ok=True)
        tmp = d / f".{key}.cubin.tmp"
        tmp.write_bytes(bytes(code))
        tmp.replace(_cubin(key))
        _sidecar(key).write_text(json.dumps(
            {"kernel_name": kernel_name, "symbol_mapping": _symbol_map(mod)}), encoding="utf-8")
        return True
    except Exception as e:  # noqa: BLE001 — a failed save must never fail the plan
        _STATS["save_errors"] += 1
        stamp("cubin disk cache: save failed (compiling will keep working, just uncached)",
              kernel=kernel_name, error=f"{type(e).__name__}: {e}")
        return False


def _load(key: str, kernel_name: str):
    """Load a cached cubin → a launchable kernel, or None (miss OR any load problem → compile).
    Every step is PHASE-tagged in the failure stamp: the round-3→4 misdiagnosis happened because
    two different lines raise the same TypeError text ("expected bytes, str found" — from_cubin
    given a path, and get_kernel given str mangled names) and the stamp didn't say which."""
    cub = _cubin(key)
    if not cub.exists():
        return None
    phase = "import cuda.core"
    try:
        from cuda.core import ObjectCode

        phase = "read sidecar"
        sym: dict = {}
        try:
            meta = json.loads(_sidecar(key).read_text(encoding="utf-8"))
            sym = _sym_bytes(meta.get("symbol_mapping") or {})   # bytes values; repairs "b'…'"
        except Exception:  # noqa: BLE001 — no/broken sidecar: still try (extern-C names need none)
            sym = {}
        # from_cubin wants the cubin BYTES, not a path (round-3 log: "expected bytes, str
        # found"); the str-path form stays as a last resort for other cuda.core versions.
        phase = "from_cubin"
        data = cub.read_bytes()
        try:
            mod = ObjectCode.from_cubin(data, symbol_mapping=sym)
        except TypeError:
            try:
                mod = ObjectCode.from_cubin(data)
            except TypeError:
                mod = ObjectCode.from_cubin(str(cub))      # oldest form: a filesystem path
            if sym:
                try:
                    mod._sym_map = dict(sym)               # the pre-kwarg cuda.core keeps it here
                except Exception:  # noqa: BLE001
                    pass
        phase = "get_kernel"
        kernel = mod.get_kernel(kernel_name)
        # Same rationale as cuRobo's own post-compile sync: get_kernel loads the binary into the
        # context (cuModuleLoadData); launching before the load completes can fail.
        phase = "cuda synchronize"
        import torch
        torch.cuda.synchronize()
        return kernel
    except Exception as e:  # noqa: BLE001 — any load problem = a miss, never an error
        _STATS["load_errors"] += 1
        stamp("cubin disk cache: load failed — falling back to compile",
              kernel=kernel_name, phase=phase, error=f"{type(e).__name__}: {e}")
        return None


def install() -> bool:
    """Idempotently patch `CudaCoreKernelCache.get_or_compile_kernel` with the disk-backed
    version. Returns whether the patch is active. Never raises — off-GPU / pybind-backend /
    kill-switched boxes simply return False and cuRobo behaves exactly as stock."""
    global _installed
    if _installed:
        return True
    if os.environ.get("REMOROO_CUROBO_DISK_CACHE", "1").lower() in ("0", "false", "off"):
        return False
    try:
        from curobo._src.curobolib.backends.cuda_core_backend import kernel_cache as kc
    except Exception:  # noqa: BLE001 — no cuda_core backend here (off-GPU, or pybind-only build)
        return False

    original = kc.CudaCoreKernelCache.get_or_compile_kernel
    if getattr(original, "_remoroo_disk_cache", False):    # another stack instance already patched
        _installed = True
        return True

    def get_or_compile_kernel(self, source_files, kernel_name, include_dirs, compile_flags):
        try:
            self.initialize()                              # arch must be set before hashing
            key = self.get_kernel_hash(source_files, kernel_name, compile_flags)
            if key in self.compiled_kernels:
                return self.compiled_kernels[key]
            kernel = _load(key, kernel_name)
            if kernel is not None:
                _STATS["hits"] += 1
                self.compiled_kernels[key] = kernel
                return kernel
            _STATS["misses"] += 1
        except Exception:  # noqa: BLE001 — ANY cache-path problem → stock behavior
            return original(self, source_files, kernel_name, include_dirs, compile_flags)

        # Miss → compile via cuRobo's own path, transiently capturing the ObjectCode (the original
        # returns only the kernel; the cubin bytes live on the module). Round-2 rig log: patching
        # `Program.compile` on the CLASS fails — "cannot set 'compile' attribute of immutable type
        # 'cuda.core._program.Program'" (a closed C-extension type). So we swap the MODULE
        # attribute instead: cuRobo's `_compile_kernel` does `from cuda.core import Program` at
        # every call, which reads `sys.modules['cuda.core'].Program` — always assignable. The
        # wrapper is behaviorally identical (construct + compile pass through), so a concurrent
        # compile seeing it is harmless. Each way the capture can still fail is COUNTED +
        # one-time-stamped (round-1 showed saves=0 with zero errors; never silent again).
        captured: dict = {}
        hooked = False
        cc_mod = None
        real_program = None
        try:
            import cuda.core as cc_mod
            real_program = cc_mod.Program

            class _CapturingProgram:
                def __init__(self, *a, **kw):
                    self._p = real_program(*a, **kw)

                def compile(self, *a, **kw):
                    mod = self._p.compile(*a, **kw)
                    captured["mod"] = mod
                    return mod

                def __getattr__(self, name):
                    return getattr(self._p, name)

            cc_mod.Program = _CapturingProgram
            hooked = True
        except Exception as e:  # noqa: BLE001 — hook plumbing failed → stock compile, no save
            _STATS["hook_failures"] += 1
            _once("cubin disk cache: Program capture hook failed — compiles won't be persisted",
                  error=f"{type(e).__name__}: {e}")
        try:
            kernel = original(self, source_files, kernel_name, include_dirs, compile_flags)
        finally:
            if hooked:
                try:
                    cc_mod.Program = real_program
                except Exception:  # noqa: BLE001
                    pass

        mod = captured.get("mod")
        if mod is None:
            if hooked:
                _STATS["capture_missed"] += 1
                _once("cubin disk cache: hook active but no module captured "
                      "(cuRobo compiled via a different path?)", kernel=kernel_name)
        elif _save(mod, key, kernel_name):
            _STATS["saves"] += 1
        return kernel

    get_or_compile_kernel._remoroo_disk_cache = True
    kc.CudaCoreKernelCache.get_or_compile_kernel = get_or_compile_kernel
    _installed = True
    stamp("cubin disk cache: installed (cuRobo kernels persist across edge restarts)",
          dir=str(cache_dir()))
    return True
