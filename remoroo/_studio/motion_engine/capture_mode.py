"""Make cuRobo's CUDA-graph capture coexist with CUDA cameras: capture_error_mode="thread_local".

cuRobo has exactly ONE capture site (curobo/_src/util/cuda_graph_util.py — `with
torch.cuda.graph(graph, pool=…, stream=…)`), and it omits `capture_error_mode`, so PyTorch
captures in "global" mode: while ANY capture is in flight, potentially-unsafe CUDA calls in
EVERY thread of the process error out. On this rig that meant a ZED `grab()` (CUDA-native, its
own thread) died with cudaErrorStreamCaptureUnsupported(900) the moment warmup captured — which
forced all capture work under the edge's bridge lock, which starved the operator for ~4 minutes
at boot (the round-5 complaint).

PyTorch's documented mode for multi-threaded processes is **"thread_local"**: capture
restrictions apply only to the CAPTURING thread; other threads' CUDA work (camera grabs, torch
ops) proceeds legally. The documented residual risk points the SAFE way: a cross-thread call
(e.g. a pinned-memory cudaEventQuery) can still INVALIDATE the capture itself — that warmup step
fails and is retried (CuroboV2Planner._recover_if_capture_poisoned resets the solver graphs);
the camera is never harmed and the process is never poisoned.

We patch `torch.cuda.graph` with a subclass defaulting to thread_local — cuRobo reads the
attribute at call time, so the vendored repo stays untouched. Version-guarded: a torch without
the parameter is left alone (and stamped, so the rig log says global capture is in force).
Kill-switch: REMOROO_CAPTURE_THREAD_LOCAL=0.
"""
from __future__ import annotations

import os

from .mtrace import stamp

_installed = False


def install() -> bool:
    """Idempotently default torch.cuda.graph captures to thread_local. Never raises; returns
    whether the patch is active (False = global capture stays — keep cameras off during warmup)."""
    global _installed
    if _installed:
        return True
    if os.environ.get("REMOROO_CAPTURE_THREAD_LOCAL", "1").lower() in ("0", "false", "off"):
        return False
    try:
        import inspect
        import torch

        base = torch.cuda.graph
        if getattr(base, "_remoroo_thread_local", False):    # another stack already patched
            _installed = True
            return True
        if "capture_error_mode" not in inspect.signature(base.__init__).parameters:
            stamp("capture mode: this torch has no capture_error_mode — GLOBAL capture stays "
                  "(cameras must not grab during cuRobo warmup)")
            return False

        class _ThreadLocalCaptureGraph(base):
            def __init__(self, cuda_graph, pool=None, stream=None,
                         capture_error_mode: str = "thread_local", **kw):
                super().__init__(cuda_graph, pool=pool, stream=stream,
                                 capture_error_mode=capture_error_mode, **kw)

        _ThreadLocalCaptureGraph._remoroo_thread_local = True
        torch.cuda.graph = _ThreadLocalCaptureGraph
        try:
            torch.cuda.graphs.graph = _ThreadLocalCaptureGraph    # keep both spellings coherent
        except Exception:  # noqa: BLE001
            pass
        _installed = True
        stamp("capture mode: torch.cuda.graph → capture_error_mode=thread_local "
              "(CUDA cameras may grab while cuRobo captures)")
        return True
    except Exception as e:  # noqa: BLE001 — a failed patch must never fail a build
        stamp("capture mode: patch failed — GLOBAL capture stays",
              error=f"{type(e).__name__}: {e}")
        return False
