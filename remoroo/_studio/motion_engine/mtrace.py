"""Motion-path tracing — every stage of the motion pipeline prints WHERE the time goes.

The operator's mandate after the minutes-with-no-motion failure: instrument everything, change
nothing. `span("tag")` prints entry/exit with wall time and nesting; `stamp("tag", k=v)` prints
one-liners. All output goes to stdout (the edge log). Zero dependencies, threads-safe depth.
Grep: `[motion]`.
"""
from __future__ import annotations

import threading
import time

_tls = threading.local()


def _depth() -> int:
    return getattr(_tls, "d", 0)


def _fmt_kv(kv: dict) -> str:
    return (" · " + " ".join(f"{k}={v}" for k, v in kv.items())) if kv else ""


def stamp(tag: str, **kv) -> None:
    print(f"[motion] {'  ' * _depth()}• {tag}{_fmt_kv(kv)}", flush=True)


class span:
    """with span("build planner", groups=2): ...  → prints ▶ on entry, ◀ with seconds on exit
    (exceptions print ✗ with the error and re-raise)."""

    def __init__(self, tag: str, **kv):
        self.tag = tag
        self.kv = kv

    def __enter__(self):
        self.t0 = time.time()
        print(f"[motion] {'  ' * _depth()}▶ {self.tag}{_fmt_kv(self.kv)}", flush=True)
        _tls.d = _depth() + 1
        return self

    def __exit__(self, et, ev, tb):
        _tls.d = max(0, _depth() - 1)
        dt = time.time() - self.t0
        mark = "✗" if et else "◀"
        err = f" · {et.__name__}: {ev}" if et else ""
        print(f"[motion] {'  ' * _depth()}{mark} {self.tag} ({dt:.2f}s){err}", flush=True)
        return False


class heartbeat:
    """with heartbeat("executor", every=5.0, info="87 wp"): ...  → prints every N seconds while
    the block runs — the 'is it hung or just slow' answer for anything that can block."""

    def __init__(self, tag: str, *, every: float = 5.0, info: str = ""):
        self.tag = tag
        self.every = every
        self.info = info
        self._stop = threading.Event()

    def __enter__(self):
        t0 = time.time()

        def _tick():
            while not self._stop.wait(self.every):
                print(f"[motion] … {self.tag} still running ({time.time() - t0:.0f}s){' · ' + self.info if self.info else ''}",
                      flush=True)

        threading.Thread(target=_tick, daemon=True, name=f"hb-{self.tag}").start()
        return self

    def __exit__(self, et, ev, tb):
        self._stop.set()
        return False
