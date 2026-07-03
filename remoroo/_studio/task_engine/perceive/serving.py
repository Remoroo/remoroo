"""ModelRegistry (COMP-06) — the serving harness: load/route/version any model behind the
operator contract. Models are an OPEN catalog: the registry does not know or care what is
inside a handle, only that it is callable. Weights are fetched at setup from upstream with
pinned hashes; nothing model-shaped ships in the wheel (plan 2.2.1).
"""
from __future__ import annotations

import threading
from typing import Any, Callable, Dict


class ModelRegistry:
    def __init__(self) -> None:
        self._loaders: Dict[str, Callable[[], Any]] = {}
        self._versions: Dict[str, str] = {}
        self._handles: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self.load_counts: Dict[str, int] = {}

    def register(self, name: str, version: str, loader: Callable[[], Any]) -> None:
        """Idempotent by (name, version); re-registering a new version drops the old handle
        (the substrate-upgrade path: authored programs re-validate vs the bank)."""
        with self._lock:
            if self._versions.get(name) != version:
                self._handles.pop(name, None)
            self._loaders[name] = loader
            self._versions[name] = version

    def get(self, name: str) -> Any:
        with self._lock:
            if name not in self._loaders:
                raise KeyError(f"no model {name!r} registered; have {sorted(self._loaders)}")
            if name not in self._handles:
                self._handles[name] = self._loaders[name]()
                self.load_counts[name] = self.load_counts.get(name, 0) + 1
            return self._handles[name]

    def versions(self) -> Dict[str, str]:
        return dict(self._versions)
