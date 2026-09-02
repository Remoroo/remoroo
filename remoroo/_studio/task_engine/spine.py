"""HOME store — the one piece of run state the engine keeps (robotics_program
2026-07-22): the joints every real motion returns to. THE RUN STARTS AT HOME —
the first motion verb stamps it from wherever the operator parked the robot;
goto_home drives back to it. The six-phase ledger this file used to hold is gone;
the class name and file format stay so existing state files keep loading.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict


class PhaseLedger:
    def __init__(self, path: str, task_slug: str = "") -> None:
        self.path = Path(path)
        self.task_slug = task_slug
        self.run_home: Dict[str, Any] = {}          # joints captured at run start
        self.instruments: Dict[str, Any] = {}       # kept for old state files

    @classmethod
    def load(cls, path: str, task_slug: str = "") -> "PhaseLedger":
        led = cls(path, task_slug)
        p = Path(path)
        if p.exists():
            d = json.loads(p.read_text(encoding="utf-8"))
            led.task_slug = d.get("task_slug", task_slug)
            led.run_home = d.get("run_home", {})
            led.instruments = d.get("instruments", {})
        return led

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({
            "task_slug": self.task_slug,
            "run_home": self.run_home,
            "instruments": self.instruments,
            "t": time.time()}, indent=1, default=str), encoding="utf-8")

    def set_run_home(self, joints: Dict[str, float]) -> None:
        self.run_home = {"joints": joints, "t": time.time()}
        self.save()
