"""The always-on deterministic supervisor (COMP-05, the old B2 blocker). Independent of every
learned component; can only TIGHTEN (hold/stop), never loosen, gate, or replace the arm's
safety-rated controller — it COMMANDS that controller's stop through Bridge.estop.

Testable core: tick(now, signals) is a pure decision; the thread loop just calls it. The
latency guarantee is the hardware's; the LOGIC is what we test (off-robot, injected faults).
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, List, Optional

from .budgets import DamageBudget
from .watchdogs import (SupervisorConfig, check_contact_anomaly, check_perception_conf,
                        check_stream_staleness)


class Supervisor:
    def __init__(self, bridge: Any, cfg: Optional[SupervisorConfig] = None) -> None:
        self.bridge = bridge
        self.cfg = cfg or SupervisorConfig()
        self.budget = DamageBudget(self.cfg.damage_units)
        self.state: str = "ok"                        # "ok" | "hold" | "stopped"
        self.events: List[Dict[str, Any]] = []
        self._last_sample_t: Optional[float] = None
        self._perception_conf: Optional[float] = None
        self._spikes = 0
        self._thread: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()

    # signal feeds (called by streams / trial code) ----------------------------------
    def feed_joint_sample(self, t: float) -> None:
        self._last_sample_t = t

    def feed_perception_conf(self, conf: float) -> None:
        self._perception_conf = float(conf)

    def feed_contact_spike(self) -> None:
        self._spikes += 1

    def debit_damage(self, cost: float, reason: str) -> None:
        self.budget.debit(cost, reason)
        self.events.append({"kind": "damage", "cost": cost, "reason": reason,
                            "remaining": self.budget.remaining})

    # the decision --------------------------------------------------------------------
    def tick(self, now: Optional[float] = None) -> str:
        if self.state == "stopped":
            return self.state
        now = time.monotonic() if now is None else now
        stop_reasons = []
        hold_reasons = []
        v = check_stream_staleness(self.cfg, now, self._last_sample_t)
        if v:
            stop_reasons.append(v)
        v = check_contact_anomaly(self.cfg, self._spikes)
        if v:
            stop_reasons.append(v)
        if self.budget.exhausted:
            stop_reasons.append("damage budget exhausted")
        v = check_perception_conf(self.cfg, self._perception_conf)
        if v:
            hold_reasons.append(v)

        if stop_reasons:
            self.state = "stopped"
            self.events.append({"kind": "stop", "reasons": stop_reasons, "t": now})
            self.bridge.estop()                        # command the rated controller's stop
        elif hold_reasons:
            self.state = "hold"
            self.events.append({"kind": "hold", "reasons": hold_reasons, "t": now})
        else:
            if self.state == "hold":
                self.events.append({"kind": "resume", "t": now})
            self.state = "ok"
        return self.state

    # the loop --------------------------------------------------------------------------
    def start(self) -> None:
        if self._thread is not None:
            return

        def loop() -> None:
            while not self._stop_flag.is_set():
                self.tick()
                if self.state == "stopped":
                    break
                self._stop_flag.wait(self.cfg.loop_period_s)

        self._thread = threading.Thread(target=loop, name="task-supervisor", daemon=True)
        self._thread.start()

    def shutdown(self) -> None:
        self._stop_flag.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def status(self) -> Dict[str, Any]:
        return {"state": self.state, "damage_remaining": self.budget.remaining,
                "events": list(self.events[-20:])}
