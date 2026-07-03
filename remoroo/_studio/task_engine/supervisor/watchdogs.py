"""Watchdogs (COMP-05) — deterministic checks over live signals. Each returns None (fine) or
a violation string. Pure functions of (config, signals, now): trivially testable, no threads.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class SupervisorConfig:
    """Frozen: the supervisor's limits cannot be loosened at runtime (CON-03). A new, wider
    config requires a new process start, which is an operator window action."""
    max_stream_gap_s: float = 0.5
    min_perception_conf: float = 0.2
    max_contact_spikes: int = 3
    damage_units: float = 10.0
    loop_period_s: float = 0.05


def check_stream_staleness(cfg: SupervisorConfig, now: float,
                           last_sample_t: Optional[float]) -> Optional[str]:
    if last_sample_t is None:
        return None                                     # not yet streaming: startup grace
    if (now - last_sample_t) > cfg.max_stream_gap_s:
        return f"joint stream stale {(now - last_sample_t):.2f}s"
    return None


def check_perception_conf(cfg: SupervisorConfig,
                          conf: Optional[float]) -> Optional[str]:
    if conf is not None and conf < cfg.min_perception_conf:
        return f"perception confidence {conf:.2f} below {cfg.min_perception_conf}"
    return None


def check_contact_anomaly(cfg: SupervisorConfig, spike_count: int) -> Optional[str]:
    if spike_count > cfg.max_contact_spikes:
        return f"{spike_count} contact spikes > {cfg.max_contact_spikes}"
    return None
