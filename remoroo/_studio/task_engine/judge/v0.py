"""Judge v0 (COMP-13) — the generic, zero-task-knowledge score. An ensemble of channels that
fail DIFFERENTLY (DEC-09):

  proprioceptive  milestones() over the atom trace (gripper evidence, stop causes; no vision)
  geometric       depth/scene change in a region (labels and lighting cannot fool it)
  semantic        an injected callable (brain-side VLM before/after verdict; sampled)

The kit protocol that proves a verifier honest is brain-side (DEC-21); this module ships the
channels, the fusion, and the Verdict shape (IFACE-06). Scores are recomputed from recorded
evidence, so history is re-judgeable (DEC-05).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import numpy as np

from ..trace import Trace


@dataclass
class Verdict:
    score: float
    ok: bool
    confidence: float
    channels: Dict[str, float] = field(default_factory=dict)
    judge_version: str = "v0"
    flags: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"score": self.score, "ok": self.ok, "confidence": self.confidence,
                "channels": self.channels, "judge_version": self.judge_version,
                "flags": self.flags}


def milestones(trace: Trace) -> float:
    """Graded partial credit from proprioception alone: a trial that grasped and dropped
    scores above one that never grasped — the optimizer has slope everywhere (PROB-06)."""
    score = 0.0
    grasped = any(s.ok for s in trace.by_name("grasp"))
    if grasped:
        score += 0.3
        if any(s.ok for s in trace.by_name("carry")):
            score += 0.2
            if any(s.ok for s in trace.by_name("release")):
                score += 0.1
    return min(score, 0.6)


def depth_delta_score(pre_depth: Any, post_depth: Any, region_mask: Any,
                      *, min_delta_m: float = 0.005) -> float:
    """Fraction of the target region whose depth changed by more than min_delta: something
    object-sized appeared (or left). Robust to labels, prompts, and lighting."""
    pre = np.asarray(pre_depth, dtype=float)
    post = np.asarray(post_depth, dtype=float)
    region = np.asarray(region_mask, dtype=bool)
    if region.sum() == 0:
        return 0.0
    delta = np.abs(post - pre) > min_delta_m
    return float((delta & region).sum() / region.sum())


class EnsembleJudge:
    """Fuse whatever channels exist; agreement raises confidence, disagreement lowers it and
    flags. geometric_fn/semantic_fn take (pre_scene, post_scene) and return a 0..1 score or
    None when they cannot judge (missing evidence is stated, not guessed)."""

    def __init__(self, *,
                 geometric_fn: Optional[Callable[[Any, Any], Optional[float]]] = None,
                 semantic_fn: Optional[Callable[[Any, Any], Optional[float]]] = None,
                 ok_threshold: float = 0.85,
                 min_scene_confidence: float = 0.5,
                 judge_version: str = "v0") -> None:
        self.geometric_fn = geometric_fn
        self.semantic_fn = semantic_fn
        self.ok_threshold = float(ok_threshold)
        self.min_scene_confidence = float(min_scene_confidence)
        self.judge_version = judge_version

    def judge(self, pre_scene: Any, post_scene: Any, trace: Trace) -> Verdict:
        channels: Dict[str, float] = {"proprio": milestones(trace)}
        # A low-confidence post OBSERVATION cannot claim an OUTCOME: an occluded/empty
        # camera read scores exactly like "all objects picked" to a naive channel (a
        # live run scored 0.75 with zero grasps this way). Below the floor, the outcome
        # channels ABSTAIN — stated, and the score falls back to proprioception.
        post_conf = getattr(getattr(post_scene, "confidence", None), "overall", None)
        low_post = post_conf is not None and post_conf < self.min_scene_confidence
        if not low_post:
            for name, fn in (("geometric", self.geometric_fn),
                             ("semantic", self.semantic_fn)):
                if fn is not None:
                    v = fn(pre_scene, post_scene)
                    if v is not None:
                        channels[name] = float(min(max(v, 0.0), 1.0))
        outcome_channels = [v for k, v in channels.items() if k != "proprio"]
        flags: List[str] = []
        if outcome_channels:
            outcome = float(np.mean(outcome_channels))
            spread = float(max(outcome_channels) - min(outcome_channels))
            if spread > 0.4:
                flags.append("channel_disagreement")
            confidence = max(0.0, 1.0 - spread)
            score = max(outcome, channels["proprio"])   # partial credit floors the score
            ok = outcome >= self.ok_threshold
        else:
            # proprioception alone can never claim task success, only progress
            score = channels["proprio"]
            confidence = 0.3
            ok = False
            flags.append("low_confidence_post_observation" if low_post
                         else "no_outcome_channel")
        return Verdict(score=score, ok=ok, confidence=confidence, channels=channels,
                       judge_version=self.judge_version, flags=flags)
