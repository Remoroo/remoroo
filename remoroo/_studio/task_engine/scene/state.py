"""SceneState (IFACE-03) — the generic entity-and-feature-channel state the agent reasons
over. One shape, no task-specific fields: a corner set is a feature channel on an entity,
never a dedicated field. Confidence is self-validated (geometry/multiview/temporal), never
outcome-derived. The header rendering is token-bounded: geometry and pixels are referenced,
not inlined (SCENE spec sections 2-4, 7).
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class FeatureChannel:
    kind: str                       # "keypoints" | "edges" | "scalar" | "vector" | "mask_stats"
    values: List[float]
    shape: List[int]
    confidence: float = 1.0

    def to_dict(self) -> dict:
        return {"kind": self.kind, "values": self.values, "shape": self.shape,
                "confidence": self.confidence}

    @classmethod
    def scalar(cls, v: float, confidence: float = 1.0) -> "FeatureChannel":
        return cls(kind="scalar", values=[float(v)], shape=[1], confidence=confidence)


@dataclass
class PerceptionConfidence:
    overall: float = 1.0
    geometric: float = 1.0
    multiview: float = 1.0
    temporal: float = 1.0

    def to_dict(self) -> dict:
        return {"overall": self.overall, "geometric": self.geometric,
                "multiview": self.multiview, "temporal": self.temporal}


@dataclass
class Entity:
    entity_id: str
    label: str
    pose: Optional[List[float]] = None      # [x,y,z,qw,qx,qy,qz]; None for pose-less bodies
    dims: Optional[List[float]] = None
    features: Dict[str, FeatureChannel] = field(default_factory=dict)
    confidence: float = 1.0

    def feature(self, channel: str) -> FeatureChannel:
        if channel not in self.features:
            raise KeyError(f"entity {self.entity_id!r} has no channel {channel!r}; "
                           f"available: {sorted(self.features)}")
        return self.features[channel]

    def to_dict(self) -> dict:
        return {"entity_id": self.entity_id, "label": self.label, "pose": self.pose,
                "dims": self.dims, "confidence": self.confidence,
                "features": {k: v.to_dict() for k, v in self.features.items()}}


@dataclass
class SceneState:
    entities_list: List[Entity] = field(default_factory=list)
    confidence: PerceptionConfidence = field(default_factory=PerceptionConfidence)
    perception_program_id: str = "v0"
    geometry_ref: str = ""
    t: float = field(default_factory=time.time)

    # queries ------------------------------------------------------------------
    def entities(self, label: Optional[str] = None) -> List[Entity]:
        if label is None:
            return list(self.entities_list)
        want = label.lower()
        return [e for e in self.entities_list if want in e.label.lower()]

    def entity(self, label: str) -> Entity:
        hits = self.entities(label)
        if not hits:
            raise KeyError(f"no entity matching {label!r}; "
                           f"have: {[e.label for e in self.entities_list]}")
        return hits[0]

    # serialization ------------------------------------------------------------
    def to_dict(self) -> dict:
        return {"entities": [e.to_dict() for e in self.entities_list],
                "confidence": self.confidence.to_dict(),
                "perception_program_id": self.perception_program_id,
                "geometry_ref": self.geometry_ref, "t": self.t}

    @classmethod
    def from_dict(cls, d: dict) -> "SceneState":
        ents = []
        for e in d.get("entities", []):
            feats = {k: FeatureChannel(**v) for k, v in (e.get("features") or {}).items()}
            ents.append(Entity(entity_id=e["entity_id"], label=e["label"], pose=e.get("pose"),
                               dims=e.get("dims"), features=feats,
                               confidence=e.get("confidence", 1.0)))
        return cls(entities_list=ents,
                   confidence=PerceptionConfidence(**d.get("confidence", {})),
                   perception_program_id=d.get("perception_program_id", "v0"),
                   geometry_ref=d.get("geometry_ref", ""), t=d.get("t", 0.0))

    # the token-bounded header (SCENE section 3) ---------------------------------
    def render_header(self, declared_channels: Optional[List[str]] = None) -> str:
        c = self.confidence
        lines = [
            f"SCENE perception={self.perception_program_id} geometry={self.geometry_ref or '-'}",
            (f"confidence: overall {c.overall:.2f} geom {c.geometric:.2f} "
             f"multiview {c.multiview:.2f} temporal {c.temporal:.2f}"),
            f"entities ({len(self.entities_list)}):",
        ]
        flags: List[str] = []
        for e in self.entities_list:
            pose = ("pose=({:.2f},{:.2f},{:.2f})".format(*e.pose[:3]) if e.pose
                    else "has_pose=no")
            chans = []
            for name, ch in e.features.items():
                if declared_channels and name not in declared_channels:
                    continue
                chans.append(f"{name}[{ch.kind}]=conf{ch.confidence:.2f}")
                if ch.confidence < 0.5:
                    flags.append(f"{e.entity_id}.{name} low-confidence ({ch.confidence:.2f})")
            lines.append(f"  {e.entity_id} label={e.label!r} {pose} conf{e.confidence:.2f} "
                         + " ".join(chans))
        if flags:
            lines.append("low-confidence flags: " + "; ".join(flags))
        lines.append("queryable: scene(), entity(label), feature(e,channel), view_image(...)")
        return "\n".join(lines)
