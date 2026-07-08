"""Model loaders (COMP-06 tail, OQ-B3) — fetch-at-setup with pinned hashes, lazy torch
import, registered into the ModelRegistry behind the operator contract. NOTHING here runs in
CI (no downloads, no torch); the functions are factories the edge calls once on a GPU cell.
Checkpoints are third-party Apache-2.0 (SAM2.1, GroundingDINO); never redistributed in the
wheel (plan 2.2.1).
"""
from __future__ import annotations

import hashlib
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .serving import ModelRegistry

# Pin BOTH url and sha256 when adopting a checkpoint; empty sha = refuse to load (stated).
PINS: Dict[str, Dict[str, str]] = {
    "segmenter": {"name": "sam2.1-hiera-small", "url": "", "sha256": "", "version": "2.1"},
    "detector": {"name": "groundingdino-swint", "url": "", "sha256": "", "version": "0.4"},
}


def fetch_pinned(url: str, sha256: str, dest_dir: str) -> Path:
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / Path(url).name
    if not path.exists():
        urllib.request.urlretrieve(url, path)             # noqa: S310 - pinned + hashed
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != sha256:
        path.unlink(missing_ok=True)
        raise RuntimeError(f"pinned-hash mismatch for {url}: got {digest[:12]}…")
    return path


def make_segmenter_loader(weights_dir: str) -> Callable[[], Any]:
    def load() -> Any:
        pin = PINS["segmenter"]
        if not pin["url"] or not pin["sha256"]:
            raise RuntimeError("segmenter checkpoint not pinned yet (PINS); "
                               "set url+sha256 at deploy time")
        ckpt = fetch_pinned(pin["url"], pin["sha256"], weights_dir)
        from sam2.build_sam import build_sam2            # lazy: GPU cell only
        from sam2.sam2_image_predictor import SAM2ImagePredictor
        predictor = SAM2ImagePredictor(build_sam2(pin["name"], str(ckpt)))

        def segment(frame: Dict[str, Any], prompt: Any):
            predictor.set_image(frame["rgb"])
            masks, _scores, _ = predictor.predict(prompt if not isinstance(prompt, str)
                                                  else None)
            return list(masks)
        return segment
    return load


def make_detector_loader(weights_dir: str) -> Callable[[], Any]:
    def load() -> Any:
        pin = PINS["detector"]
        if not pin["url"] or not pin["sha256"]:
            raise RuntimeError("detector checkpoint not pinned yet (PINS); "
                               "set url+sha256 at deploy time")
        ckpt = fetch_pinned(pin["url"], pin["sha256"], weights_dir)
        from groundingdino.util.inference import Model   # lazy: GPU cell only
        model = Model(model_checkpoint_path=str(ckpt))

        def detect(frame: Dict[str, Any], text: str):
            dets, labels = model.predict_with_caption(frame["rgb"], caption=text)
            out = []
            for box, score, label in zip(dets.xyxy, dets.confidence, labels):
                out.append({"label": label, "score": float(score),
                            "box": [float(v) for v in box]})
            return out
        return detect
    return load


def register_default_models(registry: ModelRegistry, weights_dir: str) -> None:
    registry.register("segmenter", PINS["segmenter"]["version"],
                      make_segmenter_loader(weights_dir))
    registry.register("detector", PINS["detector"]["version"],
                      make_detector_loader(weights_dir))
