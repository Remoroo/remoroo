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
# Hashes verified against the HF LFS pointers 2026-07-08. The weights install ONCE per
# cell (`remoroo models install` -> /edge/task/models_install); the agent never spends a
# token discovering or fetching them.
PINS: Dict[str, Dict[str, str]] = {
    "segmenter": {
        "name": "configs/sam2.1/sam2.1_hiera_s.yaml",     # sam2 build config id
        "url": "https://huggingface.co/facebook/sam2.1-hiera-small/resolve/main/sam2.1_hiera_small.pt",
        "sha256": "6d1aa6f30de5c92224f8172114de081d104bbd23dd9dc5c58996f0cad5dc4d38",
        "version": "2.1",
    },
    "detector": {
        "name": "groundingdino-swint-ogc",
        "url": "https://huggingface.co/ShilongLiu/GroundingDINO/resolve/main/groundingdino_swint_ogc.pth",
        "sha256": "3b3ca2563c77c69f651d7bd133e97139c186df06231157a64c507099c52bc799",
        "version": "0.4",
    },
}


def models_status(weights_dir: str) -> Dict[str, Any]:
    """Which pinned checkpoints are present+verified on this cell (no downloads)."""
    out: Dict[str, Any] = {}
    for role, pin in PINS.items():
        path = Path(weights_dir) / Path(pin["url"]).name if pin["url"] else None
        present = bool(path and path.exists())
        verified = False
        if present:
            verified = hashlib.sha256(path.read_bytes()).hexdigest() == pin["sha256"]
        out[role] = {"name": pin["name"], "pinned": bool(pin["url"] and pin["sha256"]),
                     "present": present, "verified": verified,
                     "path": str(path) if path else ""}
    return out


def models_install(weights_dir: str) -> Dict[str, Any]:
    """Fetch + verify every pinned checkpoint into weights_dir. Idempotent; a hash
    mismatch deletes the file and states the error. Run ONCE per cell (setup or
    `remoroo models install`), never inside a task run."""
    out: Dict[str, Any] = {}
    for role, pin in PINS.items():
        if not pin["url"] or not pin["sha256"]:
            out[role] = {"ok": False, "error": "not pinned"}
            continue
        try:
            path = fetch_pinned(pin["url"], pin["sha256"], weights_dir)
            out[role] = {"ok": True, "path": str(path)}
        except Exception as e:                            # noqa: BLE001 - stated
            out[role] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
    return out


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
        import groundingdino                              # lazy: GPU cell only
        from groundingdino.util.inference import Model
        cfg = Path(groundingdino.__file__).parent / "config" / "GroundingDINO_SwinT_OGC.py"
        model = Model(model_config_path=str(cfg), model_checkpoint_path=str(ckpt))

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
