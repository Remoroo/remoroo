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
    out["vla"] = vla_status(weights_dir)
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
    try:
        out["vla"] = vla_install(weights_dir)
    except Exception as e:                                # noqa: BLE001 - stated
        out["vla"] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
    return out


# --- VLA checkpoint: a multi-file HF SNAPSHOT, pinned by COMMIT --------------------------
# Unlike the single-file perception pins above, the VLA is 6 safetensors shards + config +
# bundled teachers (~12 GB); HF content-addresses a repo at a revision, so the pinned commit
# hash IS the integrity check. The pin itself lives in ONE place — LingBotRuntime.PIN — the
# same runtime that serves it. Installed only on rigs that DECLARE a policy server
# (.remoroo/vla.yaml, managed by `remoroo vla`): no silent 12 GB pulls on VLA-less cells.

def _vla_pin() -> Dict[str, str]:
    from ..vla.runtime import LingBotRuntime              # one pin, one place
    return dict(LingBotRuntime.PIN)


def _vla_dir(weights_dir: str) -> Path:
    return Path(weights_dir) / _vla_pin()["repo_id"].split("/")[-1]


def _vla_declared(weights_dir: str) -> bool:
    # weights live at <project>/.remoroo/task/weights → the rig declaration sits at
    # <project>/.remoroo/vla.yaml (vla_service.py owns that file's meaning).
    return (Path(weights_dir).resolve().parent.parent / "vla.yaml").exists()


def vla_status(weights_dir: str) -> Dict[str, Any]:
    pin = _vla_pin()
    d = _vla_dir(weights_dir)
    stamp = d / "REVISION"
    present = d.is_dir() and any(d.glob("*.safetensors"))
    verified = bool(present and stamp.exists()
                    and stamp.read_text().strip() == pin["revision"])
    return {"name": pin["repo_id"], "pinned": True, "kind": "hf_snapshot",
            "revision": pin["revision"], "present": present, "verified": verified,
            "declared": _vla_declared(weights_dir), "path": str(d)}


def vla_install(weights_dir: str) -> Dict[str, Any]:
    st = vla_status(weights_dir)
    if not st["declared"]:
        return {"ok": True, "skipped": "no .remoroo/vla.yaml on this rig — declare the "
                                       "policy server to pull the ~12 GB snapshot"}
    if st["verified"]:
        return {"ok": True, "path": st["path"]}           # idempotent
    try:
        from huggingface_hub import snapshot_download     # lazy: GPU cell only
    except ImportError:
        return {"ok": False, "error": "huggingface_hub not installed in the edge python "
                                      "(pip install huggingface_hub)"}
    pin = _vla_pin()
    d = _vla_dir(weights_dir)
    snapshot_download(repo_id=pin["repo_id"], revision=pin["revision"],
                      local_dir=str(d))                   # resumes partial pulls natively
    (d / "REVISION").write_text(pin["revision"], encoding="utf-8")
    return {"ok": True, "path": str(d)}


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


# --- The perception toolbox (COMP-11, v4.1): the agent composes from an ARSENAL, not
# two models. This catalog is INFORMATION (served to the agent via the seed and the
# toolbox verb); installation stays pin-based: file/hf_snapshot entries get pinned per
# rig like PINS/vla above, pip entries install into the edge or serve from the GPU
# worker (DEC-13). Composition doctrine (DEC-14): 3 to 6 tools per program,
# disagreement between tools is signal, fiducials anchor learned ones.
TOOLBOX = {
    "what_is_where": ["sam2.1/sam3", "groundingdino-1.5", "yolo-world", "owlv2",
                      "florence-2", "detectron2", "mmdetection", "semantic-sam",
                      "efficientsam/mobilesam", "rt-detr", "clip/siglip",
                      "dinov2/dinov3-features"],
    "where_in_3d": ["depth-anything-v2", "metric3d-v2", "unidepth", "dust3r",
                    "mast3r", "vggt", "colmap", "open3d", "pcl", "teaser++",
                    "kiss-icp", "gaussian-splatting(gsplat)", "sugar/2dgs",
                    "nerfstudio"],
    "pose_and_grasp": ["foundationpose", "megapose", "sam-6d", "anygrasp",
                       "graspnet-baseline", "contact-graspnet", "gpd"],
    "tracking_and_flow": ["sam2-video", "cutie", "cotracker3", "tapir",
                          "raft/sea-raft", "track-anything"],
    "vlm_spatial": ["qwen3-vl(on-rig, lingbot base)", "molmo(points)", "internvl",
                    "paligemma-2", "robopoint", "spatialrgpt"],
    "complete_the_unseen": ["trellis-class image->3d", "instantmesh", "wonder3d",
                            "zero123-xl", "stable-fast-3d"],
    "never_lies": ["opencv(charuco/aruco/homography)", "apriltag", "scikit-image",
                   "kornia", "ft/current sensors + microphone (contact events)"],
}


def toolbox_catalog() -> Dict[str, Any]:
    """The 55-tool arsenal, grouped by the question each answers, with the doctrine."""
    return {"groups": TOOLBOX,
            "count": sum(len(v) for v in TOOLBOX.values()),
            "doctrine": "compose 3-6 per program; disagreement is signal; fiducials "
                        "anchor learned tools; heavy models serve from the GPU "
                        "worker; pinned per rig once, never fetched mid-run"}
