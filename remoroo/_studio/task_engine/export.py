"""Dataset export (DATA-10) — LeRobotDataset-v2 LAYOUT from TrialRecords: one episode per
trial, episodes/*.jsonl frames + meta/info.json + meta/episodes.jsonl. Parquet conversion
happens on the GPU box where pyarrow lives; this writer keeps the wheel dependency-free while
matching the directory contract the LeRobot tooling and the pi0.5 finetune path consume.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from .record import TrialStore


def export_lerobot(store: TrialStore, out_dir: str, *, task_slug: str,
                   fps: int = 10) -> Dict[str, Any]:
    root = Path(out_dir)
    (root / "meta").mkdir(parents=True, exist_ok=True)
    (root / "data").mkdir(parents=True, exist_ok=True)
    episodes: List[Dict[str, Any]] = []
    n_frames = 0
    for idx, tid in enumerate(store.all_ids()):
        rec = store.load(tid)
        frames = []
        for step_i, s in enumerate(rec.trace):
            frames.append({
                "episode_index": idx, "frame_index": step_i,
                "timestamp": (s.get("t_start", 0.0) - rec.t_start),
                "action.name": s["name"], "action.args": s.get("args", {}),
                "action.ok": s.get("ok"), "action.stop_cause": s.get("stop_cause"),
                "observation.evidence": s.get("evidence", {}),
            })
        ep_path = root / "data" / f"episode_{idx:06d}.jsonl"
        ep_path.write_text("\n".join(json.dumps(f) for f in frames))
        episodes.append({
            "episode_index": idx, "trial_id": tid, "length": len(frames),
            "success": bool(rec.verdict.get("ok")), "score": rec.verdict.get("score"),
            "outcome": rec.outcome, "backend": rec.backend,
            "judge_version": rec.judge_version,
            "perception_version": rec.perception_version, "knobs": rec.knobs,
        })
        n_frames += len(frames)
    (root / "meta" / "episodes.jsonl").write_text(
        "\n".join(json.dumps(e) for e in episodes))
    info = {"codebase_version": "v2.0-layout", "task": task_slug, "fps": fps,
            "total_episodes": len(episodes), "total_frames": n_frames,
            "note": ("jsonl frames; convert to parquet with the LeRobot tooling on a box "
                     "with pyarrow. Labels are verifier verdicts (versioned, re-judgeable).")}
    (root / "meta" / "info.json").write_text(json.dumps(info, indent=1))
    return {"episodes": len(episodes), "frames": n_frames, "dir": str(root)}
