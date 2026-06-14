# Data-capture recorder — `remoroo_cell/capture/` (Phase 2 author → Phase 6 validate)

The dataset is the deliverable. The **recorder** is cell-specific: it records
THIS cell's modalities (from `cell.yaml: capture`) into the shipped episode
schema. It is **authored, not shipped** — do not assume ZED or xArm; read
whatever the Bridge exposes via `get_observation()`. The episode *schema* and
the `EpisodeWriter` base come from the shipped spine; the *mechanism* is yours.

Keep the recorder **separate from `primitives.py`** so it can evolve without
touching the action surface. Both are versioned + editable (R&D).

## What an episode must contain

- Synchronized streams at `cell.yaml: capture.rate_hz`: RGB, metric depth,
  joint positions, eef pose, gripper state, and the camera intrinsics +
  per-frame extrinsics (critical for eye-in-hand).
- Honest timestamps (monotonic) and the measured camera↔arm latency.
- A manifest: cell_id, calibration version, world version, schema version.
- Failures captured as carefully as successes (the night needs both).

## `remoroo_cell/capture/recorder.py` (adapt to this cell)

```python
"""Data-capture recorder for this cell. Authored by remoroo @robot_setup.
Records whatever the Bridge exposes; edit freely as modalities change."""
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict
import json, time
import numpy as np

try:
    from remoroo.edge import EpisodeWriter           # shipped schema writer
except Exception:                                     # R&D fallback (see bridge_primitives.md)
    from ..safety_shim import EpisodeWriter


class Recorder:
    def __init__(self, bridge, cell: Dict[str, Any], out_dir: str | Path):
        self.bridge = bridge
        self.cap = cell.get("capture", {})
        self.rate_hz = float(self.cap.get("rate_hz", 30))
        self.modalities = list(self.cap.get("modalities", []))
        self.writer = EpisodeWriter(out_dir)
        self._period = 1.0 / self.rate_hz

    def record_window(self, seconds: float, *, label: str = "safe_motion") -> Path:
        """Sample the Bridge at rate_hz for `seconds`, writing one episode.
        Called by the safe-move demo (Phase 8) and validation (Phase 6)."""
        t_end = time.monotonic() + seconds
        next_t = time.monotonic()
        while time.monotonic() < t_end:
            obs = self.bridge.get_observation()
            self.writer.add(**self._frame_from_obs(obs))
            next_t += self._period
            sleep = next_t - time.monotonic()
            if sleep > 0:
                time.sleep(sleep)
        return self.writer.close()

    def _frame_from_obs(self, obs) -> Dict[str, Any]:
        """Project the Bridge observation onto the configured modalities.
        Only include what cell.yaml asked for — no hardcoded camera/arm names."""
        frame: Dict[str, Any] = {"stamp_s": obs.stamp_s}
        if "rgb" in self.modalities:          frame["rgb"] = obs.rgb
        if "depth" in self.modalities:        frame["depth_m"] = obs.depth
        if "joint_states" in self.modalities: frame["joint_positions"] = obs.joint_positions
        if "tcp_pose" in self.modalities:     frame["eef_pos"] = obs.eef_pos; frame["eef_quat"] = obs.eef_quat
        if "gripper" in self.modalities:      frame["gripper_qpos"] = obs.gripper_qpos
        frame["intrinsics"] = obs.intrinsics
        frame["extrinsics"] = obs.extrinsics   # per-frame; required for eye-in-hand
        return frame
```

## `remoroo_cell/capture/schema.json` (describe what you actually record)

```json
{
  "schema": "remoroo_episode_v1",
  "cell_id": "site-a-cell-01",
  "rate_hz": 30,
  "streams": {
    "rgb": {"dtype": "uint8", "shape": ["H", "W", 3]},
    "depth_m": {"dtype": "float32", "shape": ["H", "W"], "units": "m"},
    "joint_positions": {"dtype": "float32", "per_arm": true, "units": "rad"},
    "eef_pos": {"dtype": "float32", "shape": [3], "frame": "base", "units": "m"},
    "eef_quat": {"dtype": "float32", "shape": [4]},
    "gripper_qpos": {"dtype": "float32"}
  },
  "embedded": ["intrinsics", "extrinsics", "calibration_version", "world_version"],
  "timing": {"clock": "monotonic", "camera_arm_latency_s": null}
}
```

## Phase 6 validation (G6) — prove one episode is correct

Record a short window during a scripted safe motion, then check:

- [ ] All configured streams present and time-aligned (no missing modality).
- [ ] Frame count ≈ `rate_hz × seconds` (no large drops); flag dropped frames.
- [ ] Timestamps monotonic; camera↔arm latency embedded.
- [ ] Calibrated intrinsics + per-frame extrinsics present (eye-in-hand sane).
- [ ] Depth is metric (not all zero/NaN).
- [ ] Schema validates against `schema.json`.
- [ ] Throughput + disk: estimate GB/hour and confirm a night fits on disk
      (`cell.yaml: capture.est_gb_per_hour`).

Write the validated episode to `remoroo_cell/capture/sample_episode/` and note
results in `setup_report.md`. At setup the content is *safe-motion* data (no
task) — that is expected; you are proving the recorder + schema, not
collecting task data.
