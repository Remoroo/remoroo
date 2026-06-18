# The Bridge — `remoroo_cell/primitives.py` (Phase 2 → G2)

`primitives.py` is the cell's **robot action surface**: the one module every
later phase and the overnight loop drive the robot through. `@robot_sprint`
reads it as "your only legal action surface for the robot." You author it for
THIS cell by adapting this contract to the real arm/camera SDKs (see
`arm_adapters.md`, `camera_capture.md`) and the values in `cell.yaml`.

It is **versioned and editable** (R&D) — do not add immutability language.
What you must NOT reauthor is the **safety supervisor / transport / episode
spine**: import it from the shipped Remoroo runtime (with the R&D fallback
shown below until the spine is published).

## G2 exit criteria (no autonomous motion in this phase)

- `primitives.py` imports cleanly.
- No-motion smoke passes: `connect()`, `get_observation()` returns real joint
  state, a camera frame is grabbed, the recorder writes one valid sample, and
  the E-stop path is reachable.
- The operator views a captured frame (`view_image`) and confirms it is the
  right camera/scene.

## Required surface

```python
"""The Bridge for this cell. Authored by remoroo @robot_setup; edit freely."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import yaml

# --- Shipped safety + transport spine -------------------------------------
# These must NOT be authored by the agent. Import from the installed Remoroo
# runtime. During R&D the spine may not be published yet; fall back to the
# local, EDITABLE shim in `safety_shim.py` (see safety_shim section below) so
# setup is never blocked. Replace the shim with the real import once shipped.
try:  # pragma: no cover - import shape depends on installed runtime
    from remoroo.edge import SafetySupervisor, EpisodeWriter  # type: ignore
except Exception:  # noqa: BLE001
    from .safety_shim import SafetySupervisor, EpisodeWriter


@dataclass
class Observation:
    """One observation tick from real hardware."""
    rgb: Optional[np.ndarray]
    depth: Optional[np.ndarray]
    joint_positions: Dict[str, np.ndarray]
    eef_pos: Dict[str, np.ndarray]
    eef_quat: Dict[str, np.ndarray]
    gripper_qpos: Dict[str, np.ndarray]
    intrinsics: Dict[str, Any] = field(default_factory=dict)
    extrinsics: Dict[str, Any] = field(default_factory=dict)
    scene: Dict[str, Any] = field(default_factory=dict)
    stamp_s: float = 0.0
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SkillResult:
    supported: bool
    success: bool = False
    info: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AttemptVerdict:
    success: bool
    confidence: float
    rationale: str


class Bridge:
    """Robot action surface for this cell. REAL hardware.

    Construct with `Bridge.from_cell_yaml("remoroo_cell/cell.yaml")`. Every
    motion goes through the SafetySupervisor (speed/bounds/keep-out/E-stop);
    autonomous moves are planned collision-free with cuRoboV2 against the
    Phase-4 world.
    """

    def __init__(self, cell: Dict[str, Any]):
        self.cell = cell
        self.arms: Tuple[str, ...] = tuple(a["name"] for a in cell["arms"])
        self.bimanual = len(self.arms) > 1
        self.safety = SafetySupervisor.from_cell(cell)  # shipped; enforces envelopes
        self._arm_drivers: Dict[str, Any] = {}          # filled in connect()
        self._cameras: Dict[str, Any] = {}              # filled in connect()
        self._planner = None                            # cuRoboV2 motion gen
        self._connected = False

    @classmethod
    def from_cell_yaml(cls, path: str | Path) -> "Bridge":
        cell = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        return cls(cell)

    # ---- Lifecycle -------------------------------------------------------
    def connect(self) -> None:
        """Open arm + camera connections. NO motion. (See arm_adapters.md /
        camera_capture.md for the per-vendor driver you adapt here.)"""
        for arm in self.cell["arms"]:
            self._arm_drivers[arm["name"]] = self._make_arm_driver(arm)  # TODO adapt
        for cam in self.cell["cameras"]:
            self._cameras[cam["name"]] = self._make_camera(cam)          # TODO adapt
        self._connected = True

    def estop(self) -> None:
        """Hard stop every arm immediately. Must be reachable at all times."""
        for drv in self._arm_drivers.values():
            drv.emergency_stop()  # TODO map to the real SDK's halt/abort call

    def close(self) -> None:
        for drv in self._arm_drivers.values():
            try:
                drv.disconnect()
            except Exception:
                pass

    # ---- Sensing ---------------------------------------------------------
    def get_observation(self) -> Observation:
        """Synchronized proprio + camera snapshot. See camera_capture.md."""
        ...  # TODO read joint states + grab frames; stamp with a monotonic clock

    # ---- Control (all motion is supervised) ------------------------------
    def move_joints(self, arm: str, q_target: np.ndarray, *, speed_frac: Optional[float] = None) -> bool:
        """Bounded joint move under the safety supervisor. NOT collision-aware
        by itself — use plan_and_move for autonomous moves against the world."""
        speed = self.safety.clamp_speed(speed_frac)
        if not self.safety.joints_within_limits(arm, q_target):
            return False
        return self._arm_drivers[arm].move_joints(q_target, speed=speed)  # TODO adapt

    # ---- Calibration joint-move contract (the SHIPPED calib engine calls this) ----
    def move_to_joints(self, joints) -> None:
        """SINGLE-ARM joint move in URDF joint order — what the shipped calibration
        engine (`calib_engine`) probes for + calls (also accepts move_joints/goto_joints/
        set_joint_positions/move_j). Distinct from the multi-arm `move_joints(arm, q)`
        above: the engine works one PlanItem (one arm) at a time and passes just the
        joint vector. Wrap the supervised move for the arm being calibrated:
            self.move_joints(self._calib_arm, np.asarray(joints, float),
                             speed_frac=self.cell['safety']['max_joint_speed_frac'])
        For DUAL-ARM, set self._calib_arm to the arm whose camera is being calibrated
        (the engine calibrates A, then B, then A↔B). For calibration the engine also
        needs get_observation() to return: joint_positions as a {urdf_joint_name: value}
        map covering EVERY revolute joint, an RGB image (rgb/color/left), and
        intrinsics {fx,fy,cx,cy,width,height} from the camera SDK. See calibration.md."""
        ...  # TODO route to the arm being calibrated

    def plan_and_move(self, arm: str, target_pose: np.ndarray, *, world=None) -> bool:
        """THE Phase-8 primitive: plan a collision-free path with cuRoboV2
        against `world` (the Phase-4 collision world), gate it through the
        safety supervisor, then execute. Returns False if no safe plan.
        """
        world = world or self.load_world()
        plan = self._planner.plan(arm, target_pose, world)  # cuRoboV2 trajopt
        if plan is None or not self.safety.trajectory_ok(arm, plan):
            return False
        return self._arm_drivers[arm].execute_trajectory(plan)  # TODO adapt

    def set_gripper(self, arm: str, opening: float) -> None:
        self._arm_drivers[arm].set_gripper(float(np.clip(opening, 0.0, 1.0)))  # TODO adapt

    # ---- World (built in Phase 4; see world_scan.md) ---------------------
    def load_world(self):
        """Load the cuRobo collision world from remoroo_cell/world/."""
        ...  # TODO load TSDF/ESDF collision world built in Phase 4

    # ---- Optional: bring-your-own VLA / world model ----------------------
    # @robot_sprint detects support via `.supported`. Leave as no-ops at setup;
    # a customer wires their VLA / world model here later.
    def execute_skill_vla(self, skill_name: str, **kwargs) -> SkillResult:
        return SkillResult(supported=False, info={"reason": "no VLA wired"})

    def predict_next_state(self, action_plan):
        return None

    def predict_affordance(self, text: str):
        return None

    # ---- Success labelling (used by the night, not by setup) -------------
    def eval_attempt(self, before: Observation, after: Observation, task: str) -> AttemptVerdict:
        # Setup never executes tasks. Authored here for the night loop; a
        # perception-based check or the brain's scope tool fills this in later.
        return AttemptVerdict(success=False, confidence=0.0, rationale="not set at setup")

    # ---- Per-vendor adapters (author these — see arm_adapters.md) --------
    def _make_arm_driver(self, arm_cfg: Dict[str, Any]):
        raise NotImplementedError("adapt to this arm's SDK; see arm_adapters.md")

    def _make_camera(self, cam_cfg: Dict[str, Any]):
        raise NotImplementedError("adapt to this camera's SDK; see camera_capture.md")
```

## The safety shim (`remoroo_cell/safety_shim.py`) — R&D fallback only

Ship-grade safety is imported from the Remoroo runtime. Until that spine is
published, author this **editable** shim so setup runs. It is deliberately
conservative (slow speeds, hard bounds) and the human supervises every move.

```python
"""R&D fallback safety spine. Replace with `from remoroo.edge import ...`
once the shipped supervisor is available. EDITABLE — tune envelopes freely."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional
import json, time
from pathlib import Path
import numpy as np


@dataclass
class SafetySupervisor:
    max_cartesian_speed_mps: float
    max_joint_speed_frac: float
    bounds_min: np.ndarray
    bounds_max: np.ndarray

    @classmethod
    def from_cell(cls, cell: Dict[str, Any]) -> "SafetySupervisor":
        s = cell.get("safety", {})
        w = (cell.get("workspace") or {}).get("bounds_m", {})
        return cls(
            max_cartesian_speed_mps=float(s.get("max_cartesian_speed_mps", 0.10)),
            max_joint_speed_frac=float(s.get("max_joint_speed_frac", 0.10)),
            bounds_min=np.asarray(w.get("min", [-0.5, -0.5, 0.0]), float),
            bounds_max=np.asarray(w.get("max", [0.5, 0.5, 0.8]), float),
        )

    def clamp_speed(self, frac: Optional[float]) -> float:
        f = self.max_joint_speed_frac if frac is None else min(frac, self.max_joint_speed_frac)
        return max(0.0, f)

    def joints_within_limits(self, arm: str, q: np.ndarray) -> bool:
        return bool(np.all(np.isfinite(q)))  # TODO add real joint-limit check

    def point_in_bounds(self, xyz: np.ndarray) -> bool:
        return bool(np.all(xyz >= self.bounds_min) and np.all(xyz <= self.bounds_max))

    def trajectory_ok(self, arm: str, plan: Any) -> bool:
        pts = getattr(plan, "cartesian_waypoints", None)
        if pts is None:
            return True  # joint-space plan; rely on planner's collision check
        return all(self.point_in_bounds(np.asarray(p, float)) for p in pts)


class EpisodeWriter:
    """Minimal episode writer (schema = remoroo_episode_v1). See data_capture.md."""
    def __init__(self, out_dir: str | Path):
        self.dir = Path(out_dir); self.dir.mkdir(parents=True, exist_ok=True)
        self._frames = []

    def add(self, **frame): self._frames.append({"t": time.time(), **frame})

    def close(self) -> Path:
        meta = {"schema": "remoroo_episode_v1", "n_frames": len(self._frames)}
        (self.dir / "meta.json").write_text(json.dumps(meta, indent=2))
        return self.dir
```

## Adaptation checklist

- Replace every `TODO adapt` with the real SDK call (arm_adapters.md /
  camera_capture.md).
- Wire `_planner` to cuRoboV2 (validated at G1).
- Keep `estop()` reachable and dead-simple — it is the last line of defense.
- The recorder lives in `remoroo_cell/capture/recorder.py` (data_capture.md);
  the Bridge just exposes `get_observation()` for it to sample.
- Smoke-test with NO motion before Phase 3.
