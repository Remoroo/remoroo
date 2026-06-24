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
    joint_positions: Dict[str, np.ndarray]   # KEYED BY THE COMBINED-URDF JOINT NAME — see below
    eef_pos: Dict[str, np.ndarray]
    eef_quat: Dict[str, np.ndarray]
    gripper_qpos: Dict[str, np.ndarray]
    intrinsics: Dict[str, Any] = field(default_factory=dict)
    extrinsics: Dict[str, Any] = field(default_factory=dict)
```

> **`joint_positions` keys MUST be the combined-URDF joint names** — the Studio's live mirror, the
> calibration engine, and the motion stack all key on them. When the cell has two identical chains
> (e.g. two `library://xarm6`, or four identical legs), the combined `robot.urdf` **uniquifies**
> duplicate joints with a `_1`/`_2`… suffix. Each controller's SDK reports plain `joint1..joint6`
> with no namespace, so you MUST remap. **The authored kinematic config — `cell.yaml: groups` — is
> the authoritative map** (one entry per actuated chain, ANY kind; written at the model gate, read
> via `calib_engine.urdf_io.robot_config`). Each group's `joint_names` is that chain's combined-URDF
> names IN SDK ORDER. Build the dict from it:
>
> ```python
> from calib_engine import urdf_io
> cfg = urdf_io.robot_config(yaml.safe_load(open("remoroo_cell/cell.yaml")),
>                            "remoroo_cell/robot_model/robot.urdf")
> joint_positions = {}
> for g in cfg["groups"]:                              # g["name"] == the group you key drivers by
>     angles = self._drivers[g["name"]].read_joint_positions()      # SDK order, radians
>     joint_positions.update(dict(zip(g["joint_names"], angles)))   # → combined-URDF names
> ```
>
> If you instead report raw `joint1..joint6` for both chains they COLLIDE (one overwrites the other)
> and the second never moves in the Studio — the live mirror shows "⚠ N streamed, 0 match URDF".
> (If ONE controller drives several groups, read it once and slice its angles across those groups'
> `joint_names`.) Likewise, key cameras (`get_observation(camera=...)`, `intrinsics`, `extrinsics`)
> by the camera's URDF **`link`** (the group's `cameras` entry), not the cell.yaml camera `name`.

```python
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
        # the actuated UNITS this bridge drives — any morphology (arms, legs, wheels, a head). Each
        # has a controller connection; you key drivers by its name. For plain arms these are
        # cell.yaml `arms[]`; the names MUST match the authored `cell.yaml: groups` (the model gate).
        self.units: Tuple[str, ...] = tuple(u["name"] for u in (cell.get("arms") or cell.get("units") or []))
        self.safety = SafetySupervisor.from_cell(cell)  # shipped; enforces envelopes
        self._drivers: Dict[str, Any] = {}          # filled in connect()
        self._cameras: Dict[str, Any] = {}              # filled in connect()
        self._motion = None                             # motion_engine.MotionStack (commission gate)
        self._connected = False

    @classmethod
    def from_cell_yaml(cls, path: str | Path) -> "Bridge":
        cell = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        return cls(cell)

    # ---- Lifecycle -------------------------------------------------------
    def connect(self) -> None:
        """Open controller + camera connections. NO motion. (See arm_adapters.md /
        camera_capture.md for the per-vendor driver you adapt here.)"""
        for u in (self.cell.get("arms") or self.cell.get("units") or []):
            self._drivers[u["name"]] = self._make_driver(u)              # TODO adapt (any kind)
        for cam in self.cell["cameras"]:
            self._cameras[cam["name"]] = self._make_camera(cam)          # TODO adapt
        self._connected = True

    def estop(self) -> None:
        """Hard stop every controller immediately. Must be reachable at all times."""
        self._estopped = True                  # latched; motion stack polls estop_tripped()
        for drv in self._drivers.values():
            drv.emergency_stop()  # TODO map to the real SDK's halt/abort call

    def _group_for_joints(self, joint_names) -> str:
        """Which driver owns these trajectory joints (controller routing for execute_trajectory).
        Match against the AUTHORED groups (cell.yaml: groups) — the source of truth for which joints
        belong to which chain. A coordinated plan spanning several groups fans out across drivers."""
        from calib_engine import urdf_io
        cfg = urdf_io.robot_config(self.cell, "remoroo_cell/robot_model/robot.urdf")
        for g in cfg["groups"]:
            if set(g.get("joint_names", [])) & set(joint_names):
                return g["name"]                 # == the driver key (group name == unit name)
        return self.units[0]

    def close(self) -> None:
        for drv in self._drivers.values():
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
        return self._drivers[arm].move_joints(q_target, speed=speed)  # TODO adapt

    # ---- Calibration joint-move contract (the SHIPPED calib engine calls this) ----
    def move_to_joints(self, joints, arm: Optional[str] = None) -> None:
        """PER-ARM joint move in URDF joint order — what the shipped calibration engine
        (`calib_engine`) calls for an arm-driven step. The `arm` id comes from the authored
        pipeline step and is LOAD-BEARING SAFETY on a multi-arm cell: drive EXACTLY that arm,
        never a shared default (passing `arm` is how the engine avoids moving the wrong arm).
        Route the supervised move:
            self.move_joints(arm or self.units[0], np.asarray(joints, float),
                             speed_frac=self.cell['safety']['max_joint_speed_frac'])
        (A single-arm cell may ignore `arm`. The engine also accepts the multi-arm
        `move_joints(arm, joints)` directly.) For calibration the engine also needs
        get_observation(camera=<urdf_link>) to return, FOR THAT CAMERA: joint_positions as a
        {urdf_joint_name: value} map over EVERY revolute joint, that camera's RGB image
        (rgb/color/left), and that camera's intrinsics {fx,fy,cx,cy,width,height} from the SDK
        — key `self._cameras` by URDF camera-link name so each camera is calibrated against its
        OWN frame. See calibration.md (authored pipeline + targets)."""
        ...  # TODO route to the named arm (NOT a shared side-channel)

    # ---- The motion stack (SHIPPED — you do NOT write cuRobo) -------------
    def motion(self):
        """The unified autonomous-motion stack — `motion_engine.MotionStack`, built ONCE (warm
        cuRobo planner, once per bring-up) from the setup artifacts (calibrated URDF + collision
        spheres + modeled obstacles + safety envelope). Its collision world at commission/demo is the
        LIVE ESDF built from the cameras (robot masked); this Bridge supplies per-camera depth, state,
        and `execute_trajectory` (the per-arm executor). Built/verified at COMMISSION; see
        commission.md."""
        if self._motion is None:
            from motion_engine import MotionStack
            self._motion = MotionStack.from_cell("remoroo_cell", bridge=self)
        return self._motion

    def execute_trajectory(self, traj, should_abort=None) -> bool:
        """The executor seam the motion stack calls. Route the planned `Trajectory` to the right
        arm driver(s) by `traj.joint_names`; a single-controller robot (humanoid) is one call, a
        multi-arm cell fans out to each driver. Each driver REPLAYS the full path (arm_adapters.md).
        The stack already audited the plan and supplies `should_abort` (the E-stop poll)."""
        arm = self._group_for_joints(traj.joint_names)        # owns the controller topology
        return self._drivers[arm].execute_trajectory(traj, should_abort=should_abort)

    def plan_and_move(self, arm: str, target_pose: np.ndarray, *, world=None) -> bool:
        """THE Phase-8 primitive, now a thin delegate to the shipped stack: plan a collision-free,
        safe-by-construction trajectory and replay it. `target_pose` = (xyz, quat-wxyz) or a 7-list.
        Returns False if no safe plan / the move aborted."""
        return self.motion().move_to_pose(arm, target_pose).ok

    def estop_tripped(self) -> bool:
        """Has the E-stop fired? The motion stack polls this between waypoints (defensive guard)."""
        return bool(getattr(self, "_estopped", False))

    def set_gripper(self, arm: str, opening: float) -> None:
        self._drivers[arm].set_gripper(float(np.clip(opening, 0.0, 1.0)))  # TODO adapt

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
    def _make_driver(self, unit_cfg: Dict[str, Any]):
        raise NotImplementedError("adapt to this controller's SDK (arm/leg/wheel/…); see arm_adapters.md")

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
    estopped: bool = False        # E-stop STATE — gate motion on this; estop()/reset_estop() flip it

    def estop(self) -> None: self.estopped = True
    def reset_estop(self) -> None: self.estopped = False

    @classmethod
    def from_cell(cls, cell: Dict[str, Any]) -> "SafetySupervisor":
        s = cell.get("safety", {}) or {}
        w = (cell.get("workspace") or {}).get("bounds_m", {}) or {}
        # None-safe: a cell.yaml key present-but-null returns None from .get(k, default).
        def _f(v, d): return float(d) if v is None else float(v)
        return cls(
            max_cartesian_speed_mps=_f(s.get("max_cartesian_speed_mps"), 0.10),
            max_joint_speed_frac=_f(s.get("max_joint_speed_frac"), 0.10),
            bounds_min=np.asarray(w.get("min") or [-0.5, -0.5, 0.0], float),
            bounds_max=np.asarray(w.get("max") or [0.5, 0.5, 0.8], float),
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
- Do **not** write cuRobo. Autonomous motion is the SHIPPED `motion_engine`
  stack (`self.motion()`); you supply `execute_trajectory` + state + E-stop.
  Build + verify it at the COMMISSION gate (commission.md).
- Keep `estop()` reachable and dead-simple — it is the last line of defense.
- The recorder lives in `remoroo_cell/capture/recorder.py` (data_capture.md);
  the Bridge just exposes `get_observation()` for it to sample.
- Smoke-test with NO motion before Phase 3.
