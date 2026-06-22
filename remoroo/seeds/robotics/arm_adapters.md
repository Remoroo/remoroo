# Arm adapters — binding the Bridge to a real arm SDK (Phase 2)

The Bridge (`bridge_primitives.md`) calls a small **driver** per arm. Author
one driver per arm in `cell.yaml`, adapting the closest pattern below to the
real SDK. Keep the surface tiny and uniform; put vendor quirks inside.

## The driver interface the Bridge expects

```python
class ArmDriver:
    def read_joint_positions(self) -> "np.ndarray": ...   # radians, shape (dof,)
    def read_eef_pose(self) -> tuple:                      # (xyz[3], quat[4]) base frame
        ...
    def move_joints(self, q, *, speed: float) -> bool: ...    # blocking, bounded (single config)
    def execute_trajectory(self, traj, should_abort=None) -> bool: ...  # REPLAY a planned PATH
    def set_gripper(self, opening: float) -> None: ...       # 0=open .. 1=closed
    def emergency_stop(self) -> None: ...                     # immediate halt
    def disconnect(self) -> None: ...
```

**`execute_trajectory` is THE per-arm executor — the one thing the shipped
`motion_engine` cannot prebuild, because every SDK follows a path differently.**
It receives a `motion_engine.Trajectory` (NOT a single joint target):

```python
traj.joint_names   # list[str]  — the joints traj.positions columns map to (this arm's joints)
traj.positions     # np.ndarray (T, dof) radians — the WHOLE collision-free path to replay
traj.velocities    # np.ndarray (T, dof) rad/s   — feed-forward (zeros if unavailable)
traj.dt            # float seconds between waypoints (the replay cadence)
len(traj)          # T waypoints;  traj.waypoint(i) -> (dof,)
```

cuRobo already made this path collision-free AND within the safety/dynamics
envelope, so your ONLY job is to replay the full time-series faithfully at
`traj.dt` — **never jump to `traj.final`** (that throws away the planned path
and can collide). Between waypoints, poll `should_abort()` (wired to the cell
E-stop): if it returns True, halt immediately and return False. **Never** stream
setpoints faster than the SDK's control loop expects.

## uFactory xArm (`xarm-python-sdk`)

```python
from xarm.wrapper import XArmAPI
import numpy as np

class XArmDriver:
    def __init__(self, host: str, dof: int = 6):
        self.api = XArmAPI(host); self.dof = dof
        self.api.motion_enable(True); self.api.set_mode(0); self.api.set_state(0)

    def read_joint_positions(self):
        _code, angles = self.api.get_servo_angle(is_radian=True)
        return np.asarray(angles[: self.dof], float)

    def read_eef_pose(self):
        _code, pose = self.api.get_position(is_radian=True)  # [x,y,z,r,p,y] mm/rad
        xyz = np.asarray(pose[:3], float) / 1000.0
        # convert rpy->quat with your math util; placeholder identity here
        return xyz, np.array([0, 0, 0, 1.0])

    def move_joints(self, q, *, speed):
        code = self.api.set_servo_angle(
            angle=list(np.asarray(q, float)), is_radian=True,
            speed=speed, wait=True,
        )
        return code == 0

    def execute_trajectory(self, traj, should_abort=None):
        # Servo-stream the whole path at traj.dt. set_mode(1) = servo joint mode.
        import time
        self.api.set_mode(1); self.api.set_state(0)
        for i in range(len(traj)):
            if should_abort and should_abort():
                self.api.set_state(4); return False     # stop, motion not completed
            self.api.set_servo_angle_j(list(traj.waypoint(i)), is_radian=True)
            time.sleep(traj.dt)
        self.api.set_mode(0); self.api.set_state(0)      # back to position mode
        return True

    def set_gripper(self, opening):              # Robotiq/xArm gripper
        self.api.set_gripper_position((1.0 - opening) * 850)  # 0..850 (open..closed inverted)

    def emergency_stop(self):
        self.api.emergency_stop()

    def disconnect(self):
        self.api.disconnect()
```

## Universal Robots (RTDE: `ur_rtde`)

```python
import rtde_control, rtde_receive
import numpy as np

class URDriver:
    def __init__(self, host: str):
        self.rc = rtde_control.RTDEControlInterface(host)
        self.rr = rtde_receive.RTDEReceiveInterface(host)

    def read_joint_positions(self):
        return np.asarray(self.rr.getActualQ(), float)

    def read_eef_pose(self):
        p = self.rr.getActualTCPPose()           # [x,y,z, rx,ry,rz] (axis-angle)
        return np.asarray(p[:3], float), None    # convert axis-angle->quat in util

    def move_joints(self, q, *, speed):
        return bool(self.rc.moveJ(list(np.asarray(q, float)), speed, 0.5))

    def execute_trajectory(self, traj, should_abort=None):
        # servoJ each waypoint at traj.dt (UR servo loop). vel/acc are servo gains here,
        # not motion limits — cuRobo already bounded the motion. lookahead/gain tuned per cell.
        import time
        for i in range(len(traj)):
            if should_abort and should_abort():
                self.rc.servoStop(); return False
            self.rc.servoJ(list(traj.waypoint(i)), 0.0, 0.0, traj.dt, 0.1, 300)
            time.sleep(traj.dt)
        self.rc.servoStop()
        return True

    def set_gripper(self, opening):
        ...  # gripper is a separate URCap/Robotiq socket; wire per cell

    def emergency_stop(self):
        self.rc.stopJ(2.0)

    def disconnect(self):
        self.rc.stopScript()
```

## Franka (FCI via `panda-py` or `franky`)

```python
import panda_py
from panda_py import libfranka
import numpy as np

class FrankaDriver:
    def __init__(self, host: str):
        self.panda = panda_py.Panda(host)
        self.gripper = libfranka.Gripper(host)

    def read_joint_positions(self):
        return np.asarray(self.panda.get_state().q, float)

    def read_eef_pose(self):
        T = np.asarray(self.panda.get_pose())     # 4x4
        return T[:3, 3], None                     # rotation->quat in util

    def move_joints(self, q, *, speed):
        self.panda.move_to_joint_position(np.asarray(q, float), speed_factor=speed)
        return True

    def execute_trajectory(self, traj, should_abort=None):
        # Prefer a single joint-trajectory controller call so Franka's reflexes see a smooth
        # path; panda-py's JointTrajectory takes the waypoint times = i*traj.dt.
        from panda_py import controllers
        import time
        # fallback: stream waypoints (chunk if your panda-py lacks a trajectory controller)
        for i in range(len(traj)):
            if should_abort and should_abort():
                self.panda.stop(); return False
            self.panda.move_to_joint_position(np.asarray(traj.waypoint(i), float), speed_factor=0.2)
        return True

    def set_gripper(self, opening):
        self.gripper.move(width=(1.0 - opening) * 0.08, speed=0.1)

    def emergency_stop(self):
        self.panda.stop()                         # also wire the hardware E-stop

    def disconnect(self):
        pass
```

## ROS 2 (`ros2_control` / MoveIt) — vendor-agnostic

If the cell exposes the arm through ROS 2, drive it through controllers
instead of a vendor SDK:

- read state: subscribe `/joint_states`.
- move: build ONE `FollowJointTrajectory` goal from the whole path — point `i`
  has `positions = traj.waypoint(i)`, `velocities = traj.velocities[i]`, and
  `time_from_start = i * traj.dt`. The controller interpolates between points,
  so the planned cadence is preserved exactly:

  ```python
  def execute_trajectory(self, traj, should_abort=None):
      goal = FollowJointTrajectory.Goal()
      goal.trajectory.joint_names = list(traj.joint_names)
      for i in range(len(traj)):
          pt = JointTrajectoryPoint()
          pt.positions = list(traj.waypoint(i))
          pt.velocities = list(traj.velocities[i]) if traj.velocities is not None else []
          pt.time_from_start = Duration(seconds=i * traj.dt).to_msg()
          goal.trajectory.points.append(pt)
      fut = self.client.send_goal_async(goal)         # cancel on E-stop:
      # poll should_abort() while spinning; self.client._cancel_goal_async(...) to halt
      return self._await(fut, should_abort)
  ```
- gripper: the gripper action server.
- E-stop: cancel the active goal AND switch controller_manager to a halt/hold
  controller AND trip the hardware E-stop.

## Rules

- **The hardware E-stop is the real guarantee.** `emergency_stop()` should
  trigger it (or the lowest-latency halt the SDK offers), not a soft pause.
  `execute_trajectory` must also honour `should_abort()` between waypoints.
- **`execute_trajectory` (replay a PATH) ≠ `move_joints` (one config).** The
  planner (`motion_engine`, cuRoboV2) outputs a full trajectory; replay all of
  it. `move_joints`/`move_to_joints` is only for calibration / point-to-point
  jogging where there is no path to follow.
- The planner is SHIPPED — you do **not** write cuRobo. You write only this
  driver (state + replay + E-stop); `motion_engine` plans and hands you the
  `Trajectory`. See `commission.md` for wiring.
- Adapt units: cuRoboV2 and the Bridge use **radians and meters**, base frame.
  xArm reports mm; UR uses axis-angle; convert at the driver boundary.
- Keep setup speeds at the `cell.yaml` `safety.max_joint_speed_frac` — slow.
  (The planner already scaled the trajectory to it; don't speed it back up.)
- Smoke-test reads (`read_joint_positions`) with NO motion at G2; only move
  under the operator gate at G3.
