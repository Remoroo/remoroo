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
    def move_joints(self, q, *, speed: float) -> bool: ...    # blocking, bounded
    def execute_trajectory(self, plan) -> bool: ...          # cuRobo plan -> motion
    def set_gripper(self, opening: float) -> None: ...       # 0=open .. 1=closed
    def emergency_stop(self) -> None: ...                     # immediate halt
    def disconnect(self) -> None: ...
```

`execute_trajectory` receives a cuRoboV2 plan (a sequence of joint waypoints /
a JointState trajectory). Convert it to the SDK's servo/trajectory call.
**Never** stream raw setpoints faster than the SDK's control loop expects.

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

    def execute_trajectory(self, plan):
        # set_mode(1) for servo streaming, or chunk waypoints via set_servo_angle
        ok = True
        for q in plan.joint_waypoints:           # TODO match cuRobo plan shape
            ok = ok and self.move_joints(q, speed=plan.speed)
        return ok

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

    def execute_trajectory(self, plan):
        path = [list(np.asarray(q, float)) + [plan.speed, 0.5, 0.0]
                for q in plan.joint_waypoints]
        return bool(self.rc.moveJ(path))         # UR accepts a blended path

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

    def execute_trajectory(self, plan):
        for q in plan.joint_waypoints:
            self.panda.move_to_joint_position(np.asarray(q, float), speed_factor=plan.speed)
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
- move: action client to `FollowJointTrajectory` on the arm controller, or
  publish to a `JointTrajectoryController`.
- gripper: the gripper action server.
- E-stop: call the controller_manager switch to a halt/hold controller AND
  trip the hardware E-stop.

## Rules

- **The hardware E-stop is the real guarantee.** `emergency_stop()` should
  trigger it (or the lowest-latency halt the SDK offers), not a soft pause.
- Adapt units: cuRoboV2 and the Bridge use **radians and meters**, base frame.
  xArm reports mm; UR uses axis-angle; convert at the driver boundary.
- Keep setup speeds at the `cell.yaml` `safety.max_joint_speed_frac` — slow.
- Smoke-test reads (`read_joint_positions`) with NO motion at G2; only move
  under the operator gate at G3.
