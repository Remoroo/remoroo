# Camera capture — RGB-D + intrinsics (Phase 2; feeds Phases 4–6)

The Bridge's `_make_camera()` returns a **camera** object the recorder and the
world scan sample. Adapt the closest pattern; expose a uniform `grab()`.

## The camera interface the Bridge/recorder expect

```python
class Camera:
    def grab(self) -> dict:
        """Return one synchronized frame:
        {
          'rgb': np.uint8[H,W,3],
          'depth_m': np.float32[H,W],     # metres; NaN/0 where invalid
          'intrinsics': {'fx','fy','cx','cy','width','height'},
          'stamp_s': float,               # monotonic capture time
        }"""
        ...
    def start(self) -> None: ...
    def stop(self) -> None: ...
```

Depth MUST be metric. A camera with no reliable depth fails the G0 sensing
gate for that coverage role (see README sensor rule).

## Intel RealSense (`pyrealsense2`)

```python
import pyrealsense2 as rs
import numpy as np, time

class RealSenseCamera:
    def __init__(self, cfg):
        self.pipe = rs.pipeline(); self.cfg = rs.config()
        w, h = cfg.get("resolution", [1280, 720])
        self.cfg.enable_stream(rs.stream.color, w, h, rs.format.bgr8, 30)
        self.cfg.enable_stream(rs.stream.depth, w, h, rs.format.z16, 30)
        self.align = rs.align(rs.stream.color)  # depth -> color frame

    def start(self):
        self.profile = self.pipe.start(self.cfg)
        self.scale = self.profile.get_device().first_depth_sensor().get_depth_scale()

    def grab(self):
        frames = self.align.process(self.pipe.wait_for_frames())
        c = frames.get_color_frame(); d = frames.get_depth_frame()
        intr = c.profile.as_video_stream_profile().intrinsics
        rgb = np.asanyarray(c.get_data())[..., ::-1].copy()      # BGR->RGB
        depth_m = np.asanyarray(d.get_data()).astype(np.float32) * self.scale
        return {
            "rgb": rgb, "depth_m": depth_m,
            "intrinsics": {"fx": intr.fx, "fy": intr.fy, "cx": intr.ppx,
                           "cy": intr.ppy, "width": intr.width, "height": intr.height},
            "stamp_s": time.monotonic(),
        }

    def stop(self):
        self.pipe.stop()
```

## Stereolabs ZED (`pyzed.sl`)

```python
import pyzed.sl as sl
import numpy as np, time

class ZedCamera:
    def __init__(self, cfg):
        self.serial = cfg.get("serial", "auto")
        self.cam = sl.Camera()
        self.init = sl.InitParameters()
        self.init.depth_mode = sl.DEPTH_MODE.NEURAL
        self.init.coordinate_units = sl.UNIT.METER

    def start(self):
        # Jetson/Argus EGL: if DISPLAY points at a dead/headless X server the SDK
        # aborts with "(Argus) Failed to initialize EGLDisplay" / "CAMERA FAILED TO
        # SETUP". Clearing DISPLAY makes it use the Tegra EGL *device* directly,
        # which works headless (this is why the zed_api systemd service succeeds).
        import os; os.environ.pop("DISPLAY", None)
        if str(self.serial).isdigit():          # select THIS cam when 2 are on the bus
            self.init.set_from_serial_number(int(self.serial))
        if self.cam.open(self.init) != sl.ERROR_CODE.SUCCESS:
            raise RuntimeError("ZED open failed")
        self._rgb = sl.Mat(); self._depth = sl.Mat()
        ci = self.cam.get_camera_information().camera_configuration.calibration_parameters.left_cam
        self._intr = {"fx": ci.fx, "fy": ci.fy, "cx": ci.cx, "cy": ci.cy,
                      "width": ci.image_size.width, "height": ci.image_size.height}

    def grab(self):
        if self.cam.grab() != sl.ERROR_CODE.SUCCESS:
            raise RuntimeError("ZED grab failed")
        self.cam.retrieve_image(self._rgb, sl.VIEW.LEFT)
        self.cam.retrieve_measure(self._depth, sl.MEASURE.DEPTH)
        rgb = self._rgb.get_data()[..., :3][..., ::-1].copy()    # BGRA->RGB
        depth_m = self._depth.get_data().astype(np.float32)
        return {"rgb": rgb, "depth_m": depth_m, "intrinsics": self._intr,
                "stamp_s": time.monotonic()}

    def stop(self):
        self.cam.close()
```

## GenICam / industrial (`harvesters`) and ROS 2

- **GenICam**: use `harvesters` with the vendor `.cti` producer; many industrial
  depth cameras expose a range component. Read intrinsics from the device or
  calibrate (see calibration.md).
- **ROS 2**: subscribe `sensor_msgs/Image` (+ `CameraInfo` for intrinsics) and
  `sensor_msgs/PointCloud2` if depth is published as a cloud. Use
  `message_filters.ApproximateTimeSynchronizer` to pair color+depth.

## Synchronization (matters for capture + scan)

- Grab color and depth from the **same trigger** (RealSense `align`, ZED single
  `grab`, ROS time-sync). Per-frame timestamp with a **monotonic** clock.
- Measure camera↔arm latency in calibration (calibration.md, time-sync) and
  record it; the recorder embeds it so episodes are temporally honest.
- Wrist (eye-in-hand) cameras move with the arm — record the arm pose at the
  **same** stamp so each frame has a valid extrinsic.

## G2 check

Grab one frame, save it, and have the operator confirm via `view_image` that
it is the right camera and scene. Verify `depth_m` has sane metric values
(not all zero/NaN) before trusting it for the world scan.
