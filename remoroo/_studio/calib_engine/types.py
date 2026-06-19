"""Data contracts for the calibration engine (the things the edge protocol and the
Studio code code against). Pure dataclasses + numpy — no cv2, no robot.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np

# The fiducial abstraction lives in `fiducials/` (a Target = named 3D points + a detector +
# min_points). The engine codes against `Target`; `BoardModel` is a thin back-compat
# constructor for checkerboard-style targets so synth + older call sites keep working.
from .fiducials.base import FiducialDetector, Target  # noqa: F401  (re-exported)


class BoardModel(Target):
    """Back-compat: a checkerboard-style planar target built from (points, rows, cols,
    square). New code builds Targets via `calib_engine.fiducials.build_target`. `min_points`
    defaults to 6 (a board view needs several corners); a single-marker target sets 4."""

    def __init__(self, points: np.ndarray, rows: int = 0, cols: int = 0, square_m: float = 0.0,
                 *, min_points: int = 6, detector: Optional[FiducialDetector] = None,
                 planar: bool = True):
        super().__init__(point_xyz=points, detector=detector, min_points=int(min_points),
                         planar=planar, type="board")
        self.rows, self.cols, self.square_m = int(rows), int(cols), float(square_m)


@dataclass
class CaptureSample:
    """One supervised capture: what the robot reported + what the camera saw.

    `joints` are the *reported* joint angles; `fk_pose` is the base->flange transform
    the controller reports (FK of the reported joints). The solver re-derives the
    flange pose as FK(joints + fk_offsets), which is how it removes systematic FK bias.

    `image` is the retained RGB frame (kept on the edge so the operator can curate the
    corners — inspect the overlay on the real frame, drag a bad corner, sub-pixel
    re-snap). It is optional: off-robot / FakeBridge runs leave it None and the Studio
    falls back to the pixel-only overlay.
    """
    id: int
    joints: np.ndarray            # (J,) reported joint angles
    fk_pose: np.ndarray           # (4,4) reported base->flange
    corner_ids: np.ndarray        # (M,) indices into BoardModel.points
    corners: np.ndarray           # (M,2) detected pixels, aligned with corner_ids
    cam: str = "left"
    image: Optional[np.ndarray] = None  # (H,W,3) retained frame for curation (edge only)


@dataclass
class PlanItem:
    """One calibration the agent derived from the rig (Pillar B). `nominal_flange_body`
    is the URDF flange->camera-body transform; `nominal_T` is the flange->optical seed
    (flange_body @ nominal body->optical). The solve refines `nominal_T`; on accept we
    back out body->optical = inv(nominal_flange_body) @ X and write it to optical_frame."""
    camera_link: str
    optical_frame: str
    kind: str                      # "eye_in_hand" | "eye_to_hand" | "base_to_base"
    flange_link: str
    nominal_flange_body: np.ndarray  # (4,4)
    nominal_T: np.ndarray            # (4,4) flange->optical seed
    arm: str = ""
    # For a WORLD-FIXED camera (kind eye_to_hand): how the board reaches its view.
    #   "handheld" (default) -> operator places/moves a board, NO robot motion; the engine
    #                           localizes the camera by PnP against the (fixed) board.
    #   "arm"               -> a presenting arm carries the board (the eye_to_hand bundle).
    # Derived as "handheld" by build_plan; the agent may switch it to "arm" + name the arm.
    board_source: str = "handheld"
    # base_to_base only: the two wrist cameras (each already calibrated eye-in-hand) whose
    # shared-marker views recover the transform between their arms' bases.
    partner_camera: str = ""
    secondary_camera: str = ""
    # ── authored-pipeline fields (set when the agent authored the step; see pipeline.py) ──
    # `id` is the step's stable id (deps + the Studio rail key); defaults to camera_link.
    # `target_id` selects which named Target this step binds (a pipeline may use several).
    # `depends_on` are the step ids that must be ACCEPTED before this one can run (e.g. a
    # base_to_base step depends on its two eye-in-hand steps). Empty for an auto-derived plan.
    id: str = ""
    target_id: str = ""
    depends_on: List[str] = field(default_factory=list)


@dataclass
class CalibResult:
    T_optical: np.ndarray         # 4x4 hand-eye X. eye_in_hand: flange->camera-optical;
                                  #   eye_to_hand: base->camera-optical (the static camera)
    T_board: np.ndarray           # 4x4 the static target pose. eye_in_hand: board->base;
                                  #   eye_to_hand: board->flange (board mounted on the gripper)
    fk_offsets: np.ndarray        # (J,) recovered per-joint angle correction (rad)
    board_scale: float
    residual_px: float            # train RMS reprojection (NOT the accept metric)
    samples_used: int
    kind: str = "eye_in_hand"     # which model produced this (eye_in_hand|eye_to_hand)
    t_lr_err_deg: Optional[float] = None  # stereo left/right self-check vs the SDK baseline
    # The camera intrinsics the bundle USED. When K was refined jointly (a multi-point board over
    # diverse views), this is the REFINED 3x3; otherwise the factory K passed in. Everything that
    # reprojects through this result must use THIS K, not the original factory K.
    K: Optional[np.ndarray] = None
    metrics: Dict[str, float] = field(default_factory=dict)
    converged: bool = True
    message: str = ""
