"""Data contracts for the calibration engine (the things the edge protocol and the
Studio code code against). Pure dataclasses + numpy — no cv2, no robot.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np


@dataclass
class BoardModel:
    """A planar target. `points` are the 3D corner coordinates in the board frame
    (z=0), indexed by corner id; `scale` is a multiplicative correction (1.0 = trust
    the printed size) the solver can refine."""
    points: np.ndarray            # (N, 3), board frame, z=0
    rows: int = 0
    cols: int = 0
    square_m: float = 0.0

    @property
    def n(self) -> int:
        return int(self.points.shape[0])


@dataclass
class CaptureSample:
    """One supervised capture: what the robot reported + what the camera saw.

    `joints` are the *reported* joint angles; `fk_pose` is the base->flange transform
    the controller reports (FK of the reported joints). The solver re-derives the
    flange pose as FK(joints + fk_offsets), which is how it removes systematic FK bias.
    """
    id: int
    joints: np.ndarray            # (J,) reported joint angles
    fk_pose: np.ndarray           # (4,4) reported base->flange
    corner_ids: np.ndarray        # (M,) indices into BoardModel.points
    corners: np.ndarray           # (M,2) detected pixels, aligned with corner_ids
    cam: str = "left"


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


@dataclass
class CalibResult:
    T_optical: np.ndarray         # 4x4 hand-eye X = flange->camera-optical
    T_board: np.ndarray           # 4x4 board->base (the static target pose)
    fk_offsets: np.ndarray        # (J,) recovered per-joint angle correction (rad)
    board_scale: float
    residual_px: float            # train RMS reprojection (NOT the accept metric)
    samples_used: int
    metrics: Dict[str, float] = field(default_factory=dict)
    converged: bool = True
    message: str = ""
