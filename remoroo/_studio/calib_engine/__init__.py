"""remoroo calib_engine — the SHIPPED calibration engine (imported, never authored).

Pure-numpy/scipy core (solve / metrics / curate / urdf_io / synth) runs in the dev
`.venv` and CI with NO cv2 and NO robot. Only `detect` needs cv2 (robot / cv2 CI image),
so it is imported lazily by callers, not here.

See ADC/remoroo_calibration_redesign.md (§8) for the design + phase plan.
"""
from __future__ import annotations

from .types import BoardModel, CalibResult, CaptureSample
from .geometry import Chain
from .solve import solve_base_to_base, solve_eye_in_hand, solve_handeye_closed, solve_static_camera
from .metrics import (
    consensus_spread,
    held_out_reprojection,
    observability,
    parameter_covariance,
    predict_uv,
    reprojection_detail,
    stereo_consistency,
    tip_landing_error,
    weak_rotation_axis,
)
from .curate import build_weights, flag_suspect_corners, homography_resnap, solve_curated
from . import urdf_io

__all__ = [
    "BoardModel", "CalibResult", "CaptureSample", "Chain",
    "solve_eye_in_hand", "solve_handeye_closed", "solve_base_to_base", "solve_static_camera", "solve_curated",
    "held_out_reprojection", "tip_landing_error", "observability",
    "parameter_covariance", "consensus_spread", "reprojection_detail", "predict_uv",
    "stereo_consistency", "weak_rotation_axis",
    "build_weights", "flag_suspect_corners", "homography_resnap", "urdf_io",
]
