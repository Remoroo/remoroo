"""remoroo calib_engine — the SHIPPED calibration engine (imported, never authored).

Pure-numpy/scipy core (solve / metrics / curate / urdf_io / synth) runs in the dev
`.venv` and CI with NO cv2 and NO robot. Only `detect` needs cv2 (robot / cv2 CI image),
so it is imported lazily by callers, not here.

See ADC/remoroo_calibration_redesign.md (§8) for the design + phase plan.
"""
from __future__ import annotations

from .types import BoardModel, CalibResult, CaptureSample
from .geometry import Chain
from .solve import solve_eye_in_hand, solve_handeye_closed
from .metrics import (
    consensus_spread,
    held_out_reprojection,
    observability,
    parameter_covariance,
    reprojection_detail,
    tip_landing_error,
)
from .curate import build_weights, flag_suspect_corners, solve_curated
from . import urdf_io

__all__ = [
    "BoardModel", "CalibResult", "CaptureSample", "Chain",
    "solve_eye_in_hand", "solve_handeye_closed", "solve_curated",
    "held_out_reprojection", "tip_landing_error", "observability",
    "parameter_covariance", "consensus_spread", "reprojection_detail",
    "build_weights", "flag_suspect_corners", "urdf_io",
]
