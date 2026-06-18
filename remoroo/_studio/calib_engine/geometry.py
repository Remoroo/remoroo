"""SE(3) / SO(3) helpers, a revolute-joint forward-kinematics Chain, and the
pinhole projection — the pure-numpy primitives the solver/metrics/synth build on.

No cv2, no robot: everything here runs in the dev `.venv` (numpy only) and in CI.
Conventions:
  * Transforms are 4x4 homogeneous matrices `T_a_b` mapping a point in frame b into
    frame a:  p_a = T_a_b @ [p_b; 1].
  * Rotations are parameterised for optimisation as so(3) rotation vectors (axis*angle).
  * URDF `rpy` is fixed-axis roll-pitch-yaw, i.e. R = Rz(yaw) @ Ry(pitch) @ Rx(roll).
"""
from __future__ import annotations

from typing import List, Sequence

import numpy as np


# --------------------------------------------------------------------------- #
# SO(3) / SE(3)                                                                #
# --------------------------------------------------------------------------- #
def rodrigues(rotvec: np.ndarray) -> np.ndarray:
    """Rotation vector (axis*angle) -> 3x3 rotation matrix (exponential map)."""
    rotvec = np.asarray(rotvec, float)
    theta = float(np.linalg.norm(rotvec))
    if theta < 1e-12:
        return np.eye(3)
    k = rotvec / theta
    K = np.array([[0.0, -k[2], k[1]], [k[2], 0.0, -k[0]], [-k[1], k[0], 0.0]])
    return np.eye(3) + np.sin(theta) * K + (1.0 - np.cos(theta)) * (K @ K)


def rotvec_from_R(R: np.ndarray) -> np.ndarray:
    """3x3 rotation matrix -> rotation vector (log map), robust near 0 and pi."""
    R = np.asarray(R, float)
    cos = (np.trace(R) - 1.0) / 2.0
    cos = float(np.clip(cos, -1.0, 1.0))
    theta = np.arccos(cos)
    if theta < 1e-8:
        return np.zeros(3)
    if np.pi - theta < 1e-6:
        # Near pi: axis from the dominant column of (R + I)/2.
        A = (R + np.eye(3)) / 2.0
        diag = np.clip(np.diag(A), 0.0, None)
        i = int(np.argmax(diag))
        axis = A[:, i] / np.sqrt(diag[i]) if diag[i] > 1e-9 else np.eye(3)[i]
        axis = axis / np.linalg.norm(axis)
        return axis * theta
    w = np.array([R[2, 1] - R[1, 2], R[0, 2] - R[2, 0], R[1, 0] - R[0, 1]])
    return w * (theta / (2.0 * np.sin(theta)))


def make_T(R: np.ndarray, t: np.ndarray) -> np.ndarray:
    T = np.eye(4)
    T[:3, :3] = R
    T[:3, 3] = np.asarray(t, float).reshape(3)
    return T


def inv_T(T: np.ndarray) -> np.ndarray:
    R = T[:3, :3]
    t = T[:3, 3]
    out = np.eye(4)
    out[:3, :3] = R.T
    out[:3, 3] = -R.T @ t
    return out


def transform_points(T: np.ndarray, pts: np.ndarray) -> np.ndarray:
    """Apply a 4x4 transform to (N,3) points -> (N,3)."""
    pts = np.asarray(pts, float)
    return pts @ T[:3, :3].T + T[:3, 3]


def rotation_angle(R: np.ndarray) -> float:
    """Geodesic rotation angle of a 3x3 rotation matrix, in radians."""
    return float(np.linalg.norm(rotvec_from_R(R)))


def transform_geodesic(Ta: np.ndarray, Tb: np.ndarray) -> tuple:
    """(rotation angle in deg, translation distance in m) between two 4x4 transforms."""
    dR = Ta[:3, :3].T @ Tb[:3, :3]
    ang = np.degrees(rotation_angle(dR))
    dt = float(np.linalg.norm(Ta[:3, 3] - Tb[:3, 3]))
    return ang, dt


# --------------------------------------------------------------------------- #
# URDF rpy <-> R                                                               #
# --------------------------------------------------------------------------- #
def rpy_to_R(rpy: Sequence[float]) -> np.ndarray:
    r, p, y = (float(v) for v in rpy)
    cr, sr = np.cos(r), np.sin(r)
    cp, sp = np.cos(p), np.sin(p)
    cy, sy = np.cos(y), np.sin(y)
    Rx = np.array([[1, 0, 0], [0, cr, -sr], [0, sr, cr]])
    Ry = np.array([[cp, 0, sp], [0, 1, 0], [-sp, 0, cp]])
    Rz = np.array([[cy, -sy, 0], [sy, cy, 0], [0, 0, 1]])
    return Rz @ Ry @ Rx


def R_to_rpy(R: np.ndarray) -> np.ndarray:
    """Inverse of rpy_to_R (R = Rz@Ry@Rx); returns (roll, pitch, yaw)."""
    R = np.asarray(R, float)
    sy = -R[2, 0]
    sy = float(np.clip(sy, -1.0, 1.0))
    pitch = np.arcsin(sy)
    if abs(sy) < 1.0 - 1e-9:
        roll = np.arctan2(R[2, 1], R[2, 2])
        yaw = np.arctan2(R[1, 0], R[0, 0])
    else:  # gimbal lock
        roll = np.arctan2(-R[1, 2], R[1, 1])
        yaw = 0.0
    return np.array([roll, pitch, yaw])


# --------------------------------------------------------------------------- #
# Pinhole projection                                                           #
# --------------------------------------------------------------------------- #
def project(K: np.ndarray, pts_cam: np.ndarray) -> np.ndarray:
    """Project (N,3) camera-frame points to (N,2) pixels with intrinsics K (no distortion)."""
    pts_cam = np.asarray(pts_cam, float)
    z = pts_cam[:, 2]
    x = pts_cam[:, 0] / z
    y = pts_cam[:, 1] / z
    u = K[0, 0] * x + K[0, 2]
    v = K[1, 1] * y + K[1, 2]
    return np.stack([u, v], axis=1)


# --------------------------------------------------------------------------- #
# Revolute-joint forward kinematics                                           #
# --------------------------------------------------------------------------- #
class Chain:
    """A serial chain of revolute joints (URDF-style): each joint i has a fixed
    `origin` (4x4 transform from the previous link to the joint at theta=0) and a
    unit `axis`. FK(theta) = prod_i origin_i @ Rot(axis_i, theta_i)  ->  base->flange.

    This is exactly what `urdf_io.chain_from_urdf` will produce from a real robot, so
    the FK-correction the solver estimates (per-joint angle offsets) maps 1:1 onto a
    real arm. In synth we build a chain programmatically.
    """

    def __init__(self, origins: Sequence[np.ndarray], axes: Sequence[np.ndarray]):
        assert len(origins) == len(axes)
        self.origins: List[np.ndarray] = [np.asarray(o, float) for o in origins]
        self.axes: List[np.ndarray] = [np.asarray(a, float) / np.linalg.norm(a) for a in axes]
        self.n = len(self.axes)

    def fk(self, theta: np.ndarray) -> np.ndarray:
        theta = np.asarray(theta, float)
        T = np.eye(4)
        for i in range(self.n):
            T = T @ self.origins[i] @ make_T(rodrigues(self.axes[i] * theta[i]), np.zeros(3))
        return T
