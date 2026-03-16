"""
Math utilities for drone telemetry.

All formulas documented inline for verification.
"""

import math
from typing import Optional, Tuple


# ── Quaternion → Euler angles (ZYX Tait-Bryan / PX4 convention) ──────────────
# PX4 quaternion: q = [w, x, y, z]  (NED frame → body frame)
# ZYX rotation order: yaw → pitch → roll (intrinsic)
# Reference: https://en.wikipedia.org/wiki/Conversion_between_quaternions_and_Euler_angles

def quat_to_euler(w: float, x: float, y: float, z: float
                  ) -> Tuple[float, float, float]:
    """
    Convert quaternion [w, x, y, z] to Euler angles (rad).

    Returns (roll, pitch, yaw) in radians.

    Formulas (ZYX Tait-Bryan):
      roll  = atan2( 2*(w*x + y*z),  1 - 2*(x² + y²) )
      pitch = arcsin( clamp( 2*(w*y - z*x), -1, 1 ) )
      yaw   = atan2( 2*(w*z + x*y),  1 - 2*(y² + z²) )
    """
    # Roll (body X-axis, forward)
    roll = math.atan2(
        2.0 * (w * x + y * z),
        1.0 - 2.0 * (x * x + y * y)
    )

    # Pitch (body Y-axis, right wing)
    # Clamp prevents NaN at ±90° singularity (gimbal lock)
    sin_p = max(-1.0, min(1.0, 2.0 * (w * y - z * x)))
    pitch = math.asin(sin_p)

    # Yaw (body Z-axis, down) = heading
    yaw = math.atan2(
        2.0 * (w * z + x * y),
        1.0 - 2.0 * (y * y + z * z)
    )

    return roll, pitch, yaw


def quat_to_tilt(w: float, x: float, y: float, z: float) -> float:
    """
    Tilt angle (rad): angle between body Z-axis and gravity vector.

    When level, body Z points straight down → tilt = 0.
    Formula: tilt = arccos( R[2][2] )
    Where R[2][2] = 1 - 2*(x² + y²)  (rotation matrix element)
    """
    r22 = max(-1.0, min(1.0, 1.0 - 2.0 * (x * x + y * y)))
    return math.acos(r22)


def ned_to_body_velocity(vn: float, ve: float, vd: float,
                          w: float, x: float, y: float, z: float
                          ) -> Tuple[float, float, float]:
    """
    Rotate NED velocity vector to body FRU frame using quaternion.

    NED input:  vN (North+), vE (East+), vD (Down+)
    FRU output: vx (Forward+), vy (Right+), vz (Up+)

    Method: Build rotation matrix R from quaternion, apply to v_ned.
    v_body = R * v_ned

    Matrix elements (R: NED → Body):
      R[0][0] = 1 - 2(y²+z²)    R[0][1] = 2(xy-wz)       R[0][2] = 2(xz+wy)
      R[1][0] = 2(xy+wz)         R[1][1] = 1 - 2(x²+z²)   R[1][2] = 2(yz-wx)
      R[2][0] = 2(xz-wy)         R[2][1] = 2(yz+wx)        R[2][2] = 1 - 2(x²+y²)

    FRU z (Up+) = -(body z Down+)  because NED z is Down, FRU z is Up.
    """
    r00 = 1.0 - 2.0 * (y*y + z*z)
    r01 = 2.0 * (x*y - w*z)
    r02 = 2.0 * (x*z + w*y)

    r10 = 2.0 * (x*y + w*z)
    r11 = 1.0 - 2.0 * (x*x + z*z)
    r12 = 2.0 * (y*z - w*x)

    r20 = 2.0 * (x*z - w*y)
    r21 = 2.0 * (y*z + w*x)
    r22 = 1.0 - 2.0 * (x*x + y*y)

    vx_fru =  r00*vn + r01*ve + r02*vd   # Forward+
    vy_fru =  r10*vn + r11*ve + r12*vd   # Right+
    vz_fru = -(r20*vn + r21*ve + r22*vd) # Up+ (negate: NED Down → FRU Up)

    return vx_fru, vy_fru, vz_fru


def haversine_m(lat1_deg: float, lon1_deg: float,
                lat2_deg: float, lon2_deg: float) -> float:
    """
    Haversine distance between two WGS84 points, in meters.

    Formula:
      a = sin²(Δlat/2) + cos(lat1) · cos(lat2) · sin²(Δlon/2)
      d = 2 · R · arcsin(√a)
    where R = 6_371_000 m (Earth mean radius)
    """
    R = 6_371_000.0
    lat1 = math.radians(lat1_deg)
    lat2 = math.radians(lat2_deg)
    dlat = math.radians(lat2_deg - lat1_deg)
    dlon = math.radians(lon2_deg - lon1_deg)
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    return 2.0 * R * math.asin(min(1.0, math.sqrt(a)))


def get_gps_lat_lon_alt(msg) -> Tuple[float, float, float]:
    """
    Extract lat/lon/alt from SensorGps, handling firmware version differences.

    Old format (px4_msgs ≤ v1.14):
      msg.lat  int32  1e-7 degrees  →  lat_deg = lat / 1e7
      msg.lon  int32  1e-7 degrees
      msg.alt  int32  millimeters   →  alt_m   = alt / 1000.0

    New format (px4_msgs v1.15+ / main):
      msg.latitude_deg   float64 degrees (direct)
      msg.longitude_deg  float64 degrees
      msg.altitude_msl_m float64 meters
    """
    if hasattr(msg, 'latitude_deg'):
        return float(msg.latitude_deg), float(msg.longitude_deg), float(msg.altitude_msl_m)
    return float(msg.lat) / 1e7, float(msg.lon) / 1e7, float(msg.alt) / 1000.0


def get_ekf_gnss_fused(flags) -> bool:
    """
    Read EKF GNSS position fusion flag, handling field rename between versions.

    PX4 v1.13: EstimatorStatusFlags.cs_gps
    PX4 v1.14+: EstimatorStatusFlags.cs_gnss_pos
    """
    return bool(
        getattr(flags, 'cs_gnss_pos',           # v1.14+ / main
        getattr(flags, 'cs_gps', False))         # v1.13 fallback
    )
