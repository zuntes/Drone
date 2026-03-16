"""
Telemetry payload builder.

Input:  DroneDataStore
Output: drone_msgs/TelemetryData

Builds a fully-typed ROS2 message. The mqtt_bridge_node serializes
this to JSON for MQTT. Any other ROS2 node can subscribe with full
type safety and field introspection.

NaN convention for optional float fields:
  float32/float64 fields that are not available are set to float('nan').
  Consumers must check the companion valid/has_* bool before using the value.
"""

import math
from builtin_interfaces.msg import Time
from drone_msgs.msg import (
    TelemetryData, FlightMode, Position, Altitude,
    GpsHealth, Orientation, Velocity, BatteryState,
)
from ..data_store import DroneDataStore
from ..math_utils import (
    quat_to_euler, quat_to_tilt, ned_to_body_velocity,
    haversine_m, get_ekf_gnss_fused,
)

_NAN = float('nan')

_NAV_STATE_LABELS = {
    0:"manual", 1:"altitude", 2:"position", 3:"mission", 4:"mission",
    5:"hold", 6:"rtl", 10:"stabilized", 14:"offboard",
    17:"takeoff", 18:"land", 20:"precision_land", 21:"orbit", 22:"vtol_takeoff",
}
_FIX_LABELS = {
    0:"no_fix", 1:"no_fix", 2:"2d_fix", 3:"3d_fix",
    4:"rtcm_diff", 5:"rtk_float", 6:"rtk_fixed", 8:"extrapolated",
}
_BATT_WARN_LABEL = {0:"none",1:"low",2:"critical",3:"emergency",4:"failed"}


def build(store: DroneDataStore, ros_clock) -> TelemetryData:
    """
    Build TelemetryData message from DroneDataStore.
    Returns None if required PX4 topics haven't arrived yet.

    Required: vehicle_global_position, vehicle_local_position,
              vehicle_attitude, vehicle_status, vehicle_land_detected
    """
    if (store.vehicle_global_position is None
            or store.vehicle_local_position is None
            or store.vehicle_attitude is None
            or store.vehicle_status is None
            or store.vehicle_land_detected is None):
        return None

    msg = TelemetryData()
    msg.stamp = ros_clock.now().to_msg()

    gpos = store.vehicle_global_position
    lpos = store.vehicle_local_position
    att  = store.vehicle_attitude
    vs   = store.vehicle_status
    land = store.vehicle_land_detected

    # ── Flight mode ───────────────────────────────────────────────────────────
    nav    = int(vs.nav_state)
    arming = int(vs.arming_state)
    msg.flight_mode = FlightMode(
        armed        = (arming == 2),           # ARMING_STATE_ARMED = 2
        in_air       = not bool(land.landed),
        landed       = bool(land.landed),
        flight_mode  = _NAV_STATE_LABELS.get(nav, f"mode_{nav}"),
        control_mode = _control_mode_label(store.vehicle_control_mode, nav),
    )

    # ── Current position (EKF-fused) ──────────────────────────────────────────
    lat = float(gpos.lat)
    lon = float(gpos.lon)
    msg.position_current = Position(lat=round(lat,7), lon=round(lon,7), valid=True)

    # ── Home position ─────────────────────────────────────────────────────────
    home = store.home_position
    if home is not None and bool(home.valid_hpos):
        hlat = float(home.lat)
        hlon = float(home.lon)
        msg.position_home = Position(lat=round(hlat,7), lon=round(hlon,7), valid=True)
        msg.alt_home      = float(home.alt) if bool(home.valid_alt) else _NAN
        # Haversine: d = 2R·arcsin(√(sin²(Δlat/2)+cos(l1)·cos(l2)·sin²(Δlon/2)))
        msg.dist_from_home_m = round(haversine_m(hlat, hlon, lat, lon), 2)
    else:
        msg.position_home    = Position(valid=False)
        msg.alt_home         = _NAN
        msg.dist_from_home_m = _NAN

    # ── Altitude ──────────────────────────────────────────────────────────────
    alt_amsl     = float(gpos.alt)
    # alt_rel_home = -(NED_z)  because NED z is Down+
    alt_rel_home = -float(lpos.z)
    # AGL: rangefinder direct (best), terrain model (fallback), NaN if neither
    if bool(lpos.dist_bottom_valid):
        alt_rel_gnd = float(lpos.dist_bottom)           # rangefinder
    elif bool(gpos.terrain_alt_valid):
        alt_rel_gnd = alt_amsl - float(gpos.terrain_alt)  # terrain: agl = amsl - terrain_amsl
    else:
        alt_rel_gnd = _NAN
    msg.alt_current = Altitude(
        alt_abs      = round(alt_amsl,     3),
        alt_rel_home = round(alt_rel_home, 3),
        alt_rel_gnd  = (round(alt_rel_gnd, 3) if not math.isnan(alt_rel_gnd) else _NAN),
    )

    # ── GPS health ────────────────────────────────────────────────────────────
    sgps = store.sensor_gps
    if sgps is not None:
        fix_type = int(sgps.fix_type)
        ekf_fused = (get_ekf_gnss_fused(store.estimator_status_flags)
                     if store.estimator_status_flags else False)
        msg.gps = GpsHealth(
            fix_type       = fix_type,
            fix_label      = _FIX_LABELS.get(fix_type, f"unknown_{fix_type}"),
            has_fix        = fix_type >= 3,
            satellites_used= int(sgps.satellites_used),
            quality_score  = _gps_quality(fix_type, int(sgps.satellites_used),
                                          float(sgps.hdop), int(sgps.jamming_indicator),
                                          ekf_fused),
            hdop           = round(float(sgps.hdop), 3),
            vdop           = round(float(sgps.vdop), 3),
            valid          = True,
        )
    else:
        msg.gps = GpsHealth(valid=False)

    # ── Orientation from quaternion ───────────────────────────────────────────
    # PX4 q=[w,x,y,z], NED→body rotation
    w = float(att.q[0]); x = float(att.q[1])
    y = float(att.q[2]); z = float(att.q[3])
    roll_r, pitch_r, yaw_r = quat_to_euler(w, x, y, z)
    tilt_r = quat_to_tilt(w, x, y, z)
    # heading = (yaw_deg + 360) % 360 → [0, 360)
    heading = (math.degrees(yaw_r) + 360.0) % 360.0
    msg.orientation = Orientation(
        roll_deg    = round(math.degrees(roll_r),  2),
        pitch_deg   = round(math.degrees(pitch_r), 2),
        yaw_deg     = round(math.degrees(yaw_r),   2),
        heading_deg = round(heading,                2),
        tilt_deg    = round(math.degrees(tilt_r),  2),
    )

    # ── Velocity (body FRU via quaternion rotation from NED) ──────────────────
    # NED: vx=North+, vy=East+, vz=Down+
    # FRU: vx=Forward+, vy=Right+, vz=Up+ (negate NED z)
    vx_f, vy_f, vz_f = ned_to_body_velocity(
        float(lpos.vx), float(lpos.vy), float(lpos.vz), w, x, y, z)
    msg.velocity = Velocity(
        vx          = round(vx_f, 3),
        vy          = round(vy_f, 3),
        vz          = round(vz_f, 3),
        heading_deg = round(heading, 2),
    )

    # ── Battery ───────────────────────────────────────────────────────────────
    batt = store.battery_status
    if batt is not None:
        temp_k = float(batt.temperature)
        # temp_c = temp_k - 273.15  (Kelvin to Celsius)
        temp_c = (temp_k - 273.15) if not math.isnan(temp_k) else _NAN
        tr = float(batt.time_remaining_s)
        # time_remaining_min = time_remaining_s / 60.0
        time_rem = (tr / 60.0) if (not math.isnan(tr) and tr >= 0) else _NAN
        msg.battery = BatteryState(
            connected          = bool(batt.connected),
            voltage_v          = round(float(batt.voltage_v),  3),
            current_a          = round(float(batt.current_a),  3),
            # remaining_pct = remaining * 100.0
            remaining_pct      = round(float(batt.remaining) * 100.0, 1),
            temperature_c      = (round(temp_c, 2) if not math.isnan(temp_c) else _NAN),
            time_remaining_min = (round(time_rem, 1) if not math.isnan(time_rem) else _NAN),
            warning            = int(batt.warning),
            valid              = True,
        )
    else:
        msg.battery = BatteryState(valid=False)

    return msg


def _control_mode_label(ctrl, nav_state: int) -> str:
    if ctrl is None:
        return _NAV_STATE_LABELS.get(nav_state, f"mode_{nav_state}")
    if getattr(ctrl, 'flag_control_offboard_enabled', False): return "offboard"
    if getattr(ctrl, 'flag_control_position_enabled', False): return "position"
    if getattr(ctrl, 'flag_control_velocity_enabled', False): return "velocity"
    if getattr(ctrl, 'flag_control_altitude_enabled', False): return "altitude"
    return _NAV_STATE_LABELS.get(nav_state, f"mode_{nav_state}")


def _gps_quality(fix_type, sats, hdop, jamming, ekf_fused) -> int:
    """
    Heuristic GPS quality 0-100:
      sats:    min(sats/12,1)*25  → 0-25 pts
      fix:     {3→20,4→22,5→25,6→30} pts
      hdop:    (5-hdop)/4*25      → 0-25 pts (hdop≤1→25, hdop≥5→0)
      jamming: <40→15, <80→7, else→0 pts
      ekf:     +5 pts bonus
    """
    s = int(min(sats/12.0,1.0)*25)
    s += {0:0,1:0,2:5,3:20,4:22,5:25,6:30}.get(fix_type,0)
    s += 25 if hdop<=1.0 else (0 if hdop>=5.0 else int((5.0-hdop)/4.0*25))
    s += 15 if jamming<40 else (7 if jamming<80 else 0)
    if ekf_fused: s += 5
    return min(s, 100)
