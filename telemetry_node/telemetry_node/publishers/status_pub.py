"""
Status payload builder.

Input:  DroneDataStore
Output: drone_msgs/BridgeStatus

PX4-observable fields only. The mqtt_bridge_node adds:
  alive, bridge_uptime_s, session_key
at MQTT serialization time.

ready_for_flight logic
──────────────────────
True ONLY when every condition below is satisfied simultaneously:

  REQUIRED TOPICS PRESENT:
    vehicle_global_position, vehicle_local_position, vehicle_attitude,
    vehicle_status, vehicle_land_detected, sensor_gps, battery_status,
    estimator_status_flags

  DRONE STATE (from vehicle_status + vehicle_land_detected):
    arming_state != ARMED  (2 = armed — reject if armed)
    landed == True         (reject if already airborne)
    failsafe == False      (reject if any failsafe active)

  GPS (from sensor_gps):
    fix_type >= 3          (3D fix or better — 3D, RTCM, RTK float, RTK fixed)

  BATTERY (from battery_status):
    connected == True
    warning < 2            (0=none, 1=low are acceptable;
                            2=critical, 3=emergency, 4=failed block flight)

  EKF ALIGNMENT (from estimator_status_flags):
    cs_tilt_align == True  (filter tilt alignment complete)
    cs_yaw_align  == True  (filter yaw alignment complete)

False in ALL other cases, including when any required topic is missing.
This means ready_for_flight starts False on boot and only becomes True
once the full sensor picture has arrived and all checks pass.
"""

import math
from drone_msgs.msg import BridgeStatus
from ..data_store import DroneDataStore

_NAN = float('nan')

# Battery warning thresholds (from BatteryStatus.warning field)
# 0=none, 1=low → acceptable for flight
# 2=critical, 3=emergency, 4=failed → block
_BATTERY_WARNING_MAX_OK = 1


def build(store: DroneDataStore, ros_clock) -> BridgeStatus:
    """Always succeeds — fields use NaN/False/'' if data unavailable."""
    msg = BridgeStatus()
    msg.stamp             = ros_clock.now().to_msg()
    msg.px4_connected     = store.px4_connected(timeout_s=3.0)
    msg.flight_duration_s = round(store.current_flight_s(), 1)
    msg.ready_for_flight  = _compute_ready_for_flight(store)

    if store.timesync_status is not None:
        ts = store.timesync_status
        # Formula: rtt_ms = round_trip_time_us / 1000.0
        rtt_ms = float(ts.round_trip_time) / 1000.0
        msg.timesync_rtt_ms  = round(rtt_ms, 2)
        msg.timesync_valid   = True
        msg.timesync_quality = (
            "excellent" if rtt_ms < 5.0  else
            "good"      if rtt_ms < 15.0 else
            "degraded"  if rtt_ms < 50.0 else "poor"
        )
    else:
        msg.timesync_rtt_ms  = _NAN
        msg.timesync_valid   = False
        msg.timesync_quality = ""

    return msg


def _compute_ready_for_flight(store: DroneDataStore) -> bool:
    """
    Evaluate all preflight conditions. Returns False on the first failure.
    Order: required topics → drone state → GPS → battery → EKF.

    Short-circuits on the first False so we don't read fields on None objects.
    """

    # ── 1. Required topics must all be present ────────────────────────────────
    # These topics are non-negotiable. Without them we cannot evaluate safety.
    if (store.vehicle_status        is None
            or store.vehicle_land_detected is None
            or store.vehicle_global_position is None
            or store.vehicle_local_position is None
            or store.vehicle_attitude       is None
            or store.sensor_gps             is None
            or store.battery_status         is None
            or store.estimator_status_flags is None):
        return False

    vs   = store.vehicle_status
    land = store.vehicle_land_detected
    gps  = store.sensor_gps
    batt = store.battery_status
    ekf  = store.estimator_status_flags

    # ── 2. Drone must be on the ground and disarmed ───────────────────────────
    # arming_state values: 0=init, 1=standby, 2=ARMED, 3=standby_error
    # We block on armed (2). Standby and init are OK for preflight.
    if int(vs.arming_state) == 2:
        return False   # already armed

    # landed must be True — drone physically on ground
    if not bool(land.landed):
        return False   # already airborne

    # ── 3. No active failsafe ─────────────────────────────────────────────────
    # vehicle_status.failsafe is the top-level flag.
    # Any individual reason (battery, geofence, rc_lost, etc.) sets this True.
    if bool(vs.failsafe):
        return False

    # ── 4. GPS must have a 3D fix or better ──────────────────────────────────
    # fix_type: 0-1=no fix, 2=2D, 3=3D, 4=RTCM diff, 5=RTK float, 6=RTK fixed
    # We require at least 3D fix (fix_type >= 3) before allowing takeoff.
    if int(gps.fix_type) < 3:
        return False

    # ── 5. Battery must be connected and not critically low ───────────────────
    # battery_status.warning: 0=none, 1=low, 2=critical, 3=emergency, 4=failed
    # We allow warning=0 (none) and warning=1 (low) — operator's discretion.
    # We block warning>=2 (critical, emergency, failed).
    if not bool(batt.connected):
        return False
    if int(batt.warning) > _BATTERY_WARNING_MAX_OK:
        return False

    # ── 6. EKF must be fully aligned ─────────────────────────────────────────
    # cs_tilt_align: filter has completed tilt (roll/pitch) alignment
    # cs_yaw_align:  filter has completed yaw alignment
    # Both must be True before position/velocity estimates are trustworthy.
    if not bool(getattr(ekf, 'cs_tilt_align', False)):
        return False
    if not bool(getattr(ekf, 'cs_yaw_align', False)):
        return False

    # All checks passed
    return True