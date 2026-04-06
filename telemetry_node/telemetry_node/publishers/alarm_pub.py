"""
Alarm payload builder + change detector.

Input:  DroneDataStore
Output: drone_msgs/AlarmData

AlarmState.has_changed() detects flag transitions and triggers
immediate publish from the telemetry_node timer.
"""

import logging
from builtin_interfaces.msg import Time
from drone_msgs.msg import (
    AlarmData, FailsafeStatus, BatteryAlarm, GpsAlarm, EkfStatus, LandState,
)
from ..data_store import DroneDataStore
from ..math_utils import get_ekf_gnss_fused

logger = logging.getLogger(__name__)

_BATT_WARN  = {0:"none",1:"low",2:"critical",3:"emergency",4:"failed"}
_JAMMING    = {0:"unknown",1:"ok",2:"mitigated",3:"detected"}
_SPOOFING   = {0:"unknown",1:"none",2:"indicated",3:"multiple"}
_NAN        = float('nan')


class AlarmState:
    """Tracks previous alarm values to detect safety-critical transitions."""

    def __init__(self):
        self.failsafe           = None
        self.battery_warning    = None
        self.gps_has_fix        = None
        self.gps_jamming_state  = None
        self.gps_spoofing_state = None
        self.ekf_dead_reckoning = None
        self.freefall           = None

    def has_changed(self, store: DroneDataStore) -> bool:
        """Check all monitored flags. Returns True if any changed."""
        changed = False

        if store.vehicle_status is not None:
            fs = bool(store.vehicle_status.failsafe)
            if fs != self.failsafe:
                if self.failsafe is not None:
                    logger.warning(f"⚠ FAILSAFE: {'TRIGGERED' if fs else 'cleared'}")
                self.failsafe = fs
                changed = True

        if store.battery_status is not None:
            bw = int(store.battery_status.warning)
            if bw != self.battery_warning:
                if self.battery_warning is not None:
                    logger.warning(f"Battery warning: {_BATT_WARN.get(bw, bw)}")
                self.battery_warning = bw
                changed = True

        if store.sensor_gps is not None:
            sg = store.sensor_gps
            hf = int(sg.fix_type) >= 3
            js = int(sg.jamming_state)
            ss = int(sg.spoofing_state)
            if hf != self.gps_has_fix:
                if self.gps_has_fix is not None:
                    logger.warning(f"GPS fix: {'acquired' if hf else 'LOST'}")
                self.gps_has_fix = hf
                changed = True
            if js != self.gps_jamming_state:
                if self.gps_jamming_state is not None and js not in (0, 1):
                    logger.warning(f"GPS jamming: {_JAMMING.get(js, js)}")
                self.gps_jamming_state = js
                if js not in (0, 1): changed = True
            if ss != self.gps_spoofing_state:
                if self.gps_spoofing_state is not None and ss not in (0, 1):
                    logger.warning(f"GPS spoofing: {_SPOOFING.get(ss, ss)}")
                self.gps_spoofing_state = ss
                if ss not in (0, 1): changed = True

        if store.estimator_status_flags is not None:
            dr = bool(getattr(store.estimator_status_flags,
                              'cs_inertial_dead_reckoning', False))
            if dr != self.ekf_dead_reckoning:
                if self.ekf_dead_reckoning is not None:
                    logger.error(f"EKF dead reckoning: {'ACTIVE' if dr else 'cleared'}")
                self.ekf_dead_reckoning = dr
                changed = True

        if store.vehicle_land_detected is not None:
            ff = bool(store.vehicle_land_detected.freefall)
            if ff != self.freefall:
                if self.freefall is not None:
                    logger.error(f"FREEFALL: {'DETECTED' if ff else 'cleared'}")
                self.freefall = ff
                changed = True

        return changed


def build(store: DroneDataStore, ros_clock) -> AlarmData:
    """Build AlarmData message. Always succeeds — valid=False if data unavailable."""
    msg = AlarmData()
    msg.stamp = ros_clock.now().to_msg()

    # ── Failsafe ──────────────────────────────────────────────────────────────
    fs_msg = FailsafeStatus()
    if store.vehicle_status is not None:
        fs_msg.active = bool(store.vehicle_status.failsafe)
    if store.failsafe_flags is not None:
        f = store.failsafe_flags
        fs_msg.reasons_available      = True
        fs_msg.battery_low            = bool(getattr(f, 'battery_low_remaining_time', False))
        fs_msg.battery_unhealthy      = bool(getattr(f, 'battery_unhealthy',           False))
        fs_msg.offboard_signal_lost   = bool(getattr(f, 'offboard_control_signal_lost',False))
        fs_msg.geofence_breached      = bool(getattr(f, 'geofence_breached',            False))
        fs_msg.navigator_failure      = bool(getattr(f, 'navigator_failure',             False))
        fs_msg.rc_signal_lost         = bool(getattr(f, 'manual_control_signal_lost',   False))
        fs_msg.no_local_position      = bool(getattr(f, 'no_local_position',             False))
    msg.failsafe = fs_msg

    # ── Battery alarm ─────────────────────────────────────────────────────────
    batt_msg = BatteryAlarm()
    if store.battery_status is not None:
        b = store.battery_status
        cells   = int(b.cell_count)
        voltage = float(b.voltage_v)
        warn    = int(b.warning)
        batt_msg.warning              = warn
        batt_msg.warning_label        = _BATT_WARN.get(warn, f"unknown_{warn}")
        batt_msg.voltage_v            = round(voltage, 3)
        # Formula: voltage_per_cell = total_voltage / cell_count
        batt_msg.voltage_per_cell_v   = round(voltage/cells, 4) if cells > 0 else _NAN
        # Formula: remaining_pct = remaining * 100.0
        batt_msg.remaining_pct        = round(float(b.remaining)*100.0, 1)
        batt_msg.connected            = bool(b.connected)
        batt_msg.charge_cycles        = int(b.charge_cycles) if hasattr(b, "charge_cycles") else _NAN
        batt_msg.valid                = True
    msg.battery = batt_msg

    # ── GPS alarm ─────────────────────────────────────────────────────────────
    gps_msg = GpsAlarm()
    if store.sensor_gps is not None:
        sg = store.sensor_gps
        ft = int(sg.fix_type)
        ji = int(sg.jamming_indicator)
        js = int(sg.jamming_state)
        ss = int(sg.spoofing_state)
        gps_msg.has_fix           = ft >= 3
        gps_msg.fix_type          = ft
        gps_msg.satellites_used   = int(sg.satellites_used)
        gps_msg.noise_per_ms      = int(sg.noise_per_ms)
        gps_msg.jamming_indicator = ji
        gps_msg.jamming_health    = "ok" if ji<40 else ("warning" if ji<80 else "critical")
        gps_msg.jamming_state     = js
        gps_msg.jamming_label     = _JAMMING.get(js, f"unknown_{js}")
        gps_msg.spoofing_state    = ss
        gps_msg.spoofing_label    = _SPOOFING.get(ss, f"unknown_{ss}")
        gps_msg.valid             = True
    if store.vehicle_global_position is not None:
        gps_msg.dead_reckoning    = bool(store.vehicle_global_position.dead_reckoning)
    if store.estimator_status_flags is not None:
        gps_msg.ekf_gnss_fused    = get_ekf_gnss_fused(store.estimator_status_flags)
    msg.gps = gps_msg

    # ── EKF status ────────────────────────────────────────────────────────────
    ekf_msg = EkfStatus()
    if store.estimator_status_flags is not None:
        f = store.estimator_status_flags
        ekf_msg.tilt_aligned   = bool(getattr(f, 'cs_tilt_align',              False))
        ekf_msg.yaw_aligned    = bool(getattr(f, 'cs_yaw_align',               False))
        ekf_msg.gnss_fused     = get_ekf_gnss_fused(f)
        ekf_msg.baro_fused     = bool(getattr(f, 'cs_baro_hgt',                False))
        ekf_msg.in_air         = bool(getattr(f, 'cs_in_air',                  False))
        ekf_msg.dead_reckoning = bool(getattr(f, 'cs_inertial_dead_reckoning', False))
        ekf_msg.valid          = True
    msg.ekf = ekf_msg

    # ── Land state ────────────────────────────────────────────────────────────
    land_msg = LandState()
    if store.vehicle_land_detected is not None:
        l = store.vehicle_land_detected
        land_msg.landed           = bool(l.landed)
        land_msg.freefall         = bool(l.freefall)
        land_msg.ground_contact   = bool(l.ground_contact)
        land_msg.in_ground_effect = bool(l.in_ground_effect)
        land_msg.valid            = True
    msg.land_state = land_msg

    return msg
