"""
Serializers: drone_msgs → JSON dicts for MQTT publishing.

Each function takes a drone_msgs message object and returns a plain dict
matching the agreed MQTT payload format. NaN floats are converted to None
(JSON null) so remote consumers can handle them cleanly.

Separate from bridge_node.py so serialization logic can be tested
independently and reused without a running MQTT connection.
"""

import math
from typing import Any


def _f(val: float) -> Any:
    """Convert float: NaN → None, finite → rounded float."""
    if math.isnan(val):
        return None
    return val


def telemetry(msg) -> dict:
    """drone_msgs/TelemetryData → MQTT payload dict."""
    gps = None
    if msg.gps.valid:
        gps = {
            "fix_type":        msg.gps.fix_type,
            "fix_label":       msg.gps.fix_label,
            "has_fix":         msg.gps.has_fix,
            "satellites_used": msg.gps.satellites_used,
            "quality_score":   msg.gps.quality_score,
            "hdop":            msg.gps.hdop,
            "vdop":            msg.gps.vdop,
        }

    battery = None
    if msg.battery.valid:
        _BATT_WARN = {0:"none",1:"low",2:"critical",3:"emergency",4:"failed"}
        battery = {
            "connected":          msg.battery.connected,
            "voltage_v":          msg.battery.voltage_v,
            "current_a":          msg.battery.current_a,
            "remaining_pct":      msg.battery.remaining_pct,
            "temperature_c":      _f(msg.battery.temperature_c),
            "time_remaining_min": _f(msg.battery.time_remaining_min),
            "warning":            msg.battery.warning,
            "warning_label":      _BATT_WARN.get(msg.battery.warning, "unknown"),
        }

    return {
        "flight_mode": {
            "armed":        msg.flight_mode.armed,
            "in_air":       msg.flight_mode.in_air,
            "landed":       msg.flight_mode.landed,
            "flight_mode":  msg.flight_mode.flight_mode,
            "control_mode": msg.flight_mode.control_mode,
        },
        "position_current": {
            "lat": msg.position_current.lat,
            "lon": msg.position_current.lon,
        },
        "position_home": {
            "lat": msg.position_home.lat,
            "lon": msg.position_home.lon,
        } if msg.position_home.valid else None,
        "alt_current": {
            "alt_abs":      msg.alt_current.alt_abs,
            "alt_rel_home": msg.alt_current.alt_rel_home,
            "alt_rel_gnd":  _f(msg.alt_current.alt_rel_gnd),
        },
        "alt_home":        _f(msg.alt_home),
        "dist_from_home":  _f(msg.dist_from_home_m),
        "gps":             gps,
        "orientation": {
            "roll_deg":    msg.orientation.roll_deg,
            "pitch_deg":   msg.orientation.pitch_deg,
            "yaw_deg":     msg.orientation.yaw_deg,
            "heading_deg": msg.orientation.heading_deg,
            "tilt_deg":    msg.orientation.tilt_deg,
        },
        "velocity": {
            "vx":          msg.velocity.vx,
            "vy":          msg.velocity.vy,
            "vz":          msg.velocity.vz,
            "heading_deg": msg.velocity.heading_deg,
        },
        "battery": battery,
    }


def status(msg, alive: bool, bridge_uptime_s: float, session_key: str) -> dict:
    """
    drone_msgs/BridgeStatus → MQTT payload dict.
    Bridge-side fields (alive, uptime, session_key) are added here
    because they are not known by telemetry_node.
    """
    return {
        "alive":              alive,
        "ready":              msg.ready_for_flight,
        "bridge_uptime_s":    bridge_uptime_s,
        "session_key":        session_key,
        "px4_connected":      msg.px4_connected,
        "timesync_quality":   msg.timesync_quality if msg.timesync_valid else None,
        "timesync_rtt_ms":    _f(msg.timesync_rtt_ms),
        "flight_duration_s":  msg.flight_duration_s,
    }


def alarm(msg) -> dict:
    """drone_msgs/AlarmData → MQTT payload dict."""
    fs = msg.failsafe
    reasons = None
    if fs.reasons_available:
        reasons = {
            "battery_low":          fs.battery_low,
            "battery_unhealthy":    fs.battery_unhealthy,
            "offboard_signal_lost": fs.offboard_signal_lost,
            "geofence_breached":    fs.geofence_breached,
            "navigator_failure":    fs.navigator_failure,
            "rc_signal_lost":       fs.rc_signal_lost,
            "no_local_position":    fs.no_local_position,
        }

    b = msg.battery
    battery_payload = None
    if b.valid:
        # `charge_cycles` exists in drone_msgs/BatteryState; convert NaN to None like other floats.
        charge_cycles = None
        if hasattr(b, "charge_cycles"):
            charge_cycles = _f(b.charge_cycles)

        battery_payload = {
            "warning":           b.warning,
            "warning_label":     b.warning_label,
            "voltage_v":         b.voltage_v,
            "voltage_per_cell_v": _f(b.voltage_per_cell_v),
            "remaining_pct":     b.remaining_pct,
            "connected":         b.connected,
            "charge_cycles":     charge_cycles,
        }

    g = msg.gps
    gps_payload = None
    if g.valid:
        gps_payload = {
            "has_fix":           g.has_fix,
            "fix_type":          g.fix_type,
            "satellites_used":   g.satellites_used,
            "noise_per_ms":      g.noise_per_ms,
            "jamming_indicator": g.jamming_indicator,
            "jamming_health":    g.jamming_health,
            "jamming_state":     g.jamming_state,
            "jamming_label":     g.jamming_label,
            "spoofing_state":    g.spoofing_state,
            "spoofing_label":    g.spoofing_label,
            "dead_reckoning":    g.dead_reckoning,
            "ekf_gnss_fused":    g.ekf_gnss_fused,
        }

    e = msg.ekf
    ekf_payload = None
    if e.valid:
        ekf_payload = {
            "tilt_aligned":   e.tilt_aligned,
            "yaw_aligned":    e.yaw_aligned,
            "gnss_fused":     e.gnss_fused,
            "baro_fused":     e.baro_fused,
            "in_air":         e.in_air,
            "dead_reckoning": e.dead_reckoning,
        }

    l = msg.land_state
    land_payload = None
    if l.valid:
        land_payload = {
            "landed":           l.landed,
            "freefall":         l.freefall,
            "ground_contact":   l.ground_contact,
            "in_ground_effect": l.in_ground_effect,
        }

    return {
        "failsafe":   {"active": fs.active, "reasons": reasons},
        "battery":    battery_payload,
        "gps":        gps_payload,
        "ekf":        ekf_payload,
        "land_state": land_payload,
    }


def task_status(msg) -> dict:
    """drone_msgs/TaskStatus → MQTT payload dict."""
    return {
        "task_id":            msg.task_id,
        "task_status":        msg.task_status,
        "current_sequence":   msg.current_sequence,
        "current_command_id": msg.current_command_id,
        "command_status":     msg.command_status,
        "waypoints_total":    msg.waypoints_total,
        "waypoints_done":     msg.waypoints_done,
        "abort_reason":       msg.abort_reason if msg.abort_reason else None,
    }
