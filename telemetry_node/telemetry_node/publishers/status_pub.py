"""
Status payload builder.

Input:  DroneDataStore
Output: drone_msgs/BridgeStatus

PX4-observable fields only. The mqtt_bridge_node adds:
  alive, bridge_uptime_s, session_key
at MQTT serialization time.
"""

import math
from builtin_interfaces.msg import Time
from drone_msgs.msg import BridgeStatus
from ..data_store import DroneDataStore

_NAN = float('nan')


def build(store: DroneDataStore, ros_clock) -> BridgeStatus:
    """Always succeeds — fields use NaN/False if data unavailable."""
    msg = BridgeStatus()
    msg.stamp         = ros_clock.now().to_msg()
    msg.px4_connected = store.px4_connected(timeout_s=3.0)
    msg.flight_duration_s = round(store.current_flight_s(), 1)

    if store.timesync_status is not None:
        ts = store.timesync_status
        # Formula: rtt_ms = round_trip_time_us / 1000.0
        rtt_ms = float(ts.round_trip_time) / 1000.0
        msg.timesync_rtt_ms   = round(rtt_ms, 2)
        msg.timesync_valid    = True
        msg.timesync_quality  = (
            "excellent" if rtt_ms < 5.0  else
            "good"      if rtt_ms < 15.0 else
            "degraded"  if rtt_ms < 50.0 else "poor"
        )
    else:
        msg.timesync_rtt_ms  = _NAN
        msg.timesync_valid   = False
        msg.timesync_quality = ""

    return msg
