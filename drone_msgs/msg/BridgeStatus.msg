# Bridge and PX4 connection health published by telemetry_node at ~1 Hz
# Topic: /mqtt_bridge/in/status
# Note: mqtt_bridge_node adds alive, bridge_uptime_s, session_key at MQTT publish time

builtin_interfaces/Time stamp

bool px4_connected             # true if PX4 message arrived in last 3 seconds
bool ready_for_flight          # true if telemetry_node has received all data needed to compute flight duration
string timesync_quality        # "excellent","good","degraded","poor","" if no data
float32 timesync_rtt_ms        # PX4<->companion RTT. NaN if no data
float32 flight_duration_s      # seconds airborne this leg (bridge-computed)
bool timesync_valid            # false = timesync_status topic not yet received
