# Top-level failsafe + individual reasons from /fmu/out/failsafe_flags
bool active                    # vehicle_status.failsafe — top-level flag
# Individual reasons (valid only when reasons_available=true)
bool reasons_available         # false = failsafe_flags topic not in dds_topics.yaml
bool battery_low
bool battery_unhealthy
bool offboard_signal_lost
bool geofence_breached
bool navigator_failure
bool rc_signal_lost
bool no_local_position
