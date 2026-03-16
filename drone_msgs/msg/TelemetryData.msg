# Full telemetry snapshot published by telemetry_node at ~5 Hz
# Topic: /mqtt_bridge/in/telemetry

builtin_interfaces/Time stamp  # ROS2 node time at publish

FlightMode    flight_mode
Position      position_current
Position      position_home      # valid=false until first arm + home set
Altitude      alt_current
float32       alt_home           # m AMSL of home point. NaN if not set
float32       dist_from_home_m   # haversine distance. NaN if home not set
GpsHealth     gps                # valid=false until GPS topic arrives
Orientation   orientation
Velocity      velocity
BatteryState  battery            # valid=false until battery topic arrives
