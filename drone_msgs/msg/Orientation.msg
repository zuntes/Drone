# Euler angles derived from EKF quaternion (ZYX Tait-Bryan)
# PX4 quaternion q=[w,x,y,z] NED->body, converted in telemetry_node
float32 roll_deg         # positive = right side down
float32 pitch_deg        # positive = nose up
float32 yaw_deg          # [-180, 180] positive = clockwise from above
float32 heading_deg      # [0, 360) same as yaw_deg but non-negative
float32 tilt_deg         # angle from level [0,180], 0 = perfectly level
