# Raw GPS chip signal quality (NOT EKF-fused position)
# Use for diagnostics only. Use TelemetryData.position_current for navigation.
uint8 FIX_NONE=0
uint8 FIX_2D=2
uint8 FIX_3D=3
uint8 FIX_RTCM=4
uint8 FIX_RTK_FLOAT=5
uint8 FIX_RTK_FIXED=6

uint8 fix_type
string fix_label         # human readable: "no_fix","3d_fix","rtk_fixed",...
bool has_fix             # fix_type >= 3
uint8 satellites_used
uint8 quality_score      # 0-100 heuristic (sats + fix + hdop + jamming + ekf)
float32 hdop
float32 vdop
bool valid               # false = GPS topic not yet received
