# Altitude in three reference frames
float32 alt_abs          # m AMSL (above mean sea level), EKF-fused
float32 alt_rel_home     # m above home point = -(NED local z)
float32 alt_rel_gnd      # m AGL (rangefinder or terrain model). NaN if unavailable
