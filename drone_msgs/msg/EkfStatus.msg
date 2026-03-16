# EKF2 fusion health from /fmu/out/estimator_status_flags
bool tilt_aligned              # filter tilt alignment complete
bool yaw_aligned               # filter yaw alignment complete
bool gnss_fused                # GNSS position fusion active (cs_gnss_pos / cs_gps)
bool baro_fused                # barometer height fusion active
bool in_air                    # EKF thinks vehicle is airborne
bool dead_reckoning            # no position constraint, drifting on inertia
bool valid
