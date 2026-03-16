# Landing detector state from /fmu/out/vehicle_land_detected
bool landed
bool freefall                  # no ground contact AND falling — critical alarm
bool ground_contact            # props touching ground but not fully landed
bool in_ground_effect          # near ground, aerodynamic effect active
bool valid
