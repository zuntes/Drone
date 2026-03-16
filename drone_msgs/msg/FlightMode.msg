# Current flight mode and arming state
bool armed
bool in_air
bool landed
string flight_mode       # "manual","mission","hold","rtl","offboard","takeoff","land",...
string control_mode      # "offboard","position","velocity","altitude" or same as flight_mode
