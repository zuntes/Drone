# Payload for a single command item in a TaskCommand
# Fields that don't apply to the command_type should be marked invalid via has_* flags

float64 latitude           # WGS84 degrees (has_position must be true)
float64 longitude          # WGS84 degrees
float32 altitude_m         # meters (has_altitude must be true)
string  altitude_ref       # "home" | "amsl" | "ground"— always set when has_altitude=true
float32 speed_ms           # m/s (has_speed must be true)
float32 arrival_radius_m   # meters, how close = "arrived" (has_arrival_radius must be true)

bool has_position
bool has_altitude
bool has_speed
bool has_arrival_radius
