# Battery alarm — subset of BatteryState focused on safety thresholds
uint8 WARNING_NONE=0
uint8 WARNING_LOW=1
uint8 WARNING_CRITICAL=2
uint8 WARNING_EMERGENCY=3
uint8 WARNING_FAILED=4

uint8 warning
string warning_label           # "none","low","critical","emergency","failed"
float32 voltage_v
float32 voltage_per_cell_v     # voltage_v / cell_count
float32 remaining_pct
bool connected
bool valid
