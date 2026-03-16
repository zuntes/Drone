# Battery status from /fmu/out/battery_status_v1
uint8 WARNING_NONE=0
uint8 WARNING_LOW=1
uint8 WARNING_CRITICAL=2
uint8 WARNING_EMERGENCY=3
uint8 WARNING_FAILED=4

bool connected
float32 voltage_v
float32 current_a
float32 remaining_pct    # 0.0 - 100.0
float32 temperature_c    # NaN if no temperature sensor
float32 time_remaining_min  # NaN if unknown
uint8 warning            # 0=none, 1=low, 2=critical, 3=emergency, 4=failed
bool valid               # false = battery topic not yet received
