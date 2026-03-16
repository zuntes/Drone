# GPS signal health — jamming, spoofing, fix loss
uint8 JAMMING_UNKNOWN=0
uint8 JAMMING_OK=1
uint8 JAMMING_MITIGATED=2
uint8 JAMMING_DETECTED=3

uint8 SPOOFING_UNKNOWN=0
uint8 SPOOFING_NONE=1
uint8 SPOOFING_INDICATED=2
uint8 SPOOFING_MULTIPLE=3

bool has_fix
uint8 fix_type
uint8 satellites_used
int32 noise_per_ms
int32 jamming_indicator        # 0-255; <40=ok, <80=warning, >=80=critical
string jamming_health          # "ok","warning","critical"
uint8 jamming_state
string jamming_label
uint8 spoofing_state
string spoofing_label
bool dead_reckoning            # true = GPS lost, running on IMU only
bool ekf_gnss_fused            # true = EKF is actively using GNSS
bool valid
