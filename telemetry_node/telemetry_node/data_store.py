"""
DroneDataStore — shared in-process state for the telemetry node.

All PX4 subscriber callbacks write the latest message here.
All three payload builders (telemetry, status, alarm) read from here.

Thread safety: Python's GIL makes single-field object assignments atomic.
Subscribers write their own field only. Builders only read. No races.

Verified topic → field mapping (from `ros2 topic list` on hardware):
  /fmu/out/vehicle_global_position    → vehicle_global_position
  /fmu/out/vehicle_local_position_v1  → vehicle_local_position
  /fmu/out/home_position_v1           → home_position
  /fmu/out/vehicle_attitude           → vehicle_attitude
  /fmu/out/vehicle_gps_position       → sensor_gps        (SensorGps type)
  /fmu/out/estimator_status_flags     → estimator_status_flags
  /fmu/out/battery_status_v1          → battery_status
  /fmu/out/vehicle_status_v2          → vehicle_status
  /fmu/out/vehicle_land_detected      → vehicle_land_detected
  /fmu/out/vehicle_control_mode       → vehicle_control_mode  (optional)
  /fmu/out/failsafe_flags             → failsafe_flags         (optional)
  /fmu/out/timesync_status            → timesync_status
"""

import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DroneDataStore:
    # ── PX4 messages (None until first message arrives) ───────────────────────
    vehicle_global_position:   Optional[object] = None
    vehicle_local_position:    Optional[object] = None
    home_position:             Optional[object] = None
    vehicle_attitude:          Optional[object] = None
    sensor_gps:                Optional[object] = None
    estimator_status_flags:    Optional[object] = None
    battery_status:            Optional[object] = None
    vehicle_status:            Optional[object] = None
    vehicle_land_detected:     Optional[object] = None
    vehicle_control_mode:      Optional[object] = None   # optional
    failsafe_flags:            Optional[object] = None   # optional
    timesync_status:           Optional[object] = None

    # ── PX4 connection health ─────────────────────────────────────────────────
    # Updated every time ANY PX4 message arrives.
    # Status builder uses this to report px4_connected = (age < 2s).
    last_px4_msg_time: float = 0.0

    # ── Bridge-side flight timer ──────────────────────────────────────────────
    # PX4 has no published flight-time topic.
    # Computed by watching vehicle_land_detected.landed transitions.
    takeoff_time:    Optional[float] = None   # wall-clock time of last takeoff
    total_flight_s:  float = 0.0              # accumulated past flights this session
    prev_landed:     bool = True              # last known landed state

    # ── Node startup time ─────────────────────────────────────────────────────
    node_start_time: float = field(default_factory=time.time)

    # ── Public helpers ────────────────────────────────────────────────────────

    def mark_px4_msg(self) -> None:
        """Call on every PX4 callback to track liveness."""
        self.last_px4_msg_time = time.time()

    def px4_connected(self, timeout_s: float = 3.0) -> bool:
        """True if a PX4 message arrived within the last timeout_s seconds."""
        return (self.last_px4_msg_time > 0
                and (time.time() - self.last_px4_msg_time) < timeout_s)

    def current_flight_s(self) -> float:
        """Seconds airborne since last takeoff. 0.0 if on ground."""
        if self.takeoff_time is None:
            return 0.0
        return time.time() - self.takeoff_time

    def on_land_detected(self, landed: bool) -> None:
        """
        Call whenever vehicle_land_detected.landed changes.
        Maintains takeoff_time and total_flight_s.

        Transition logic:
          prev=True, new=False → TAKEOFF: start timer
          prev=False, new=True → LANDING: accumulate elapsed time
        """
        if self.prev_landed and not landed:
            # Ground → Air (TAKEOFF)
            self.takeoff_time = time.time()
        elif not self.prev_landed and landed:
            # Air → Ground (LANDING)
            if self.takeoff_time is not None:
                self.total_flight_s += time.time() - self.takeoff_time
            self.takeoff_time = None
        self.prev_landed = landed
