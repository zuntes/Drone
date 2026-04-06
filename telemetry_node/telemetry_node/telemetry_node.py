"""
TelemetryNode — subscribes to PX4 DDS topics, publishes drone_msgs to ROS2.

Publishers (drone_msgs types):
  /mqtt_bridge/in/telemetry  drone_msgs/TelemetryData  @ 5 Hz
  /mqtt_bridge/in/status     drone_msgs/BridgeStatus   @ 1 Hz
  /mqtt_bridge/in/alarm      drone_msgs/AlarmData      on-change + periodic

PX4 topic names verified against `ros2 topic list` on hardware.
"""

import logging
import time

import rclpy
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy

from px4_msgs.msg import (
    BatteryStatus, EstimatorStatusFlags, FailsafeFlags,
    HomePosition, OnboardComputerStatus, SensorGps,
    TimesyncStatus, VehicleAttitude, VehicleControlMode,
    VehicleGlobalPosition, VehicleLandDetected,
    VehicleLocalPosition, VehicleStatus,
)
from drone_msgs.msg import TelemetryData, BridgeStatus, AlarmData

from .data_store import DroneDataStore
from .publishers import telemetry_pub, status_pub, alarm_pub
from .publishers.alarm_pub import AlarmState

logger = logging.getLogger(__name__)

# ── QoS ──────────────────────────────────────────────────────────────────────
# PX4 subscriptions: VOLATILE — compatible with both VOLATILE and TRANSIENT_LOCAL
# publishers. TRANSIENT_LOCAL subscriber + VOLATILE publisher = silent zero messages.
_PX4_QOS = QoSProfile(
    reliability=ReliabilityPolicy.BEST_EFFORT,
    durability=DurabilityPolicy.VOLATILE,
    history=HistoryPolicy.KEEP_LAST,
    depth=10,
)
_PX4_PUB_QOS = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    durability=DurabilityPolicy.VOLATILE,
    history=HistoryPolicy.KEEP_LAST,
    depth=1,
)
# Internal ROS2 publishers → mqtt_bridge_node
_BEST_EFFORT = QoSProfile(
    reliability=ReliabilityPolicy.BEST_EFFORT,
    durability=DurabilityPolicy.VOLATILE,
    history=HistoryPolicy.KEEP_LAST,
    depth=10,
)
_RELIABLE = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    durability=DurabilityPolicy.VOLATILE,
    history=HistoryPolicy.KEEP_LAST,
    depth=10,
)


class TelemetryNode(Node):

    def __init__(self):
        super().__init__('telemetry_node')
        self._declare_params()
        p = self._load_params()

        self._store       = DroneDataStore()
        self._alarm_state = AlarmState()
        self._alarm_period_s      = 1.0 / max(p['alarm_rate_hz'], 0.1)
        self._last_alarm_pub_time = 0.0
        self._pub_counts = {"telemetry": 0, "status": 0, "alarm": 0}

        # ── ROS2 publishers (typed drone_msgs) ────────────────────────────────
        self._pub_telemetry = self.create_publisher(
            TelemetryData, '/mqtt_bridge/in/telemetry', _BEST_EFFORT)
        self._pub_status = self.create_publisher(
            BridgeStatus,  '/mqtt_bridge/in/status',    _RELIABLE)
        self._pub_alarm = self.create_publisher(
            AlarmData,     '/mqtt_bridge/in/alarm',     _RELIABLE)

        # ── PX4 companion heartbeat publisher ─────────────────────────────────
        self._pub_computer = self.create_publisher(
            OnboardComputerStatus, '/fmu/in/onboard_computer_status', _PX4_PUB_QOS)

        # ── PX4 subscriptions ─────────────────────────────────────────────────
        self._setup_px4_subscriptions()

        # ── Timers ────────────────────────────────────────────────────────────
        tel_period = 1.0 / max(p['telemetry_rate_hz'], 0.1)
        sta_period = 1.0 / max(p['status_rate_hz'],    0.1)
        self.create_timer(tel_period, self._timer_telemetry)
        self.create_timer(sta_period, self._timer_status)
        self.create_timer(sta_period, self._timer_alarm)
        self.create_timer(30.0,       self._timer_stats)

        self.get_logger().info(
            f"\n{'='*55}\n"
            f"  TelemetryNode READY\n"
            f"  Publishing drone_msgs to:\n"
            f"    /mqtt_bridge/in/telemetry  TelemetryData @ {p['telemetry_rate_hz']} Hz\n"
            f"    /mqtt_bridge/in/status     BridgeStatus  @ {p['status_rate_hz']} Hz\n"
            f"    /mqtt_bridge/in/alarm      AlarmData     on-change + {p['alarm_rate_hz']} Hz\n"
            f"{'='*55}"
        )

    def _declare_params(self):
        self.declare_parameters(namespace='', parameters=[
            ('telemetry_rate_hz', 5.0),
            ('status_rate_hz',    1.0),
            ('alarm_rate_hz',     0.2),
        ])

    def _load_params(self) -> dict:
        return {k: self.get_parameter(k).value for k in
                ['telemetry_rate_hz', 'status_rate_hz', 'alarm_rate_hz']}

    def _setup_px4_subscriptions(self):
        q = _PX4_QOS

        def store(field):
            """Store msg + update PX4 liveness timestamp."""
            def cb(m):
                setattr(self._store, field, m)
                self._store.mark_px4_msg()
            return cb

        # ── Required ─────────────────────────────────────────────────────────
        self.create_subscription(VehicleGlobalPosition,
            '/fmu/out/vehicle_global_position',
            store('vehicle_global_position'), q)
        self.create_subscription(VehicleLocalPosition,
            '/fmu/out/vehicle_local_position_v1',
            store('vehicle_local_position'), q)
        self.create_subscription(HomePosition,
            '/fmu/out/home_position_v1',
            store('home_position'), q)
        self.create_subscription(VehicleAttitude,
            '/fmu/out/vehicle_attitude',
            store('vehicle_attitude'), q)
        self.create_subscription(SensorGps,
            '/fmu/out/vehicle_gps_position',
            store('sensor_gps'), q)
        self.create_subscription(EstimatorStatusFlags,
            '/fmu/out/estimator_status_flags',
            store('estimator_status_flags'), q)
        self.create_subscription(BatteryStatus,
            '/fmu/out/battery_status_v1',
            store('battery_status'), q)
        self.create_subscription(VehicleStatus,
            '/fmu/out/vehicle_status_v2',
            store('vehicle_status'), q)
        self.create_subscription(VehicleLandDetected,
            '/fmu/out/vehicle_land_detected',
            self._cb_land_detected, q)
        self.create_subscription(TimesyncStatus,
            '/fmu/out/timesync_status',
            store('timesync_status'), q)

        # ── Optional ──────────────────────────────────────────────────────────
        self.create_subscription(VehicleControlMode,
            '/fmu/out/vehicle_control_mode',
            store('vehicle_control_mode'), q)
        self.create_subscription(FailsafeFlags,
            '/fmu/out/failsafe_flags',
            store('failsafe_flags'), q)

        self.get_logger().info(
            "Subscribed: 10 required + 2 optional PX4 topics")

    def _cb_land_detected(self, msg: VehicleLandDetected) -> None:
        self._store.on_land_detected(bool(msg.landed))
        self._store.vehicle_land_detected = msg
        self._store.mark_px4_msg()

    # ── Timers ────────────────────────────────────────────────────────────────

    def _timer_telemetry(self) -> None:
        msg = telemetry_pub.build(self._store, self.get_clock())
        self._pub_telemetry.publish(msg)
        self._pub_counts["telemetry"] += 1

    def _timer_status(self) -> None:
        msg = status_pub.build(self._store, self.get_clock())
        self._pub_status.publish(msg)
        self._pub_counts["status"] += 1
        self._send_px4_heartbeat()

    def _timer_alarm(self) -> None:
        changed  = self._alarm_state.has_changed(self._store)
        periodic = (time.time() - self._last_alarm_pub_time) >= self._alarm_period_s
        if not (changed or periodic):
            return
        msg = alarm_pub.build(self._store, self.get_clock())
        self._pub_alarm.publish(msg)
        self._pub_counts["alarm"] += 1
        self._last_alarm_pub_time = time.time()

    def _timer_stats(self) -> None:
        self.get_logger().info(
            f"Published — "
            f"telemetry:{self._pub_counts['telemetry']}  "
            f"status:{self._pub_counts['status']}  "
            f"alarm:{self._pub_counts['alarm']}  "
            f"| PX4: {'connected' if self._store.px4_connected() else 'waiting...'}"
        )

    def _send_px4_heartbeat(self) -> None:
        try:
            msg = OnboardComputerStatus()
            msg.timestamp = int(time.time() * 1e6)
            msg.type = 0
            try:
                import psutil
                for i, pct in enumerate(psutil.cpu_percent(percpu=True)[:8]):
                    msg.cpu_cores[i] = int(pct)
                msg.ram_usage = int(psutil.virtual_memory().used // (1024*1024))
            except ImportError:
                pass
            self._pub_computer.publish(msg)
        except Exception as e:
            logger.error(f"PX4 heartbeat error: {e}", exc_info=True)
