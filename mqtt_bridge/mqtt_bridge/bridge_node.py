"""
MQTTBridgeNode — pure transport between ROS2 typed messages and MQTT.

Knows about: MQTT, tenant_id, drone_id, envelope format, JSON serialization.
Does NOT know about: PX4, DDS, sensor math.

Outbound (ROS2 → MQTT) — two topics only:
  drone/{t}/{id}/ack         QoS 1 retained  — liveness heartbeat (1 Hz) + LWT
  drone/{t}/{id}/telemetry   QoS 0           — combined: status + alarm + telemetry + task_status

Inbound (MQTT → ROS2):
  drone/{t}/{id}/task_command → validate → /mqtt_bridge/out/task_command  drone_msgs/TaskCommand

Header envelope added to every MQTT message:
  {
    "header": { "tenant_id", "drone_id", "drone_serial", "type", "timestamp" },
    "payload": { ... }
  }
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import rclpy
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy

from drone_msgs.msg import (
    TelemetryData, BridgeStatus, AlarmData,
    TaskCommand, CommandItem, CommandPayload, TaskStatus,
)

from .mqtt_client import MQTTClient
from . import serializers

logger = logging.getLogger(__name__)

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


class MQTTBridgeNode(Node):

    def __init__(self):
        super().__init__('mqtt_bridge_node')
        self._declare_params()
        p = self._load_params()

        self._tenant_id    = p['tenant_id']
        self._drone_id     = p['drone_id']
        self._drone_serial = p['drone_serial']
        self._session_key  = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self._start_time   = time.time()

        # Track last seen task_id to handle MQTT QoS-1 duplicate delivery
        self._last_task_id: str = ""

        # Cached latest ROS2 messages for combined telemetry publish
        self._last_status:      Optional[BridgeStatus] = None
        self._last_alarm:       Optional[AlarmData]     = None
        self._last_task_status: Optional[TaskStatus]    = None

        # MQTT topic paths — two outbound + one inbound
        _base = f"drone/{self._tenant_id}/{self._drone_serial}"
        self._t_ack          = f"{_base}/ack"
        self._t_telemetry    = f"{_base}/telemetry"
        self._t_task_command = f"{_base}/task_command"

        self._pub_counts = {"telemetry": 0, "ack": 0}

        # ── MQTT client ───────────────────────────────────────────────────────
        _lwt = {
            "metadata":  self._make_header("ack"),
            "payload": {
                "alive":          False,
                "session_key":    self._session_key,
                "bridge_uptime_s": 0.0,
            },
        }
        self._mqtt = MQTTClient(
            host=p['mqtt_host'],
            port=p['mqtt_port'],
            client_id=f"mqtt_bridge_{self._drone_id}_{int(time.time())}",
            lwt_topic=self._t_ack,
            lwt_payload=_lwt,
            username=p['mqtt_username'],
            password=p['mqtt_password'],
            use_tls=p['mqtt_tls'],
            transport=p['mqtt_transport'],
            ws_path=p['mqtt_ws_path'],
        )
        self._mqtt.connect()

        # ── ROS2 subscriptions (cached for combined outbound telemetry) ──────
        self.create_subscription(
            TelemetryData, '/mqtt_bridge/in/telemetry',
            self._cb_telemetry, _BEST_EFFORT)
        self.create_subscription(
            BridgeStatus, '/mqtt_bridge/in/status',
            self._cb_status, _RELIABLE)
        self.create_subscription(
            AlarmData, '/mqtt_bridge/in/alarm',
            self._cb_alarm, _RELIABLE)
        self.create_subscription(
            TaskStatus, '/mqtt_bridge/in/task_status',
            self._cb_task_status, _RELIABLE)

        # ── ROS2 publisher (inbound: MQTT task_command → mission_executor) ────
        self._pub_task_command = self.create_publisher(
            TaskCommand, '/mqtt_bridge/out/task_command', _RELIABLE)

        # ── Subscribe to MQTT task_command topic ──────────────────────────────
        self._mqtt.subscribe(self._t_task_command, qos=1,
                             callback=self._on_mqtt_task_command)

        # ── Timers ────────────────────────────────────────────────────────────
        self.create_timer(1.0, self._timer_ack)
        self.create_timer(30.0, self._timer_stats)

        self.get_logger().info(
            f"\n{'='*60}\n"
            f"  MQTTBridgeNode READY\n"
            f"  Identity: {self._tenant_id} / {self._drone_id} ({self._drone_serial})\n"
            f"  Session:  {self._session_key}\n"
            f"  Outbound (ROS2 → MQTT):\n"
            f"    ack        → {self._t_ack}  (QoS 1, retained, 1 Hz)\n"
            f"    telemetry  → {self._t_telemetry}  (QoS 0, 5 Hz combined)\n"
            f"  Inbound (MQTT → ROS2):\n"
            f"    {self._t_task_command} → /mqtt_bridge/out/task_command\n"
            f"{'='*60}"
        )

    # ── Outbound: combined telemetry (ROS2 → MQTT) ──────────────────────────

    def _cb_telemetry(self, msg: TelemetryData) -> None:
        ros_connected = self._last_status is not None
        payload = {
            "status":      serializers.status_block(self._last_status, msg.flight_mode, ros_connected)
                           if self._last_status else None,
            "alarm":       serializers.alarm(self._last_alarm)
                           if self._last_alarm else None,
            "telemetry":   serializers.telemetry(msg),
            "task_status": serializers.task_status(self._last_task_status)
                           if self._last_task_status else None,
        }
        envelope = {"metadata": self._make_header("telemetry"), "payload": payload}
        if self._mqtt.publish(self._t_telemetry, envelope, qos=0):
            self._pub_counts["telemetry"] += 1

    def _cb_status(self, msg: BridgeStatus) -> None:
        self._last_status = msg

    def _cb_alarm(self, msg: AlarmData) -> None:
        self._last_alarm = msg

    def _cb_task_status(self, msg: TaskStatus) -> None:
        self._last_task_status = msg

    # ── Outbound: ack heartbeat ──────────────────────────────────────────────

    def _publish_ack(self, alive: bool) -> None:
        envelope = {
            "metadata": self._make_header("ack"),
            "payload": {
                "alive":          alive,
                "session_key":    self._session_key,
                "bridge_uptime_s": round(time.time() - self._start_time, 1),
            },
        }
        if self._mqtt.publish(self._t_ack, envelope, qos=1, retain=True):
            self._pub_counts["ack"] += 1

    def _timer_ack(self) -> None:
        self._publish_ack(alive=True)

    # ── Inbound callback (MQTT → ROS2) ────────────────────────────────────────

    def _on_mqtt_task_command(self, topic: str, raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.error(f"task_command: invalid JSON: {e}")
            return

        header   = data.get("metadata", {})
        task_id  = header.get("task_id", "")
        tenant_id = header.get("tenant_id", "")
        drone_id = header.get("drone_id", "")
        drone_serial = header.get("drone_serial", "")
        msg_type = header.get("type", "")
        payloads = data.get("payloads", [])
        protocol_version = header.get("protocol_version", "")

        # Validation
        if not task_id:
            logger.warning("task_command rejected: missing header.task_id")
            return
        if not tenant_id:
            logger.warning(f"task_command '{task_id}' rejected: missing header.tenant_id")
            return
        if not drone_id:
            logger.warning(f"task_command '{task_id}' rejected: missing header.drone_id")
            return
        if not drone_serial:
            logger.warning(f"task_command '{task_id}' rejected: missing header.drone_serial")
            return
        if not msg_type:
            logger.warning(f"task_command '{task_id}' rejected: missing header.type")
            return
        if not protocol_version:
            logger.warning(f"task_command '{task_id}' rejected: missing header.protocol_version")
            return
        if protocol_version != "1.0":
            logger.warning(f"task_command '{task_id}' rejected: unsupported protocol_version='{protocol_version}'")
            return
        if not msg_type.startswith("task_command"):
            logger.warning(f"task_command '{task_id}' rejected: invalid header.type='{msg_type}'")
            return
        if not isinstance(payloads, list) or len(payloads) == 0:
            logger.warning(f"task_command '{task_id}' rejected: payloads must be a non-empty list")
            return

        # Identity check
        if drone_id != self._drone_id:
            logger.warning(
                f"task_command rejected: drone_id='{drone_id}' "
                f"!= configured='{self._drone_id}'"
            )
            return

        # Duplicate check (MQTT QoS-1 re-delivery)
        if task_id == self._last_task_id:
            logger.info(f"task_command: duplicate task_id='{task_id}' — dropped")
            return
        self._last_task_id = task_id

        # Build drone_msgs/TaskCommand
        ros_msg = self._build_task_command_msg(data, header, payloads)
        if ros_msg is None:
            logger.warning(f"task_command '{task_id}' rejected: failed to parse command items")
            return

        self._pub_task_command.publish(ros_msg)
        logger.info(f"task_command '{task_id}' forwarded to /mqtt_bridge/out/task_command "
                    f"({len(payloads)} commands)")

    def _build_task_command_msg(self, data: dict, header: dict,
                                payloads: list) -> TaskCommand:
        try:
            msg = TaskCommand()
            msg.task_id          = str(header.get("task_id", ""))
            msg.tenant_id        = str(header.get("tenant_id", ""))
            msg.drone_id         = str(header.get("drone_id", ""))
            msg.timestamp        = str(header.get("timestamp", ""))
            msg.protocol_version = str(header.get("protocol_version", "1.0"))

            for item in payloads:
                ci = CommandItem()
                ci.sequence     = int(item.get("sequence", 0))
                ci.command_id   = str(item.get("command_id", ""))
                ci.command_type = str(item.get("command_type", ""))

                pl = item.get("payload", {})
                cp = CommandPayload()

                if pl.get("latitude") is not None:
                    cp.latitude     = float(pl["latitude"])
                    cp.longitude    = float(pl["longitude"])
                    cp.has_position = True

                if pl.get("altitude_m") is not None:
                    cp.altitude_m   = float(pl["altitude_m"])
                    cp.altitude_ref = str(pl.get("altitude_ref", "home"))
                    cp.has_altitude = True

                if pl.get("speed_ms") is not None:
                    cp.speed_ms     = float(pl["speed_ms"])
                    cp.has_speed    = True

                if pl.get("arrival_radius_m") is not None:
                    cp.arrival_radius_m   = float(pl["arrival_radius_m"])
                    cp.has_arrival_radius = True

                ci.payload = cp
                msg.payloads.append(ci)

            return msg
        except (KeyError, TypeError, ValueError) as e:
            logger.error(f"Failed to parse task_command: {e}", exc_info=True)
            return None

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _make_header(self, msg_type: str) -> dict:
        now = datetime.now(timezone.utc)
        return {
            "tenant_id":    self._tenant_id,
            "drone_id":     self._drone_id,
            "drone_serial": self._drone_serial,
            "type":         msg_type,
            "timestamp": {
                "date": now.strftime("%Y-%m-%d"),
                "time": now.strftime("%H:%M:%S.") + f"{now.microsecond//1000:03d}",
            },
        }

    def _timer_stats(self) -> None:
        self.get_logger().info(
            f"Forwarded — "
            f"telemetry:{self._pub_counts['telemetry']}  "
            f"ack:{self._pub_counts['ack']}  "
            f"| MQTT: {'connected' if self._mqtt.is_connected else 'DISCONNECTED'}"
        )

    def shutdown(self) -> None:
        self.get_logger().info("Shutting down MQTT bridge...")
        self._publish_ack(alive=False)
        time.sleep(0.2)
        self._mqtt.disconnect()
        self.get_logger().info("MQTT bridge stopped.")

    def _declare_params(self):
        self.declare_parameters(namespace='', parameters=[
            ('tenant_id',       'default_tenant'),
            ('drone_id',        'drone_01'),
            ('drone_serial',    'SN000000'),
            ('mqtt_host',       'localhost'),
            ('mqtt_port',       1883),
            ('mqtt_username',   ''),
            ('mqtt_password',   ''),
            ('mqtt_tls',        False),
            ('mqtt_transport',  'tcp'),
            ('mqtt_ws_path',    '/mqtt'),
        ])

    def _load_params(self) -> dict:
        return {k: self.get_parameter(k).value for k in [
            'tenant_id','drone_id','drone_serial',
            'mqtt_host','mqtt_port','mqtt_username','mqtt_password','mqtt_tls',
            'mqtt_transport','mqtt_ws_path',
        ]}
