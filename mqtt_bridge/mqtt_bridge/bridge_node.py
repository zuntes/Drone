"""
MQTTBridgeNode — pure transport between ROS2 typed messages and MQTT.

Knows about: MQTT, tenant_id, drone_id, envelope format, JSON serialization.
Does NOT know about: PX4, DDS, sensor math.

Outbound (ROS2 → MQTT):
  /mqtt_bridge/in/telemetry  drone_msgs/TelemetryData → drone/{t}/{id}/telemetry  QoS 0
  /mqtt_bridge/in/status     drone_msgs/BridgeStatus  → drone/{t}/{id}/status     QoS 1 retain
  /mqtt_bridge/in/alarm      drone_msgs/AlarmData     → drone/{t}/{id}/alarm      QoS 1 retain

Inbound (MQTT → ROS2):
  drone/{t}/{id}/task_command → validate → /mqtt_bridge/in/task_command  drone_msgs/TaskCommand
  /mqtt_bridge/in/task_status drone_msgs/TaskStatus → drone/{t}/{id}/task_status  QoS 1

Header envelope added to every MQTT message:
  {
    "header": { "tenant_id", "drone_id", "drone_serial", "type", "timestamp" },
    "payload": { ...serialized from drone_msgs... }
  }
"""

import json
import logging
import time
from datetime import datetime, timezone

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

        # MQTT topic paths
        _base = f"drone/{self._tenant_id}/{self._drone_id}"
        self._t_telemetry    = f"{_base}/telemetry"
        self._t_status       = f"{_base}/status"
        self._t_alarm        = f"{_base}/alarm"
        self._t_task_command = f"{_base}/task_command"
        self._t_task_status  = f"{_base}/task_status"

        self._pub_counts = {"telemetry":0,"status":0,"alarm":0,"task_status":0}

        # ── MQTT client ───────────────────────────────────────────────────────
        _lwt = {
            "header":  self._make_header("status"),
            "payload": {
                "alive":       False,
                "session_key": self._session_key,
                "reason":      "unexpected_disconnect",
            },
        }
        self._mqtt = MQTTClient(
            host=p['mqtt_host'],
            port=p['mqtt_port'],
            client_id=f"mqtt_bridge_{self._drone_id}_{int(time.time())}",
            lwt_topic=self._t_status,
            lwt_payload=_lwt,
            username=p['mqtt_username'],
            password=p['mqtt_password'],
            use_tls=p['mqtt_tls'],
            transport=p['mqtt_transport'],
            ws_path=p['mqtt_ws_path'],
        )
        self._mqtt.connect()

        # ── ROS2 subscriptions (outbound: telemetry_node → MQTT) ──────────────
        self.create_subscription(
            TelemetryData, '/mqtt_bridge/in/telemetry',
            self._cb_telemetry, _BEST_EFFORT)
        self.create_subscription(
            BridgeStatus, '/mqtt_bridge/in/status',
            self._cb_status, _RELIABLE)
        self.create_subscription(
            AlarmData, '/mqtt_bridge/in/alarm',
            self._cb_alarm, _RELIABLE)

        # ── ROS2 subscription (inbound: mission_executor → MQTT) ──────────────
        self.create_subscription(
            TaskStatus, '/mqtt_bridge/in/task_status',
            self._cb_task_status, _RELIABLE)

        # ── ROS2 publisher (inbound: MQTT task_command → mission_executor) ────
        self._pub_task_command = self.create_publisher(
            TaskCommand, '/mqtt_bridge/out/task_command', _RELIABLE)

        # ── Subscribe to MQTT task_command topic ──────────────────────────────
        # Done via MQTTClient callback after connection is established
        self._mqtt.subscribe(self._t_task_command, qos=1,
                             callback=self._on_mqtt_task_command)

        # ── Diagnostics timer ─────────────────────────────────────────────────
        self.create_timer(30.0, self._timer_stats)

        self.get_logger().info(
            f"\n{'='*60}\n"
            f"  MQTTBridgeNode READY\n"
            f"  Identity: {self._tenant_id} / {self._drone_id} ({self._drone_serial})\n"
            f"  Session:  {self._session_key}\n"
            f"  Outbound (ROS2 → MQTT):\n"
            f"    TelemetryData → {self._t_telemetry}\n"
            f"    BridgeStatus  → {self._t_status}\n"
            f"    AlarmData     → {self._t_alarm}\n"
            f"    TaskStatus    → {self._t_task_status}\n"
            f"  Inbound (MQTT → ROS2):\n"
            f"    {self._t_task_command} → /mqtt_bridge/out/task_command\n"
            f"{'='*60}"
        )

    # ── Outbound callbacks (ROS2 → MQTT) ─────────────────────────────────────

    def _cb_telemetry(self, msg: TelemetryData) -> None:
        """Serialize TelemetryData → JSON → MQTT. QoS 0: high-rate, drop on disconnect."""
        payload = serializers.telemetry(msg)
        envelope = {"header": self._make_header("telemetry"), "payload": payload}
        if self._mqtt.publish(self._t_telemetry, envelope, qos=0):
            self._pub_counts["telemetry"] += 1

    def _cb_status(self, msg: BridgeStatus) -> None:
        """
        Serialize BridgeStatus → JSON → MQTT.
        Merges PX4-side data (from telemetry_node) with bridge-side data (alive, uptime).
        QoS 1 retained: new MQTT subscribers immediately see bridge state.
        """
        payload = serializers.status(
            msg,
            alive=True,
            bridge_uptime_s=round(time.time() - self._start_time, 1),
            session_key=self._session_key,
        )
        envelope = {"header": self._make_header("status"), "payload": payload}
        if self._mqtt.publish(self._t_status, envelope, qos=1, retain=True):
            self._pub_counts["status"] += 1

    def _cb_alarm(self, msg: AlarmData) -> None:
        """Serialize AlarmData → JSON → MQTT. QoS 1 retained: always visible."""
        payload = serializers.alarm(msg)
        envelope = {"header": self._make_header("alarm"), "payload": payload}
        if self._mqtt.publish(self._t_alarm, envelope, qos=1, retain=True):
            self._pub_counts["alarm"] += 1

    def _cb_task_status(self, msg: TaskStatus) -> None:
        """Forward TaskStatus from mission_executor to MQTT."""
        payload = serializers.task_status(msg)
        envelope = {"header": self._make_header("task_status"), "payload": payload}
        if self._mqtt.publish(self._t_task_status, envelope, qos=1):
            self._pub_counts["task_status"] += 1

    # ── Inbound callback (MQTT → ROS2) ────────────────────────────────────────

    def _on_mqtt_task_command(self, topic: str, raw: str) -> None:
        """
        Receive task_command from MQTT, validate, publish to ROS2.

        Validation:
          1. Valid JSON
          2. Required fields present: header.task_id, header.drone_id, commands[]
          3. drone_id matches this bridge's configured drone_id
          4. commands[] is non-empty
          5. Duplicate task_id check (MQTT QoS-1 can deliver twice)

        On validation failure: publish REJECTED status to MQTT.
        On success: publish RECEIVED status + forward to ROS2.
        """
        # Step 1: Parse JSON
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.error(f"task_command: invalid JSON: {e}")
            self._publish_rejection("unknown", f"invalid_json: {e}")
            return

        header   = data.get("header", {})
        task_id  = header.get("task_id", "")
        tenant_id = header.get("tenant_id", "")
        drone_id = header.get("drone_id", "")
        drone_serial = header.get("drone_serial", "")
        type = header.get("type", "")
        payloads = data.get("payloads", [])
        protocol_version = header.get("protocol_version", "")
        
        # Step 2: Required fields
        if not task_id:
            self._publish_rejection("unknown", "missing header.task_id")
            return
        if not tenant_id:
            self._publish_rejection(task_id, "missing header.tenant_id")
            return
        if not drone_id:
            self._publish_rejection(task_id, "missing header.drone_id")
            return
        if not drone_serial:
            self._publish_rejection(task_id, "missing header.drone_serial")
            return
        if not type:
            self._publish_rejection(task_id, "missing header.type")
            return
        if not protocol_version:
            self._publish_rejection(task_id, "missing header.protocol_version")
            return
        if protocol_version != "1.0":
            self._publish_rejection(task_id, f"unsupported protocol_version='{protocol_version}'")
            return
        if not type.startswith("task_command"): 
            self._publish_rejection(task_id, f"invalid header.type='{type}'")
            return
        if not isinstance(payloads, list) or len(payloads) == 0:
            self._publish_rejection(task_id, "payloads must be a non-empty list")
            return

        # Step 3: Identity check
        if drone_id != self._drone_id:
            logger.warning(
                f"task_command rejected: drone_id='{drone_id}' "
                f"!= configured='{self._drone_id}'"
            )
            # Don't publish to MQTT — this message is for a different drone
            return

        # Step 4: Duplicate check (MQTT QoS-1 re-delivery)
        if task_id == self._last_task_id:
            logger.info(f"task_command: duplicate task_id='{task_id}' — dropped")
            return
        self._last_task_id = task_id

        # Step 5: Build drone_msgs/TaskCommand
        ros_msg = self._build_task_command_msg(data, header, payloads)
        if ros_msg is None:
            self._publish_rejection(task_id, "failed to parse command items")
            return

        # Publish to ROS2 (mission_executor subscribes here)
        self._pub_task_command.publish(ros_msg)
        logger.info(f"task_command '{task_id}' forwarded to /mqtt_bridge/in/task_command "
                    f"({len(payloads)} commands)")

        # Acknowledge to cloud: bridge received and forwarded the command
        self._publish_task_status_update(
            task_id=task_id,
            task_status="RECEIVED",
            current_sequence=0,
            current_command_id="",
            command_status="PENDING",
            total=len(payloads),
            done=0,
        )

    def _build_task_command_msg(self, data: dict, header: dict,
                                payloads: list) -> TaskCommand:
        """Parse raw dict into drone_msgs/TaskCommand. Returns None on failure."""
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

    def _publish_rejection(self, task_id: str, reason: str) -> None:
        logger.warning(f"task_command REJECTED: task_id='{task_id}' reason='{reason}'")
        self._publish_task_status_update(
            task_id=task_id, task_status="REJECTED",
            current_sequence=0, current_command_id="",
            command_status="FAILED", total=0, done=0,
            abort_reason=reason,
        )

    def _publish_task_status_update(self, *, task_id, task_status, current_sequence,
                                    current_command_id, command_status,
                                    total, done, abort_reason="") -> None:
        payloads = {
            "task_id":            task_id,
            "task_status":        task_status,
            "current_sequence":   current_sequence,
            "current_command_id": current_command_id,
            "command_status":     command_status,
            "waypoints_total":    total,
            "waypoints_done":     done,
            "abort_reason":       abort_reason if abort_reason else None,
        }
        envelope = {"header": {"task_id": task_id, **self._make_header("task_status")}, "payloads": payloads}
        self._mqtt.publish(self._t_task_status, envelope, qos=1)

    def _timer_stats(self) -> None:
        self.get_logger().info(
            f"Forwarded — "
            f"telemetry:{self._pub_counts['telemetry']}  "
            f"status:{self._pub_counts['status']}  "
            f"alarm:{self._pub_counts['alarm']}  "
            f"task_status:{self._pub_counts['task_status']}  "
            f"| MQTT: {'connected' if self._mqtt.is_connected else 'DISCONNECTED'}"
        )

    def shutdown(self) -> None:
        self.get_logger().info("Shutting down MQTT bridge...")
        offline = {
            "header": self._make_header("status"),
            "payload": {
                "alive":           False,
                "bridge_uptime_s": round(time.time() - self._start_time, 1),
                "session_key":     self._session_key,
                "reason":          "clean_shutdown",
            },
        }
        self._mqtt.publish(self._t_status, offline, qos=1, retain=True)
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