"""
MQTTBridgeNode — pure transport between ROS2 typed messages and MQTT.

Knows about: MQTT, tenant_id, drone_id, envelope format, JSON serialization.
Does NOT know about: PX4, DDS, sensor math.

Outbound (ROS2 → MQTT, timer-driven, retained):
  telemetry_node BridgeStatus  → cache → drone/{t}/{sn}/status       1 Hz  QoS 1 retain
  telemetry_node AlarmData     → cache → drone/{t}/{sn}/alarm        1 Hz  QoS 1 retain
  telemetry_node TelemetryData → cache → drone/{t}/{sn}/telemetry    5 Hz  QoS 0 retain
  mission_exec   TaskStatus    → cache → drone/{t}/{sn}/task_status  1 Hz  QoS 1 retain

Inbound (MQTT → ROS2):
  drone/{t}/{sn}/task_command → validate → /mqtt_bridge/out/task_command  drone_msgs/TaskCommand

Publish model: every outbound topic is published on a fixed timer regardless of
whether ROS data has arrived. Sub-fields are null until the first matching
ROS message is cached. All outbound topics are retained so EMQX shows latest
state to new subscribers.

Envelope added to every outbound MQTT message:
  {
    "event_id":  "<uuid4>",
    "timestamp": "YYYY-MM-DDTHH:MM:SS.mmmZ",
    "metadata":  { "tenant_id", "drone_id", "drone_serial", "type",
                   "task_id" (task_status only) },
    "payload":   { ... }
  }
"""

import json
import logging
import time
import uuid
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

# Task statuses that mark end of an active task (clears _in_task).
_TERMINAL_TASK_STATUSES = {"COMPLETED", "FAILED", "CANCELED"}


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

        # ── Cached latest ROS2 messages (None until first arrival) ────────────
        self._last_telemetry:   Optional[TelemetryData] = None
        self._last_status:      Optional[BridgeStatus]  = None
        self._last_alarm:       Optional[AlarmData]     = None
        self._last_task_status: Optional[TaskStatus]    = None

        # True once any TelemetryData has arrived (drives status.ros_connected).
        self._telemetry_received: bool = False

        # Bridge-side task activity flag. True while a task is in flight.
        self._in_task: bool = False

        # MQTT topic paths
        _base = f"drone/{self._tenant_id}/{self._drone_serial}"
        self._t_telemetry    = f"{_base}/telemetry"
        self._t_status       = f"{_base}/status"
        self._t_alarm        = f"{_base}/alarm"
        self._t_task_command = f"{_base}/task_command"
        self._t_task_status  = f"{_base}/task_status"

        self._pub_counts = {"telemetry": 0, "status": 0, "alarm": 0, "task_status": 0}

        # ── MQTT client ───────────────────────────────────────────────────────
        # LWT is static (set once at connect). event_id/timestamp baked in here.
        _lwt = self._make_envelope("status", self._build_status_payload(alive=False))
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

        # ── ROS2 subscriptions (cache-only; publish happens on timers) ────────
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

        # ── Publish timers (always fire; payload carries nulls if no data) ────
        self.create_timer(0.2, self._timer_publish_telemetry)    # 5 Hz
        self.create_timer(1.0, self._timer_publish_status)       # 1 Hz
        self.create_timer(1.0, self._timer_publish_alarm)        # 1 Hz
        self.create_timer(1.0, self._timer_publish_task_status)  # 1 Hz

        # ── Diagnostics timer ─────────────────────────────────────────────────
        self.create_timer(30.0, self._timer_stats)

        self.get_logger().info(
            f"\n{'='*60}\n"
            f"  MQTTBridgeNode READY\n"
            f"  Identity: {self._tenant_id} / {self._drone_id} ({self._drone_serial})\n"
            f"  Session:  {self._session_key}\n"
            f"  Outbound (timer-driven, all retained):\n"
            f"    telemetry   → {self._t_telemetry}   (5 Hz)\n"
            f"    status      → {self._t_status}      (1 Hz)\n"
            f"    alarm       → {self._t_alarm}       (1 Hz)\n"
            f"    task_status → {self._t_task_status} (1 Hz)\n"
            f"  Inbound (MQTT → ROS2):\n"
            f"    {self._t_task_command} → /mqtt_bridge/out/task_command\n"
            f"{'='*60}"
        )

    # ── ROS2 subscription callbacks (cache only; no MQTT publish) ────────────

    def _cb_telemetry(self, msg: TelemetryData) -> None:
        self._last_telemetry = msg
        self._telemetry_received = True

    def _cb_status(self, msg: BridgeStatus) -> None:
        self._last_status = msg

    def _cb_alarm(self, msg: AlarmData) -> None:
        self._last_alarm = msg

    def _cb_task_status(self, msg: TaskStatus) -> None:
        self._last_task_status = msg
        # Clear the in-task flag when task reaches a terminal state.
        if (msg.task_status or "").upper() in _TERMINAL_TASK_STATUSES:
            self._in_task = False

    # ── Periodic publish timers ──────────────────────────────────────────────

    def _timer_publish_telemetry(self) -> None:
        """5 Hz — publish cached TelemetryData (or null payload if none yet)."""
        if self._last_telemetry is not None:
            payload = serializers.telemetry(self._last_telemetry)
        else:
            payload = None
        envelope = self._make_envelope("telemetry", payload)
        if self._mqtt.publish(self._t_telemetry, envelope, qos=0, retain=True):
            self._pub_counts["telemetry"] += 1

    def _timer_publish_status(self) -> None:
        """1 Hz — always publish status with latest cached data (null fields if no data)."""
        payload = self._build_status_payload(alive=True)
        envelope = self._make_envelope("status", payload)
        if self._mqtt.publish(self._t_status, envelope, qos=1, retain=True):
            self._pub_counts["status"] += 1

    def _timer_publish_alarm(self) -> None:
        """1 Hz — publish cached AlarmData (or null payload if none yet)."""
        if self._last_alarm is not None:
            payload = serializers.alarm(self._last_alarm)
        else:
            payload = None
        envelope = self._make_envelope("alarm", payload)
        if self._mqtt.publish(self._t_alarm, envelope, qos=1, retain=True):
            self._pub_counts["alarm"] += 1

    def _timer_publish_task_status(self) -> None:
        """1 Hz — publish cached TaskStatus (null fields with in_task=False if no task).
        task_id lives in metadata (not in payload) per protocol."""
        payload = serializers.task_status(self._last_task_status, in_task=self._in_task)
        task_id = (self._last_task_status.task_id
                   if self._last_task_status is not None else "")
        envelope = self._make_envelope("task_status", payload, task_id=task_id)
        if self._mqtt.publish(self._t_task_status, envelope, qos=1, retain=True):
            self._pub_counts["task_status"] += 1

    # ── Status payload builder (shared by timer, LWT, clean shutdown) ────────

    def _build_status_payload(self, *, alive: bool) -> dict:
        flight_mode = self._last_telemetry.flight_mode if self._last_telemetry else None
        return serializers.status(
            self._last_status,
            alive=alive,
            bridge_uptime_s=round(time.time() - self._start_time, 1),
            session_key=self._session_key,
            ros_connected=self._telemetry_received,
            in_task=self._in_task,
            flight_mode=flight_mode,
        )

    # ── Inbound callback (MQTT → ROS2) ────────────────────────────────────────

    def _on_mqtt_task_command(self, topic: str, raw: str) -> None:
        """
        Receive task_command from MQTT, validate, publish to ROS2.

        Envelope (new format):
          {
            "event_id": "...",
            "timestamp": <epoch_ms>,
            "metadata": {
              "task_id", "tenant_id", "drone_id", "drone_serial", "type"
            },
            "payload": [ { sequence, command_id, command_type,
                           payload: { latitude, longitude, altitude, speed } }, ... ]
          }

        Validation:
          1. Valid JSON
          2. metadata.task_id / tenant_id / drone_serial / type present
          3. tenant_id AND drone_serial must match this bridge's config
             (mismatch → FAILED task_status with reason)
          4. payload is a non-empty list
          5. Duplicate task_id drop (MQTT QoS-1 re-delivery)
        """
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.error(f"task_command: invalid JSON: {e}")
            self._publish_rejection("unknown", f"invalid_json: {e}")
            return

        event_id     = data.get("event_id", "")
        timestamp    = data.get("timestamp", "")
        metadata     = data.get("metadata", {}) or {}
        task_id      = metadata.get("task_id", "")
        tenant_id    = metadata.get("tenant_id", "")
        drone_id     = metadata.get("drone_id", "")
        drone_serial = metadata.get("drone_serial", "")
        msg_type     = metadata.get("type", "")
        commands     = data.get("payload", [])

        if not task_id:
            self._publish_rejection("unknown", "missing metadata.task_id")
            return
        if not tenant_id:
            self._publish_rejection(task_id, "missing metadata.tenant_id")
            return
        if not drone_serial:
            self._publish_rejection(task_id, "missing metadata.drone_serial")
            return
        if not msg_type:
            self._publish_rejection(task_id, "missing metadata.type")
            return
        if not isinstance(commands, list) or len(commands) == 0:
            self._publish_rejection(task_id, "payload must be a non-empty list of commands")
            return

        # Identity check: tenant_id AND drone_serial must match configured
        if tenant_id != self._tenant_id:
            reason = (f"tenant_id mismatch: got='{tenant_id}' "
                      f"expected='{self._tenant_id}'")
            logger.warning(f"task_command {task_id} rejected: {reason}")
            self._publish_rejection(task_id, reason)
            return
        if drone_serial != self._drone_serial:
            reason = (f"drone_serial mismatch: got='{drone_serial}' "
                      f"expected='{self._drone_serial}'")
            logger.warning(f"task_command {task_id} rejected: {reason}")
            self._publish_rejection(task_id, reason)
            return

        # Duplicate check (MQTT QoS-1 re-delivery)
        if task_id == self._last_task_id:
            logger.info(f"task_command: duplicate task_id='{task_id}' — dropped")
            return
        self._last_task_id = task_id

        # Build drone_msgs/TaskCommand
        ros_msg = self._build_task_command_msg(
            task_id, tenant_id, drone_id, timestamp, commands)
        if ros_msg is None:
            self._publish_rejection(task_id, "failed to parse command items")
            return

        self._pub_task_command.publish(ros_msg)
        self._in_task = True
        logger.info(f"task_command '{task_id}' (event_id={event_id}) forwarded to "
                    f"/mqtt_bridge/out/task_command ({len(commands)} commands)")

        # Acknowledge to cloud: bridge received and forwarded the command.
        # Seeds _last_task_status so the next task_status timer publishes RECEIVED.
        self._publish_task_status_update(
            task_id=task_id,
            task_status="RECEIVED",
            current_sequence=0,
            current_command_id="",
            command_status="PENDING",
            total=len(commands),
            done=0,
        )

    def _build_task_command_msg(self, task_id, tenant_id, drone_id,
                                timestamp, commands: list) -> TaskCommand:
        """Parse metadata + commands into drone_msgs/TaskCommand. Returns None on failure."""
        try:
            msg = TaskCommand()
            msg.task_id          = str(task_id)
            msg.tenant_id        = str(tenant_id)
            msg.drone_id         = str(drone_id)
            msg.timestamp        = str(timestamp) if timestamp else ""
            msg.protocol_version = "1.0"

            for item in commands:
                ci = CommandItem()
                ci.sequence     = int(item.get("sequence", 0))
                ci.command_id   = str(item.get("command_id", ""))
                ci.command_type = str(item.get("command_type", ""))

                pl = item.get("payload", {}) or {}
                cp = CommandPayload()

                lat = pl.get("latitude")
                lon = pl.get("longitude")
                if lat is not None and lon is not None:
                    cp.latitude     = float(lat)
                    cp.longitude    = float(lon)
                    cp.has_position = True

                alt = pl.get("altitude")
                if alt is not None:
                    cp.altitude_m   = float(alt)
                    cp.altitude_ref = "home"
                    cp.has_altitude = True

                spd = pl.get("speed")
                if spd is not None:
                    cp.speed_ms  = float(spd)
                    cp.has_speed = True

                ci.payload = cp
                msg.payloads.append(ci)

            return msg
        except (KeyError, TypeError, ValueError) as e:
            logger.error(f"Failed to parse task_command: {e}", exc_info=True)
            return None

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _make_metadata(self, msg_type: str, task_id: Optional[str] = None) -> dict:
        md = {
            "tenant_id":    self._tenant_id,
            "drone_id":     self._drone_id,
            "drone_serial": self._drone_serial,
            "type":         msg_type,
        }
        if task_id is not None:
            md["task_id"] = task_id
        return md

    def _make_envelope(self, msg_type: str, payload, task_id: Optional[str] = None) -> dict:
        now = datetime.now(timezone.utc)
        return {
            "event_id":  str(uuid.uuid4()),
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond//1000:03d}Z",
            "metadata":  self._make_metadata(msg_type, task_id=task_id),
            "payload":   payload,
        }

    def _publish_rejection(self, task_id: str, reason: str) -> None:
        """Emit a FAILED task_status with reason. Used when the bridge
        refuses a task_command (bad envelope, identity mismatch, etc.)."""
        logger.warning(f"task_command REJECTED: task_id='{task_id}' reason='{reason}'")
        self._in_task = False
        self._publish_task_status_update(
            task_id=task_id, task_status="FAILED",
            current_sequence=0, current_command_id="",
            command_status="FAILED", total=0, done=0,
            abort_reason=reason,
        )

    def _publish_task_status_update(self, *, task_id, task_status, current_sequence,
                                    current_command_id, command_status,
                                    total, done, abort_reason="") -> None:
        """
        Synthesize a TaskStatus ROS message into the cache so the next timer tick
        publishes it. Avoids direct out-of-band MQTT publishes on the task_status
        topic (keeps a single publishing path: the timer).
        """
        synthetic = TaskStatus()
        synthetic.task_id            = str(task_id)
        synthetic.task_status        = str(task_status)
        synthetic.current_sequence   = int(current_sequence)
        synthetic.current_command_id = str(current_command_id)
        synthetic.command_status     = str(command_status)
        synthetic.waypoints_total    = int(total)
        synthetic.waypoints_done     = int(done)
        synthetic.abort_reason       = str(abort_reason) if abort_reason else ""
        self._last_task_status = synthetic
        if task_status.upper() in _TERMINAL_TASK_STATUSES:
            self._in_task = False

    def _timer_stats(self) -> None:
        self.get_logger().info(
            f"Pub — "
            f"telemetry:{self._pub_counts['telemetry']}  "
            f"status:{self._pub_counts['status']}  "
            f"alarm:{self._pub_counts['alarm']}  "
            f"task_status:{self._pub_counts['task_status']}  "
            f"| MQTT: {'connected' if self._mqtt.is_connected else 'DISCONNECTED'}"
        )

    def shutdown(self) -> None:
        self.get_logger().info("Shutting down MQTT bridge...")
        offline = self._make_envelope("status", self._build_status_payload(alive=False))
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
