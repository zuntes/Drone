"""
MQTT Client — pure transport.

Responsibilities:
  - Connection lifecycle with auto-reconnect
  - publish(): dict → JSON → MQTT
  - subscribe(): register callback for inbound topics
  - LWT (Last Will Testament) provided by caller
  - Thread-safe: paho network thread + ROS2 callback thread

Design: topic-agnostic. No drone_id, tenant_id, or message structure.
All naming and semantics live in bridge_node.py.
"""

import json
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


class MQTTClient:

    def __init__(
        self,
        host:               str,
        port:               int,
        client_id:          str,
        lwt_topic:          Optional[str]  = None,
        lwt_payload:        Optional[Dict] = None,
        username:           str  = "",
        password:           str  = "",
        use_tls:            bool = False,
        keepalive:          int  = 60,
        reconnect_delay_s:  float = 5.0,
    ):
        self._host             = host
        self._port             = port
        self._username         = username
        self._password         = password
        self._use_tls          = use_tls
        self._keepalive        = keepalive
        self._reconnect_delay  = reconnect_delay_s
        self._lwt_topic        = lwt_topic
        self._lwt_payload      = lwt_payload

        self._connected   = False
        self._should_run  = False
        self._lock        = threading.Lock()
        self._reconnect_thread: Optional[threading.Thread] = None

        # Pending subscriptions: registered before connection is established
        # or after reconnect. (topic, qos, callback) triples.
        self._subscriptions: List[Tuple[str, int, Callable]] = []

        self._client = mqtt.Client(
            client_id=client_id,
            protocol=mqtt.MQTTv311,
            clean_session=True,
        )
        self._configure_client()

    # ── Configuration ─────────────────────────────────────────────────────────

    def _configure_client(self) -> None:
        if self._username:
            self._client.username_pw_set(self._username, self._password)
        if self._use_tls:
            self._client.tls_set()

        if self._lwt_topic and self._lwt_payload:
            self._client.will_set(
                topic=self._lwt_topic,
                payload=json.dumps(self._lwt_payload),
                qos=1,
                retain=True,
            )

        self._client.on_connect    = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message    = self._on_message
        self._client.on_publish    = self._on_publish

    # ── Public API ────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """Start connection. Non-blocking."""
        self._should_run = True
        self._start_connection()

    def disconnect(self) -> None:
        """Graceful shutdown."""
        self._should_run = False
        if self._reconnect_thread and self._reconnect_thread.is_alive():
            self._reconnect_thread.join(timeout=2.0)
        self._client.loop_stop()
        self._client.disconnect()
        logger.info("MQTT client disconnected cleanly.")

    def subscribe(self, topic: str, qos: int, callback: Callable) -> None:
        """
        Register a topic subscription and callback.

        If already connected, subscribes immediately.
        If not yet connected, queues for subscription on next connect.
        Re-subscribed automatically after reconnect.

        Args:
            topic:    MQTT topic string
            qos:      0, 1, or 2
            callback: fn(topic: str, payload: str) called on message arrival
        """
        self._subscriptions.append((topic, qos, callback))
        if self._connected:
            self._client.subscribe(topic, qos)
            logger.info(f"MQTT subscribed: {topic} (QoS {qos})")

    def publish(
        self,
        topic:   str,
        payload: Dict[str, Any],
        qos:     int  = 0,
        retain:  bool = False,
    ) -> bool:
        """Publish dict as JSON. Returns True on success."""
        if not self._connected:
            return False
        try:
            result = self._client.publish(
                topic,
                json.dumps(payload, default=_json_default),
                qos=qos,
                retain=retain,
            )
            return result.rc == mqtt.MQTT_ERR_SUCCESS
        except Exception as e:
            logger.error(f"MQTT publish error on {topic}: {e}")
            return False

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ── Connection management ─────────────────────────────────────────────────

    def _start_connection(self) -> None:
        try:
            self._client.connect(self._host, self._port, self._keepalive)
            self._client.loop_start()
            logger.info(f"MQTT connecting to {self._host}:{self._port}...")
        except (ConnectionRefusedError, OSError) as e:
            logger.warning(f"MQTT initial connect failed ({e}). Retrying...")
            self._client.loop_start()
            self._start_reconnect_loop()

    def _start_reconnect_loop(self) -> None:
        if self._reconnect_thread and self._reconnect_thread.is_alive():
            return
        self._reconnect_thread = threading.Thread(
            target=self._reconnect_loop, name="mqtt_reconnect", daemon=True)
        self._reconnect_thread.start()

    def _reconnect_loop(self) -> None:
        while self._should_run and not self._connected:
            time.sleep(self._reconnect_delay)
            if not self._should_run:
                break
            try:
                logger.info(f"MQTT reconnect → {self._host}:{self._port}...")
                self._client.reconnect()
            except Exception as e:
                logger.warning(f"MQTT reconnect failed: {e}")

    # ── Paho callbacks ────────────────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, rc: int) -> None:
        if rc == 0:
            with self._lock:
                self._connected = True
            logger.info(f"MQTT connected to {self._host}:{self._port}")
            # Re-subscribe to all registered topics (handles reconnect case)
            for topic, qos, _ in self._subscriptions:
                self._client.subscribe(topic, qos)
                logger.info(f"MQTT subscribed: {topic} (QoS {qos})")
        else:
            _err = {1:"bad protocol",2:"bad client id",3:"unavailable",
                    4:"bad credentials",5:"not authorized"}
            logger.error(f"MQTT connect refused: {_err.get(rc, f'rc={rc}')}")
            if rc not in (4, 5):
                self._start_reconnect_loop()

    def _on_disconnect(self, client, userdata, rc: int) -> None:
        with self._lock:
            self._connected = False
        if rc == 0:
            logger.info("MQTT disconnected cleanly.")
        else:
            logger.warning(f"MQTT unexpected disconnect rc={rc}, reconnecting...")
            if self._should_run:
                self._start_reconnect_loop()

    def _on_message(self, client, userdata, message) -> None:
        """Route inbound message to the registered callback for its topic."""
        topic   = message.topic
        payload = message.payload.decode('utf-8', errors='replace')
        for reg_topic, _, callback in self._subscriptions:
            if _topic_matches(reg_topic, topic):
                try:
                    callback(topic, payload)
                except Exception as e:
                    logger.error(f"Callback error for topic {topic}: {e}", exc_info=True)
                return
        logger.warning(f"MQTT message on unhandled topic: {topic}")

    def _on_publish(self, client, userdata, mid: int) -> None:
        logger.debug(f"MQTT ack mid={mid}")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _topic_matches(pattern: str, topic: str) -> bool:
    """
    Check if topic matches pattern (supports + and # wildcards).
    Exact match in most cases since we use specific topic strings.
    """
    if pattern == topic:
        return True
    # Simple wildcard support: # matches anything, + matches one level
    p_parts = pattern.split('/')
    t_parts = topic.split('/')
    for i, pp in enumerate(p_parts):
        if pp == '#':
            return True
        if i >= len(t_parts):
            return False
        if pp != '+' and pp != t_parts[i]:
            return False
    return len(p_parts) == len(t_parts)


def _json_default(obj: Any) -> Any:
    """Fallback JSON serializer for non-standard types."""
    if hasattr(obj, '__float__'): return float(obj)
    if hasattr(obj, '__int__'):   return int(obj)
    if hasattr(obj, '__iter__'):  return list(obj)
    return str(obj)
