#!/usr/bin/env python3
"""
drone_controller/gcs_heartbeat_node.py
VTP Robotics - ROS2 node that sends MAVLink GCS heartbeat to PX4

Replaces QGroundControl on ARM/Jetson where QGC AppImage cannot run.
Publishes /gcs/armed (std_msgs/Bool) for controller_node to gate on.
"""

import socket
import threading
import time

import rclpy
from rclpy.node import Node
from std_msgs.msg import Bool, String

try:
    from pymavlink import mavutil
    HAS_PYMAVLINK = True
except ImportError:
    HAS_PYMAVLINK = False

DEFAULT_CONN       = 'udp:127.0.0.1:14550'
HEARTBEAT_HZ       = 1.0
DISABLE_RC_FAILSAFE = True


class GCSHeartbeatNode(Node):

    def __init__(self):
        super().__init__('gcs_heartbeat_node')

        self.declare_parameter('connection',          DEFAULT_CONN)
        self.declare_parameter('heartbeat_hz',        HEARTBEAT_HZ)
        self.declare_parameter('disable_rc_failsafe', DISABLE_RC_FAILSAFE)

        self._conn_str   = self.get_parameter('connection').value
        self._hb_hz      = self.get_parameter('heartbeat_hz').value
        self._disable_rc = self.get_parameter('disable_rc_failsafe').value

        self._pub_armed = self.create_publisher(Bool,   '/gcs/armed', 10)
        self._pub_mode  = self.create_publisher(String, '/gcs/mode',  10)

        self._armed   = False
        self._mode    = 'UNKNOWN'
        self._mav     = None
        self._running = False

        if not HAS_PYMAVLINK:
            self.get_logger().error(
                'pymavlink not installed! '
                'pip install pymavlink --break-system-packages')
            return

        self.get_logger().info(f'GCS connecting to: {self._conn_str}')
        threading.Thread(
            target=self._connect_and_run, daemon=True).start()
        self.create_timer(1.0, self._publish_state)

    def _connect_and_run(self):
        try:
            self._mav = mavutil.mavlink_connection(
                self._conn_str, source_system=255, source_component=0)
            self.get_logger().info('Waiting for PX4 heartbeat...')
            hb = self._mav.wait_heartbeat(timeout=30)
            if hb is None:
                self.get_logger().error(
                    'No heartbeat! Check connection string and UDP port.')
                return
            self.get_logger().info(
                f'[GCS] Connected  sys={self._mav.target_system}  '
                f'comp={self._mav.target_component}')
            if self._disable_rc:
                time.sleep(1.0)
                self._set_param('COM_RCL_EXCEPT', 4.0)
            self._running = True
            threading.Thread(
                target=self._heartbeat_loop, daemon=True).start()
            threading.Thread(
                target=self._receive_loop,   daemon=True).start()
        except Exception as e:
            self.get_logger().error(f'GCS connect failed: {e}')

    def _heartbeat_loop(self):
        interval = 1.0 / self._hb_hz
        while self._running:
            try:
                self._mav.mav.heartbeat_send(
                    mavutil.mavlink.MAV_TYPE_GCS,
                    mavutil.mavlink.MAV_AUTOPILOT_INVALID,
                    0, 0, mavutil.mavlink.MAV_STATE_ACTIVE)
            except Exception as e:
                self.get_logger().warn(f'Heartbeat error: {e}')
            time.sleep(interval)

    def _receive_loop(self):
        while self._running:
            try:
                msg = self._mav.recv_match(blocking=True, timeout=0.5)
                if msg is None:
                    continue
                mt = msg.get_type()
                if mt == 'HEARTBEAT':
                    mode = mavutil.mode_string_v10(msg) or str(msg.custom_mode)
                    self._mode  = mode
                    self._armed = bool(
                        msg.base_mode &
                        mavutil.mavlink.MAV_MODE_FLAG_SAFETY_ARMED)
                elif mt == 'STATUSTEXT':
                    sev = {0:'EMG',1:'ALR',2:'CRT',3:'ERR',
                           4:'WRN',5:'NTC',6:'INF',7:'DBG'}
                    lvl  = sev.get(msg.severity, '?')
                    text = msg.text.rstrip('\x00').strip()
                    if text:
                        self.get_logger().info(f'[PX4/{lvl}] {text}')
                elif mt == 'COMMAND_ACK':
                    results = {0:'ACCEPTED',1:'TEMP_REJECTED',2:'DENIED',
                               3:'UNSUPPORTED',4:'FAILED'}
                    r = results.get(msg.result, str(msg.result))
                    self.get_logger().info(
                        f'[GCS] CMD_ACK  cmd={msg.command}  result={r}')
            except Exception as e:
                if self._running:
                    self.get_logger().warn(f'Receive error: {e}')

    def _set_param(self, name: str, value: float):
        try:
            encoded = name.encode('utf-8').ljust(16, b'\x00')[:16]
            self._mav.mav.param_set_send(
                self._mav.target_system,
                self._mav.target_component,
                encoded, value,
                mavutil.mavlink.MAV_PARAM_TYPE_INT32)
            self.get_logger().info(f'[GCS] PARAM_SET {name}={value}')
        except Exception as e:
            self.get_logger().warn(f'PARAM_SET failed: {e}')

    def _publish_state(self):
        armed_msg = Bool(); armed_msg.data = self._armed
        self._pub_armed.publish(armed_msg)
        mode_msg  = String(); mode_msg.data = self._mode
        self._pub_mode.publish(mode_msg)

    def destroy_node(self):
        self._running = False
        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = GCSHeartbeatNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
