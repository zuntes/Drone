#!/usr/bin/env python3
"""
drone_controller/controller_node.py
VTP Robotics — Drone Controller
PX4 v1.16.1 / px4_msgs v1.16.1

Bugs fixed vs previous version
───────────────────────────────
1. INIT re-entry loop
   Old code: phase stayed INIT while waiting for position → _step() reset
   _phase_cycle=0 every single cycle → header printed every cycle.
   Fix: INIT transitions immediately to WAITING_DATA, which is the
   actual waiting state. INIT prints the header exactly once.

2. QoS mismatch (silent subscription failure)
   PX4 DDS topics are BEST_EFFORT + VOLATILE.
   Old code used TRANSIENT_LOCAL → silently receives zero messages.
   Fix: all px4 subscribers use BEST_EFFORT + VOLATILE.

3. GCS connection drop ("No connection to ground control station")
   The warn message in PX4 means the MAVLink GCS heartbeat stopped.
   gcs_heartbeat_node must be running and its UDP port must reach PX4.
   The node now prints clear diagnostics if GCS is not alive.

PX4 v1.16.1 topic names used
──────────────────────────────
  /fmu/out/vehicle_status_v2           ← versioned
  /fmu/out/vehicle_local_position_v1   ← versioned
  /fmu/out/vehicle_land_detected
  /fmu/out/vehicle_global_position

GO_TO uses VEHICLE_CMD_DO_REPOSITION (MAVLink 192)
  param1=speed, param5=lat, param6=lon, param7=alt_amsl
  PX4 navigates internally — no NED conversion required.
"""

import math
import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy

from px4_msgs.msg import (
    OffboardControlMode, TrajectorySetpoint, VehicleCommand,
    VehicleLocalPosition, VehicleStatus, VehicleLandDetected,
    VehicleGlobalPosition,
)
from drone_msgs.msg import TaskCommand, TaskStatus
from std_msgs.msg import Bool

# ── Tunable ───────────────────────────────────────────────────────────────────
CONTROL_HZ          = 20.0
OFFBOARD_WARMUP     = 25        # cycles before OFFBOARD switch  (~1.25 s)
ALT_TOLERANCE_M     = 1.5       # m: takeoff done when alt >= target - tol
ARM_RETRY_CYCLES    = 40        # cycles between ARM retries
ARM_TIMEOUT_S       = 20.0
DEFAULT_SPEED_MS    = 5.0
DEFAULT_ARRIVAL_R_M = 1.0
LAND_ALT_FALLBACK_M = 0.5       # sim: also declare landed if alt < this
EARTH_R             = 6_371_000.0
# ─────────────────────────────────────────────────────────────────────────────


def haversine_m(lat1, lon1, lat2, lon2) -> float:
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return 2 * EARTH_R * math.asin(min(1.0, math.sqrt(a)))


class Phase:
    INIT         = 'INIT'          # print header once, then → WAITING_DATA
    WAITING_DATA = 'WAITING_DATA'  # gate on prerequisites (pos, gcs, etc.)
    WARMUP       = 'WARMUP'        # stream offboard before mode switch
    ARMING       = 'ARMING'
    CLIMBING     = 'CLIMBING'
    FLYING       = 'FLYING'
    WAITING      = 'WAITING'       # waiting for PX4 native cmd to finish
    HOLDING      = 'HOLDING'       # PAUSED state


class DroneControllerNode(Node):

    def __init__(self):
        super().__init__('drone_controller_node')

        # ── QoS ──────────────────────────────────────────────────────────
        # PX4 DDS: BEST_EFFORT + VOLATILE  (must match exactly)
        px4_sub_qos = QoSProfile(
            reliability=ReliabilityPolicy.BEST_EFFORT,
            durability=DurabilityPolicy.VOLATILE,
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
        )
        px4_pub_qos = QoSProfile(
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.VOLATILE,
            history=HistoryPolicy.KEEP_LAST,
            depth=1,
        )
        ros_qos = QoSProfile(
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.VOLATILE,
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
        )

        # ── Publishers ────────────────────────────────────────────────────
        self._pub_offboard   = self.create_publisher(
            OffboardControlMode, '/fmu/in/offboard_control_mode', px4_pub_qos)
        self._pub_trajectory = self.create_publisher(
            TrajectorySetpoint,  '/fmu/in/trajectory_setpoint',   px4_pub_qos)
        self._pub_command    = self.create_publisher(
            VehicleCommand,      '/fmu/in/vehicle_command',        px4_pub_qos)
        self._pub_status     = self.create_publisher(
            TaskStatus,          '/mqtt_bridge/in/task_status',    ros_qos)

        # ── PX4 subscribers  (BEST_EFFORT + VOLATILE) ─────────────────────
        self.create_subscription(
            VehicleStatus,
            '/fmu/out/vehicle_status_v1',
            lambda m: setattr(self, '_vstatus', m), px4_sub_qos)
        self.create_subscription(
            VehicleLocalPosition,
            '/fmu/out/vehicle_local_position',
            lambda m: setattr(self, '_lpos', m), px4_sub_qos)
        self.create_subscription(
            VehicleLandDetected,
            '/fmu/out/vehicle_land_detected',
            self._on_land_detected, px4_sub_qos)
        self.create_subscription(
            VehicleGlobalPosition,
            '/fmu/out/vehicle_global_position',
            lambda m: setattr(self, '_gpos', m), px4_sub_qos)

        # ── Internal ──────────────────────────────────────────────────────
        self.create_subscription(
            TaskCommand, '/mqtt_bridge/out/task_command',
            self._on_task_command, ros_qos)
        self.create_subscription(
            Bool, '/gcs/armed',
            lambda m: setattr(self, '_gcs_alive', True), 10)

        # ── Vehicle state ─────────────────────────────────────────────────
        self._vstatus   = None
        self._lpos      = None
        self._gpos      = None
        self._landed    = True
        self._gcs_alive = False

        # ── Task state ────────────────────────────────────────────────────
        self._task        = None
        self._cmds        = []
        self._cidx        = 0
        self._phase       = Phase.INIT
        self._phase_cycle = 0
        self._task_state  = 'IDLE'

        # ── Offboard (TAKEOFF only) ───────────────────────────────────────
        self._ob_active = False
        self._ob_target = [0.0, 0.0, 0.0]

        # ── GO_TO target ──────────────────────────────────────────────────
        self._goto_lat     = 0.0
        self._goto_lon     = 0.0
        self._goto_arrival = DEFAULT_ARRIVAL_R_M

        # ── Misc ──────────────────────────────────────────────────────────
        self._arm_start_time = None
        self._arm_retry_at   = 0
        self._home_amsl      = None
        self._cycle          = 0

        self.create_timer(1.0 / CONTROL_HZ, self._loop)

        self.get_logger().info('=' * 60)
        self.get_logger().info('  VTP Robotics — DroneControllerNode')
        self.get_logger().info('  PX4 v1.16.1  |  px4_msgs v1.16.1')
        self.get_logger().info('  Listening: /mqtt_bridge/out/task_command')
        self.get_logger().info('=' * 60)

    # ── Land detection ────────────────────────────────────────────────────────

    def _on_land_detected(self, msg):
        self._landed = msg.landed

    def _is_landed(self) -> bool:
        """3-signal landed check (robust in simulation)."""
        if self._landed:
            return True
        alt = self._alt_rel_home()
        if alt is not None and alt < LAND_ALT_FALLBACK_M:
            return True
        # PX4 auto-disarms after touchdown
        if (self._vstatus is not None
                and self._vstatus.arming_state != 2
                and self._phase == Phase.WAITING):
            return True
        return False

    # ── Home AMSL ─────────────────────────────────────────────────────────────

    def _get_home_amsl(self) -> float:
        if self._home_amsl is not None:
            return self._home_amsl
        if self._gpos is not None and self._lpos is not None:
            rel = self._alt_rel_home()
            if rel is not None:
                self._home_amsl = float(self._gpos.alt) - rel
                self.get_logger().info(
                    f'Home AMSL cached: {self._home_amsl:.2f} m')
        return self._home_amsl or 0.0

    # ── Task ingestion ────────────────────────────────────────────────────────

    def _on_task_command(self, msg: TaskCommand):
        if self._task_state not in ('IDLE', 'COMPLETED', 'ABORTED'):
            self.get_logger().warn(
                f'Task {msg.task_id} ignored — '
                f'{self._task.task_id} still {self._task_state}')
            return

        cmds = sorted(msg.payloads, key=lambda c: c.sequence)
        ok, reason = self._validate(cmds)
        if not ok:
            self.get_logger().error(f'Task {msg.task_id} ABORTED: {reason}')
            self._publish_status(msg.task_id, 'ABORTED', 0, '', 'FAILED',
                                 len(cmds), 0, reason)
            return

        self._task        = msg
        self._cmds        = cmds
        self._cidx        = 0
        self._phase       = Phase.INIT
        self._phase_cycle = 0
        self._task_state  = 'EXECUTING'
        self._ob_active   = False
        self._cycle       = 0
        self.get_logger().info(
            f'Task {msg.task_id} accepted  ({len(cmds)} commands)')
        self._publish_status(msg.task_id, 'RECEIVED', 0, '', 'PENDING',
                             len(cmds), 0)

    def _validate(self, cmds):
        for cmd in cmds:
            ct, pl, cid = cmd.command_type, cmd.payload, cmd.command_id
            if ct == 'TAKEOFF':
                if not pl.has_altitude:
                    return False, (
                        f'{cid}: TAKEOFF requires altitude '
                        f'(has_altitude=true, altitude_m)')
            elif ct == 'GO_TO':
                if not pl.has_position:
                    return False, (
                        f'{cid}: GO_TO requires position '
                        f'(has_position=true, lat/lon)')
                if not pl.has_altitude:
                    return False, (
                        f'{cid}: GO_TO requires altitude '
                        f'(has_altitude=true, altitude_m)')
                if pl.latitude == 0.0 and pl.longitude == 0.0:
                    return False, (
                        f'{cid}: GO_TO lat=0 lon=0 — missing position payload')
            elif ct not in ('PAUSE', 'CONTINUE', 'RETURN_TO_HOME',
                            'LAND', 'ABORT'):
                return False, f'{cid}: unknown command_type="{ct}"'
        return True, ''

    # ── Main loop ─────────────────────────────────────────────────────────────

    def _loop(self):
        self._cycle += 1

        if self._ob_active:
            self._stream_offboard()

        if self._task_state == 'PAUSED':
            if self._cidx < len(self._cmds):
                if self._cmds[self._cidx].command_type == 'CONTINUE':
                    self._step()
            return

        if self._task_state != 'EXECUTING':
            return
        if self._cidx >= len(self._cmds):
            self._complete()
            return
        self._step()

    def _step(self):
        cmd = self._cmds[self._cidx]

        # INIT: runs exactly ONCE per command, then transitions to WAITING_DATA
        if self._phase == Phase.INIT:
            self._phase_cycle = 0
            self.get_logger().info('──────────────────────────────────────────')
            self.get_logger().info(
                f'[{cmd.command_id}] {cmd.command_type}  '
                f'seq={cmd.sequence}/{len(self._cmds)}')
            self._publish_status(
                self._task.task_id, 'EXECUTING',
                cmd.sequence, cmd.command_id, 'IN_PROGRESS',
                len(self._cmds), self._cidx)
            self._phase = Phase.WAITING_DATA   # ← prevents INIT re-entry

        dispatch = {
            'TAKEOFF':        self._exec_takeoff,
            'GO_TO':          self._exec_go_to,
            'PAUSE':          self._exec_pause,
            'CONTINUE':       self._exec_continue,
            'RETURN_TO_HOME': self._exec_rth,
            'LAND':           self._exec_land,
            'ABORT':          self._exec_abort_cmd,
        }
        done = dispatch[cmd.command_type](cmd)
        self._phase_cycle += 1

        if done:
            self.get_logger().info(f'[{cmd.command_id}] ✔  COMPLETED')
            self._publish_status(
                self._task.task_id, 'EXECUTING',
                cmd.sequence, cmd.command_id, 'COMPLETED',
                len(self._cmds), self._cidx + 1)
            self._cidx       += 1
            self._phase       = Phase.INIT
            self._phase_cycle = 0

    # ── TAKEOFF ───────────────────────────────────────────────────────────────

    def _exec_takeoff(self, cmd) -> bool:
        target_alt = float(cmd.payload.altitude_m)

        if self._phase == Phase.WAITING_DATA:
            ready = True
            if self._lpos is None:
                if self._phase_cycle % int(CONTROL_HZ * 2) == 0:
                    self.get_logger().warn(
                        '  TAKEOFF: waiting for local position '
                        '(is DDS agent running? ROS_LOCALHOST_ONLY=1 set?)')
                ready = False
            if not self._gcs_alive:
                if self._phase_cycle % int(CONTROL_HZ * 2) == 0:
                    self.get_logger().warn(
                        '  TAKEOFF: waiting for GCS heartbeat '
                        '(gcs_heartbeat_node must be running and connected)')
                ready = False
            if not ready:
                return False
            # Start offboard stream at current position
            self._ob_target = [
                float(self._lpos.x),
                float(self._lpos.y),
                float(self._lpos.z),
            ]
            self._ob_active = True
            self._phase     = Phase.WARMUP
            self.get_logger().info(
                f'  TAKEOFF: offboard stream started  target={target_alt} m')
            return False

        if self._phase == Phase.WARMUP:
            if self._phase_cycle >= OFFBOARD_WARMUP:
                self._send_cmd(VehicleCommand.VEHICLE_CMD_DO_SET_MODE,
                               p1=1.0, p2=6.0)
                self._send_cmd(VehicleCommand.VEHICLE_CMD_COMPONENT_ARM_DISARM,
                               p1=1.0)
                self._arm_start_time = self.get_clock().now()
                self._arm_retry_at   = self._cycle + ARM_RETRY_CYCLES
                self._phase          = Phase.ARMING
                self.get_logger().info('  TAKEOFF: OFFBOARD + ARM sent')
            return False

        if self._phase == Phase.ARMING:
            if self._is_armed():
                self._ob_target[2] = -target_alt
                self._phase        = Phase.CLIMBING
                self.get_logger().info(
                    f'  TAKEOFF: armed! climbing to {target_alt} m...')
                return False
            elapsed = (
                self.get_clock().now() - self._arm_start_time
            ).nanoseconds / 1e9
            if elapsed > ARM_TIMEOUT_S:
                nav = self._vstatus.nav_state    if self._vstatus else '?'
                arm = self._vstatus.arming_state if self._vstatus else '?'
                self._abort_task(
                    f'TAKEOFF arm timeout {ARM_TIMEOUT_S:.0f}s. '
                    f'nav_state={nav} arming_state={arm}. '
                    f'Check: GPS 3D fix, COM_RCL_EXCEPT, GCS heartbeat.')
                return False
            if self._cycle >= self._arm_retry_at:
                self._arm_retry_at = self._cycle + ARM_RETRY_CYCLES
                nav = self._vstatus.nav_state    if self._vstatus else '?'
                arm = self._vstatus.arming_state if self._vstatus else '?'
                self.get_logger().warn(
                    f'  ARM retry  nav_state={nav}  arming_state={arm}')
                self._send_cmd(VehicleCommand.VEHICLE_CMD_DO_SET_MODE,
                               p1=1.0, p2=6.0)
                self._send_cmd(VehicleCommand.VEHICLE_CMD_COMPONENT_ARM_DISARM,
                               p1=1.0)
            return False

        if self._phase == Phase.CLIMBING:
            alt = self._alt_rel_home()
            if self._phase_cycle % int(CONTROL_HZ) == 0 and alt is not None:
                self.get_logger().info(
                    f'  Climbing: {alt:.1f} m / {target_alt} m')
            if alt is not None and alt >= target_alt - ALT_TOLERANCE_M:
                return True
        return False

    # ── GO_TO ─────────────────────────────────────────────────────────────────

    def _exec_go_to(self, cmd) -> bool:
        pl        = cmd.payload
        speed     = float(pl.speed_ms)         if pl.has_speed          else DEFAULT_SPEED_MS
        arrival_r = float(pl.arrival_radius_m) if pl.has_arrival_radius else DEFAULT_ARRIVAL_R_M
        tgt_lat   = float(pl.latitude)
        tgt_lon   = float(pl.longitude)
        tgt_alt_m = float(pl.altitude_m)
        ref       = (pl.altitude_ref.strip().lower()
                     if pl.altitude_ref else 'home')

        if self._phase == Phase.WAITING_DATA:
            if self._gpos is None:
                if self._phase_cycle % int(CONTROL_HZ * 2) == 0:
                    self.get_logger().warn(
                        '  GO_TO: waiting for global position...')
                return False

            home_amsl = self._get_home_amsl()
            alt_amsl  = tgt_alt_m if ref == 'amsl' else tgt_alt_m + home_amsl

            self._goto_lat     = tgt_lat
            self._goto_lon     = tgt_lon
            self._goto_arrival = arrival_r
            self._ob_active    = False   # DO_REPOSITION works without offboard

            self._send_cmd(
                VehicleCommand.VEHICLE_CMD_DO_REPOSITION,
                p1=speed,
                p5=tgt_lat,
                p6=tgt_lon,
                p7=alt_amsl,
            )
            self._phase = Phase.FLYING
            self.get_logger().info(
                f'  GO_TO: lat={tgt_lat:.6f} lon={tgt_lon:.6f} '
                f'alt_amsl={alt_amsl:.1f} m  speed={speed} m/s  '
                f'arrival_r={arrival_r} m')
            return False

        if self._phase == Phase.FLYING:
            if self._gpos is None:
                return False
            dist = haversine_m(
                float(self._gpos.lat), float(self._gpos.lon),
                self._goto_lat, self._goto_lon)
            if self._phase_cycle % int(CONTROL_HZ) == 0:
                self.get_logger().info(
                    f'  GO_TO: {dist:.1f} m to target  '
                    f'(arrival_r={arrival_r:.1f} m)')
            if dist <= arrival_r:
                self.get_logger().info(f'  GO_TO: arrived!  dist={dist:.2f} m')
                return True
        return False

    # ── PAUSE ─────────────────────────────────────────────────────────────────

    def _exec_pause(self, cmd) -> bool:
        if self._phase == Phase.WAITING_DATA:
            self.get_logger().info(
                '  PAUSE: switching to HOLD, awaiting CONTINUE')
            self._ob_active  = False
            self._send_cmd(VehicleCommand.VEHICLE_CMD_DO_SET_MODE,
                           p1=1.0, p2=4.0)
            self._task_state = 'PAUSED'
            self._phase      = Phase.HOLDING
            self._publish_status(
                self._task.task_id, 'PAUSED',
                cmd.sequence, cmd.command_id, 'IN_PROGRESS',
                len(self._cmds), self._cidx)
        return False

    # ── CONTINUE ──────────────────────────────────────────────────────────────

    def _exec_continue(self, cmd) -> bool:
        self.get_logger().info('  CONTINUE: resuming')
        self._task_state = 'EXECUTING'
        return True

    # ── RTH ───────────────────────────────────────────────────────────────────

    def _exec_rth(self, cmd) -> bool:
        if self._phase == Phase.WAITING_DATA:
            self.get_logger().info('  RTH: Return-to-Launch')
            self._ob_active = False
            self._send_cmd(VehicleCommand.VEHICLE_CMD_NAV_RETURN_TO_LAUNCH)
            self._phase = Phase.WAITING
            return False
        if self._phase == Phase.WAITING:
            if self._phase_cycle % int(CONTROL_HZ) == 0:
                alt = self._alt_rel_home()
                self.get_logger().info(
                    f'  RTH: alt={alt:.1f} m' if alt is not None
                    else '  RTH: returning...')
            if self._is_landed():
                self.get_logger().info('  RTH: landed.')
                return True
        return False

    # ── LAND ──────────────────────────────────────────────────────────────────

    def _exec_land(self, cmd) -> bool:
        if self._phase == Phase.WAITING_DATA:
            self.get_logger().info('  LAND: NAV_LAND')
            self._ob_active = False
            self._send_cmd(VehicleCommand.VEHICLE_CMD_NAV_LAND)
            self._phase = Phase.WAITING
            return False
        if self._phase == Phase.WAITING:
            if self._phase_cycle % int(CONTROL_HZ) == 0:
                alt = self._alt_rel_home()
                self.get_logger().info(
                    f'  LAND: alt={alt:.2f} m' if alt is not None
                    else '  LAND: descending...')
            if self._is_landed():
                self.get_logger().info('  LAND: landed!')
                return True
        return False

    # ── ABORT ─────────────────────────────────────────────────────────────────

    def _exec_abort_cmd(self, cmd) -> bool:
        self._abort_task(f'ABORT command ({cmd.command_id})')
        return False

    # ── Task lifecycle ────────────────────────────────────────────────────────

    def _complete(self):
        self._task_state = 'COMPLETED'
        self._ob_active  = False
        self.get_logger().info(f'✅  Task {self._task.task_id} COMPLETED')
        self._publish_status(
            self._task.task_id, 'COMPLETED', 0, '', 'COMPLETED',
            len(self._cmds), len(self._cmds))

    def _abort_task(self, reason: str):
        self._task_state = 'ABORTED'
        self._ob_active  = False
        self.get_logger().error(f'🚨 ABORT: {reason}')
        self._send_cmd(VehicleCommand.VEHICLE_CMD_NAV_RETURN_TO_LAUNCH)
        cmd = self._cmds[self._cidx] if self._cidx < len(self._cmds) else None
        self._publish_status(
            self._task.task_id, 'ABORTED',
            cmd.sequence   if cmd else 0,
            cmd.command_id if cmd else '',
            'FAILED',
            len(self._cmds), self._cidx, reason)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _ts(self):
        return self.get_clock().now().nanoseconds // 1000

    def _stream_offboard(self):
        ocm = OffboardControlMode()
        ocm.position  = True
        ocm.velocity  = ocm.acceleration = ocm.attitude = ocm.body_rate = False
        ocm.timestamp = self._ts()
        self._pub_offboard.publish(ocm)
        sp = TrajectorySetpoint()
        sp.position  = [float(v) for v in self._ob_target]
        sp.yaw       = float('nan')
        sp.timestamp = self._ts()
        self._pub_trajectory.publish(sp)

    def _send_cmd(self, command,
                  p1=0.0, p2=0.0, p3=0.0, p4=0.0,
                  p5=0.0, p6=0.0, p7=0.0):
        msg = VehicleCommand()
        msg.command       = command
        msg.param1        = float(p1);  msg.param2 = float(p2)
        msg.param3        = float(p3);  msg.param4 = float(p4)
        msg.param5        = float(p5);  msg.param6 = float(p6)
        msg.param7        = float(p7)
        msg.target_system    = 1;  msg.target_component = 1
        msg.source_system    = 1;  msg.source_component = 1
        msg.from_external    = True
        msg.timestamp        = self._ts()
        self._pub_command.publish(msg)

    def _is_armed(self):
        return self._vstatus is not None and self._vstatus.arming_state == 2

    def _alt_rel_home(self):
        if self._lpos is None:
            return None
        return -float(self._lpos.z)

    def _publish_status(self, task_id, task_status, seq, cmd_id,
                        cmd_status, total, done, abort_reason=''):
        msg = TaskStatus()
        msg.task_id            = task_id
        msg.task_status        = task_status
        msg.current_sequence   = int(seq)
        msg.current_command_id = cmd_id
        msg.command_status     = cmd_status
        msg.waypoints_total    = int(total)
        msg.waypoints_done     = int(done)
        msg.abort_reason       = abort_reason
        self._pub_status.publish(msg)


def main(args=None):
    rclpy.init(args=args)
    node = DroneControllerNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        node.get_logger().warn('Interrupted — RTL for safety')
        node._ob_active = False
        node._send_cmd(VehicleCommand.VEHICLE_CMD_NAV_RETURN_TO_LAUNCH)
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
