#!/usr/bin/env python3
"""
drone_controller/controller_node.py
VTP Robotics — Drone Controller
PX4 v1.16.1 / px4_msgs v1.16.1

Design: delegate trajectory/stabilization to PX4 autopilot. The node
issues high-level navigation commands and lets PX4's native state
machine execute them. No manual offboard streaming for TAKEOFF/GO_TO/
RTL/LAND — that is what the autopilot is for.

Commands used
─────────────
  TAKE_OFF        → ARM + VEHICLE_CMD_NAV_TAKEOFF   (PX4 AUTO.TAKEOFF)
  GO_TO           → OFFBOARD + TrajectorySetpoint   (NED position)
  RETURN_TO_HOME  → VEHICLE_CMD_NAV_RETURN_TO_LAUNCH (AUTO.RTL)
  LAND            → VEHICLE_CMD_NAV_LAND             (AUTO.LAND)
  CANCEL          → stop current task, hover (AUTO.LOITER if airborne)

CANCEL semantics
────────────────
A new task is normally rejected while another is running. CANCEL bypasses
this gate: it may appear only as cmds[0] of an incoming task. On arrival
it stops any in-flight command, marks the running task CANCELED, and the
remaining commands of the new task execute on the now-hovering drone.

TAKE_OFF uses VEHICLE_CMD_NAV_TAKEOFF (MAVLink 22): PX4 handles the
takeoff ramp, yaw, and climb speed with its tuned controllers.

GO_TO uses OFFBOARD mode (not DO_REPOSITION): PX4 v1.16.1's commander
returns "command 192 unsupported" for DO_REPOSITION on multicopter
builds, so external waypoint control is done through the officially
supported OFFBOARD path. Target lat/lon is projected into local NED
using VehicleLocalPosition.ref_lat/ref_lon, then streamed as
TrajectorySetpoint at the control loop rate.

Incoming command payload (via mqtt_bridge):
  latitude/longitude (opt), altitude (m, treated as AGL/home-rel),
  speed (m/s, opt). Accepts either "TAKEOFF" or "TAKE_OFF".

PX4 topic names (v1.16.1)
─────────────────────────
  /fmu/out/vehicle_status_v1
  /fmu/out/vehicle_local_position
  /fmu/out/vehicle_land_detected
  /fmu/out/vehicle_global_position
"""

import math
import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy

from px4_msgs.msg import (
    OffboardControlMode, TrajectorySetpoint, VehicleCommand,
    VehicleCommandAck, VehicleLocalPosition, VehicleStatus,
    VehicleLandDetected, VehicleGlobalPosition,
)
from drone_msgs.msg import TaskCommand, TaskStatus
from std_msgs.msg import Bool

# ── Tunable ───────────────────────────────────────────────────────────────────
CONTROL_HZ          = 20.0
ALT_TOLERANCE_M     = 1.5       # m: takeoff done when alt >= target - tol
ARM_RETRY_CYCLES    = 40        # cycles between ARM retries (~2 s at 20 Hz)
ARM_TIMEOUT_S       = 20.0
DEFAULT_SPEED_MS    = 5.0
DEFAULT_ARRIVAL_R_M = 2.0
LAND_ALT_FALLBACK_M = 0.5       # sim: also declare landed if alt < this
# GO_TO sequencing (OFFBOARD path — PX4 v1.16.1 rejects DO_REPOSITION for
# multicopter, so we stream TrajectorySetpoint + OffboardControlMode, then
# switch into OFFBOARD mode once PX4 has seen enough setpoints).
OFFBOARD_PRESTREAM_CYCLES = 20   # stream setpoints ~1 s before mode switch
EARTH_R             = 6_371_000.0

# Status strings the rest of the system expects
CMD_PENDING    = 'PENDING'
CMD_RUNNING    = 'RUNNING'
CMD_COMPLETED  = 'COMPLETED'
CMD_FAILED     = 'FAILED'
CMD_CANCELED   = 'CANCELED'
TASK_CANCELED  = 'CANCELED'
# ─────────────────────────────────────────────────────────────────────────────


def haversine_m(lat1, lon1, lat2, lon2) -> float:
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return 2 * EARTH_R * math.asin(min(1.0, math.sqrt(a)))


def _latlon_to_ne(ref_lat, ref_lon, lat, lon):
    """Equirectangular projection relative to (ref_lat, ref_lon).
    Accurate to ~cm for ranges of a few km (typical mission scale).
    Returns (north_m, east_m) in PX4 NED frame."""
    ref_lat_r = math.radians(ref_lat)
    n = math.radians(lat - ref_lat) * EARTH_R
    e = math.radians(lon - ref_lon) * EARTH_R * math.cos(ref_lat_r)
    return n, e


class Phase:
    INIT         = 'INIT'          # print header once, then → WAITING_DATA
    WAITING_DATA = 'WAITING_DATA'  # gate on prerequisites (pos, gcs, etc.)
    ARMING       = 'ARMING'
    CLIMBING     = 'CLIMBING'
    FLYING       = 'FLYING'
    WAITING      = 'WAITING'       # waiting for PX4 native cmd to finish


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
        # VehicleCommand: high-level commands (arm, NAV_TAKEOFF, NAV_LAND, RTL, SET_MODE)
        # OffboardControlMode + TrajectorySetpoint: used for GO_TO (OFFBOARD mode).
        # PX4 v1.16.1 does not dispatch DO_REPOSITION for multicopter, so
        # GO_TO runs through the officially supported offboard path.
        self._pub_command    = self.create_publisher(
            VehicleCommand,      '/fmu/in/vehicle_command',       px4_pub_qos)
        self._pub_offboard   = self.create_publisher(
            OffboardControlMode, '/fmu/in/offboard_control_mode', px4_pub_qos)
        self._pub_trajectory = self.create_publisher(
            TrajectorySetpoint,  '/fmu/in/trajectory_setpoint',   px4_pub_qos)
        self._pub_status     = self.create_publisher(
            TaskStatus,          '/mqtt_bridge/in/task_status',   ros_qos)

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
        self.create_subscription(
            VehicleCommandAck,
            '/fmu/out/vehicle_command_ack',
            self._on_command_ack, px4_sub_qos)

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

        # ── TAKE_OFF / GO_TO targets ──────────────────────────────────────
        self._takeoff_target_alt_m = 0.0
        self._goto_lat      = 0.0
        self._goto_lon      = 0.0
        self._goto_alt_m    = 0.0                  # home-relative
        self._goto_speed    = DEFAULT_SPEED_MS
        self._goto_arrival  = DEFAULT_ARRIVAL_R_M
        # Offboard streaming (NED setpoint towards GO_TO target).
        self._ob_active  = False
        self._ob_target_ned = [0.0, 0.0, 0.0]      # [N, E, D]
        self._ob_yaw     = float('nan')            # NaN = current
        self._flying_entry_cycle = 0               # cycle when FLYING started

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

    def _on_command_ack(self, msg):
        """Log PX4 rejections for commands we send so we can see what
        happened (armed, takeoff, set_mode, etc.). Accepted commands
        are quiet to avoid log spam."""
        if msg.result in (
            VehicleCommandAck.VEHICLE_CMD_RESULT_TEMPORARILY_REJECTED,
            VehicleCommandAck.VEHICLE_CMD_RESULT_UNSUPPORTED,
            VehicleCommandAck.VEHICLE_CMD_RESULT_DENIED,
            VehicleCommandAck.VEHICLE_CMD_RESULT_FAILED,
        ):
            self.get_logger().warn(
                f'  PX4 rejected cmd={msg.command} result={msg.result}')

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
        cmds = sorted(msg.payloads, key=lambda c: c.sequence)
        is_cancel_task = bool(cmds) and cmds[0].command_type == 'CANCEL'

        busy = self._task_state not in ('IDLE', 'COMPLETED', 'FAILED', 'CANCELED')
        if busy and not is_cancel_task:
            self.get_logger().warn(
                f'Task {msg.task_id} rejected — '
                f'{self._task.task_id} still {self._task_state} '
                f'(send CANCEL as first command to pre-empt)')
            self._publish_status(msg.task_id, 'FAILED', 0, '', CMD_FAILED,
                                 len(cmds), 0,
                                 'controller busy; prepend CANCEL to pre-empt')
            return

        ok, reason = self._validate(cmds)
        if not ok:
            self.get_logger().error(f'Task {msg.task_id} REJECTED: {reason}')
            self._publish_status(msg.task_id, 'FAILED', 0, '', CMD_FAILED,
                                 len(cmds), 0, reason)
            return

        if is_cancel_task:
            # Force hover regardless of whether a previous task was still
            # running. PX4 can be left in OFFBOARD briefly after a task
            # completes (setpoint timeout failsafe window), so always send
            # an explicit LOITER when the operator says CANCEL.
            self._cancel_running_task(
                'pre-empted by new task' if busy else 'operator hover')

        self._task        = msg
        self._cmds        = cmds
        self._cidx        = 0
        self._phase       = Phase.INIT
        self._phase_cycle = 0
        self._task_state  = 'EXECUTING'
        self._cycle       = 0
        self.get_logger().info(
            f'Task {msg.task_id} accepted  ({len(cmds)} commands)')
        self._publish_status(msg.task_id, 'RECEIVED', 0, '', CMD_PENDING,
                             len(cmds), 0)

    def _validate(self, cmds):
        """
        Sequence-aware validation. Walks the command list tracking the
        expected in_air state after each step, rejecting transitions that
        would be meaningless or impossible (e.g. TAKE_OFF while already
        airborne, GO_TO without taking off first, LAND/RTH while landed).

        CANCEL is accepted only as the first command; it does not change
        the expected in_air state (drone keeps whatever altitude it had).
        """
        if not cmds:
            return False, 'empty command list'

        expected_in_air = not self._is_landed()

        for i, cmd in enumerate(cmds):
            ct, pl, cid = cmd.command_type, cmd.payload, cmd.command_id

            if ct == 'CANCEL':
                if i != 0:
                    return False, f'{cid}: CANCEL only allowed as first command'
                continue

            if ct in ('TAKEOFF', 'TAKE_OFF'):
                if not pl.has_altitude:
                    return False, f'{cid}: TAKE_OFF requires altitude'
                if expected_in_air:
                    return False, (f'{cid}: TAKE_OFF rejected — '
                                   f'drone already airborne')
                expected_in_air = True

            elif ct == 'GO_TO':
                if not pl.has_position:
                    return False, f'{cid}: GO_TO requires latitude/longitude'
                if not pl.has_altitude:
                    return False, f'{cid}: GO_TO requires altitude'
                if pl.latitude == 0.0 and pl.longitude == 0.0:
                    return False, f'{cid}: GO_TO lat=0 lon=0 — invalid position'
                if not expected_in_air:
                    return False, (f'{cid}: GO_TO rejected — '
                                   f'drone not airborne (TAKE_OFF first)')

            elif ct == 'LAND':
                if not expected_in_air:
                    return False, f'{cid}: LAND rejected — drone already landed'
                expected_in_air = False

            elif ct == 'RETURN_TO_HOME':
                if not expected_in_air:
                    return False, (f'{cid}: RETURN_TO_HOME rejected — '
                                   f'drone not airborne')
                expected_in_air = False

            else:
                return False, f'{cid}: unknown command_type="{ct}"'

        return True, ''

    def _cancel_running_task(self, reason: str):
        """Force the drone into hover and, if a task was actually running,
        emit a CANCELED task_status for it. Safe to call when no task is
        active (just sends LOITER). Caller installs the new task after."""
        was_running = self._task_state == 'EXECUTING'
        self._ob_active = False
        if not self._is_landed():
            self._send_cmd(VehicleCommand.VEHICLE_CMD_DO_SET_MODE,
                           p1=1.0, p2=4.0, p3=3.0)  # AUTO.LOITER
        if was_running and self._task is not None:
            cmd = self._cmds[self._cidx] if self._cidx < len(self._cmds) else None
            self.get_logger().info(
                f'⏹  Task {self._task.task_id} CANCELED — {reason}')
            self._publish_status(
                self._task.task_id, TASK_CANCELED,
                cmd.sequence   if cmd else 0,
                cmd.command_id if cmd else '',
                CMD_CANCELED,
                len(self._cmds), self._cidx, reason)

    # ── Main loop ─────────────────────────────────────────────────────────────

    def _loop(self):
        self._cycle += 1

        # Offboard must stream before AND while in OFFBOARD mode (PX4 drops
        # out of OFFBOARD if setpoints stop for ~0.5 s).
        if self._ob_active:
            self._stream_offboard()

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
                cmd.sequence, cmd.command_id, CMD_RUNNING,
                len(self._cmds), self._cidx)
            self._phase = Phase.WAITING_DATA   # ← prevents INIT re-entry

        dispatch = {
            'TAKEOFF':        self._exec_takeoff,
            'TAKE_OFF':       self._exec_takeoff,
            'GO_TO':          self._exec_go_to,
            'RETURN_TO_HOME': self._exec_rth,
            'LAND':           self._exec_land,
            'CANCEL':         self._exec_cancel,
        }
        done = dispatch[cmd.command_type](cmd)
        self._phase_cycle += 1

        if done:
            self.get_logger().info(f'[{cmd.command_id}] ✔  COMPLETED')
            self._publish_status(
                self._task.task_id, 'EXECUTING',
                cmd.sequence, cmd.command_id, CMD_COMPLETED,
                len(self._cmds), self._cidx + 1)
            self._cidx       += 1
            self._phase       = Phase.INIT
            self._phase_cycle = 0

    # ── TAKEOFF ───────────────────────────────────────────────────────────────

    def _exec_takeoff(self, cmd) -> bool:
        """
        PX4-native TAKE_OFF:
          1. Wait for local+global position.
          2. Send NAV_TAKEOFF with AMSL altitude (PX4 enters AUTO.TAKEOFF).
          3. Send ARM. PX4 executes takeoff ramp/climb with its own controllers.
          4. Poll altitude until within ALT_TOLERANCE_M of target.
        """
        target_alt = float(cmd.payload.altitude_m)

        if self._phase == Phase.WAITING_DATA:
            ready = True
            if self._lpos is None:
                if self._phase_cycle % int(CONTROL_HZ * 2) == 0:
                    self.get_logger().warn(
                        '  TAKE_OFF: waiting for local position '
                        '(is DDS agent running? ROS_LOCALHOST_ONLY=1 set?)')
                ready = False
            if self._gpos is None:
                if self._phase_cycle % int(CONTROL_HZ * 2) == 0:
                    self.get_logger().warn('  TAKE_OFF: waiting for global position (GPS fix)')
                ready = False
            if not self._gcs_alive:
                if self._phase_cycle % int(CONTROL_HZ * 2) == 0:
                    self.get_logger().warn(
                        '  TAKE_OFF: waiting for GCS heartbeat '
                        '(gcs_heartbeat_node must be running and connected)')
                ready = False
            if not ready:
                return False

            self._takeoff_target_alt_m = target_alt
            alt_amsl = self._get_home_amsl() + target_alt

            # Per-command climb speed: DO_CHANGE_SPEED type=2 (CLIMB_SPEED)
            # must be sent BEFORE NAV_TAKEOFF so PX4's AUTO.TAKEOFF uses it.
            if cmd.payload.has_speed:
                self._send_cmd(VehicleCommand.VEHICLE_CMD_DO_CHANGE_SPEED,
                               p1=2.0, p2=float(cmd.payload.speed_ms), p3=-1.0)

            # NAV_TAKEOFF must be sent BEFORE arming so PX4 switches to
            # AUTO.TAKEOFF mode; otherwise ARM is rejected by the preflight
            # check in non-manual modes.
            self._send_cmd(
                VehicleCommand.VEHICLE_CMD_NAV_TAKEOFF,
                p1=-1.0,                      # pitch: use default
                p4=float('nan'),              # yaw: current
                p5=float(self._gpos.lat),
                p6=float(self._gpos.lon),
                p7=alt_amsl,
            )
            self._send_cmd(VehicleCommand.VEHICLE_CMD_COMPONENT_ARM_DISARM,
                           p1=1.0, p2=0.0)
            self._arm_start_time = self.get_clock().now()
            self._arm_retry_at   = self._cycle + ARM_RETRY_CYCLES
            self._phase          = Phase.ARMING
            self.get_logger().info(
                f'  TAKE_OFF: NAV_TAKEOFF + ARM sent  '
                f'target_alt_home={target_alt} m  amsl={alt_amsl:.1f} m')
            return False

        if self._phase == Phase.ARMING:
            if self._is_armed():
                self._phase = Phase.CLIMBING
                self.get_logger().info(
                    f'  TAKE_OFF: armed — PX4 climbing to {target_alt} m')
                return False
            elapsed = (
                self.get_clock().now() - self._arm_start_time
            ).nanoseconds / 1e9
            if elapsed > ARM_TIMEOUT_S:
                nav = self._vstatus.nav_state    if self._vstatus else '?'
                arm = self._vstatus.arming_state if self._vstatus else '?'
                self._fail_task(
                    f'TAKE_OFF arm timeout {ARM_TIMEOUT_S:.0f}s. '
                    f'nav_state={nav} arming_state={arm}. '
                    f'Check: GPS 3D fix, COM_RCL_EXCEPT, GCS heartbeat.')
                return False
            if self._cycle >= self._arm_retry_at:
                self._arm_retry_at = self._cycle + ARM_RETRY_CYCLES
                nav = self._vstatus.nav_state    if self._vstatus else '?'
                arm = self._vstatus.arming_state if self._vstatus else '?'
                self.get_logger().warn(
                    f'  ARM retry  nav_state={nav}  arming_state={arm}')
                alt_amsl = self._get_home_amsl() + target_alt
                self._send_cmd(
                    VehicleCommand.VEHICLE_CMD_NAV_TAKEOFF,
                    p1=-1.0, p4=float('nan'),
                    p5=float(self._gpos.lat) if self._gpos else 0.0,
                    p6=float(self._gpos.lon) if self._gpos else 0.0,
                    p7=alt_amsl,
                )
                self._send_cmd(VehicleCommand.VEHICLE_CMD_COMPONENT_ARM_DISARM,
                               p1=1.0, p2=0.0)
            return False

        if self._phase == Phase.CLIMBING:
            alt = self._alt_rel_home()
            nav = self._vstatus.nav_state if self._vstatus else -1
            if self._phase_cycle % int(CONTROL_HZ) == 0 and alt is not None:
                self.get_logger().info(
                    f'  Climbing: {alt:.1f} m / {target_alt} m  nav_state={nav}')
            # Complete when altitude reached AND PX4 has exited AUTO.TAKEOFF.
            # PX4 transitions TAKEOFF → LOITER once the navigator considers
            # the climb done; waiting for this ensures the next command
            # starts from a stable hover.
            alt_ok = alt is not None and alt >= target_alt - ALT_TOLERANCE_M
            mode_ok = nav != VehicleStatus.NAVIGATION_STATE_AUTO_TAKEOFF
            if alt_ok and mode_ok:
                return True
        return False

    # ── GO_TO ─────────────────────────────────────────────────────────────────

    def _exec_go_to(self, cmd) -> bool:
        """
        GO_TO via OFFBOARD + TrajectorySetpoint (PX4 v1.16.1 multicopter).

        PX4 v1.16.1's commander does not dispatch DO_REPOSITION (192) for
        multicopter in this build — every send is rejected as "unsupported"
        and the drone only moves as a side effect of the preceding mode
        switch. The officially supported external-control path for PX4 is
        OFFBOARD mode fed with TrajectorySetpoint.

        Sequencing:
          1. Wait for global position, local position with valid NED
             reference (ref_lat/ref_lon), and PX4 to be out of AUTO.TAKEOFF.
          2. Convert target (lat/lon/alt_home) → NED using the local
             position reference and home AMSL.
          3. Start streaming OffboardControlMode + TrajectorySetpoint at
             the control-loop rate. PX4 requires ≥2 Hz setpoints for ~1 s
             before it will accept OFFBOARD; we pre-stream then switch.
          4. Send DO_SET_MODE → OFFBOARD (main=1, sub=6). PX4 executes
             smooth trajectory generation internally using its tuned
             position controller. max_horizontal_speed is respected via
             yaw-aligned velocity limiting in PX4's MPC_* params (speed
             passed via TrajectorySetpoint.velocity magnitude hint by
             leaving velocity NaN and relying on MPC_XY_CRUISE).
          5. Hold setpoint at arrival; caller can chain next GO_TO, LAND,
             RTH, etc. Offboard stream is turned off when GO_TO exits.
        """
        pl        = cmd.payload
        speed     = float(pl.speed_ms) if pl.has_speed else DEFAULT_SPEED_MS
        arrival_r = DEFAULT_ARRIVAL_R_M
        tgt_lat   = float(pl.latitude)
        tgt_lon   = float(pl.longitude)
        tgt_alt_m = float(pl.altitude_m)

        if self._phase == Phase.WAITING_DATA:
            if self._gpos is None or self._lpos is None:
                if self._phase_cycle % int(CONTROL_HZ * 2) == 0:
                    self.get_logger().warn(
                        '  GO_TO: waiting for global+local position...')
                return False
            if not (self._lpos.xy_global and self._lpos.z_global):
                if self._phase_cycle % int(CONTROL_HZ * 2) == 0:
                    self.get_logger().warn(
                        '  GO_TO: local position has no global reference yet')
                return False
            nav = self._vstatus.nav_state if self._vstatus else -1
            if nav == VehicleStatus.NAVIGATION_STATE_AUTO_TAKEOFF:
                if self._phase_cycle % int(CONTROL_HZ) == 0:
                    self.get_logger().info(
                        '  GO_TO: waiting for PX4 to finish AUTO.TAKEOFF...')
                return False

            # lat/lon → local NED using PX4's NED reference.
            n, e = _latlon_to_ne(
                float(self._lpos.ref_lat), float(self._lpos.ref_lon),
                tgt_lat, tgt_lon)
            # Down = -(alt_home + home_amsl - ref_alt). ref_alt is AMSL.
            target_amsl = self._get_home_amsl() + tgt_alt_m
            d = -(target_amsl - float(self._lpos.ref_alt))

            self._goto_lat      = tgt_lat
            self._goto_lon      = tgt_lon
            self._goto_alt_m    = tgt_alt_m
            self._goto_speed    = speed
            self._goto_arrival  = arrival_r
            self._ob_target_ned = [n, e, d]

            # Per-command cruise speed for OFFBOARD position-setpoint flight.
            # DO_CHANGE_SPEED type=1 (GROUNDSPEED) overrides MPC_XY_CRUISE for
            # this leg so each GO_TO honors its own speed_ms.
            if pl.has_speed:
                self._send_cmd(VehicleCommand.VEHICLE_CMD_DO_CHANGE_SPEED,
                               p1=1.0, p2=float(speed), p3=-1.0)
            # Yaw towards target (heading from current NED position).
            dn = n - float(self._lpos.x)
            de = e - float(self._lpos.y)
            self._ob_yaw = math.atan2(de, dn) if (dn*dn + de*de) > 1.0 else float('nan')

            # Begin streaming immediately; PX4 needs a few setpoints before
            # OFFBOARD is accepted. The mode switch happens after the
            # pre-stream period elapses.
            self._ob_active = True
            self._phase = Phase.FLYING
            self._flying_entry_cycle = self._cycle
            self.get_logger().info(
                f'  GO_TO: target lat={tgt_lat:.6f} lon={tgt_lon:.6f} '
                f'alt_home={tgt_alt_m:.1f} m  ned=({n:.1f},{e:.1f},{d:.1f})  '
                f'speed={speed} m/s → OFFBOARD')
            return False

        if self._phase == Phase.FLYING:
            if self._gpos is None or self._lpos is None:
                return False

            # After pre-streaming setpoints, switch to OFFBOARD once.
            cycles_since_entry = self._cycle - self._flying_entry_cycle
            nav = self._vstatus.nav_state if self._vstatus else -1
            if (cycles_since_entry == OFFBOARD_PRESTREAM_CYCLES
                    and nav != VehicleStatus.NAVIGATION_STATE_OFFBOARD):
                self._send_cmd(VehicleCommand.VEHICLE_CMD_DO_SET_MODE,
                               p1=1.0, p2=6.0)
                self.get_logger().info('  GO_TO: sent SET_MODE → OFFBOARD')

            dist = haversine_m(
                float(self._gpos.lat), float(self._gpos.lon),
                self._goto_lat, self._goto_lon)
            alt  = self._alt_rel_home()
            alt_err = abs((alt or 0.0) - tgt_alt_m)
            if self._phase_cycle % int(CONTROL_HZ) == 0:
                self.get_logger().info(
                    f'  GO_TO: {dist:.1f} m to target  '
                    f'(arrival_r={arrival_r:.1f} m)  alt_err={alt_err:.1f} m  '
                    f'nav_state={nav}')
            # Arrived when both horizontal distance AND altitude are within bounds
            if dist <= arrival_r and alt_err <= ALT_TOLERANCE_M:
                self.get_logger().info(
                    f'  GO_TO: arrived!  dist={dist:.2f} m  alt_err={alt_err:.2f} m')
                # Park PX4 in AUTO.LOITER before stopping the stream; otherwise
                # PX4 stays in OFFBOARD, sees setpoints stop, and triggers the
                # setpoint-timeout failsafe (~0.5 s → descent + land).
                self._send_cmd(VehicleCommand.VEHICLE_CMD_DO_SET_MODE,
                               p1=1.0, p2=4.0, p3=3.0)
                self._ob_active = False
                return True
        return False

    # ── CANCEL ────────────────────────────────────────────────────────────────

    def _exec_cancel(self, cmd) -> bool:
        """First-command-only marker. The actual pre-emption (stopping the
        previous task and putting the drone into LOITER) ran in
        _cancel_running_task before this task was installed. Here we just
        complete the CANCEL step so the dispatcher advances to the rest
        of the new task."""
        self.get_logger().info('  CANCEL: previous task pre-empted, hovering')
        return True

    # ── RTH ───────────────────────────────────────────────────────────────────

    def _exec_rth(self, cmd) -> bool:
        if self._phase == Phase.WAITING_DATA:
            self.get_logger().info('  RTH: Return-to-Launch')
            self._ob_active = False
            if cmd.payload.has_speed:
                self._send_cmd(VehicleCommand.VEHICLE_CMD_DO_CHANGE_SPEED,
                               p1=1.0, p2=float(cmd.payload.speed_ms), p3=-1.0)
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
            # Per-command descent speed: type=3 (DESCEND_SPEED) before NAV_LAND.
            if cmd.payload.has_speed:
                self._send_cmd(VehicleCommand.VEHICLE_CMD_DO_CHANGE_SPEED,
                               p1=3.0, p2=float(cmd.payload.speed_ms), p3=-1.0)
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

    # ── Task lifecycle ────────────────────────────────────────────────────────

    def _complete(self):
        self._task_state = 'COMPLETED'
        self._ob_active  = False
        self.get_logger().info(f'✅  Task {self._task.task_id} COMPLETED')
        self._publish_status(
            self._task.task_id, 'COMPLETED', 0, '', CMD_COMPLETED,
            len(self._cmds), len(self._cmds))

    def _fail_task(self, reason: str):
        """Fail the running task in place (no auto-RTL). Operator must
        decide next action via a new CANCEL or RETURN_TO_HOME task."""
        self._task_state = 'FAILED'
        self._ob_active  = False
        self.get_logger().error(f'🚨 FAILED: {reason}')
        cmd = self._cmds[self._cidx] if self._cidx < len(self._cmds) else None
        self._publish_status(
            self._task.task_id, 'FAILED',
            cmd.sequence   if cmd else 0,
            cmd.command_id if cmd else '',
            CMD_FAILED,
            len(self._cmds), self._cidx, reason)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _ts(self):
        return self.get_clock().now().nanoseconds // 1000

    def _stream_offboard(self):
        """Publish OffboardControlMode + TrajectorySetpoint at the loop rate.
        PX4 requires a continuous setpoint stream before and during OFFBOARD;
        drop below ~2 Hz and PX4 exits OFFBOARD as a safety failsafe."""
        ocm = OffboardControlMode()
        ocm.position     = True
        ocm.velocity     = False
        ocm.acceleration = False
        ocm.attitude     = False
        ocm.body_rate    = False
        ocm.timestamp    = self._ts()
        self._pub_offboard.publish(ocm)

        sp = TrajectorySetpoint()
        sp.position = [float(v) for v in self._ob_target_ned]
        sp.velocity = [float('nan')] * 3
        sp.acceleration = [float('nan')] * 3
        sp.jerk = [float('nan')] * 3
        sp.yaw       = float(self._ob_yaw)
        sp.yawspeed  = float('nan')
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
