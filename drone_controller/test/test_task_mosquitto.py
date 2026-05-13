#!/usr/bin/env python3
"""
test_task_mosquitto.py
VTP Robotics — MQTT task publisher for real drone testing

Usage:
  python3 test_task_mosquitto.py --task takeoff
  python3 test_task_mosquitto.py --task land
  python3 test_task_mosquitto.py --task rtl
  python3 test_task_mosquitto.py --task cancel
  python3 test_task_mosquitto.py --task cancel_rtl
  python3 test_task_mosquitto.py --task go_to_pos
  python3 test_task_mosquitto.py --task go_north
  python3 test_task_mosquitto.py --task go_south
  python3 test_task_mosquitto.py --task go_east
  python3 test_task_mosquitto.py --task go_west

  # Override broker
  python3 test_task_mosquitto.py --task takeoff --host 192.168.1.10 --port 1883

  # Override identity
  python3 test_task_mosquitto.py --task takeoff \\
      --tenant Hanoi --drone-id drone_01 --drone-serial SN000001
"""

import argparse
import json
import math
import ssl
import sys
import time
import uuid

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print('[ERROR] pip install paho-mqtt')
    sys.exit(1)

# ── Fill these before flying ──────────────────────────────────────────────────
HOME_LAT = 21.276178   # TODO: replace with actual field latitude
HOME_LON = 105.898707  # TODO: replace with actual field longitude

# go_to_pos target — fill before flying
GOTO_LAT = 0.0   # TODO
GOTO_LON = 0.0   # TODO
GOTO_ALT = 20.0  # m

# directional go parameters
GO_DIST_M  = 100.0  # distance to fly in each direction (m)
GO_ALT_M   = 20.0   # altitude (m)
GO_SPEED   = 5.0    # m/s

# ── Default broker ────────────────────────────────────────────────────────────
DEFAULT_HOST      = '100.104.34.77'
DEFAULT_PORT      = 1883
DEFAULT_TRANSPORT = 'tcp'
DEFAULT_WS_PATH   = ''
DEFAULT_USERNAME  = ''
DEFAULT_PASSWORD  = ''
DEFAULT_TLS       = False

# ── Default drone identity ────────────────────────────────────────────────────
DEFAULT_TENANT       = 'Hanoi'
DEFAULT_DRONE_ID     = 'drone_01'
DEFAULT_DRONE_SERIAL = 'SN-000001'


# ── Envelope builder ──────────────────────────────────────────────────────────

def make_envelope(task_id, tenant_id, drone_id, drone_serial, commands):
    return {
        'event_id':  f'evt-{uuid.uuid4()}',
        'timestamp': int(time.time() * 1000),
        'metadata': {
            'task_id':      task_id,
            'tenant_id':    tenant_id,
            'drone_id':     drone_id,
            'drone_serial': drone_serial,
            'type':         'task_command',
        },
        'payload': commands,
    }


# ── Command builders ──────────────────────────────────────────────────────────

def cmd_takeoff(seq, task_id, alt_m, speed=None):
    return {
        'sequence':     seq,
        'command_id':   f'{task_id}-{seq:03d}',
        'command_type': 'TAKE_OFF',
        'payload': {'latitude': None, 'longitude': None, 'altitude': alt_m, 'speed': speed},
    }


def cmd_goto(seq, task_id, lat, lon, alt_m, speed=5.0):
    return {
        'sequence':     seq,
        'command_id':   f'{task_id}-{seq:03d}',
        'command_type': 'GO_TO',
        'payload': {'latitude': lat, 'longitude': lon, 'altitude': alt_m, 'speed': speed},
    }


def cmd_land(seq, task_id):
    return {
        'sequence':     seq,
        'command_id':   f'{task_id}-{seq:03d}',
        'command_type': 'LAND',
        'payload': {'latitude': None, 'longitude': None, 'altitude': 0.0, 'speed': 1.0},
    }


def cmd_rth(seq, task_id, alt_m=20.0, speed=5.0):
    return {
        'sequence':     seq,
        'command_id':   f'{task_id}-{seq:03d}',
        'command_type': 'RETURN_TO_HOME',
        'payload': {'latitude': None, 'longitude': None, 'altitude': alt_m, 'speed': speed},
    }


def cmd_cancel(seq, task_id):
    return {
        'sequence':     seq,
        'command_id':   f'{task_id}-{seq:03d}',
        'command_type': 'CANCEL',
        'payload': {'latitude': None, 'longitude': None, 'altitude': None, 'speed': None},
    }


# ── Task definitions ──────────────────────────────────────────────────────────

def task_takeoff(_cfg):
    tid = 'TASK_TAKEOFF_001'
    print(f'\n[Task] TAKE_OFF → 20 m')
    return tid, [
        cmd_takeoff(1, tid, 20.0),
    ]


def task_land(_cfg):
    tid = 'TASK_LAND_001'
    print(f'\n[Task] LAND  (drone must already be airborne)')
    return tid, [
        cmd_land(1, tid),
    ]


def task_rtl(_cfg):
    tid = 'TASK_RTL_001'
    print(f'\n[Task] RETURN_TO_HOME  (drone must already be airborne)')
    return tid, [
        cmd_rth(1, tid, alt_m=20.0),
    ]


def task_cancel(_cfg):
    tid = 'TASK_CANCEL_001'
    print(f'\n[Task] CANCEL  (stop current task, hover)')
    return tid, [
        cmd_cancel(1, tid),
    ]


def task_cancel_rtl(_cfg):
    tid = 'TASK_CANCEL_RTL_001'
    print(f'\n[Task] CANCEL → RETURN_TO_HOME')
    return tid, [
        cmd_cancel(1, tid),
        cmd_rth(2, tid, alt_m=20.0),
    ]


def task_go_to_pos(_cfg):
    tid = 'TASK_GOTO_POS_001'
    if GOTO_LAT == 0.0 or GOTO_LON == 0.0:
        print('[ERROR] Fill GOTO_LAT / GOTO_LON at the top of this file before flying.')
        sys.exit(1)
    print(f'\n[Task] GO_TO position: {GOTO_LAT:.6f}, {GOTO_LON:.6f}  alt={GOTO_ALT} m')
    return tid, [
        cmd_goto(1, tid, GOTO_LAT, GOTO_LON, GOTO_ALT),
    ]


def _check_home():
    if HOME_LAT == 21.276178 and HOME_LON == 105.898707:
        print('[WARN] HOME_LAT/HOME_LON still set to placeholder — update before flying.')


def task_go_north(_cfg):
    tid = 'TASK_GO_NORTH_001'
    _check_home()
    dlat = math.degrees(GO_DIST_M / 6_371_000.0)
    target_lat = HOME_LAT + dlat
    print(f'\n[Task] GO_NORTH {GO_DIST_M:.0f} m → return home')
    print(f'       target: {target_lat:.6f}, {HOME_LON:.6f}  alt={GO_ALT_M} m')
    return tid, [
        cmd_goto(1, tid, target_lat, HOME_LON, GO_ALT_M, speed=GO_SPEED),
        cmd_goto(2, tid, HOME_LAT,   HOME_LON, GO_ALT_M, speed=GO_SPEED),
    ]


def task_go_south(_cfg):
    tid = 'TASK_GO_SOUTH_001'
    _check_home()
    dlat = math.degrees(GO_DIST_M / 6_371_000.0)
    target_lat = HOME_LAT - dlat
    print(f'\n[Task] GO_SOUTH {GO_DIST_M:.0f} m → return home')
    print(f'       target: {target_lat:.6f}, {HOME_LON:.6f}  alt={GO_ALT_M} m')
    return tid, [
        cmd_goto(1, tid, target_lat, HOME_LON, GO_ALT_M, speed=GO_SPEED),
        cmd_goto(2, tid, HOME_LAT,   HOME_LON, GO_ALT_M, speed=GO_SPEED),
    ]


def task_go_east(_cfg):
    tid = 'TASK_GO_EAST_001'
    _check_home()
    dlon = math.degrees(GO_DIST_M / (6_371_000.0 * math.cos(math.radians(HOME_LAT))))
    target_lon = HOME_LON + dlon
    print(f'\n[Task] GO_EAST {GO_DIST_M:.0f} m → return home')
    print(f'       target: {HOME_LAT:.6f}, {target_lon:.6f}  alt={GO_ALT_M} m')
    return tid, [
        cmd_goto(1, tid, HOME_LAT, target_lon, GO_ALT_M, speed=GO_SPEED),
        cmd_goto(2, tid, HOME_LAT, HOME_LON,   GO_ALT_M, speed=GO_SPEED),
    ]


def task_go_west(_cfg):
    tid = 'TASK_GO_WEST_001'
    _check_home()
    dlon = math.degrees(GO_DIST_M / (6_371_000.0 * math.cos(math.radians(HOME_LAT))))
    target_lon = HOME_LON - dlon
    print(f'\n[Task] GO_WEST {GO_DIST_M:.0f} m → return home')
    print(f'       target: {HOME_LAT:.6f}, {target_lon:.6f}  alt={GO_ALT_M} m')
    return tid, [
        cmd_goto(1, tid, HOME_LAT, target_lon, GO_ALT_M, speed=GO_SPEED),
        cmd_goto(2, tid, HOME_LAT, HOME_LON,   GO_ALT_M, speed=GO_SPEED),
    ]


TASKS = {
    'takeoff':      task_takeoff,
    'land':         task_land,
    'rtl':          task_rtl,
    'cancel':       task_cancel,
    'cancel_rtl':   task_cancel_rtl,
    'go_to_pos':    task_go_to_pos,
    'go_north':     task_go_north,
    'go_south':     task_go_south,
    'go_east':      task_go_east,
    'go_west':      task_go_west,
}


# ── MQTT client ───────────────────────────────────────────────────────────────

def build_client(args):
    client = mqtt.Client(
        client_id=f'vtp_test_{int(time.time())}',
        protocol=mqtt.MQTTv5,
        transport=args.transport,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    if args.username:
        client.username_pw_set(args.username, args.password)
    if args.transport == 'websockets':
        client.ws_set_options(path=args.ws_path)
    if args.tls:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        try:
            import certifi
            ctx.load_verify_locations(certifi.where())
        except ImportError:
            ctx.load_default_certs()
        client.tls_set_context(ctx)
    return client


def on_task_status(c, ud, msg):
    try:
        data = json.loads(msg.payload)
        md = data.get('metadata', {}) or {}
        pl = data.get('payload',  {}) or {}
        task_id = md.get('task_id', '?')
        print(
            f'  [STATUS] task={task_id}  '
            f'state={pl.get("task_status","?")}  '
            f'seq={pl.get("current_sequence","?")}  '
            f'cmd={pl.get("current_command_id","?")}  '
            f'cmd_status={pl.get("command_status","?")}  '
            f'done={pl.get("commands_done","?")}/{pl.get("commands_total","?")}'
            + (f'  reason={pl["abort_reason"]}' if pl.get('abort_reason') else '')
        )
    except Exception as e:
        print(f'  [STATUS parse error] {e}  raw={msg.payload!r}')


def connect(args):
    client = build_client(args)
    client.on_message = on_task_status
    client.connect(args.host, args.port, keepalive=30)
    client.loop_start()

    status_topic = f'drone/{args.tenant}/{args.drone_serial}/task_status'
    client.subscribe(status_topic, qos=1)
    print(f'[MQTT] {args.host}:{args.port}  transport={args.transport}  tls={args.tls}')
    print(f'       status ← {status_topic}')
    time.sleep(0.5)
    return client


def publish(client, args, envelope):
    topic = f'drone/{args.tenant}/{args.drone_serial}/task_command'
    tid   = envelope['metadata']['task_id']
    cmds  = envelope['payload']
    print(f'[PUB] {tid} → {topic}  ({len(cmds)} commands)')
    for c in cmds:
        pl   = c['payload']
        line = f'      seq={c["sequence"]}  {c["command_type"]}'
        if pl.get('latitude')  is not None: line += f'  lat={pl["latitude"]:.6f}'
        if pl.get('longitude') is not None: line += f'  lon={pl["longitude"]:.6f}'
        if pl.get('altitude')  is not None: line += f'  alt={pl["altitude"]}m'
        if pl.get('speed')     is not None: line += f'  speed={pl["speed"]}m/s'
        print(line)
    result = client.publish(topic, json.dumps(envelope), qos=1)
    result.wait_for_publish(timeout=5)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__)
    ap.add_argument('--task', required=True, choices=sorted(TASKS.keys()),
                    help='Task to publish')
    ap.add_argument('--wait',      type=float, default=120.0,
                    help='Seconds to listen for task_status after publish')
    ap.add_argument('--host',      default=DEFAULT_HOST)
    ap.add_argument('--port',      type=int, default=DEFAULT_PORT)
    ap.add_argument('--transport', default=DEFAULT_TRANSPORT,
                    choices=['tcp', 'websockets'])
    ap.add_argument('--ws-path',   default=DEFAULT_WS_PATH)
    ap.add_argument('--username',  default=DEFAULT_USERNAME)
    ap.add_argument('--password',  default=DEFAULT_PASSWORD)
    ap.add_argument('--tls',       dest='tls', action='store_true', default=DEFAULT_TLS)
    ap.add_argument('--no-tls',    dest='tls', action='store_false')
    ap.add_argument('--tenant',       default=DEFAULT_TENANT)
    ap.add_argument('--drone-id',     default=DEFAULT_DRONE_ID)
    ap.add_argument('--drone-serial', default=DEFAULT_DRONE_SERIAL)
    args = ap.parse_args()

    cfg = {}
    task_id, commands = TASKS[args.task](cfg)

    client = connect(args)
    envelope = make_envelope(
        task_id=task_id,
        tenant_id=args.tenant,
        drone_id=args.drone_id,
        drone_serial=args.drone_serial,
        commands=commands,
    )
    publish(client, args, envelope)

    print(f'[WAIT] Listening {args.wait:.0f}s for task_status...')
    try:
        time.sleep(args.wait)
    except KeyboardInterrupt:
        print('[INT] stopping')

    client.loop_stop()
    client.disconnect()
    print('[DONE]')


if __name__ == '__main__':
    main()
