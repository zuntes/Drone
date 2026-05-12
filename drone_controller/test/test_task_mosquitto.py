#!/usr/bin/env python3
"""
test_task_publish.py
VTP Robotics — MQTT task test publisher (EMQX-ready)

Publishes tasks to the mqtt_bridge over MQTT using the current envelope:
  metadata.task_id, tenant_id, drone_id, drone_serial, type="command"
  payload = [ { sequence, command_id, command_type, payload: {...} } ]

Subscribes to task_status to show progress back.

Default broker:
  wss://dev-lae-mqtt.viettelpost.vn:443/mqtt   (EMQX via HAProxy)

SITL home (matches Gazebo spherical_coordinates in default.sdf):
  lat=21.276178  lon=105.898707  elevation=10.0 m  (Hanoi)

Usage:
  # Individual tasks
  python3 test_task_publish.py --task takeoff
  python3 test_task_publish.py --task goto_north
  python3 test_task_publish.py --task goto_home
  python3 test_task_publish.py --task rth
  python3 test_task_publish.py --task land

  # Long-range tasks (require Gazebo ground plane >= 10 km x 10 km)
  python3 test_task_publish.py --task goto_3km --wait 600
  python3 test_task_publish.py --task goto_3km_and_land --wait 600

  # CANCEL pre-emption
  python3 test_task_publish.py --task cancel_only
  python3 test_task_publish.py --task cancel_then_goto_home

  # Combined mission
  python3 test_task_publish.py --task combined

  # Negative tests
  python3 test_task_publish.py --task error_no_altitude
  python3 test_task_publish.py --task error_wrong_tenant
  python3 test_task_publish.py --task error_wrong_serial

  # Override broker (local EMQX)
  python3 test_task_publish.py --task takeoff \\
      --host localhost --port 1883 --transport tcp --no-tls

  # Override identity
  python3 test_task_publish.py --task takeoff \\
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

# ── SITL home (matches Gazebo spherical_coordinates in default.sdf) ──────────
HOME_LAT = 21.276178
HOME_LON = 105.898707

FORWARD_M = 50.0
NORTH_LAT = HOME_LAT + math.degrees(FORWARD_M / 6_371_000.0)
NORTH_LON = HOME_LON  # due north

LONG_FORWARD_M = 1200.0
LONG_NORTH_LAT = HOME_LAT + math.degrees(LONG_FORWARD_M / 6_371_000.0)
LONG_NORTH_LON = HOME_LON  # due north

# ── Default broker (EMQX via HAProxy) ────────────────────────────────────────
DEFAULT_HOST      = '100.104.34.77'
DEFAULT_PORT      = 1883
DEFAULT_TRANSPORT = 'tcp'
DEFAULT_WS_PATH   = ''
DEFAULT_USERNAME  = ''
DEFAULT_PASSWORD  = ''
DEFAULT_TLS       = False

# ── Default drone identity (must match bridge config) ────────────────────────
DEFAULT_TENANT       = 'Hanoi'
DEFAULT_DRONE_ID     = 'drone_01'
DEFAULT_DRONE_SERIAL = 'SN-000001'


# ── Envelope builder ─────────────────────────────────────────────────────────

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


# ── Command builders (new simplified payload format) ─────────────────────────

def takeoff(seq, task_id, alt_m, speed=None):
    return {
        'sequence':     seq,
        'command_id':   f'{task_id}-{seq:03d}',
        'command_type': 'TAKE_OFF',
        'payload': {
            'latitude':  None,
            'longitude': None,
            'altitude':  alt_m,
            'speed':     speed,
        },
    }


def goto(seq, task_id, lat, lon, alt_m, speed=5.0):
    return {
        'sequence':     seq,
        'command_id':   f'{task_id}-{seq:03d}',
        'command_type': 'GO_TO',
        'payload': {
            'latitude':  lat,
            'longitude': lon,
            'altitude':  alt_m,
            'speed':     speed,
        },
    }


def cancel(seq, task_id):
    return {
        'sequence':     seq,
        'command_id':   f'{task_id}-{seq:03d}',
        'command_type': 'CANCEL',
        'payload': {
            'latitude': None, 'longitude': None,
            'altitude': None, 'speed': None,
        },
    }


def rth(seq, task_id, alt_m=50.0, speed=5.0):
    return {
        'sequence':     seq,
        'command_id':   f'{task_id}-{seq:03d}',
        'command_type': 'RETURN_TO_HOME',
        'payload': {
            'latitude':  None,
            'longitude': None,
            'altitude':  alt_m,
            'speed':     speed,
        },
    }


def land(seq, task_id):
    return {
        'sequence':     seq,
        'command_id':   f'{task_id}-{seq:03d}',
        'command_type': 'LAND',
        'payload': {
            'latitude':  None,
            'longitude': None,
            'altitude':  0.0,
            'speed':     1.0,
        },
    }


# ── Task presets ─────────────────────────────────────────────────────────────

def task_takeoff(cfg):
    tid = 'TASK_TAKEOFF_001'
    print(f'\n[Task] TAKE_OFF 20 m → hold')
    return tid, [takeoff(1, tid, 20.0)]


def task_goto_north(cfg):
    tid = 'TASK_GOTO_NORTH_001'
    print(f'\n[Task] TAKE_OFF 30 m → GO_TO {FORWARD_M} m north')
    print(f'       target: {NORTH_LAT:.6f}, {NORTH_LON:.6f}')
    return tid, [
        takeoff(1, tid, 30.0),
        goto(2, tid, NORTH_LAT, NORTH_LON, 30.0, speed=5.0),
    ]


def task_goto_home(cfg):
    tid = 'TASK_GOTO_HOME_001'
    print(f'\n[Task] GO_TO home  (expects drone already airborne)')
    print(f'       target: {HOME_LAT:.6f}, {HOME_LON:.6f}')
    return tid, [
        goto(1, tid, HOME_LAT, HOME_LON, 30.0, speed=5.0),
    ]


def task_cancel_only(cfg):
    tid = 'TASK_CANCEL_001'
    print(f'\n[Task] CANCEL  (stop running task, hover)')
    return tid, [cancel(1, tid)]


def task_cancel_then_goto_home(cfg):
    tid = 'TASK_CANCEL_GOTO_001'
    print(f'\n[Task] CANCEL → GO_TO home  (pre-empt then fly home)')
    print(f'       home: {HOME_LAT:.6f}, {HOME_LON:.6f}')
    return tid, [
        cancel(1, tid),
        goto(2, tid, HOME_LAT, HOME_LON, 30.0, speed=5.0),
    ]


def task_rth(cfg):
    tid = 'TASK_RTH_001'
    print(f'\n[Task] RETURN_TO_HOME  (expects drone already airborne)')
    return tid, [rth(1, tid, alt_m=50.0, speed=5.0)]


def task_land(cfg):
    tid = 'TASK_LAND_001'
    print(f'\n[Task] LAND  (expects drone already airborne)')
    return tid, [land(1, tid)]


def task_combined(cfg):
    tid = 'TASK_COMBINED_001'
    print(f'\n[Task] COMBINED: TAKE_OFF → GO_TO north → GO_TO home → RTH')
    print(f'       north target: {NORTH_LAT:.6f}, {NORTH_LON:.6f}')
    print(f'       home: {HOME_LAT:.6f}, {HOME_LON:.6f}')
    return tid, [
        takeoff(1, tid, 40.0),
        goto(2, tid, NORTH_LAT, NORTH_LON, 40.0, speed=6.0),
        goto(3, tid, HOME_LAT, HOME_LON, 40.0, speed=6.0),
        rth(4, tid, alt_m=50.0, speed=5.0),
    ]


def task_goto_3km(cfg):
    tid = 'TASK_GOTO_3km_001'
    print(f'\n[Task] TAKE_OFF 40 m → GO_TO {LONG_FORWARD_M:.0f} m north')
    print(f'       target: {LONG_NORTH_LAT:.6f}, {LONG_NORTH_LON:.6f}')
    print(f'       WARNING: requires Gazebo ground plane >= 10 km x 10 km')
    return tid, [
        takeoff(1, tid, 40.0),
        goto(2, tid, LONG_NORTH_LAT, LONG_NORTH_LON, 40.0, speed=10.0),
    ]


def task_goto_3km_and_land(cfg):
    tid = 'TASK_GOTO_3km_LAND_001'
    print(f'\n[Task] TAKE_OFF 40 m → GO_TO 4 km north → LAND')
    print(f'       target: {LONG_NORTH_LAT:.6f}, {LONG_NORTH_LON:.6f}')
    print(f'       WARNING: requires Gazebo ground plane >= 10 km x 10 km')
    return tid, [
        takeoff(1, tid, 40.0),
        goto(2, tid, LONG_NORTH_LAT, LONG_NORTH_LON, 40.0, speed=10.0),
        land(3, tid),
    ]


# ── Negative tests ───────────────────────────────────────────────────────────

def task_error_no_altitude(cfg):
    tid = 'TASK_ERR_ALT_001'
    print(f'\n[Test] TAKE_OFF with altitude=null → controller should REJECT (FAILED)')
    return tid, [{
        'sequence':     1,
        'command_id':   f'{tid}-001',
        'command_type': 'TAKE_OFF',
        'payload': {
            'latitude': None, 'longitude': None,
            'altitude': None, 'speed': 0.0,
        },
    }]


def task_error_wrong_tenant(cfg):
    """Bridge should reject with FAILED because tenant_id mismatches."""
    tid = 'TASK_ERR_TENANT_001'
    print(f'\n[Test] tenant_id mismatch → bridge should REJECT (FAILED)')
    cfg['override_tenant'] = 'WRONG_TENANT'
    return tid, [takeoff(1, tid, 20.0)]


def task_error_wrong_serial(cfg):
    """Bridge should reject with FAILED because drone_serial mismatches."""
    tid = 'TASK_ERR_SERIAL_001'
    print(f'\n[Test] drone_serial mismatch → bridge should REJECT (FAILED)')
    cfg['override_serial'] = 'SN_WRONG'
    return tid, [takeoff(1, tid, 20.0)]


TASKS = {
    'takeoff':                task_takeoff,
    'goto_north':             task_goto_north,
    'goto_home':              task_goto_home,
    'cancel_only':            task_cancel_only,
    'cancel_then_goto_home':  task_cancel_then_goto_home,
    'rth':                    task_rth,
    'land':                   task_land,
    'combined':               task_combined,
    'goto_3km':               task_goto_3km,
    'goto_3km_and_land':      task_goto_3km_and_land,
    'error_no_altitude':      task_error_no_altitude,
    'error_wrong_tenant':     task_error_wrong_tenant,
    'error_wrong_serial':     task_error_wrong_serial,
}


# ── MQTT client ──────────────────────────────────────────────────────────────

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

    # Subscribe to task_status for the bridge we're targeting
    status_topic = f'drone/{args.tenant}/{args.drone_serial}/task_status'
    client.subscribe(status_topic, qos=1)
    print(f'[MQTT] {args.host}:{args.port}  transport={args.transport}  '
          f'tls={args.tls}')
    print(f'       status ← {status_topic}')
    time.sleep(0.5)
    return client


def publish(client, args, cfg, envelope):
    tenant = cfg.get('override_tenant', args.tenant)
    serial = cfg.get('override_serial', args.drone_serial)
    cmd_topic = f'drone/{tenant}/{serial}/task_command'

    # If we're deliberately testing wrong identity, still publish to the
    # bridge's real topic so the bridge receives and rejects it.
    real_topic = f'drone/{args.tenant}/{args.drone_serial}/task_command'
    pub_topic = real_topic
    if cfg.get('override_tenant') or cfg.get('override_serial'):
        # keep topic at real bridge topic; envelope metadata carries wrong id
        envelope['metadata']['tenant_id']    = tenant
        envelope['metadata']['drone_serial'] = serial

    tid  = envelope['metadata']['task_id']
    cmds = envelope['payload']
    print(f'[PUB] {tid} → {pub_topic}  ({len(cmds)} commands)')
    for c in cmds:
        pl = c['payload']
        line = f'      seq={c["sequence"]}  {c["command_type"]}'
        if pl.get('latitude')  is not None: line += f'  lat={pl["latitude"]:.6f}'
        if pl.get('longitude') is not None: line += f'  lon={pl["longitude"]:.6f}'
        if pl.get('altitude')  is not None: line += f'  alt={pl["altitude"]}m'
        if pl.get('speed')     is not None: line += f'  speed={pl["speed"]}m/s'
        print(line)

    result = client.publish(pub_topic, json.dumps(envelope), qos=1)
    result.wait_for_publish(timeout=5)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__)
    ap.add_argument('--task', required=True, choices=sorted(TASKS.keys()),
                    help='Task preset to publish')
    ap.add_argument('--wait', type=float, default=120.0,
                    help='Seconds to listen for task_status after publish')
    ap.add_argument('--host',         default=DEFAULT_HOST)
    ap.add_argument('--port',         type=int, default=DEFAULT_PORT)
    ap.add_argument('--transport',    default=DEFAULT_TRANSPORT,
                    choices=['tcp', 'websockets'])
    ap.add_argument('--ws-path',      default=DEFAULT_WS_PATH)
    ap.add_argument('--username',     default=DEFAULT_USERNAME)
    ap.add_argument('--password',     default=DEFAULT_PASSWORD)
    ap.add_argument('--tls',          dest='tls', action='store_true',
                    default=DEFAULT_TLS)
    ap.add_argument('--no-tls',       dest='tls', action='store_false')
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
    publish(client, args, cfg, envelope)

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