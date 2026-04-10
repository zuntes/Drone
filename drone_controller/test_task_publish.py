#!/usr/bin/env python3
"""
test_task_publish.py
VTP Robotics — MQTT task test publisher

SITL home from vehicle_gps_position:
  lat=37.412173210128394  lon=-121.99887844299644  alt_msl=38.175m

Task 1: TAKEOFF 50m → GO_TO 50m north → LAND
Task 2: TAKEOFF 50m → GO_TO back to home → LAND

Usage:
  python3 test_task_publish.py --task 1
  python3 test_task_publish.py --task 2
  python3 test_task_publish.py --error-test   # missing altitude → ABORTED
  python3 test_task_publish.py --host 100.x.x.x  # remote broker
"""

import argparse
import json
import math
import time
import sys

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print('[ERROR] pip install paho-mqtt')
    sys.exit(1)

# ── SITL home ────────────────────────────────────────────────────────────────
HOME_LAT  = 37.412173210128394
HOME_LON  = -121.99887844299644
HOME_AMSL = 38.175

FORWARD_M = 50.0
NORTH_LAT = HOME_LAT + math.degrees(FORWARD_M / 6_371_000.0)
NORTH_LON = HOME_LON   # due north

# ── Config ───────────────────────────────────────────────────────────────────
MQTT_HOST = '100.104.34.77'
MQTT_PORT = 1883
TENANT_ID = 'Hanoi'
DRONE_ID  = 'drone_01'
CMD_TOPIC = f'drone/{TENANT_ID}/{DRONE_ID}/task_command'
STA_TOPIC = f'drone/{TENANT_ID}/{DRONE_ID}/task_status'


def ts() -> str:
    return str(int(time.time() * 1000))


def make_task(task_id, commands):
    return {
        "header": {
            "task_id":          task_id,
            "tenant_id":        TENANT_ID,
            "drone_id":         DRONE_ID,
            "drone_serial":     "SN000001",
            "type":             "task_command",
            "protocol_version": "1.0",
            "timestamp":        ts(),
        },
        "payloads": commands,
    }


def takeoff_cmd(seq, task_id, alt_m):
    return {
        "sequence":     seq,
        "command_id":   f'{task_id}-{seq:03d}',
        "command_type": "TAKEOFF",
        "payload": {
            "latitude": 0.0, "longitude": 0.0,
            "altitude_m": alt_m, "altitude_ref": "home",
            "speed_ms": 0.0, "arrival_radius_m": 0.0,
            "has_position": False, "has_altitude": True,
            "has_speed": False, "has_arrival_radius": False,
        },
    }


def goto_cmd(seq, task_id, lat, lon, alt_m, speed=5.0, arrival_r=2.0):
    return {
        "sequence":     seq,
        "command_id":   f'{task_id}-{seq:03d}',
        "command_type": "GO_TO",
        "payload": {
            "latitude": lat, "longitude": lon,
            "altitude_m": alt_m, "altitude_ref": "home",
            "speed_ms": speed, "arrival_radius_m": arrival_r,
            "has_position": True, "has_altitude": True,
            "has_speed": True, "has_arrival_radius": True,
        },
    }


def land_cmd(seq, task_id):
    return {
        "sequence":     seq,
        "command_id":   f'{task_id}-{seq:03d}',
        "command_type": "LAND",
        "payload": {
            "latitude": 0.0, "longitude": 0.0,
            "altitude_m": 0.0, "altitude_ref": "",
            "speed_ms": 0.0, "arrival_radius_m": 0.0,
            "has_position": False, "has_altitude": False,
            "has_speed": False, "has_arrival_radius": False,
        },
    }


def task_forward():
    tid = "TASK_FWD_001"
    print(f'\nTask 1: TAKEOFF 50m → GO_TO {FORWARD_M}m north → LAND')
    print(f'  Home:   {HOME_LAT:.6f}, {HOME_LON:.6f}')
    print(f'  Target: {NORTH_LAT:.6f}, {NORTH_LON:.6f}')
    return make_task(tid, [
        takeoff_cmd(1, tid, 50.0),
        goto_cmd(2, tid, NORTH_LAT, NORTH_LON, 50.0, speed=5.0, arrival_r=2.0),
        land_cmd(3, tid),
    ])


def task_backward():
    tid = "TASK_BCK_001"
    print(f'\nTask 2: TAKEOFF 50m → GO_TO home → LAND')
    print(f'  From:   {NORTH_LAT:.6f}, {NORTH_LON:.6f}')
    print(f'  Home:   {HOME_LAT:.6f}, {HOME_LON:.6f}')
    return make_task(tid, [
        takeoff_cmd(1, tid, 50.0),
        goto_cmd(2, tid, HOME_LAT, HOME_LON, 50.0, speed=5.0, arrival_r=2.0),
        land_cmd(3, tid),
    ])


def task_error():
    tid = "TASK_ERR_001"
    print(f'\n[TEST] Error task: TAKEOFF with no altitude → should ABORT')
    return make_task(tid, [{
        "sequence": 1,
        "command_id": f'{tid}-001',
        "command_type": "TAKEOFF",
        "payload": {
            "latitude": 0.0, "longitude": 0.0,
            "altitude_m": 0.0, "altitude_ref": "",
            "speed_ms": 0.0, "arrival_radius_m": 0.0,
            "has_position": False, "has_altitude": False,  # ← missing!
            "has_speed": False, "has_arrival_radius": False,
        },
    }])


def connect(host, port):
    client = mqtt.Client(client_id='vtp_test', protocol=mqtt.MQTTv311)
    client.connect(host, port, keepalive=30)
    client.loop_start()

    def on_message(c, ud, msg):
        try:
            data = json.loads(msg.payload)
            pl = data.get('payloads', data)
            print(f'  [STATUS] task={pl.get("task_status","?")}  '
                  f'seq={pl.get("current_sequence","?")}  '
                  f'cmd={pl.get("current_command_id","?")}  '
                  f'cmd_status={pl.get("command_status","?")}  '
                  f'wp={pl.get("waypoints_done","?")}/{pl.get("waypoints_total","?")}'
                  + (f'  reason={pl["abort_reason"]}' if pl.get('abort_reason') else ''))
        except Exception as e:
            print(f'  [STATUS parse error] {e}')

    client.subscribe(STA_TOPIC, qos=1)
    client.on_message = on_message
    print(f'[MQTT] {host}:{port}  cmd→{CMD_TOPIC}  status←{STA_TOPIC}')
    time.sleep(0.3)
    return client


def pub(client, task):
    tid  = task['header']['task_id']
    result = client.publish(CMD_TOPIC, json.dumps(task), qos=1)
    result.wait_for_publish(timeout=5)
    cmds = task['payloads']
    print(f'[PUB] {tid}  ({len(cmds)} commands):')
    for c in cmds:
        pl = c['payload']
        line = f'  seq={c["sequence"]}  {c["command_type"]}'
        if pl['has_position']:
            line += f'  lat={pl["latitude"]:.6f} lon={pl["longitude"]:.6f}'
        if pl['has_altitude']:
            line += f'  alt={pl["altitude_m"]}m ({pl["altitude_ref"]})'
        if pl['has_speed']:
            line += f'  speed={pl["speed_ms"]}m/s'
        print(line)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--host',       default=MQTT_HOST)
    ap.add_argument('--port',       type=int, default=MQTT_PORT)
    ap.add_argument('--task',       type=int, default=0,
                    help='1=forward 2=backward 0=both')
    ap.add_argument('--error-test', action='store_true')
    args = ap.parse_args()

    client = connect(args.host, args.port)

    if args.error_test:
        pub(client, task_error())
        time.sleep(5)
    elif args.task == 1:
        pub(client, task_forward())
        print('Waiting 120s for completion...')
        time.sleep(120)
    elif args.task == 2:
        pub(client, task_backward())
        print('Waiting 120s for completion...')
        time.sleep(120)
    else:
        pub(client, task_forward())
        print('\nTask 1 sent. Press Enter when drone has landed before sending Task 2...')
        try:
            input()
        except EOFError:
            time.sleep(90)
        time.sleep(2)
        pub(client, task_backward())
        print('Waiting 120s...')
        time.sleep(120)

    print('[DONE]')
    client.loop_stop()
    client.disconnect()


if __name__ == '__main__':
    main()
