# siyi_streaming

ROS2 package that runs a MediaMTX-based video streaming server for the SIYI
A8 mini gimbal. Pulls H.265 from the camera over RTSP and republishes it as
RTSP, WebRTC, and HLS. Optionally exposes a public HTTPS URL via Tailscale
Funnel — anyone with that link can watch in a browser without installing
Tailscale.

Works the same on Jetson, Pi 5, and x86_64 laptops because MediaMTX is a
single Go binary with zero dependencies, and H.265 passthrough means
nothing on the host has to encode anything.

---

## Build and run

```bash
# Build (same as any other package in the workspace)
cd ~/your_ws
colcon build --packages-select siyi_streaming
source install/setup.bash

# Run — first launch triggers a one-time download of the MediaMTX binary
ros2 launch siyi_streaming streaming.launch.py
```

On first launch, `scripts/install_mediamtx.sh` runs automatically if
`/usr/local/bin/mediamtx` doesn't exist. It downloads the right binary for
your CPU (arm64 / arm / x86_64).

---

## How other machines see the stream

You have **three URL types** depending on where the viewer is sitting. The
launch tells you exactly which ones to use.

### 1. Inside the tailnet (operators with Tailscale installed)

This is the default and the fastest path. Replace `<host>` with the Tailscale
name of the machine running `streaming.launch.py`:

```bash
# What's my Tailscale name?
tailscale status
# Look at the first line: <hostname>.<tailnet>.ts.net
# Example: drone-jetson-01.tail9abc.ts.net
```

Then any device on the same tailnet (Windows, Mac, Linux, phone with
Tailscale app) can open:

| What                    | URL                                                 | Latency |
|-------------------------|-----------------------------------------------------|---------|
| **Browser, low latency**| `http://<host>:8889/main`                           | ~300 ms |
| **Browser, simple**     | `http://<host>:8888/main/`                          | ~2 s    |
| **VLC / ffplay**        | `rtsp://<host>:8554/main`                           | <100 ms |

Concrete example. If the streaming machine shows up in `tailscale status` as
`drone-jetson-01.tail9abc.ts.net`, an operator with Tailscale opens:

```
http://drone-jetson-01.tail9abc.ts.net:8889/main
```

Tailscale handles the routing and encryption — no port forwarding, no public IP.

### 2. Same LAN (no Tailscale needed)

If the viewer is on the same WiFi/Ethernet as the streaming machine, use the
LAN IP instead of the tailnet name:

```bash
# Find the LAN IP on the streaming machine
ip -4 addr show | grep inet
# Example: 192.168.1.42
```

Then anywhere on that LAN:

```
http://192.168.1.42:8889/main          → WebRTC in browser
http://192.168.1.42:8888/main/         → HLS in browser
rtsp://192.168.1.42:8554/main          → VLC
```

### 3. Public Internet (no Tailscale, anywhere in the world)

Launch with `funnel:=true`:

```bash
ros2 launch siyi_streaming streaming.launch.py funnel:=true
```

The launch prints something like:

```
========================================================================
  ✓ Public URL is live. Share it:
------------------------------------------------------------------------
  HLS playlist (works in VLC, ffplay, any HLS player):
    https://drone-jetson-01.tail9abc.ts.net/main/index.m3u8

  Quick test:
    ffplay -fflags nobuffer https://drone-jetson-01.tail9abc.ts.net/main/index.m3u8
========================================================================
```

That URL works from any browser, anywhere — the viewer needs **nothing
installed**. Open the link, video plays. To make it nicer-looking, host the
included `web/viewer.html` somewhere (or open it locally as a file) — the
viewer page auto-loads `<same-origin>/main/index.m3u8`.

Latency on the public link is ~1.5–3 s. WebRTC does not work through Funnel
(it needs UDP) so the public URL is HLS only. Operators who need sub-second
latency should join your tailnet.

---

## Where the URLs come from

The `mediamtx.yml` file decides the **path slug** (`main` in all the URLs
above). Want a different name?

```yaml
# config/mediamtx.yml
paths:
  rear_cam:                       # changing "main" → "rear_cam"
    source: rtsp://...
```

After rebuild, all the URLs become `/rear_cam/` instead of `/main/`. You can
have several paths at the same time and switch by URL:

```yaml
paths:
  main:    { source: rtsp://192.168.144.25:8554/main.264 }
  rear:    { source: rtsp://192.168.144.30:8554/main.264 }
```

→ `http://<host>:8889/main` and `http://<host>:8889/rear` both work.

---

## Changing the camera source

Edit `config/mediamtx.yml`, change `paths.main.source`, rebuild and relaunch:

```bash
colcon build --packages-select siyi_streaming
ros2 launch siyi_streaming streaming.launch.py
```

Anything MediaMTX understands works as `source:`

```yaml
source: rtsp://192.168.144.25:8554/main.264     # SIYI gimbal
source: rtsp://user:pass@cam.local/stream1       # generic IP camera
source: rtmp://feeder.local/live/key             # someone pushing RTMP
source: publisher                                # accept ffmpeg/OBS pushes
```

---

## When H.265 won't play in a viewer's browser

Safari, iOS, macOS Chrome, and most desktops with hardware HEVC decode play
H.265 HLS fine. Firefox and some Android browsers can't.

Two options:
1. Tell viewers to use Chrome or Safari.
2. Uncomment the `runOnReady` ffmpeg block in `config/mediamtx.yml` to add a
   parallel H.264 path at `/main_h264`. Requires `sudo apt install ffmpeg`.
   Cheap on laptop/Jetson; ~2 cores at 720p on Pi 5 (no H.264 hardware
   encoder) so drop to 480p or 15 fps if it struggles.

---

## What the launch file does

| Step | Action |
|------|--------|
| 1 | Locate `mediamtx` binary; auto-install via `install_mediamtx.sh` if missing |
| 2 | (Optional) Turn on Tailscale Funnel — prints the public URL |
| 3 | Exec MediaMTX with `config/mediamtx.yml` from the package share dir |
| 4 | On Ctrl-C / shutdown, turn Funnel back off |

If MediaMTX crashes, the launch shuts down so failures are visible.

---

## Launch arguments

```bash
# Streams reachable inside your tailnet only (default):
ros2 launch siyi_streaming streaming.launch.py

# Add the public URL via Tailscale Funnel:
ros2 launch siyi_streaming streaming.launch.py funnel:=true

# Override the binary or config path:
ros2 launch siyi_streaming streaming.launch.py \
    mediamtx_bin:=/opt/mediamtx/mediamtx \
    config_file:=/etc/mediamtx-prod.yml
```

---

## Files

```
siyi_streaming/
├── package.xml
├── setup.py
├── setup.cfg
├── resource/siyi_streaming        ament marker
├── siyi_streaming/__init__.py     empty — no Python nodes
├── launch/streaming.launch.py     the one entry point
├── config/mediamtx.yml            source URLs, ports, HLS tuning
├── scripts/
│   ├── install_mediamtx.sh        cross-arch installer
│   ├── funnel_on.sh               public URL on
│   └── funnel_off.sh              public URL off
├── web/viewer.html                browser player (HLS + hls.js fallback)
└── README.md
```

---

## Troubleshooting

```bash
# Is the camera reachable?
ffprobe -rtsp_transport tcp rtsp://192.168.144.25:8554/main.264

# Did MediaMTX bind its ports?
ss -tln | grep -E '8554|8888|8889'

# What's my Tailscale URL?
tailscale status

# Funnel state
tailscale funnel status
```
