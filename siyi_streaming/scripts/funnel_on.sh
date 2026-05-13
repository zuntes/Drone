#!/usr/bin/env bash
###############################################################################
# funnel_on.sh — expose this machine's MediaMTX HLS endpoint to the public
# Internet via Tailscale Funnel.
#
# After this script:
#   - Anyone with the URL can watch — no Tailscale on their side.
#   - Traffic is HTTPS, valid Let's Encrypt cert provisioned by Tailscale.
#   - Funnel handles HTTP(S) only, so we expose HLS (port 8888). WebRTC keeps
#     working for tailnet operators; it just isn't reachable from outside.
#
# Latency on the public link: ~1.5–3 s with the LL-HLS settings in
# config/mediamtx.yml.
#
# Called automatically by streaming.launch.py when `funnel:=true` is passed.
# Safe to run by hand too.
###############################################################################
set -euo pipefail

FUNNEL_PORT="${FUNNEL_PORT:-443}"   # Funnel allows only 443, 8443, 10000
HLS_PORT="${HLS_PORT:-8888}"

if ! command -v tailscale >/dev/null 2>&1; then
  echo "ERROR: tailscale CLI not found."; exit 1
fi

# This machine's public DNS name on the tailnet, e.g. drone-01.tailxyz.ts.net
DNS_NAME=$(tailscale status --json 2>/dev/null \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print(d['Self']['DNSName'].rstrip('.'))" \
  || true)
if [ -z "$DNS_NAME" ]; then
  echo "ERROR: Tailscale not logged in. Run: tailscale up"; exit 1
fi

echo "Enabling Funnel: https://${DNS_NAME}/  →  http://localhost:${HLS_PORT}/"
sudo tailscale funnel --bg --https=${FUNNEL_PORT} http://localhost:${HLS_PORT}

PUBLIC_BASE="https://${DNS_NAME}"
[ "$FUNNEL_PORT" = "443" ] || PUBLIC_BASE="${PUBLIC_BASE}:${FUNNEL_PORT}"

cat <<EOF

========================================================================
  ✓ Public URL is live. Share it:
------------------------------------------------------------------------
  HLS playlist (works in VLC, ffplay, any HLS player):
    ${PUBLIC_BASE}/main/index.m3u8

  Quick test:
    ffplay -fflags nobuffer ${PUBLIC_BASE}/main/index.m3u8

  Stop public exposure:
    bash funnel_off.sh   # or just Ctrl-C the launch
========================================================================
EOF
