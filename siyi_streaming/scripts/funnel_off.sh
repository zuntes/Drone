#!/usr/bin/env bash
# Turn off public exposure. Streams remain reachable inside the tailnet.
set -euo pipefail
sudo tailscale funnel --https=443 off 2>/dev/null || true
sudo tailscale funnel reset 2>/dev/null || true
echo "✓ Funnel off. Streams are tailnet-only again."
