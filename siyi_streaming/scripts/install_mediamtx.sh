#!/usr/bin/env bash
###############################################################################
# install_mediamtx.sh
#
# Installs MediaMTX into /usr/local/bin.
#
# Used automatically by:
#   ros2 launch siyi_streaming streaming.launch.py
#
# Manual usage:
#   bash install_mediamtx.sh
#
# Optional custom version:
#   MEDIAMTX_VERSION=1.18.1 bash install_mediamtx.sh
#
# Supports:
#   - x86_64
#   - Jetson / ARM64
#   - ARMv7
#
###############################################################################

set -euo pipefail

###############################################################################
# Default version
###############################################################################

VERSION="${MEDIAMTX_VERSION:-1.18.1}"

###############################################################################
# Detect architecture
###############################################################################

case "$(uname -m)" in
  x86_64)
    ARCH="linux_amd64"
    ;;

  aarch64 | arm64)
    ARCH="linux_arm64"
    ;;

  armv7l)
    ARCH="linux_armv7"
    ;;

  *)
    echo "Unsupported architecture: $(uname -m)"
    exit 1
    ;;
esac

echo "Detected $(uname -m) → MediaMTX release: ${ARCH}"

###############################################################################
# Temp workspace
###############################################################################

TMP=$(mktemp -d)
trap "rm -rf $TMP" EXIT

cd "$TMP"

###############################################################################
# Download
###############################################################################

URL="https://github.com/bluenviron/mediamtx/releases/download/v${VERSION}/mediamtx_v${VERSION}_${ARCH}.tar.gz"

echo "Downloading MediaMTX v${VERSION} from GitHub..."
echo "URL: $URL"

curl -fL -o mediamtx.tar.gz "$URL"

###############################################################################
# Extract
###############################################################################

tar xzf mediamtx.tar.gz

###############################################################################
# Install
###############################################################################

echo "Installing /usr/local/bin/mediamtx..."

sudo install -m 0755 mediamtx /usr/local/bin/mediamtx

###############################################################################
# Verify
###############################################################################

echo
echo "✓ MediaMTX installed successfully."
echo

/usr/local/bin/mediamtx --version

echo
echo "Launch with:"
echo "ros2 launch siyi_streaming streaming.launch.py"
echo