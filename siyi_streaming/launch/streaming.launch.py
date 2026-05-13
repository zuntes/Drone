"""
streaming.launch.py — single entry point for SIYI gimbal streaming.

This is the only file you need to invoke. It:
  1. Locates the MediaMTX binary; runs scripts/install_mediamtx.sh if missing.
  2. Loads the YAML config shipped at share/siyi_streaming/config/mediamtx.yml.
  3. Runs MediaMTX as a child process — it dies when the launch dies.
  4. (Optional) Turns on Tailscale Funnel for the duration of the launch,
     and turns it back off on shutdown.

Usage:

    # Streams reachable inside the tailnet only:
    ros2 launch siyi_streaming streaming.launch.py

    # Streams also reachable from the public Internet:
    ros2 launch siyi_streaming streaming.launch.py funnel:=true

    # Override anything:
    ros2 launch siyi_streaming streaming.launch.py \\
        mediamtx_bin:=/opt/mediamtx/mediamtx \\
        config_file:=/etc/mediamtx-prod.yml \\
        funnel:=true
"""

import os
import shutil
import subprocess

from launch import LaunchDescription
from launch.actions import (
    DeclareLaunchArgument, ExecuteProcess, LogInfo, OpaqueFunction,
    RegisterEventHandler, Shutdown,
)
from launch.event_handlers import OnShutdown
from launch.substitutions import LaunchConfiguration

from ament_index_python.packages import get_package_share_directory


# ── Helpers ──────────────────────────────────────────────────────────────────
# Run as OpaqueFunctions so we can do filesystem work at launch time (after
# CLI args are resolved). Pure substitutions can't shell out for us.

def _resolve_mediamtx_bin(context, *args, **kwargs):
    """Find the MediaMTX binary; bootstrap it if not installed."""
    requested = LaunchConfiguration('mediamtx_bin').perform(context)

    if requested and os.path.isfile(requested) and os.access(requested, os.X_OK):
        return [LogInfo(msg=f"[siyi_streaming] Using MediaMTX at {requested}")]

    # Fall back to PATH lookup.
    found = shutil.which('mediamtx')
    if found:
        return [LogInfo(msg=f"[siyi_streaming] Using MediaMTX from PATH: {found}")]

    # Not installed — run the bundled installer once.
    pkg_share = get_package_share_directory('siyi_streaming')
    installer = os.path.join(pkg_share, 'scripts', 'install_mediamtx.sh')
    if not os.path.isfile(installer):
        raise RuntimeError(
            f"MediaMTX not found and installer missing at {installer}. "
            "Run `colcon build --packages-select siyi_streaming` and source the workspace.")

    log = [LogInfo(msg=f"[siyi_streaming] MediaMTX not found — running {installer}")]
    rc = subprocess.call(['bash', installer])
    if rc != 0:
        raise RuntimeError(f"Installer exited with status {rc}")
    log.append(LogInfo(msg="[siyi_streaming] MediaMTX installed successfully."))
    return log


def _maybe_start_funnel(context, *args, **kwargs):
    """If funnel:=true, run funnel_on.sh once (it daemonizes via --bg)."""
    funnel = LaunchConfiguration('funnel').perform(context).lower() in ('true', '1', 'yes')
    if not funnel:
        return [LogInfo(msg="[siyi_streaming] Tailscale Funnel disabled "
                            "(streams tailnet-only). Pass funnel:=true to expose publicly.")]

    pkg_share = get_package_share_directory('siyi_streaming')
    script = os.path.join(pkg_share, 'scripts', 'funnel_on.sh')
    if not os.path.isfile(script):
        return [LogInfo(msg=f"[siyi_streaming] Funnel script missing at {script} — skipping.")]

    # Run synchronously — funnel_on.sh prints the public URL, which we want
    # in the launch log. It returns quickly because --bg detaches the proxy.
    subprocess.call(['bash', script])
    return [LogInfo(msg="[siyi_streaming] Tailscale Funnel ON — see URL above.")]


def _stop_funnel(context, *args, **kwargs):
    """Best-effort Funnel teardown on shutdown."""
    funnel = LaunchConfiguration('funnel').perform(context).lower() in ('true', '1', 'yes')
    if not funnel:
        return []
    pkg_share = get_package_share_directory('siyi_streaming')
    script = os.path.join(pkg_share, 'scripts', 'funnel_off.sh')
    if os.path.isfile(script):
        subprocess.call(['bash', script])
    return [LogInfo(msg="[siyi_streaming] Tailscale Funnel turned off.")]


# ── Launch description ──────────────────────────────────────────────────────

def generate_launch_description():
    pkg_share = get_package_share_directory('siyi_streaming')
    default_config = os.path.join(pkg_share, 'config', 'mediamtx.yml')

    args = [
        DeclareLaunchArgument(
            'mediamtx_bin',
            default_value='/usr/local/bin/mediamtx',
            description='Path to the mediamtx binary. Auto-installed if missing.'),
        DeclareLaunchArgument(
            'config_file',
            default_value=default_config,
            description='MediaMTX YAML config to load.'),
        DeclareLaunchArgument(
            'funnel',
            default_value='false',
            description='Set true to enable Tailscale Funnel (public URL).'),
    ]

    # Step 1: ensure the binary exists (synchronous OpaqueFunction).
    bootstrap = OpaqueFunction(function=_resolve_mediamtx_bin)

    # Step 2: optionally turn Funnel on (also synchronous so its log appears
    # before MediaMTX's stdout floods the console).
    funnel_on = OpaqueFunction(function=_maybe_start_funnel)

    # Step 3: run MediaMTX. emulate_tty pipes its stdout through colorized
    # logging like a normal ROS2 node.
    mediamtx = ExecuteProcess(
        cmd=[LaunchConfiguration('mediamtx_bin'),
             LaunchConfiguration('config_file')],
        name='mediamtx',
        output='screen',
        emulate_tty=True,
        # If MediaMTX dies, take the whole launch down so the operator sees
        # something is wrong (rather than silently losing the video stream).
        on_exit=Shutdown(reason='MediaMTX exited'),
    )

    # Step 4: when the launch shuts down (Ctrl-C or upstream Shutdown), undo
    # the Funnel so we don't leave a public URL pointing at a dead server.
    funnel_off_on_shutdown = RegisterEventHandler(
        OnShutdown(on_shutdown=[OpaqueFunction(function=_stop_funnel)]))

    return LaunchDescription(args + [
        bootstrap,
        funnel_on,
        mediamtx,
        funnel_off_on_shutdown,
    ])
