""" Full system launch — starts both telemetry_node and mqtt_bridge_node."""
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import Node
from launch_ros.substitutions import FindPackageShare


def generate_launch_description():
    params_file = PathJoinSubstitution(
        [FindPackageShare('drone_bringup'), 'config', 'params.yaml'])

    args = [
        # Identity (mqtt_bridge_node)
        DeclareLaunchArgument('tenant_id',         default_value='Hanoi'),
        DeclareLaunchArgument('drone_id',           default_value='drone_01'),
        DeclareLaunchArgument('drone_serial',       default_value='SN-000001'),
        # MQTT connection (mqtt_bridge_node)
        DeclareLaunchArgument('mqtt_host',          default_value='100.104.34.77'),
        DeclareLaunchArgument('mqtt_port',          default_value='1883'),
        DeclareLaunchArgument('mqtt_username',      default_value=''),
        DeclareLaunchArgument('mqtt_password',      default_value=''),
        DeclareLaunchArgument('mqtt_tls',           default_value='false'),
        DeclareLaunchArgument('mqtt_transport',     default_value='tcp'),
        DeclareLaunchArgument('mqtt_ws_path',       default_value=''),

        # Publish rates (telemetry_node)
        DeclareLaunchArgument('telemetry_rate_hz',  default_value='5.0'),
        DeclareLaunchArgument('status_rate_hz',     default_value='1.0'),
        DeclareLaunchArgument('alarm_rate_hz',      default_value='0.2'),
    ]

    telemetry = Node(
        package='telemetry_node',
        executable='telemetry_node',
        name='telemetry_node',
        output='screen',
        parameters=[
            params_file,
            {
                'telemetry_rate_hz': LaunchConfiguration('telemetry_rate_hz'),
                'status_rate_hz':    LaunchConfiguration('status_rate_hz'),
                'alarm_rate_hz':     LaunchConfiguration('alarm_rate_hz'),
            },
        ],
    )

    bridge = Node(
        package='mqtt_bridge',
        executable='mqtt_bridge_node',
        name='mqtt_bridge_node',
        output='screen',
        parameters=[
            params_file,
            {
                'tenant_id':    LaunchConfiguration('tenant_id'),
                'drone_id':     LaunchConfiguration('drone_id'),
                'drone_serial': LaunchConfiguration('drone_serial'),
                'mqtt_host':    LaunchConfiguration('mqtt_host'),
                'mqtt_port':    LaunchConfiguration('mqtt_port'),
                'mqtt_username':LaunchConfiguration('mqtt_username'),
                'mqtt_password':LaunchConfiguration('mqtt_password'),
                'mqtt_tls':     LaunchConfiguration('mqtt_tls'),
                'mqtt_transport':LaunchConfiguration('mqtt_transport'),
                'mqtt_ws_path': LaunchConfiguration('mqtt_ws_path'),
            },
        ],
    )

    return LaunchDescription(args + [telemetry, bridge])