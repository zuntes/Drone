"""Launch telemetry_node only (PX4 → ROS2 internal topics)."""
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import Node
from launch_ros.substitutions import FindPackageShare


def generate_launch_description():
    params_file = PathJoinSubstitution(
        [FindPackageShare('drone_bringup'), 'config', 'params.yaml'])

    return LaunchDescription([
        DeclareLaunchArgument('telemetry_rate_hz', default_value='5.0'),
        DeclareLaunchArgument('status_rate_hz',    default_value='1.0'),
        DeclareLaunchArgument('alarm_rate_hz',     default_value='0.2'),

        Node(
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
        ),
    ])
