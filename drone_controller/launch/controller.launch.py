from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    return LaunchDescription([
        DeclareLaunchArgument('gcs_conn', default_value='udp:127.0.0.1:14550',
            description='MAVLink GCS connection string. '
                        'SITL: udp:127.0.0.1:14550  '
                        'Pixhawk USB: /dev/ttyACM0,57600  '
                        'Jetson UART: /dev/ttyTHS1,921600'),
        DeclareLaunchArgument('disable_rc_failsafe', default_value='true',
            description='Set COM_RCL_EXCEPT=4 (no RC required for OFFBOARD)'),

        # Node 1: MAVLink GCS heartbeat
        # Sends 1 Hz MAV_TYPE_GCS heartbeat — PX4 requires this to ARM
        # Publishes /gcs/armed so controller_node knows GCS is live
        Node(
            package='drone_controller',
            executable='gcs_heartbeat_node',
            name='gcs_heartbeat_node',
            output='screen',
            emulate_tty=True,
            parameters=[{
                'connection':          LaunchConfiguration('gcs_conn'),
                'heartbeat_hz':        1.0,
                'disable_rc_failsafe': LaunchConfiguration('disable_rc_failsafe'),
            }],
        ),

        # Node 2: Task executor
        # Subscribes to /mqtt_bridge/out/task_command
        # Executes TAKEOFF/GO_TO/PAUSE/CONTINUE/RTH/LAND/ABORT
        # Publishes progress to /mqtt_bridge/in/task_status
        Node(
            package='drone_controller',
            executable='controller_node',
            name='drone_controller_node',
            output='screen',
            emulate_tty=True,
        ),
    ])
