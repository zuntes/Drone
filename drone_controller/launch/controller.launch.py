from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    return LaunchDescription([
        # Subscribes to /mqtt_bridge/out/task_command
        # Executes TAKE_OFF/GO_TO/RETURN_TO_HOME/LAND/CANCEL
        # Publishes progress to /mqtt_bridge/in/task_status
        Node(
            package='drone_controller',
            executable='controller_node',
            name='drone_controller_node',
            output='screen',
            emulate_tty=True,
        ),
    ])
