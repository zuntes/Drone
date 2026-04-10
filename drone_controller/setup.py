from setuptools import find_packages, setup

package_name = 'drone_controller'

setup(
    name=package_name,
    version='1.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        ('share/' + package_name + '/launch',
            ['launch/controller.launch.py']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='VTP Robotics',
    maintainer_email='dev@vtp.vn',
    description='Drone task executor: MQTT task_command → PX4 OFFBOARD',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'controller_node    = drone_controller.controller_node:main',
            'gcs_heartbeat_node = drone_controller.gcs_heartbeat_node:main',
        ],
    },
)
