from setuptools import find_packages, setup
setup(
    name='mqtt_bridge',
    version='1.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/mqtt_bridge']),
        ('share/mqtt_bridge', ['package.xml']),
    ],
    install_requires=['setuptools', 'paho-mqtt'],
    entry_points={'console_scripts': [
        'mqtt_bridge_node = mqtt_bridge.main:main',
    ]},
)
