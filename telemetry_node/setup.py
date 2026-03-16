from setuptools import find_packages, setup
setup(
    name='telemetry_node',
    version='1.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/telemetry_node']),
        ('share/telemetry_node', ['package.xml']),
    ],
    install_requires=['setuptools'],
    entry_points={'console_scripts': [
        'telemetry_node = telemetry_node.main:main',
    ]},
)
