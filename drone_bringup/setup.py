from setuptools import setup
import os
from glob import glob

setup(
    name='drone_bringup',
    version='1.0.0',
    packages=[],
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/drone_bringup']),
        ('share/drone_bringup', ['package.xml']),
        (os.path.join('share', 'drone_bringup', 'launch'),
            glob('launch/*.launch.py')),
        (os.path.join('share', 'drone_bringup', 'config'),
            glob('config/*.yaml')),
    ],
    install_requires=['setuptools'],
    entry_points={'console_scripts': []},
)
