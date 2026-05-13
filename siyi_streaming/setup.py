from setuptools import find_packages, setup
import os
from glob import glob

setup(
    name='siyi_streaming',
    version='1.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/siyi_streaming']),
        ('share/siyi_streaming', ['package.xml']),
        # Launch files
        (os.path.join('share', 'siyi_streaming', 'launch'),
            glob('launch/*.launch.py')),
        # MediaMTX YAML — read at launch time
        (os.path.join('share', 'siyi_streaming', 'config'),
            glob('config/*.yml')),
        # Install / Funnel scripts — shipped so the launch can invoke them
        (os.path.join('share', 'siyi_streaming', 'scripts'),
            glob('scripts/*.sh')),
        # Browser viewer page
        (os.path.join('share', 'siyi_streaming', 'web'),
            glob('web/*.html')),
    ],
    install_requires=['setuptools'],
    entry_points={'console_scripts': []},
)
