#!/usr/bin/env python
import os

#import pkgconfig
from setuptools import setup, find_packages

# http://stackoverflow.com/questions/6344076/differences-between-distribute-distutils-setuptools-and-distutils2

# how to get version info into the project
exec(open('pywren/version.py').read())

setup(
    name='pywren',
    version=__version__,
    url='https://github.com/ericmjonas/github',
    author='Eric Jonas',
    author_email='jonas@ericjonas.com',
    packages=['pywren', 'pywren.scripts', 'pywren.cloudpickle'],
    install_requires=[
        'numpy', 'Click', 'boto3', 'cloudpickle', 'PyYAML', 
        'enum34', 'flaky', 'glob2', 'multiprocess', 
        'watchtower', 'tblib' # it's nuts that we need both botos
    ],
    entry_points =
    { 'console_scripts' : ['pywren=pywren.scripts.pywrencli:cli', 
                           'pywren-server=pywren.scripts.standalone:server']},
    package_data={'pywren': ['default_config.yaml', 
                             'ec2_standalone_files/ec2standalone.cloudinit.template', 
                             'ec2_standalone_files/supervisord.conf', 
                             'ec2_standalone_files/supervisord.init', 
                             'ec2_standalone_files/cloudwatch-agent.config', 
    ]},
    dependency_links=['http://github.com/ericmjonas/watchtower/tarball/master#egg=watchtower-1.0jonas'],

    include_package_data=True
)
