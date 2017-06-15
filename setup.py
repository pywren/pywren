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
    url='http://pywren.io',
    author='Eric Jonas',
    description='Run many jobs transparently on AWS Lambda and other cloud services',
    long_description="PyWren lets you transparently run your python functions on AWS cloud services, including AWS Lambda and AWS EC2.", 
    author_email='jonas@ericjonas.com',
    packages=find_packages(),
    install_requires=[
        'numpy', 'Click', 'boto3', 'PyYAML', 
        'enum34', 'flaky', 'glob2', 
        'watchtower', 'tblib' # it's nuts that we need both botos
    ],
    entry_points =
    { 'console_scripts' : ['pywren=pywren.scripts.pywrencli:main', 
                           'pywren-setup=pywren.scripts.setupscript:interactive_setup', 
                           'pywren-server=pywren.scripts.standalone:server']},
    package_data={'pywren': ['default_config.yaml', 
                             'ec2_standalone_files/ec2standalone.cloudinit.template', 
                             'ec2_standalone_files/supervisord.conf', 
                             'ec2_standalone_files/supervisord.init', 
                             'ec2_standalone_files/cloudwatch-agent.config', 
    ]},
    dependency_links=['https://github.com/kislyuk/watchtower/tarball/master#egg=watchtower-0.3.4'],
    include_package_data=True
)
