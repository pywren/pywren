#
# Copyright 2018 PyWren Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pylint: skip-file
#!/usr/bin/env python
import sys

#import pkgconfig
from setuptools import setup, find_packages

if sys.version_info < (2, 7):
    sys.exit('Sorry, Python < 2.7 is not supported')

if sys.version_info > (3,) and sys.version_info < (3, 4):
    sys.exit('Sorry, Python3 version < 3.4 is not supported')

install_requires = [
    'Click', 'boto3', 'PyYAML',
    'flaky', 'glob2', 'watchtower',
    'tblib'  # it's nuts that we need both botos
]
if sys.version_info < (3, 4):
    install_requires.append('enum34')

# http://stackoverflow.com/questions/6344076/differences-between-distribute-distutils-setuptools-and-distutils2

# how to get version info into the project
exec(open('pywren/version.py').read())

setup(
    name='pywren',
    version=__version__,
    url='http://pywren.io',
    author='Eric Jonas',
    description='Run many jobs transparently on AWS Lambda and other cloud services',
    long_description="PyWren lets you transparently run your python functions"
    "on AWS cloud services, including AWS Lambda and AWS EC2.",
    author_email='jonas@ericjonas.com',
    packages=find_packages(),
    install_requires=install_requires,
    tests_requires=[
        'pytest', 'numpy',
    ],
    entry_points={
        'console_scripts' : ['pywren=pywren.scripts.pywrencli:main',
                             'pywren-setup=pywren.scripts.setupscript:interactive_setup',
                             'pywren-server=pywren.scripts.standalone:server']},
    package_data={
        'pywren': ['default_config.yaml',
                   'ec2_standalone_files/ec2standalone.cloudinit.template',
                   'ec2_standalone_files/supervisord.conf',
                   'ec2_standalone_files/supervisord.init',
                   'ec2_standalone_files/cloudwatch-agent.config',
                   'jobrunner/jobrunner.py'
        ]},
    dependency_links=['https://github.com/kislyuk/watchtower/tarball/master#egg=watchtower-0.3.4'],
    include_package_data=True
)
