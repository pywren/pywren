#!/usr/bin/env python
import numpy as np
import os
from distutils.core import setup
#import pkgconfig

setup(name='pywren',
      version='1.0',
      description='Simple python lambda client for embarassing parallelism',
      author='Eric Jonas',
      author_email='jonas@eecs.berkeley.edu',
      url='https://www.github.com/ericmjonas/pywren/',
      packages=['pywren'],
      scripts=['bin/pywren']
     )
