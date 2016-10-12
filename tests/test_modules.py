"""
Test our ability to import other modules

"""

import pytest
import time
import boto3 
import uuid
import numpy as np
import time
import pywren
import subprocess
import logging
import unittest
import numpy as np

class SimpleAsync(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.default_executor()

    def test_simple(self):

        def foo(x):
            return extmodule.foo_add(x)

        x = 1.0
        fut = self.wrenexec.call_async(foo, x)

        res = fut.result() 
        self.assertEqual(res, 2.0)
