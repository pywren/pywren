"""
Tests for lambda only 
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
from six.moves import cPickle as pickle
import unittest
import numpy as np
from flaky import flaky
import sys


lamb = pytest.mark.skipif(
    not pytest.config.getoption("--runlambda", False),
    reason="need --runlambda option to run"
)

class Timeout(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.lambda_executor(job_max_runtime=40)

    @lamb
    def test_simple(self):

        def take_forever():
            time.sleep(45)
            return True

        fut = self.wrenexec.call_async(take_forever, None)
        with pytest.raises(Exception) as execinfo:
            res = fut.result() 
            
    @lamb
    def test_we_dont_raise(self):

        def take_forever():
            time.sleep(45)
            return True

        fut = self.wrenexec.call_async(take_forever, None)
        res = fut.result(throw_except=False)

            
