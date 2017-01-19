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
from flaky import flaky

class CloudwatchLogTest(unittest.TestCase):
    """
    Simple test to see if we can get any logs
    """

    def setUp(self):
        self.wrenexec = pywren.default_executor()

    def test_simple(self):

        def sum_list(x):
            return np.sum(x)

        x = np.arange(10)
        fut = self.wrenexec.call_async(sum_list, x)

        res = fut.result() 
        self.assertEqual(res, np.sum(x))

        time.sleep(10) # wait for logs to propagate
        
        logs = self.wrenexec.get_logs(fut)
        
        assert len(logs) >= 3 # make sure we have start, end, report

        
