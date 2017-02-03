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
import pywren.queues

class SimpleAsync(unittest.TestCase):
    """
    Test sqs dispatch but with local runner
    """
    def setUp(self):
        self.wrenexec = pywren.remote_executor()

    def test_simple(self):

        def sum_list(x):
            return np.sum(x)

        x = np.arange(10)
        fut = self.wrenexec.call_async(sum_list, x)
        pywren.queues.sqs_run_local(self.wrenexec.aws_region, 
                                    self.wrenexec.invoker.sqs_queue_name)

        res = fut.result() 
        self.assertEqual(res, np.sum(x))

