import pytest
import time
import boto3 
import uuid
import numpy as np
import time
import pywren
import subprocess
import logging
import cPickle as pickle
import unittest
import numpy as np
from flaky import flaky
import sys


class MacroReduce(unittest.TestCase):
    """
    Test running with both a lambda executor and a remote
    executor
    """
    def setUp(self):
        self.lambda_exec = pywren.lambda_executor()
        self.remote_exec = pywren.remote_executor()

    def test_reduce(self):

        def plus_one(x):
            return x + 1
        N = 10

        x = np.arange(N)
        futures = self.lambda_exec.map(plus_one, x)
        
        reduce_future = self.remote_exec.reduce(sum, futures)

        np.testing.assert_array_equal(reduce_future.result(), 55)
