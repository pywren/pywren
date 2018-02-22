import pytest
import time
import boto3 
import uuid
import numpy as np
import time
import os
import pywren
import pywren.runtime
import subprocess
import logging
from six.moves import cPickle as pickle

import unittest
import numpy as np
from flaky import flaky
import sys
import pywren.invokers as invokers



class StandaloneMany(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.default_executor()

    def test_parallel(self):
        if isinstance(self.wrenexec.invoker, invokers.LambdaInvoker):
            return 0
        EXECUTOR_PARALLELISM = int(os.environ["EXECUTOR_PARALLELISM"])
        N = EXECUTOR_PARALLELISM
        def test_fn(i):
            open("/tmp/potato_{0}".format(i), "w+").close()
            time.sleep(120)
            return [x for x in os.listdir("/tmp/") if "potato" in x]

        futures = self.wrenexec.map(test_fn, range(EXECUTOR_PARALLELISM))
        for f in futures:
            assert(len(f.result()) == EXECUTOR_PARALLELISM)

