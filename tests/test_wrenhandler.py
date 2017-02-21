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
from pywren import wrenconfig


class Timeout(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.default_executor(job_max_runtime=40)

    def test_simple(self):

        def take_forever(x):
            time.sleep(45)
            return True

        fut = self.wrenexec.call_async(take_forever, None)
        with pytest.raises(Exception) as excinfo:
            res = fut.result() 
        assert 'out of time' in str(excinfo.value)
            

class General(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.default_executor()

    def test_version_error(self):

        def foo(x):
            return x + 1
        overwrite_invoke_args = {'pywren_version' : "BADVERSION"}
        
        fut = self.wrenexec.map(foo, [None], 
                                overwrite_invoke_args=overwrite_invoke_args)[0]

        with pytest.raises(Exception) as excinfo:
            res = fut.result() 
        assert 'version' in str(excinfo.value)
            

    def test_missing_func_key(self):

        def foo(x):
            return x + 1

        config = wrenconfig.default()
        s3_bucket = config['s3']['bucket']
        s3_prefix = config['s3']['pywren_prefix']

        overwrite_invoke_args = {'func_key' : (s3_bucket, "nonsense")}

        fut = self.wrenexec.map(foo, [None], 
                                overwrite_invoke_args=overwrite_invoke_args)[0]

        with pytest.raises(Exception) as excinfo:
            res = fut.result() 

            

