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

class SimpleAsync(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.dummy_executor()

    def test_simple(self):

        def sum_list(x):
            return np.sum(x)

        x = np.arange(10)
        fut = self.wrenexec.call_async(sum_list, x)
        
        self.wrenexec.invoker.run_jobs()

        res = fut.result() 
        self.assertEqual(res, np.sum(x))

    def test_exception(self):
        def throwexcept(x):
            raise Exception("Throw me out!")

        wrenexec = pywren.default_executor()
        fut = self.wrenexec.call_async(throwexcept, None)
        self.wrenexec.invoker.run_jobs()
        
        with pytest.raises(Exception) as execinfo:
            res = fut.result() 
        assert 'Throw me out!' in str(execinfo.value)


    def test_subprocess(self):
        def uname(x):
            return subprocess.check_output("uname -a", shell=True)
        
        fut = self.wrenexec.call_async(uname, None)
        self.wrenexec.invoker.run_jobs()

        res = fut.result() 


