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
        self.wrenexec = pywren.default_executor()

    def test_simple(self):

        def sum_list(x):
            return np.sum(x)

        x = np.arange(10)
        fut = self.wrenexec.call_async(sum_list, x)

        res = fut.result() 
        self.assertEqual(res, np.sum(x))

    def test_exception(self):
        def throwexcept(x):
            raise Exception("Throw me out!")

        wrenexec = pywren.default_executor()
        fut = self.wrenexec.call_async(throwexcept, None)

        with pytest.raises(Exception) as execinfo:
            res = fut.result() 
        assert 'Throw me out!' in str(execinfo.value)


    def test_subprocess(self):
        def uname(x):
            return subprocess.check_output("uname -a", shell=True)
        
        fut = self.wrenexec.call_async(uname, None)

        res = fut.result() 


class SimpleMap(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.default_executor()

    def test_map(self):

        def plus_one(x):
            return x + 1
        N = 10

        x = np.arange(N)
        futures = self.wrenexec.map(plus_one, x)

        result_count = 0
        while result_count < N:
            
            fs_dones, fs_notdones = pywren.wait(futures)
            result_count = len(fs_dones)

        res = np.array([f.result() for f in futures])
        np.testing.assert_array_equal(res, x + 1)


class RuntimeCaching(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.default_executor()

    @flaky(max_runs=3)
    def test_cached_runtime(self):
        """
        Test the runtime caching by manually running with it off
        and then running with it on and comparing invocation times. 
        Note that due to aws lambda internals this might not 
        do the right thing so we mark it as flaky
        """

        def test_add(x):
            return x + 7

        t1 = time.time()
        fut = self.wrenexec.map(test_add, [10], use_cached_runtime=False)[0]
        res = fut.result() 
        t2 = time.time()
        non_cached_latency = t2-t1

        assert fut._run_status['runtime_cached'] == False
        assert res == 17

        t1 = time.time()
        fut = self.wrenexec.map(test_add, [10], use_cached_runtime=True)[0]
        res = fut.result() 
        t2 = time.time()
        cached_latency = t2-t1

        assert res == 17
        assert fut._run_status['runtime_cached'] == True

        assert cached_latency < non_cached_latency
