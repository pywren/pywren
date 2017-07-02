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

    def test_simple2(self):
        
        def sum_list(x):
            return np.sum(x)

        x = np.arange(10)
        fut = self.wrenexec.call_async(sum_list, x)

        res = fut.result() 
        self.assertEqual(res, np.sum(x))

    def test_exception(self):
        """
        Simple exception test
        """
        def throwexcept(x):
            raise Exception("Throw me out!")

        wrenexec = pywren.default_executor()
        fut = self.wrenexec.call_async(throwexcept, None)

        with pytest.raises(Exception) as execinfo:
            res = fut.result() 
        assert 'Throw me out!' in str(execinfo.value)


    def test_exception2(self):
        """
        More complex exception
        """

        def throw_exception(x):
            1 / 0
            return 10


        wrenexec = pywren.default_executor()

        fut = wrenexec.call_async(throw_exception, None)

        try:
            throw_exception(1)
        except Exception as e:

            exc_type_true, exc_value_true, exc_traceback_true = sys.exc_info()


        try:
            fut.result()
        except Exception as e:
            exc_type_wren, exc_value_wren, exc_traceback_wren = sys.exc_info()

        assert exc_type_wren == exc_type_true
        assert type(exc_value_wren) == type(exc_value_true)


class SimpleMap(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.default_executor()

    def test_empty_map(self):
        futures = self.wrenexec.map(lambda x: x, [])
        res = np.array([f.result() for f in futures])
        np.testing.assert_array_equal(res, [])

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

    def test_map_doublewait(self):
        """
        Make sure we can call wait on a list of futures twice
        """
        def plus_one(x):
            return x + 1
        N = 10

        x = np.arange(N)
        futures = self.wrenexec.map(plus_one, x)
        pywren.wait(futures)
        pywren.wait(futures)

        res = np.array([f.result() for f in futures])
        np.testing.assert_array_equal(res, x + 1)

    def test_get_all_results(self):
        def plus_one(x):
            return x + 1
        N = 10

        x = np.arange(N)
        futures = self.wrenexec.map(plus_one, x)

        res = np.array(pywren.get_all_results(futures))
        np.testing.assert_array_equal(res, x + 1)

class SimpleReduce(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.default_executor()

    def test_reduce(self):

        def plus_one(x):
            return x + 1
        N = 10

        x = np.arange(N)
        futures = self.wrenexec.map(plus_one, x)
        
        reduce_future = self.wrenexec.reduce(sum, futures)

        np.testing.assert_array_equal(reduce_future.result(), 55)

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

        assert fut.run_status['runtime_cached'] == False
        assert res == 17

        t1 = time.time()
        fut = self.wrenexec.map(test_add, [10], use_cached_runtime=True)[0]
        res = fut.result() 
        t2 = time.time()
        cached_latency = t2-t1

        assert res == 17
        assert fut.run_status['runtime_cached'] == True

        assert cached_latency < non_cached_latency


class SerializeFutures(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.default_executor()

    def test_map(self):

        def plus_one(x):
            return x + 1
        N = 10

        x = np.arange(N)
        futures_original = self.wrenexec.map(plus_one, x)
        futures_str = pickle.dumps(futures_original)
        futures = pickle.loads(futures_str)

        result_count = 0
        while result_count < N:
            
            fs_dones, fs_notdones = pywren.wait(futures)
            result_count = len(fs_dones)

        res = np.array([f.result() for f in futures])
        np.testing.assert_array_equal(res, x + 1)


class ConfigErrors(unittest.TestCase):
    def test_version_mismatch(self):

        my_version_str = pywren.runtime.version_str(sys.version_info)
        for supported_version in pywren.wrenconfig.default_runtime.keys():
            if my_version_str != supported_version:
                wrong_version = supported_version
            
                config = pywren.wrenconfig.default()
                config['runtime']['s3_key'] = pywren.wrenconfig.default_runtime[wrong_version]
                
                    
                with pytest.raises(Exception) as excinfo:
                    pywren.lambda_executor(config)
                assert 'python version' in str(excinfo.value)

class WaitTest(unittest.TestCase):
    def setUp(self):
        self.wrenexec = pywren.default_executor()

    def test_all_complete(self):
        def wait_x_sec_and_plus_one(x):
            time.sleep(x)
            return x + 1

        N = 10
        x = np.arange(N)

        futures = pywren.default_executor().map(wait_x_sec_and_plus_one, x)

        fs_dones, fs_notdones = pywren.wait(futures,
                                        return_when=pywren.wren.ALL_COMPLETED)
        res = np.array([f.result() for f in fs_dones])
        np.testing.assert_array_equal(res, x+1)

    def test_any_complete(self):
        def wait_x_sec_and_plus_one(x):
            time.sleep(x)
            return x + 1

        N = 10
        x = np.arange(N)

        futures = pywren.default_executor().map(wait_x_sec_and_plus_one, x)

        fs_notdones = futures
        while (len(fs_notdones) > 0):
            fs_dones, fs_notdones = pywren.wait(fs_notdones,
                                            return_when=pywren.wren.ANY_COMPLETED,
                                            WAIT_DUR_SEC=1)
            self.assertTrue(len(fs_dones) > 0)
        res = np.array([f.result() for f in futures])
        np.testing.assert_array_equal(res, x+1)


class RuntimeSharding(unittest.TestCase):
    """
    The purpose of runtime sharding is to increase simultaneous
    throughput for reading the runtimes, thus it's
    hard to test in a unit test case. But we can test
    that the metadata propagates properly and we actually
    download the real key
    """
    def test_no_shard(self):
        config = pywren.wrenconfig.default()
        old_key = config['runtime']['s3_key']
        prefix, tar_gz = os.path.split(old_key)
        # Use the staging key to test as it doesn't have shards
        config['runtime']['s3_key'] = os.path.join("pywren.runtime.staging", tar_gz)
        wrenexec = pywren.default_executor(config=config)

        def test_func(x):
            return x + 1

        future = wrenexec.call_async(test_func, 7)
        result = future.result()
        base_runtime_key = config['runtime']['s3_key']
        self.assertEqual(future.run_status['runtime_s3_key_used'],
                         base_runtime_key)

    def test_shard(self):
        config = pywren.wrenconfig.default()
        old_key = config['runtime']['s3_key']
        prefix, tar_gz = os.path.split(old_key)
        # Use a runtime that has shards
        config['runtime']['s3_key'] = os.path.join("pywren.runtime", tar_gz)
        wrenexec = pywren.default_executor(config=config)

        def test_func(x):
            return x + 1

        base_runtime_key = config['runtime']['s3_key']

        future = wrenexec.call_async(test_func, 7)
        result = future.result()
        # NOTE: There is some probability we will hit the base key ? 
        self.assertNotEqual(future.run_status['runtime_s3_key_used'], 
                         base_runtime_key)

class RuntimePaths(unittest.TestCase):
    """
    Test to make sure that we have the correct python and
    other utils in our path at runtime
    """

    def test_paths(self):

        def run_command(x):
            return subprocess.check_output(x, shell=True).decode('ascii')

        cmd = "conda info"
        wrenexec = pywren.default_executor()
        fut = wrenexec.call_async(run_command, cmd)

        res = fut.result() 
        assert "Current conda install" in res


