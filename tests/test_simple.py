#
# Copyright 2018 PyWren Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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

    def test_cancel(self):

        def sleep(x):
            time.sleep(x)
            return 0

        fut = self.wrenexec.call_async(sleep, 30)
        time.sleep(2)
        fut.cancel()

        with pytest.raises(Exception) as execinfo:
            _ = fut.result()

        assert "cancelled" in str(execinfo.value)

    def test_exit(self):
        """
        what if the process just dies
        """
        def just_die(x):
            sys.exit(-1)
        
        wrenexec = pywren.default_executor()

        fut = wrenexec.call_async(just_die, 1)

        with pytest.raises(Exception) as execinfo:
            res = fut.result() 
        assert 'non-zero return code' in str(execinfo.value)

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

    def test_multiple_callset_id(self):
        def wait_x_sec_and_plus_one(x):
            time.sleep(x)
            return x + 1

        N = 10
        x = np.arange(N)

        pywx = pywren.default_executor()

        futures1 = pywx.map(wait_x_sec_and_plus_one, x)
        futures2 = pywx.map(wait_x_sec_and_plus_one, x)

        fs_dones, fs_notdones = pywren.wait(futures1 + futures2,
                                        return_when=pywren.wren.ALL_COMPLETED)
        res = np.array([f.result() for f in fs_dones])
        np.testing.assert_array_equal(res, np.concatenate((x,x))+1)

    def test_multiple_callset_id_diff_executors(self):
        def wait_x_sec_and_plus_one(x):
            time.sleep(x)
            return x + 1

        N = 10
        x = np.arange(N)

        futures1 = pywren.default_executor().map(wait_x_sec_and_plus_one, x)
        futures2 = pywren.default_executor().map(wait_x_sec_and_plus_one, x)

        fs_dones, fs_notdones = pywren.wait(futures1 + futures2,
                return_when=pywren.wren.ALL_COMPLETED)
        res = np.array([f.result() for f in fs_dones])
        np.testing.assert_array_equal(res, np.concatenate((x,x))+1)


# Comment this test out as it doesn't work with the multiple executors (Vaishaal)
# If we need this later we need to do some more monkey patching but is unclear we actually need this

'''
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
'''


class Limits(unittest.TestCase):
    """
    Tests basic seatbelts
    """
    
    def test_map_item_limit(self):

        TOO_BIG_COUNT = 100
        conf = pywren.wrenconfig.default()
        if 'scheduler' not in conf:
            conf['scheduler'] = {}
        conf['scheduler']['map_item_limit'] = TOO_BIG_COUNT
        wrenexec = pywren.default_executor(config=conf)

        def plus_one(x):
            return x + 1
        N = 10

        x = np.arange(N)
        futures = wrenexec.map(plus_one, x)
        pywren.get_all_results(futures)

        # now too big
        
        with pytest.raises(ValueError) as excinfo:

            x = np.arange(TOO_BIG_COUNT+1)
            
            futures = wrenexec.map(plus_one, x )           


class EnvVars(unittest.TestCase):
    """
    Can we set the environment vars to map?
    """
    def test_env(self):

        def get_env(_):
            return dict(os.environ)

        wrenexec = pywren.default_executor()
        extra_env = {"HELLO" : "WORLD"}
        fut = wrenexec.call_async(get_env, None,
                                  extra_env=extra_env)

        res = fut.result()
        assert "HELLO" in res.keys()
        assert res["HELLO"] == "WORLD"

class Futures(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.default_executor()

    def test_succeeded_errored(self):

        def sum_list(x):
            return np.sum(x)

        def sum_error(_):
            raise Exception("whaaaa")

        x = np.arange(10)
        fut = self.wrenexec.call_async(sum_list, x)
        assert not fut.succeeded()
        assert not fut.errored()
        res = fut.result()
        self.assertEqual(res, np.sum(x))
        assert fut.succeeded()
        assert not fut.errored()


        fut = self.wrenexec.call_async(sum_error, x)
        assert not fut.succeeded()
        assert not fut.errored()
        with pytest.raises(Exception):
            _ = fut.result()
        assert not fut.succeeded()
        assert fut.errored()



    def test_done(self):
        """
        Check if done works correctly
        """
        
        def sum_except(x):
            s = np.sum(x)
            if s >= 1:
                raise Exception("whaaaa")
            return s

        x = np.zeros(10)
        fut = self.wrenexec.call_async(sum_except, x)
        while not fut.done():
            time.sleep(1)
            
        x = np.zeros(10) + 17
        fut = self.wrenexec.call_async(sum_except, x)
        while not fut.done():
            time.sleep(1)
            
            

