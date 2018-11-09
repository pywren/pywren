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
import os


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

@lamb            
def test_too_big_runtime():
    """
    Sometimes we accidentally build a runtime that's too big. 
    When this happens, the runtime was leaving behind crap
    and we could never test the runtime again. 
    This tests if we now return a sane exception and can re-run code. 

    There are problems with this test. It is:
    1. lambda only 
    2. depends on lambda having a 512 MB limit. When that is raised someday, 
    this test will always pass. 
    3. Is flaky, because it might be the case that we get _new_
    workers on the next invocation to map that don't have the left-behind
    crap. 
    """


    too_big_config = pywren.wrenconfig.default()
    too_big_config['runtime']['s3_bucket'] = 'pywren-runtimes-public-us-west-2'
    ver_str = "{}.{}".format(sys.version_info[0], sys.version_info[1])
    too_big_config['runtime']['s3_key'] = "pywren.runtimes/too_big_do_not_use_{}.tar.gz".format(ver_str)


    default_config = pywren.wrenconfig.default()


    wrenexec_toobig = pywren.default_executor(config=too_big_config)
    wrenexec = pywren.default_executor(config=default_config)


    def simple_foo(x):
        return x
    MAP_N = 10

    futures = wrenexec_toobig.map(simple_foo, range(MAP_N))
    for f in futures:
        with pytest.raises(Exception) as excinfo:
            f.result()
        assert excinfo.value.args[1] == 'RUNTIME_TOO_BIG'

    # these ones should work
    futures = wrenexec.map(simple_foo, range(MAP_N))
    for f in futures:
        f.result()

@lamb   
def test_big_args():
    """
    This is a test to see if we can upload large args

    Note this test takes a long time because of the large amount of
    data that must be uploaded. 
    """

    wrenexec = pywren.default_executor()

    DATA_MB = 200

    data = "0"*(DATA_MB*1000000)

    ## data argument large
    def simple_foo(x):
        return 1.0
    
    f = wrenexec.call_async(simple_foo, data)
    assert f.result() == 1.0
    
    ## func large
    def simple_foo_2(x):
        #capture data in the closure
        return len(data)

    f = wrenexec.call_async(simple_foo_2, None)
    assert f.result() == len(data)

@lamb
def test_lambda_env_vars():
    """
    Test if we are setting the environment variables 
    we expect 
    """
    def get_env(_):
        return dict(os.environ)

    wrenexec = pywren.default_executor()
    fut = wrenexec.call_async(get_env, None)

    res = fut.result() 
    assert res["OMP_NUM_THREADS"] == "1"

