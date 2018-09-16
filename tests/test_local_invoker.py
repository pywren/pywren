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
import pywren
import subprocess
import logging
import unittest
import numpy as np
from flaky import flaky

class SimpleAsync(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.local_executor()

    def test_simple(self):

        def sum_list(x):
            return np.sum(x)

        x = np.arange(10)
        fut = self.wrenexec.call_async(sum_list, x)
        
        res = fut.result() 
        self.assertEqual(res, np.sum(x))

    def test_simple_map(self):

        def plus_one(x):
            return x + 1

        x = np.arange(4)
        futures = self.wrenexec.map(plus_one, x)
        
        res = pywren.get_all_results(futures)
        np.testing.assert_array_equal(res, x + 1)

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


