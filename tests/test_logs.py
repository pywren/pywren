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

class CloudwatchLogTest(unittest.TestCase):
    """
    Simple test to see if we can get any logs
    """

    def setUp(self):
        self.wrenexec = pywren.default_executor()
    
    @pytest.mark.skip(reason="This test is way too noisy")
    def test_simple(self):

        def sum_list(x):
            return np.sum(x)

        x = np.arange(10)
        fut = self.wrenexec.call_async(sum_list, x)

        res = fut.result() 
        self.assertEqual(res, np.sum(x))

        time.sleep(10) # wait for logs to propagate
        
        logs = self.wrenexec.get_logs(fut, True)
        
        assert len(logs) >= 3 # make sure we have start, end, report

        
