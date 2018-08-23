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
import pywren.queues

class SimpleAsync(unittest.TestCase):
    """
    Test sqs dispatch but with local runner
    """
    def setUp(self):
        config = pywren.wrenconfig.default()
        self.aws_region = config['account']['aws_region']
        self.wrenexec = pywren.remote_executor()

    def test_simple(self):

        def sum_list(x):
            return np.sum(x)

        x = np.arange(10)
        fut = self.wrenexec.call_async(sum_list, x)
        pywren.queues.sqs_run_local(self.aws_region,
                                    self.wrenexec.invoker.sqs_queue_name)

        res = fut.result() 
        self.assertEqual(res, np.sum(x))

