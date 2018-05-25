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

import pywren
from numba import jit
import time
from six.moves import range

from flaky import flaky

import unittest

def foo(N):
    x = 0.0
    for i in range(N):
        x += 1.0
        x = x * 3.0
    return x

@jit
def bar(N):
    x = 0.0
    for i in range(N):
        x += 1.0
        x = x * 3.0
    return x


# Time isn't supported in numba so we have to wrap
def time_foo(N):
    t1 = time.time()
    for i in range(5):
        foo(N)
    t2 = time.time()
    return t2-t1

def time_bar(N):
    t1 = time.time()
    for i in range(5):
        bar(N)
    t2 = time.time()
    return t2-t1


class NumbaTest(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.default_executor()

    @flaky(max_runs=3)
    def test_numba(self):
        """
        Simple numba test, compares two loops, makes sure
        one runs much faster than the other

        """
        

        N = 10000000
        results = self.wrenexec.map(time_foo, [N])
        pywren.wait(results)
        regular_time = results[0].result()
        print('regular time', regular_time)

        results = self.wrenexec.map(time_bar, [N])
        pywren.wait(results)
        numba_time = results[0].result()
        print('numba time', numba_time)


        speed_gain = regular_time / numba_time

        self.assertTrue(speed_gain > 8.0)


