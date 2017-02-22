"""
Test our ability to import other modules

"""

import numpy as np
import pywren
import subprocess
import unittest
import numpy as np
import extmodule

class SimpleAsync(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.default_executor()

    def test_simple(self):

        def foo(x):
            return extmodule.foo_add(x)

        x = 1.0
        fut = self.wrenexec.call_async(foo, x)

        res = fut.result() 
        self.assertEqual(res, 2.0)

    def test_utf8_module(self):
        pass

class DummyExecutorImport(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.dummy_executor()

    def test_simple(self):

        def sum_list(x):
            print("running sumlist")
            return np.sum(x)

        x = np.arange(10)
        fut = self.wrenexec.call_async(sum_list, x)
        
        self.wrenexec.invoker.run_jobs()

        res = fut.result() 
        self.assertEqual(res, np.sum(x))
