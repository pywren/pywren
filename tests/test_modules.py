"""
Test our ability to import other modules

"""

import numpy as np
import pywren
import subprocess
import unittest
import numpy as np
import extmodule
import extmoduleutf8

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
        def foo(x):
            return extmoduleutf8.foo_add(x)

        x = 1.0
        fut = self.wrenexec.call_async(foo, x)
        
        self.wrenexec.invoker.run_jobs()

        res = fut.result() 
        self.assertEqual(res, 2.0)

    def test_utf8_string(self):
        def foo(x):
            return extmoduleutf8.unicode_str(x)

        x = 1.0
        fut = self.wrenexec.call_async(foo, x)
        
        self.wrenexec.invoker.run_jobs()

        res = fut.result() 
        self.assertEqual(res, extmoduleutf8.TEST_STR)


class DummyExecutorImport(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.dummy_executor()

    def test_simple(self):

        def foo(x):
            return extmoduleutf8.foo_add(x)

        x = 1.0
        fut = self.wrenexec.call_async(foo, x)
        
        self.wrenexec.invoker.run_jobs()

        res = fut.result() 
        self.assertEqual(res, 2.0)
