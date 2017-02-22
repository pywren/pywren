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
from pywren.cloudpickle import serialize
from pywren import wrenconfig, wrenutil, runtime

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
        
        res = fut.result() 
        self.assertEqual(res, 2.0)

    def test_utf8_str(self):
        def foo(x):
            return extmoduleutf8.unicode_str(x)

        x = 1.0
        fut = self.wrenexec.call_async(foo, x)
        
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

    def test_utf8_str(self):
        def foo(x):
            return extmoduleutf8.unicode_str(x)

        x = 1.0
        fut = self.wrenexec.call_async(foo, x)
        
        self.wrenexec.invoker.run_jobs()

        res = fut.result() 
        self.assertEqual(res, extmoduleutf8.TEST_STR)

class SerializeTest(unittest.TestCase):
    def test_simple(x):

        def func(x):
            return x + 1
        data = list(range(5))

        serializer = serialize.SerializeIndependent()
        func_and_data_ser, mod_paths = serializer([func] + data)
        for m in mod_paths:
            print(m)

        config =  pywren.wrenconfig.default()

        runtime_bucket = config['runtime']['s3_bucket']
        runtime_key =  config['runtime']['s3_key']
        info = runtime.get_runtime_info(runtime_bucket, runtime_key)
        print(info.keys())
        for f in info['pkg_ver_list']:
            print(f[0])

