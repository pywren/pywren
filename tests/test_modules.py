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
import extmodule_otherencode
from pywren import wrenconfig, wrenutil, runtime
import os

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

    def test_otherencode_module(self):
        def foo(x):
            return extmodule_otherencode.foo_add(x)

        x = 1.0
        fut = self.wrenexec.call_async(foo, x)
        
        res = fut.result() 
        self.assertEqual(res, 2.0)

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

# class SerializeTest(unittest.TestCase):
#     def test_simple(x):

#         def func(x):
#             return x + 1
#         data = list(range(5))

#         serializer = serialize.SerializeIndependent()
#         func_and_data_ser, mod_paths = serializer([func] + data)
#         for m in mod_paths:
#             print(m)

        config = pywren.wrenconfig.default()

        info = runtime.get_runtime_info(config['runtime'])
        print(info.keys())
        for f in info['pkg_ver_list']:
            print(f[0])

class InteractiveTest(unittest.TestCase):

    ''' pywren handles module serialization slightly differently in interactive mode vs regular script mode
        this is important because prior to f7a900f0a3aa185406abce2af3a1697759f464da module imports were failing 
        in interactive mode, thus the common usecase of launching pywren in an jupyter notebook would fail.
        This is a (somewhat brittle) test that simulates an interactive python session using the -c flag
    '''

    def test_simple(self):
        ret = os.system("cd tests; python -c \"import pywren; import sys; import extmodule; pwex = pywren.default_executor();results = pwex.map(extmodule.foo_add, [0]);print(results[0].result())\"")
        self.assertEqual(ret, 0)




class ExcludeTest(unittest.TestCase):

    '''
        Exclude modules in map and check if the exclusion worked
    '''
    def setUp(self):
        self.wrenexec = pywren.default_executor()

    def test_simple(self):

        def foo(x):
            return extmodule.foo_add(x)
        x = 1.0
        fut0 = self.wrenexec.map(foo, [x])
        res = fut0[0].result()
        self.assertEqual(res, 2.0)

        fut = self.wrenexec.map(foo, [x], exclude_modules=["extmodule"])
        try:
            fut[0].result()
            self.fail("shouldn't happen")
        except ImportError as e:
            pass

