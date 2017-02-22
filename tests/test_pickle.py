import pytest
import time
import boto3 
import uuid
import numpy as np
import time
import pywren
import pywren.runtime
import subprocess
import logging
from six.moves import cPickle as pickle

import unittest
import numpy as np
from flaky import flaky
import sys


class CantPickle(object):
    """
    Some objects can't be pickled, and either fail at
    save or restore. We need to catch these and, as
    the Hydraulic press guy says, "deal with it" 
    """

    def __init__(self, foo, dump_fail=False, load_fail=False):
        self.foo = foo
        self.dump_fail = dump_fail
        self.load_fail = load_fail
    
    def __getstate__(self):
        print("getstate called")
        if self.dump_fail:
            raise Exception("cannot pickle dump this object")

        return {'foo' : self.foo, 
                'dump_fail' : self.dump_fail, 
                'load_fail' : self.load_fail}

    def __setstate__(self, arg):
        print("setstate called")
        if arg['load_fail']:
            raise Exception("cannot pickle load this object")

        self.load_fail = arg['load_fail']
        self.dump_fail = arg['dump_fail']
        self.foo = arg['foo']

class CantPickleException(Exception):
    """
    Some objects can't be pickled, and either fail at
    save or restore. We need to catch these and, as
    the Hydraulic press guy says, "deal with it" 
    """

    def __init__(self, foo, dump_fail=False, load_fail=False, 
                 skip_super = False):
        if not skip_super :
            super(Exception, self).__init__(str(foo))
        self.foo = foo
        self.dump_fail = dump_fail
        self.load_fail = load_fail
        

    def __getstate__(self):
        print("getstate called")
        if self.dump_fail:
            raise Exception("cannot pickle dump this object")

        return {'foo' : self.foo, 
                'dump_fail' : self.dump_fail, 
                'load_fail' : self.load_fail}

    def __setstate__(self, arg):
        print("setstate called")
        if arg['load_fail']:
            raise Exception("cannot pickle load this object")

        self.load_fail = arg['load_fail']
        self.dump_fail = arg['dump_fail']
        self.foo = arg['foo']

class PickleSafety(unittest.TestCase):

    def setUp(self):
        self.wrenexec = pywren.default_executor()

    def test_subprocess_fail(self):
        """
        Subprocess command-not-found fails
        """
        def uname(x):
            return subprocess.check_output("fakecommand", shell=True)

        
        fut = self.wrenexec.call_async(uname, None)
        with pytest.raises(Exception) as execinfo:
            res = fut.result() 
        assert "Command 'fakecommand' returned" in str(execinfo.value)
    
    def test_unpickleable_return_dump(self):
        def f(x):
            cp = CantPickle(x, dump_fail = True)
            return cp

        wrenexec = pywren.default_executor()
        fut = self.wrenexec.call_async(f, None)

        with pytest.raises(Exception) as execinfo:
            res = fut.result() 
        print(str(execinfo.value))
        assert 'cannot pickle dump this object' in str(execinfo.value)

    def test_unpickleable_return_load(self):
        def f(x):
            cp = CantPickle(x, load_fail = True)
            return cp

        wrenexec = pywren.default_executor()
        fut = self.wrenexec.call_async(f, None)

        with pytest.raises(Exception) as execinfo:
            res = fut.result() 
        assert 'cannot pickle load this object' in str(execinfo.value)
        

    def test_unpickleable_raise_except_dump(self):
        def f(x):
            cp = CantPickleException(x, dump_fail = True)
            raise cp

        wrenexec = pywren.default_executor()
        fut = self.wrenexec.call_async(f, None)

        with pytest.raises(CantPickleException) as execinfo:
            res = fut.result() 
        #assert 'cannot pickle dump this object' in str(execinfo.value)


    def test_unpickleable_raise_except_load(self):
        def f(x):
            cp = CantPickleException(x, load_fail = True)
            raise cp

        wrenexec = pywren.default_executor()
        fut = self.wrenexec.call_async(f, None)

        with pytest.raises(Exception) as execinfo:
            res = fut.result() 

    def test_unpickleable_raise_except_nosuper(self):
        def f(x):
            cp = CantPickleException(x, skip_super = True)
            raise cp

        wrenexec = pywren.default_executor()
        fut = self.wrenexec.call_async(f, "Fun exception")

        with pytest.raises(Exception) as execinfo:
            res = fut.result() 
            assert 'Fun exception' in str(execinfo.value)

