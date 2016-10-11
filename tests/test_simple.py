import pytest
import time
import boto3 
import uuid
import numpy as np
import time
import pywren
import subprocess
import logging

MAT_N = 1024

def compute_flops(loopcount):
    
    A = np.arange(MAT_N**2, dtype=np.float64).reshape(MAT_N, MAT_N)
    B = np.arange(MAT_N**2, dtype=np.float64).reshape(MAT_N, MAT_N)

    t1 = time.time()
    for i in range(loopcount):
        c = np.sum(np.dot(A, B))

    FLOPS = 2 *  MAT_N**3 * loopcount
    t2 = time.time()
    return FLOPS / (t2-t1)

def subprocess_invoke(x):
    return subprocess.check_output("uname -a", shell=True)



def test_simple():

    def sum_list(x):
        return np.sum(x)
    wrenexec = pywren.default_executor()
    x = np.arange(10)
    fut = wrenexec.call_async(sum_list, x)
    print fut.callset_id

    res = fut.result() 
    assert_equal(res, np.sum(x))

def test_exception():

    def throwexcept(x):
        raise Exception("Throw me out!")

    wrenexec = pywren.default_executor()
    fut = wrenexec.call_async(throwexcept, None)
    print fut.callset_id
    with pytest.raises(Exception) as execinfo:
        res = fut.result() 
    assert 'Throw me out!' in str(execinfo.value)

