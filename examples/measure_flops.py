#from gevent import monkey

#monkey.patch_socket()
#monkey.patch_ssl()

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

if __name__ == "__main__":

    fh = logging.FileHandler('simpletest.log')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(pywren.wren.formatter)
    pywren.wren.logger.addHandler(fh)

    t1 = time.time()

    wrenexec = pywren.default_executor()
    fut = wrenexec.call_async(compute_flops, 10)
    print fut.callset_id

    res = fut.result() 
    print res/1e9, "GFLOPS"

