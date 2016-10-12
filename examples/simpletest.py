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

def uname_a(x):
    return subprocess.check_output("uname -a", shell=True)

def proc_cpu(x):
    return subprocess.check_output("cat /proc/cpuinfo", shell=True)

def instance_metadata(x):
    return subprocess.check_output("ls ", shell=True)


def throwexcept(x):
    raise Exception("Throw me out!")

if __name__ == "__main__":

    fh = logging.FileHandler('simpletest.log')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(pywren.wren.formatter)
    pywren.wren.logger.addHandler(fh)

    t1 = time.time()

    wrenexec = pywren.default_executor()
    fut = wrenexec.call_async(instance_metadata, None)
    print fut.callset_id

    res = fut.result() 
    print res

