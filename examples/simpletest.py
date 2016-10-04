#from gevent import monkey

#monkey.patch_socket()
#monkey.patch_ssl()

import time
import boto3 
import uuid
import numpy as np
import time
import sys
sys.path.append("../")
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

def proc_cpu(x):
    return subprocess.check_output("uname -a", shell=True)
    #return subprocess.check_output("cat /proc/self/cgroup", shell=True)
    """
    9:perf_event:/
    8:memory:/sandbox-cdf823
    7:hugetlb:/
    6:freezer:/sandbox-684456
    5:devices:/
    4:cpuset:/
    3:cpuacct:/sandbox-9330f9
    2:cpu:/sandbox-root-8ZJPiN/sandbox-16222b
    1:blkio:/


    """
    return subprocess.check_output("", shell=True)


def throwexcept(x):
    raise Exception("Throw me out!")


# if __name__ == "__main__":
#     t1 = time.time()

#     LOOPCOUNT = 5

#     fut = pywren.call_async(compute_flops, LOOPCOUNT)
#     res = fut.result() 
#     print "Ran at", res/1e9, "GLFOPS"


# if __name__ == "__main__":
#     t1 = time.time()

#     LOOPCOUNT = 5
#     iters = np.arange(10)
    
#     def f(x):
#         return compute_flops(LOOPCOUNT)
#     futures = pywren.map(f, iters)
#     for f in futures:
#         print f.result()/1e9

#     # res = fut.result() 
#     # print "Ran at", res/1e9, "GLFOPS"



if __name__ == "__main__":

    fh = logging.FileHandler('simpletest.log')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(pywren.wren.formatter)
    pywren.wren.logger.addHandler(fh)

    t1 = time.time()

    fut = pywren.call_async(proc_cpu, None)
    print fut.callset_id

    res = fut.result() 
    print res

