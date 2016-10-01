import pywren
import time
import boto3 
import uuid
import numpy as np
import time



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
    t1 = time.time()

    LOOPCOUNT = 5
    iters = np.arange(10)
    
    def f(x):
        return compute_flops(LOOPCOUNT)
    futures = pywren.map(f, iters)
    for f in futures:
        print f.result()/1e9
