import time
import uuid
from multiprocessing.pool import ThreadPool

import boto3
import numpy as np
from pywren import wren

MAT_N = 4096

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

    sdbclient = boto3.client('sdb', region_name='us-west-2')

    job_id = str(uuid.uuid1())
    print "job_id=", job_id

    N = 10
    LOOPCOUNT = 5
    extra_env = {"OMP_NUM_THREADS" :  "1"}

    pool = ThreadPool(64)


    call_result_objs = []
    for i in range(N):
        def f():
            wren.call_async(compute_flops, LOOPCOUNT, job_id=job_id,
                            extra_env=extra_env)
        cb = pool.apply_async(f)
        call_result_objs.append(cb)
    invocation_done = False
    while not invocation_done:
        invocation_done = True
        for result_obj in call_result_objs:
            if not result_obj.ready():
                invocation_done = False
                time.sleep(1)

    print "invocation done, dur=", time.time() - t1

    result_count = 0
    while result_count < N:
        r = sdbclient.select(
            SelectExpression="select count(*) from test_two where job_id='{}'".format(job_id))
        result_count = int(r['Items'][0]['Attributes'][0]['Value'])
        est_flop = 2 * result_count * LOOPCOUNT * MAT_N**3

        est_gflops = est_flop / 1e9/(time.time() - t1)
        print "jobs done: {:5d} runtime: {:5.1f}s {:8.1f} GFLOPS ".format(result_count,
                                                                          time.time()-t1,
                                                                          est_gflops)

        if result_count == N:
            break

        time.sleep(1)
    all_done = time.time()
    total_time = all_done - t1
    print "total time", total_time
    est_flop = result_count * 2 * LOOPCOUNT * MAT_N**3

    print est_flop / 1e9/total_time, "GFLOPS"

