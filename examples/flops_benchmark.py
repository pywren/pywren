import numpy as np
import time
import sys
import pywren
import logging
import exampleutils
import cPickle as pickle
import click


def compute_flops(loopcount, MAT_N):
    
    A = np.arange(MAT_N**2, dtype=np.float64).reshape(MAT_N, MAT_N)
    B = np.arange(MAT_N**2, dtype=np.float64).reshape(MAT_N, MAT_N)

    t1 = time.time()
    for i in range(loopcount):
        c = np.sum(np.dot(A, B))

    FLOPS = 2 *  MAT_N**3 * loopcount
    t2 = time.time()
    return FLOPS / (t2-t1)


@click.command()
@click.option('--workers', default=10, help='how many workers', type=int)
@click.option('--outfile', default='flops_benchmark.pickle', 
              help='filename to save results in')
@click.option('--loopcount', default=6, help='Number of matmuls to do.', type=int)
@click.option('--matn', default=1024, help='size of matrix', type=int)
def benchmark(outfile, loopcount, workers, matn):

        
    t1 = time.time()
    N = workers

    iters = np.arange(N)
    
    def f(x):
        hwaddr = exampleutils.get_hwaddr()
        uptime = exampleutils.get_uptime()[0]
        return {'flops' : compute_flops(loopcount, matn), 
                'hw_addr' : hwaddr, 
                'uptime' : uptime}

    pwex = pywren.default_executor()
    futures = pwex.map(f, iters)

    print "invocation done, dur=", time.time() - t1
    print "callset id: ", futures[0].callset_id

    local_jobs_done_timeline = []
    result_count = 0
    while result_count < N:
        fs_dones, fs_notdones = pywren.wait(futures, pywren.wren.ALWAYS)
        result_count = len(fs_dones)
        
        local_jobs_done_timeline.append((time.time(), result_count))
        est_flop = 2 * result_count * loopcount * matn**3
        
        est_gflops = est_flop / 1e9/(time.time() - t1)
        print "jobs done: {:5d}    runtime: {:5.1f}s   {:8.1f} GFLOPS ".format(result_count, 
                                                                           time.time()-t1, 
                                                                           est_gflops)
        
        if result_count == N:
            break

        time.sleep(1)
    print "getting results" 
    results = [f.result() for f in futures]
    print "getting status" 
    run_statuses = [f.run_status for f in futures]
    invoke_statuses = [f.invoke_status for f in futures]

    all_done = time.time()
    total_time = all_done - t1
    print "total time", total_time
    est_flop = result_count * 2 * loopcount * matn**3
    
    print est_flop / 1e9/total_time, "GFLOPS"
    pickle.dump({'total_time' : total_time, 
                 'est_flop' : est_flop, 
                 'run_statuses' : run_statuses, 
                 'invoke_statuses' : invoke_statuses, 
                 'callset_id' : futures[0].callset_id, 
                 'local_jobs_done_timeline' : local_jobs_done_timeline, 
                 'results' : results}, 
                open(outfile, 'w'), -1)

    
if __name__ == "__main__":
    benchmark()
