import numpy as np
import time
import sys
import pywren
import logging
import exampleutils
import cPickle as pickle
import click


@click.command()
@click.option('--workers', default=10, help='how many workers', type=int)
@click.option('--experiments', default=10, help='how many experiments to run', type=int)
@click.option('--outfile', default='flops_benchmark.pickle', 
              help='filename to save results in')
def benchmark(outfile, workers, experiments):

    experiment_data = []
    for exp_i in range(experiments):
        N = workers
        t1 = time.time()
        iters = np.arange(N)

        def f(x):
            hwaddr = exampleutils.get_hwaddr()
            uptime = exampleutils.get_uptime()[0]
            return {'hw_addr' : hwaddr, 
                    'uptime' : uptime}

        pwex = pywren.default_executor()
        futures = pwex.map(f, iters)

        print "invocation done, dur=", time.time() - t1
        print "callset id: ", futures[0].callset_id
        fs_dones, fs_notdones = pywren.wait(futures)
        print "getting results" 
        results = [f.result() for f in futures]
        print "getting status" 
        run_statuses = [f._run_status for f in futures]
        t2 = time.time()
        total_time = t2-t1
        exp = {'total_time' : total_time, 
               'exp_i' : exp_i, 
               'run_statuses' : run_statuses, 
               'callset_id' : futures[0].callset_id, 
               'results' : results}, 
        experiment_data.append(exp)
    pickle.dump(experiment_data, 
                open(outfile, 'w'), -1)

    
if __name__ == "__main__":
    benchmark()
