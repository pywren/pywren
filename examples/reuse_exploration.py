import numpy as np
import time
import sys
import pywren
import logging
import exampleutils
import cPickle as pickle
import click
import exampleutils


@click.command()
@click.option('--workers', default=10, help='how many workers', type=int)
@click.option('--experiments', default=2, help='how many experiments to run', type=int)
@click.option('--outfile', default='reuse_explore.pickle')
@click.option('--eta', default=1.0, help='increase dealy between exp by eta', type=float)
@click.option('--sleep', default=1.0, help='time to sleep', type=float)
def benchmark(outfile, workers, experiments, eta, sleep):

    experiment_data = []
    for exp_i in range(experiments):
        print "running experiment {} ---------------------------".format(exp_i)
        N = workers
        t1 = time.time()
        iters = np.arange(N)

        def fingerprint(x):
            timing_responses = {}
            for server in exampleutils.NTP_SERVERS:
                ts_os = exampleutils.get_time_offset(server, 4)
                timing_responses[server] = ts_os


            hwaddr = exampleutils.get_hwaddr()
            uptime = exampleutils.get_uptime()[0]
            time.sleep(sleep)
            return {'hw_addr' : hwaddr, 
                    'ntp_offsets' : timing_responses, 
                    'uptime' : uptime}

        pwex = pywren.default_executor()
        futures = pwex.map(fingerprint, iters)

        print "invocation done, dur=", time.time() - t1
        print "callset id: ", futures[0].callset_id
        fs_dones, fs_notdones = pywren.wait(futures)
        # get the job state of all of them
        print len(fs_dones), len(fs_notdones)
        for f in futures:
            if f._state == pywren.wren.JobState.success or f._state == pywren.wren.JobState.error:
                pass
            else:
                print f._state

        
        print "getting results" 
        results = [f.result() for f in futures]
        print "getting status" 
        run_statuses = [f._run_status for f in futures]
        t2 = time.time()
        total_time = t2-t1
        sleep_for = eta**exp_i
        exp = {'total_time' : total_time, 
               'exp_i' : exp_i, 
               'sleep_for' : sleep_for,
               'run_statuses' : run_statuses, 
               'callset_id' : futures[0].callset_id, 
               'results' : results}
        experiment_data.append(exp)
        print "seeping for", sleep_for/60.0, "min"
        time.sleep(sleep_for)
    pickle.dump(experiment_data, 
                open(outfile, 'w'), -1)

    
if __name__ == "__main__":
    benchmark()
