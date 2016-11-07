"""
Benchmark microtransactions for s3 -- each thread tries to read and write
a ton of little objects and logging what values are read. 

"""

import time
import boto3 
import uuid
import numpy as np
import time
import pywren
import subprocess
import logging
import sys
import boto3
import hashlib
import cPickle as pickle
import uuid
import click
import exampleutils
import pandas as pd
import struct
import ntplib
import exampleutils
import base64


NTP_SERVER = 'ntp1.net.berkeley.edu'

OBJ_METADATA_KEY = "benchmark_metadata"
DOMAIN_NAME = "test-domain" 

@click.command()
@click.option('--bucket_name', default=None,  help='bucket to save files in')
@click.option('--key_prefix', default='', help='S3 key prefix')
@click.option('--workers', default=10, help='how many workers', type=int)
@click.option('--outfile', default='net_benchmark.results.pickle', 
              help='filename to save results in')
@click.option('--region', default='us-west-2', help="AWS Region")
@click.option('--begin_delay', default=0, help="start delay ")
def benchmark(bucket_name, key_prefix, workers, 
              outfile, region, begin_delay, mode):

    start_time = time.time()
    print "bucket_name =", bucket_name

    host_start_time = time.time()
    wait_until = host_start_time + begin_delay


    def run_command(job_id):
        # get timing offset
        timing_offsets = exampleutils.get_time_offset(NTP_SERVER, 4)

        # first pause (for sync)
        sleep_duration = wait_until - time.time()
        if sleep_duration > 0:
            time.sleep(sleep_duration)

        # start the job
        job_start = time.time()


        return {'job_start' : job_start, 
                'timing_offsets' : timing_offsets, 
                'job_end' : job_end}


    print "starting transactions"


    wrenexec = pywren.default_executor()

    fut = wrenexec.map(run_command, range(workers))
    print "launch took {:3.2f} sec".format(time.time()-host_start_time)
    res = [f.result() for f in fut]
    pickle.dump({
        'host_start_time' : host_start_time, 
        'begin_delay' : begin_delay, 
        'workers' : workers, 
        'res' : res}
                open(outfile, 'w'))

if __name__ == '__main__':
    benchmark()


