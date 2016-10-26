"""
Benchmark aggregate throughput from S3

Downloads a big file (~400 MB) from S3 several times
per job, computes the md5sum, and returns the
bandwidth time. 

use:

python s3_benchmark.py 100

where 100 is the number of simultaneous workers. 

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

loops = 5
blocksize = 1024*1024

def run_command(x):
    client = boto3.client('s3', 'us-west-2')

    m = hashlib.md5()
    bytes_read = 0

    t1 = time.time()
    for i in range(loops):
        a = client.get_object(Bucket='ericmjonas-public',Key='anaconda.sh')
    
        fileobj = a['Body']

        buf = fileobj.read(blocksize)
        while len(buf) > 0:
            bytes_read += len(buf)
            m.update(buf)
            buf = fileobj.read(blocksize)
    t2 = time.time()

    a = m.hexdigest()
    mb_rate = bytes_read/(t2-t1)/1e6
    return t1, t2, mb_rate

if __name__ == "__main__":

    wrenexec = pywren.default_executor()
    N = int(sys.argv[1])
    fut = wrenexec.map(run_command, range(N))

    res = [f.result() for f in fut]
    pickle.dump(res, open('s3_benchmark_read.output.pickle', 'w'))
