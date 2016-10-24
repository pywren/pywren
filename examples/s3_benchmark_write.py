"""
Benchmark aggregate write throughput to S3
use:

python s3_benchmark.py s3bucket jobnumber

Note that you want to write to the root
of an s3 bucket (no prefix) to get maximum
performance as s3 apparently shards
based on the first six chars of the keyname. 

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

if __name__ == "__main__":
    bucket_name = sys.argv[1]
    MB_to_write = int(sys.argv[2]) 
    number_to_write = int(sys.argv[3])

    def run_command(key_name):

        data = np.random.rand(int(MB_to_write*1e6/8)).tostring()
        actual_bytes = len(data)

        client = boto3.client('s3', 'us-west-2')
        t1 = time.time()
        client.put_object(Bucket=bucket_name, 
                          Key = key_name,
                          Body=actual_bytes)
        t2 = time.time()


        mb_rate = actual_bytes/(t2-t1)/1e6
        return t1, t2, mb_rate

    wrenexec = pywren.default_executor()

    # create list of random keys
    keynames = [ str(uuid.uuid4().get_hex().upper()) for _ in range(number_to_write)]
    
    fut = wrenexec.map(run_command, range(N))

    res = [f.result() for f in fut]
    pickle.dump(res, open('s3_benchmark_write.output.pickle', 'w'))
