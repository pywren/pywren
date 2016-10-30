"""
Benchmark microtransactions for s3 -- each thread tries to read and write
a ton of little objects and logging what values are read. 

time python s3_transaction_benchmark.py benchmark  --bucket_name=jonas-pywren-benchmark --keyspace_size=1000 --workers=800 --txn_per_worker=1000

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


NTP_SERVER = 'ntp1.net.berkeley.edu'

OBJ_METADATA_KEY = "benchmark_metadata"
@click.group()
def cli():
    pass


def obj_read_write_txn(s3client, bucket_name, key, id, iter, 
                       skip_read=False):
    """
    simple object transaction
    """
    
    t1 = time.time()
    if not skip_read:
        obj = s3client.get_object(Bucket=bucket_name, Key=key)
        read_data = obj['Body'].read()
        old_id, old_iter = struct.unpack('qq', read_data)
    else:
        old_id = -1
        old_iter = -1
    t2 = time.time()
    write_data = struct.pack('qq', id, iter)
    obj = s3client.put_object(Bucket=bucket_name, 
                              Key = key, 
                              Body=write_data)
    t3 = time.time()
    return old_id, old_iter, t2-t1, t3-t2, 

    

def head_read_write_txn(s3client, bucket_name, key, id, iter, 
                       skip_read=False):
    """
    transaction with just modifying the headers. Note objects
    must already exist! 
    """

    
    t1 = time.time()
    if not skip_read:
        obj = s3client.head_object(Bucket=bucket_name, Key=key)
        metadata_dict = obj['Metadata']
        old_id, old_iter = struct.unpack('qq', read_data)
    else:
        old_id = -1
        old_iter = -1
    t2 = time.time()
    write_data = struct.pack('qq', id, iter)

    copy_source = {
        'Bucket': bucket_name, 
        'Key': key, 
    }
    s3.copy(
        copy_source, bucket_name, key, 
        ExtraArgs={
            "Metadata": {
                "my-new-key": "my-new-value"
            },
            "MetadataDirective": "REPLACE"
        }
    )

    obj = s3client.put_object(Bucket=bucket_name, 
                              Key = key, 
                              Body=write_data)
    t3 = time.time()
    return old_id, old_iter, t2-t1, t3-t2, 

    

@cli.command()
@click.option('--bucket_name', help='bucket to save files in')
@click.option('--keyspace_size', help='how many keys in the space', type=int)
@click.option('--key_prefix', default='', help='S3 key prefix')
@click.option('--workers', default=10, help='how many workers', type=int)
@click.option('--txn_per_worker', default=10, help="transactions attempted by each worker", type=int)
@click.option('--outfile', default='s3_transaction_benchmark.results.pickle', 
              help='filename to save results in')
@click.option('--region', default='us-west-2', help="AWS Region")
@click.option('--begin_delay', default=0, help="start delay ")
def benchmark(bucket_name, keyspace_size, key_prefix, workers, 
              txn_per_worker, outfile, region, begin_delay):

    start_time = time.time()
    print "bucket_name =", bucket_name

    # create a list of a bunch of keys
    keylist = [ key_prefix + str(uuid.uuid4().get_hex().upper()) for _ in range(keyspace_size)]

    print "writing key initial values"
    # write all keys
    local_s3_conn = boto3.client('s3', region)
    [obj_read_write_txn(local_s3_conn, bucket_name, 
                        key, -1, -1, True) for key in keylist]
    print "writing key initial values took {:3.2f} sec".format(time.time() - start_time)
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
        client = boto3.client('s3', region)
        np.random.seed(job_id)

        runlog = []
        for txn_i in range(txn_per_worker):
            t1 = time.time()
            key = np.random.choice(keylist)
            read_id, read_iter, read_dur, write_dur = obj_read_write_txn(client, bucket_name, 
                                                                           key, job_id, 
                                                                           txn_i)
            t2 = time.time()
            log = {'txn_duration' : t2-t1, 
                   'sleep_duration' : sleep_duration,
                   'txn_start' : t1, 
                   'txn_end' : t2, 
                   'txn_i' : txn_i, 
                   'key' : key, 
                   'read_id' : read_id, 
                   'read_dur' : read_dur, 
                   'write_dur' : write_dur, 
                   'read_iter' : read_iter}
            runlog.append(log)
        job_end = time.time()
        return {'runlog' : runlog, 
                'job_start' : job_start, 
                'timeing_offsets' : timing_offsets, 
                'job_end' : job_end}


    print "starting transactions"


    wrenexec = pywren.default_executor()

    fut = wrenexec.map(run_command, range(workers))
    print "launch took {:3.2f} sec".format(time.time()-host_start_time)
    res = [f.result() for f in fut]
    pickle.dump({'log' : res,
                 'host_start_time' : host_start_time, 
                 'begin_delay' : begin_delay, 
                 'keyspace_size' : keyspace_size, 
                 'workers' : workers, 
                 'txn_per_worker' : txn_per_worker,}, 
                open(outfile, 'w'))

if __name__ == '__main__':
    cli()
