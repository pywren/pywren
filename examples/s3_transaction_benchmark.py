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


@click.group()
def cli():
    pass


def obj_read_write_txn(s3client, key, id, iter, 
                       skip_read=False):
    """
    simple object transaction
    """
    
    obj = s3client.get_object(Bucket=bucket_name, Key=key)
    
    if not skip_read:
        read_data = obj['Body'].read()
        old_id, old_iter = struct.unpack('qq', read_data)
    else:
        old_id = -1
        old_iter = -1

    write_data = struct.pack('qq', id, iter)
    obj = s3client.put_object(Bucket=bucket_name, 
                            Key = key, 
                            Body=write_data)
    return old_id, old_iter

    

@cli.command()
@click.option('--bucket_name', help='bucket to save files in')
@click.option('--keyspace_size', help='how many keys in the space', type=int)
@click.option('--key_prefix', default='', help='S3 key prefix')
@click.option('--workers', defaults=10, help='how many workers', type=int)
@click.option('--txn_per_job', default=10, help="transactions attempted by each job", type=int)
@click.option('--outfile', default='s3_transaction_benchmark.results.pickle', 
              help='filename to save results in')
@click.option('--region', default='us-west-2', help="AWS Region")
def write(bucket_name, keyspace_size, key_prefix, workers, 
          txn_per_job,outfile, region):

    print "bucket_name =", bucket_name

    # create a list of a bunch of keys
    keynames = [ key_prefix + str(uuid.uuid4().get_hex().upper()) for _ in range(keyspace_size)]

    def run_command(job_id):
        client = boto3.client('s3', region)
        np.random.seed(job_id)

        runlog = []
        for txn_i in range(txn_per_job):
            t1 = time.time()
            key = np.random.choice(keynames)
            old_id, old_iter = obj_read_write_txn(client, 
                                                  key, job_id, 
                                                  txn_i)
            t2 = time.time()
            log = {'txn_duration' : t2-t1, 
                   'txn_i' : txn_i, 
                   'key' : key, 
                   'read_id' : read_id, 
                   'read_iter' : read_iter}
            runlog.append(log)
        return runlog

    wrenexec = pywren.default_executor()

    # create list of random keys
    keynames = [ key_prefix + str(uuid.uuid4().get_hex().upper()) for _ in range(number)]
                
    fut = wrenexec.map(run_command, range(workers))

    res = [f.result() for f in fut]
    pickle.dump(res, open(outfile, 'w'))

if __name__ == '__main__':
    cli()
