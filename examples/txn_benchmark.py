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
        read_data = metadata_dict[OBJ_METADATA_KEY]
        old_id, old_iter = struct.unpack('qq', base64.b64decode(read_data))
    else:
        old_id = -1
        old_iter = -1
    t2 = time.time()
    write_data = struct.pack('qq', id, iter)

    copy_source = {
        'Bucket': bucket_name, 
        'Key': key, 
    }
    s3client.copy(
        copy_source, bucket_name, key, 
        ExtraArgs={
            "Metadata": {
                OBJ_METADATA_KEY: base64.b64encode(write_data)
            },
            "MetadataDirective": "REPLACE"
        }
    )

    t3 = time.time()
    return old_id, old_iter, t2-t1, t3-t2, 


def sdb_read_write_txn(sdb_client, domain_name, 
                       item_name, 
                       id, iter, 
                       skip_read=False):
    """
    simpledb 
    """
    
    t1 = time.time()
    if not skip_read:
        resp = sdb_client.get_attributes(
            DomainName=domain_name,
            ItemName=item_name,
            AttributeNames=['writer_id', 'iter'],
            ConsistentRead=False)

        a = exampleutils.sdb_attr_to_dict(resp['Attributes'])        
        old_id = int(a['writer_id'])
        old_iter = int(a['iter'] )
    else:
        old_id = -1
        old_iter = -1
    t2 = time.time()
    write_dict = {'writer_id' : str(id), 
                  'iter' : str(iter)}
    
    write_attr = exampleutils.dict_to_sdb_attr(write_dict, True)
    r = sdb_client.put_attributes(DomainName = domain_name, 
                                  ItemName = item_name, 
                                  Attributes = write_attr)
    t3 = time.time()
    return old_id, old_iter, t2-t1, t3-t2, 

        

@click.command()
@click.option('--bucket_name', default=None,  help='bucket to save files in')
@click.option('--mode', default='s3obj', help='what resource [s3obj, s3head, sdb]')
@click.option('--keyspace_size', help='how many keys in the space', type=int)
@click.option('--key_prefix', default='', help='S3 key prefix')
@click.option('--workers', default=10, help='how many workers', type=int)
@click.option('--txn_per_worker', default=10, help="transactions attempted by each worker", type=int)
@click.option('--outfile', default='s3_transaction_benchmark.results.pickle', 
              help='filename to save results in')
@click.option('--region', default='us-west-2', help="AWS Region")
@click.option('--begin_delay', default=0, help="start delay ")
def benchmark(bucket_name, keyspace_size, key_prefix, workers, 
              txn_per_worker, outfile, region, begin_delay, mode):

    start_time = time.time()
    print "bucket_name =", bucket_name

    # create a list of a bunch of keys
    keylist = [ key_prefix + str(uuid.uuid4().get_hex().upper()) for _ in range(keyspace_size)]

    print "writing key initial values"


    if mode == 's3obj' or mode == 's3head':

        local_s3_conn = boto3.client('s3', region)
        [obj_read_write_txn(local_s3_conn, bucket_name, 
                            key, -1, -1, True) for key in keylist]
        if mode == 's3obj' : 
            txn_func = obj_read_write_txn
        else:
            #also write metadata header
            [head_read_write_txn(local_s3_conn, bucket_name, 
                                 key, -1, -1, True) for key in keylist]
            txn_func = head_read_write_txn
    elif mode == 'sdb':
        local_sdb_conn =  boto3.client('sdb', region)
        # create item list: 
        for ks in np.array_split(keylist, keyspace_size // 20):
            items = []
        
            for k in ks:
                items.append({'Name' : k, 
                              'Attributes' : [
                                  {'Name' : 'writer_id', 
                                   'Value' : '-1', 
                                   'Replace' : True}, 
                                  {'Name' : 'iter', 
                                   'Value' : '-1', 
                                   'Replace' : True}]
                              })
            response = local_sdb_conn.batch_put_attributes(
                DomainName=bucket_name,
                Items=items)
        txn_func = sdb_read_write_txn  
    else:
        raise ValueError("unknown mode {}".format(mode))

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
        if mode == 's3obj' or mode == 's3head':
            
            client = boto3.client('s3', region)
        elif mode == 'sdb':
            client = boto3.client('sdb', region)

        np.random.seed(job_id)

        runlog = []
        for txn_i in range(txn_per_worker):
            t1 = time.time()
            key_i = np.random.randint(keyspace_size)
            key = keylist[key_i]
            read_id, read_iter, read_dur, write_dur = txn_func(client, bucket_name, 
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
                'timing_offsets' : timing_offsets, 
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
                 'keylist' : keylist,                 
                 'mode' : mode, 
                 'workers' : workers, 
                 'txn_per_worker' : txn_per_worker,}, 
                open(outfile, 'w'))

if __name__ == '__main__':
    benchmark()
