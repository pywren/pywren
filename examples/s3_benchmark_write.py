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
import click

@click.group()
def cli():
    pass

class RandomDataGenerator(object):

    def __init__(self, bytes_total):
        self.bytes_total = bytes_total
        self.pos = 0

    def tell(self):
        print "tell", self.pos
        return self.pos

    def seek(self, pos, whence=0):
        print "seek","pos=", pos, "whence=", whence
        if whence == 0:
            self.pos = pos
        elif whence == 1:
            self.pos += pos
        elif whence == 2:
            self.pos = self.bytes_total - pos

    def read(self, bytes_requested):
        remaining_bytes = self.bytes_total - self.pos
        if remaining_bytes == 0:
            print "requested", bytes_requested, "returning 0"
            return ""
        
        data = np.arange(self.pos, self.pos + bytes_requested, dtype=np.uint8).tostring()
        
        bytes_out = min(remaining_bytes, bytes_requested)

        byte_data = data[:bytes_out]
                
        self.pos += bytes_out

        print "requested", bytes_requested, "returning", len(byte_data)

        return byte_data

@cli.command()
@click.option('--bucket_name', help='bucket to save files in')
@click.option('--mb_per_file', help='MB of each object in S3', type=int)
@click.option('--number', help='number of files', type=int)
@click.option('--key_prefix', default='', help='S3 key prefix')
@click.option('--outfile', default='s3_benchmark_write.output.pickle', 
              help='filename to save results in')
@click.option('--key_file', default=None, help="filename to write keynames to (for later read)")
@click.option('--region', default='us-west-2', help="AWS Region")
def write(bucket_name, mb_per_file, number, key_prefix, 
              outfile, key_file, region):

    print "bucket_name =", bucket_name
    print "mb_per_file =", mb_per_file
    print "number=", number
    print "key_prefix=", key_prefix

    def run_command(key_name):
        bytes_n = mb_per_file * 1024**2
        d = RandomDataGenerator(bytes_n)

        client = boto3.client('s3', region)
        t1 = time.time()
        client.put_object(Bucket=bucket_name, 
                          Key = key_name,
                          Body=d)
        t2 = time.time()


        mb_rate = bytes_n/(t2-t1)/1e6
        return t1, t2, mb_rate

    wrenexec = pywren.default_executor()

    # create list of random keys
    keynames = [ key_prefix + str(uuid.uuid4().get_hex().upper()) for _ in range(number)]
    
    if key_file is not None:
        fid = open(key_file, 'w')
        for k in keynames:
            fid.write("{}\n".format(k))
                
    fut = wrenexec.map(run_command, keynames)

    res = [f.result() for f in fut]
    pickle.dump(res, open(outfile, 'w'))

@cli.command()
@click.option('--bucket_name', help='bucket to save files in')
@click.option('--number', help='number of objects to read', type=int)
@click.option('--outfile', default='s3_benchmark_read.output.pickle', 
              help='filename to save results in')
@click.option('--key_file', default=None, help="filename to write keynames to (for later read)")
@click.option('--s3_key', default=None, help="s3 key to read (repeat)")
@click.option('--read_times', default=1, help="number of times to read each s3 key")
@click.option('--region', default='us-west-2', help="AWS Region")
def read(bucket_name, number, 
         outfile, key_file, s3_key, read_times, region):
    if key_file is None and s3_key is None:
        print "must specify either a single key to repeatedly read ( --s3_key) or a text file with keynames (--key_file)"
        sys.exit(1)
    
    blocksize = 1024*1024

    def run_command(key):
        client = boto3.client('s3', region)

        m = hashlib.md5()
        bytes_read = 0

        t1 = time.time()
        for i in range(read_times):
            obj = client.get_object(Bucket=bucket_name, Key=key)

            fileobj = obj['Body']

            buf = fileobj.read(blocksize)
            while len(buf) > 0:
                bytes_read += len(buf)
                m.update(buf)
                buf = fileobj.read(blocksize)
        t2 = time.time()

        a = m.hexdigest()
        mb_rate = bytes_read/(t2-t1)/1e6
        return t1, t2, mb_rate, 

    wrenexec = pywren.default_executor()
    
    if s3_key is not None:
        keylist = [s3_key] * number
    else:
        fid = open(key_file, 'r')
        keylist = [k.strip() for k in fid.readlines()]

    fut = wrenexec.map(run_command, keylist)

    res = [f.result() for f in fut]
    pickle.dump(res, open('s3_benchmark.read.output.pickle', 'w'))

if __name__ == '__main__':
    cli()
