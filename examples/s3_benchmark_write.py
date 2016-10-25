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

@click.command()
@click.option('--bucket_name', help='bucket to save files in')
@click.option('--mb_per_file', help='MB of each object in S3', type=int)
@click.option('--number', help='number of files', type=int)
@click.option('--key_prefix', default='', help='S3 key prefix')
@click.option('--outfile', default='s3_benchmark_write.output.pickle', 
              help='filename to save results in')
def benchmark(bucket_name, mb_per_file, number, key_prefix, outfile):

    print "bucket_name =", bucket_name
    print "mb_per_file =", mb_per_file
    print "number=", number
    print "key_prefix=", key_prefix

    def run_command(key_name):
        bytes_n = mb_per_file * 1024**2
        d = RandomDataGenerator(bytes_n)

        client = boto3.client('s3', 'us-west-2')
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
    
    fut = wrenexec.map(run_command, keynames)

    res = [f.result() for f in fut]
    pickle.dump(res, open(outfile, 'w'))


if __name__ == '__main__':
    benchmark()
