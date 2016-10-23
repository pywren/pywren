#from gevent import monkey

#monkey.patch_socket()
#monkey.patch_ssl()

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

def run_command(x):
    client = boto3.client('s3')
    
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('ericmjonas-public')
    all_keys = []
    for obj in bucket.objects.all():
        all_keys.append(str(obj.key))
    return all_keys

if __name__ == "__main__":

    wrenexec = pywren.default_executor()
    fut = wrenexec.call_async(run_command, None)
    print fut.callset_id

    res = fut.result() 
    print res
