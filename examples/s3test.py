import gevent

from gevent import monkey

monkey.patch_socket()
monkey.patch_ssl()
monkey.patch_all()

import sys
sys.path.append("../")
import pywren.wrenconfig
import boto3
from multiprocessing.pool import ThreadPool
import time


global_s3_client =  boto3.client('s3', region_name = pywren.wrenconfig.AWS_REGION)

invoke_pool_threads = 32

REQUESTS = 1000

KEY = "pywren.jobs/02f6537c-a195-497b-8c0b-41fb2ccb1690/55a88e7b-b780-49bb-9e7e-7d998b97c814/output.pickle"
BUCKET = "jonas-testbucket2"

def get():
    s3 = global_s3_client
    r = s3.get_object(Bucket = BUCKET, Key = KEY)
    result = r['Body'].read()
    return result


pool = ThreadPool(invoke_pool_threads)
t1 = time.time()

# for i in range(REQUESTS):
#     call_id = "{:05d}".format(i)

#     cb = pool.apply_async(get)
    
# pool.close()

# pool.join()

jobs = [gevent.spawn(get) for _ in range(REQUESTS)]
gevent.wait(jobs)

t2 = time.time()
print "Made", REQUESTS, "in", t2-t1, "sec ({:3.2f} req/s".format(REQUESTS/(t2-t1)), ")" 

