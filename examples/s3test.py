import gevent

from gevent import monkey

# monkey.patch_socket()
# monkey.patch_ssl()
# monkey.patch_all()
# from gevent.pool import Pool

import sys
sys.path.append("../")
import pywren.wrenconfig
import botocore
from multiprocessing.pool import ThreadPool
import time


config = botocore.config.Config(max_pool_connections=20)

session = botocore.session.get_session()
global_s3_client = session.create_client('s3', region_name = pywren.wrenconfig.AWS_REGION, config=config)

invoke_pool_threads = 512

REQUESTS = 400

KEY = "pywren.jobs/02f6537c-a195-497b-8c0b-41fb2ccb1690/55a88e7b-b780-49bb-9e7e-7d998b97c814/output.pickle"
BUCKET = "jonas-testbucket2"

#pool = Pool(64)

def get():


    s3 = global_s3_client
    r = s3.get_object(Bucket = BUCKET, Key = KEY)
    result = r['Body'].read()
    return result

t1 = time.time()

pool = ThreadPool(invoke_pool_threads)

for i in range(REQUESTS):
    call_id = "{:05d}".format(i)

    cb = pool.apply_async(get)
    
pool.close()
pool.join()

#jobs = [pool.spawn(get) for _ in range(REQUESTS)]
#pool.join()

t2 = time.time()
print "Made", REQUESTS, "in", t2-t1, "sec ({:3.2f} req/s".format(REQUESTS/(t2-t1)), ")" 

