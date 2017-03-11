from __future__ import absolute_import

try:
    from six.moves import cPickle as pickle
except:
    import pickle
from tblib import pickling_support
import logging
import os
import time
from multiprocessing.pool import ThreadPool
pickling_support.install()

from pywren.executor import Executor
from pywren.future import JobState
import pywren.wrenconfig as wrenconfig
import pywren.invokers as invokers
import pywren.s3util as s3util

logger = logging.getLogger(__name__)


def default_executor(**kwargs):
    executor_str = 'lambda'
    if 'PYWREN_EXECUTOR' in os.environ:
        executor_str = os.environ['PYWREN_EXECUTOR']

    if executor_str == 'lambda':
        return lambda_executor(**kwargs)
    elif executor_str == 'remote' or executor_str=='standalone':
        return remote_executor(**kwargs)
    elif executor_str == 'dummy':
        return dummy_executor(**kwargs)
    return lambda_executor(**kwargs)


def lambda_executor(config= None, job_max_runtime=280, shard_runtime=False):

    if config is None:
        config = wrenconfig.default()

    AWS_REGION = config['account']['aws_region']
    FUNCTION_NAME = config['lambda']['function_name']
    S3_BUCKET = config['s3']['bucket']
    S3_PREFIX = config['s3']['pywren_prefix']

    invoker = invokers.LambdaInvoker(AWS_REGION, FUNCTION_NAME)
    return Executor(AWS_REGION, S3_BUCKET, S3_PREFIX, invoker, config,
                    job_max_runtime, shard_runtime=shard_runtime)


def dummy_executor(shard_runtime=False):
    config = wrenconfig.default()
    AWS_REGION = config['account']['aws_region']
    S3_BUCKET = config['s3']['bucket']
    S3_PREFIX = config['s3']['pywren_prefix']
    invoker = invokers.DummyInvoker()
    return Executor(AWS_REGION, S3_BUCKET, S3_PREFIX, invoker, config,
                    100, shard_runtime=shard_runtime)

def remote_executor(config= None, job_max_runtime=3600,
                    shard_runtime=False):
    if config is None:
        config = wrenconfig.default()

    AWS_REGION = config['account']['aws_region']
    SQS_QUEUE = config['standalone']['sqs_queue_name']
    S3_BUCKET = config['s3']['bucket']
    S3_PREFIX = config['s3']['pywren_prefix']
    invoker = invokers.SQSInvoker(AWS_REGION, SQS_QUEUE)
    return Executor(AWS_REGION, S3_BUCKET, S3_PREFIX, invoker, config,
                    job_max_runtime, shard_runtime=shard_runtime)


ALL_COMPLETED = 1
ANY_COMPLETED = 2
ALWAYS = 3

def wait(fs, return_when=ALL_COMPLETED, THREADPOOL_SIZE=64, 
         WAIT_DUR_SEC=5):
    """
    this will eventually provide an optimization for checking if a large
    number of futures have completed without too much network traffic
    by exploiting the callset
    
    From python docs:
    
    Wait for the Future instances (possibly created by different Executor
    instances) given by fs to complete. Returns a named 2-tuple of
    sets. The first set, named "done", contains the futures that completed
    (finished or were cancelled) before the wait completed. The second
    set, named "not_done", contains uncompleted futures.


    http://pythonhosted.org/futures/#concurrent.futures.wait

    """
    N = len(fs)

    if return_when==ALL_COMPLETED:
        result_count = 0
        while result_count < N:

            fs_dones, fs_notdones = _wait(fs, THREADPOOL_SIZE)
            result_count = len(fs_dones)

            if result_count == N:
                return fs_dones, fs_notdones
            else:
                time.sleep(WAIT_DUR_SEC)

    elif return_when == ANY_COMPLETED:
        while True:
            fs_dones, fs_notdones = _wait(fs, THREADPOOL_SIZE)

            if len(fs_dones) != 0:
                return fs_dones, fs_notdones
            else:
                time.sleep(WAIT_DUR_SEC)

    elif return_when == ALWAYS:
        return _wait(fs, THREADPOOL_SIZE)
    else:
        raise ValueError()

def _wait(fs, THREADPOOL_SIZE):
    """
    internal function that performs the majority of the WAIT task
    work. 
    """


    # get all the futures that are not yet done
    not_done_futures =  [f for f in fs if f._state not in [JobState.success, 
                                                       JobState.error]]
    if len(not_done_futures) == 0:
        return fs, []

    # check if the not-done ones have the same callset_id
    present_callsets = set([f.callset_id for f in not_done_futures])
    if len(present_callsets) > 1:
        raise NotImplementedError()

    # get the list of all objects in this callset
    callset_id = present_callsets.pop() # FIXME assume only one
    f0 = not_done_futures[0] # This is a hack too 

    callids_done = s3util.get_callset_done(f0.s3_bucket, 
                                           f0.s3_prefix,
                                           callset_id)
    callids_done = set(callids_done)

    fs_dones = []
    fs_notdones = []

    f_to_wait_on = []
    for f in fs:
        if f._state in [JobState.success, JobState.error]:
            # done, don't need to do anything
            fs_dones.append(f)
        else:
            if f.call_id in callids_done:
                f_to_wait_on.append(f)
                fs_dones.append(f)
            else:
                fs_notdones.append(f)
    def test(f):
        f.result(throw_except=False)
    pool = ThreadPool(THREADPOOL_SIZE)
    pool.map(test, f_to_wait_on)

    pool.close()
    pool.join()

    return fs_dones, fs_notdones

    
def log_test():
    logger.info("logging from pywren.wren")


def get_all_results(fs):
    """
    Take in a list of futures and block until they are repeated, 
    call result on each one individually, and return those
    results. 
    
    Will throw an exception if any future threw an exception
    """
    wait(fs, return_when=ALL_COMPLETED)
    return [f.result() for f in fs]
