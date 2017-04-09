from __future__ import absolute_import

try:
    from six.moves import cPickle as pickle
except:
    import pickle
from tblib import pickling_support
import logging
import botocore
import glob2
import random
import os
pickling_support.install()

from pywren.executor import Executor
import pywren.wrenconfig as wrenconfig
import pywren.invokers as invokers
from pywren.wait import wait
from pywren.wait import *

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


def lambda_executor(config=None, job_max_runtime=280):
    if config is None:
        config = wrenconfig.default()

    AWS_REGION = config['account']['aws_region']
    FUNCTION_NAME = config['lambda']['function_name']
    invoker = invokers.LambdaInvoker(AWS_REGION, FUNCTION_NAME)

    return Executor(invoker, config, job_max_runtime)


def dummy_executor(config=None, job_max_runtime=100):
    if config is None:
        config = wrenconfig.default()

    invoker = invokers.DummyInvoker()
    return Executor(invoker, config, job_max_runtime)


def remote_executor(config=None, job_max_runtime=3600):
    if config is None:
        config = wrenconfig.default()

    AWS_REGION = config['account']['aws_region']
    SQS_QUEUE = config['standalone']['sqs_queue_name']
    invoker = invokers.SQSInvoker(AWS_REGION, SQS_QUEUE)

    return Executor(invoker, config, job_max_runtime)

standalone_executor = remote_executor


def save_futures_to_string(futures):
    """
    Saving a list of futures to string.
    :param futures: A list of futures to save.
    :return: A serialized string.
    """
    for f in list(futures):
        f.storage = None
    return pickle.dumps(list(futures))


def load_futures_from_string(fut_str):
    """
    Load futures from a string.
    :param fut_str: A string.
    :return: A list of futures.
    """
    return list(pickle.loads(fut_str))


def save_futures(futures, filename):
    """
    Save a list of futures into a file.
    :param futures: A list of futures
    :param filename: file name
    :return: None
    """
    for f in list(futures):
        f.storage = None
    pickle.dump(list(futures), open(filename, "wb"))


def load_futures(filename):
    """
    Load futures from file
    :param filename: file name to load from
    :return: A list of futures
    """
    return list(pickle.load(open(filename, "rb")))


def get_all_results(fs):
    """
    Take in a list of futures and block until they are repeated, 
    call result on each one individually, and return those
    results. 
    
    Will throw an exception if any future threw an exception
    """
    wait(fs, return_when=ALL_COMPLETED)
    return [f.result() for f in fs]
