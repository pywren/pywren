from __future__ import absolute_import

import logging
import os

import pywren.invokers as invokers
import pywren.queues as queues
import pywren.wrenconfig as wrenconfig
from pywren.executor import Executor
from pywren.wait import wait, ALL_COMPLETED, ANY_COMPLETED, ALWAYS # pylint: disable=unused-import

logger = logging.getLogger(__name__)


def default_executor(**kwargs):
    executor_str = 'lambda'
    if 'PYWREN_EXECUTOR' in os.environ:
        executor_str = os.environ['PYWREN_EXECUTOR']

    if executor_str == 'lambda':
        return lambda_executor(**kwargs)
    elif executor_str == 'remote' or executor_str == 'standalone':
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


def dummy_executor(config=None, job_max_runtime=300):
    if config is None:
        config = wrenconfig.default()

    invoker = invokers.DummyInvoker()
    return Executor(invoker, config, job_max_runtime)


def remote_executor(config=None, job_max_runtime=3600):
    if config is None:
        config = wrenconfig.default()

    AWS_REGION = config['account']['aws_region']
    SQS_QUEUE = config['standalone']['sqs_queue_name']
    invoker = queues.SQSInvoker(AWS_REGION, SQS_QUEUE)

    return Executor(invoker, config, job_max_runtime)

def local_executor(invoker_object=None, 
                   config=None, job_max_runtime=300):
    if config is None:
        config = wrenconfig.default()
    if invoker_object is None:
        run_dir = config.get("local_run_dir", "/tmp/task")
        invoker = invokers.LocalInvoker(run_dir=run_dir)
    else:
        invoker = invoker_object
    return Executor(invoker, config, job_max_runtime)

standalone_executor = remote_executor


def get_all_results(fs):
    """
    Take in a list of futures and block until they are repeated,
    call result on each one individually, and return those
    results.

    Will throw an exception if any future threw an exception
    """
    wait(fs, return_when=ALL_COMPLETED)
    return [f.result() for f in fs]
