#
# Copyright 2018 PyWren Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
    """
    Initialize and return an executor object.

    :param config: Settings passed in here will override those in `pywren_config`. Default None.
    :param job_max_runtime: Max time per lambda. Default 200
    :return `executor` object.

    Usage
      >>> import pywren
      >>> pwex = pywren.default_executor()
    """
    executor_str = 'lambda'
    if 'PYWREN_EXECUTOR' in os.environ:
        executor_str = os.environ['PYWREN_EXECUTOR']

    if executor_str == 'lambda':
        return lambda_executor(**kwargs)
    elif executor_str in ('remote', 'standalone'):
        return remote_executor(**kwargs)
    elif executor_str == 'dummy':
        return dummy_executor(**kwargs)
    elif executor_str == 'local':
        return local_executor(**kwargs)
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
        if "local_run_dir" in config:
            invoker = invokers.LocalInvoker(run_dir=config["local_run_dir"])
        else:
            invoker = invokers.LocalInvoker()
    else:
        invoker = invoker_object
    return Executor(invoker, config, job_max_runtime)

standalone_executor = remote_executor


def get_all_results(fs):
    """
    Take in a list of futures and block until they are completed.
    call result on each one individually, and return those
    results.

    :param fs: a list of futures.
    :return: A list of the results of each futures
    :rtype: list

    Usage
      >>> pwex = pywren.default_executor()
      >>> futures = pwex.map(foo, data)
      >>> results = get_all_results(futures)
    """
    wait(fs, return_when=ALL_COMPLETED)
    return [f.result() for f in fs]
