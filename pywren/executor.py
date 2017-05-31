from __future__ import absolute_import

import boto3
import json
try:
    from six.moves import cPickle as pickle
except:
    import pickle
import multiprocessing
from multiprocessing.pool import ThreadPool
import threading
import time
import enum
import random
import logging
import botocore
import glob2
import os
import Queue

import pywren.version as version
import pywren.wrenconfig as wrenconfig
import pywren.wrenutil as wrenutil
import pywren.runtime as runtime
import pywren.storage as storage
from pywren.serialize import cloudpickle, serialize
from pywren.serialize import create_mod_data
from pywren.future import ResponseFuture, JobState
from pywren.wait import *
from pywren.scheduler import Scheduler, SchedulerCommand

logger = logging.getLogger(__name__)


class CallSet(object):
    def __init__(self, storage_config, job_desp):
        self.callset_id = wrenutil.create_callset_id()
        self.call_ids = ["{:05d}".format(i) for i in range(len(job_desp.iterdata))]
        self.storage_config = storage_config
        self.job_desp = job_desp
        self.futures = [ResponseFuture(call_id,
                        self.callset_id, {}, self.storage_config) for call_id in self.call_ids]


class JobDescription(object):
    def __init__(self, func, iterdata, extra_env = None, extra_meta = None, data_all_as_one=True,
                 overwrite_invoke_args = None, rate=10000):
        self.func = func
        self.iterdata = iterdata
        self.extra_env = extra_env
        self.extra_meta = extra_meta
        self.data_all_as_one = data_all_as_one
        self.overwrite_invoke_args = overwrite_invoke_args
        self.rate = rate


class Executor(object):

    def __init__(self, invoker, config, job_max_runtime, invoke_pool_threads=64, use_cached_runtime=True):

        self.storage_config = wrenconfig.extract_storage_config(config)

        self.job_queue = Queue.Queue()
        self.scheduler = Scheduler(self.job_queue, invoker, config,
                                   job_max_runtime, invoke_pool_threads, use_cached_runtime)

        self.scheduler_thread = threading.Thread(target=self.start_scheduler)
        self.scheduler_thread.start()

    def start_scheduler(self):
        self.scheduler.run()

    def map(self, func, iterdata, extra_env = None, extra_meta = None, data_all_as_one=True,
            overwrite_invoke_args = None, rate=10000):
        job_desp = JobDescription(func, list(iterdata), extra_env, extra_meta,
                                  data_all_as_one, overwrite_invoke_args, rate)
        job = CallSet(self.storage_config, job_desp)
        self.job_queue.put(job)
        self.job_queue.put(SchedulerCommand.NO_MORE_JOB)
        self.scheduler_thread.join()
        return job.futures



    def call_async(self, func, data, extra_env = None,
                   extra_meta=None):
        return self.map(func, [data],  extra_env, extra_meta)[0]


    def reduce(self, function, list_of_futures,
               extra_env = None, extra_meta = None):
        """
        Apply a function across all futures.

        # FIXME change to lazy iterator
        """
        #if self.invoker.TIME_LIMIT:
        wait(list_of_futures, return_when=ALL_COMPLETED) # avoid race condition

        def reduce_func(fut_list):
            # FIXME speed this up for big reduce
            accum_list = []
            for f in fut_list:
                accum_list.append(f.result())
            return function(accum_list)

        return self.call_async(reduce_func, list_of_futures,
                               extra_env=extra_env, extra_meta=extra_meta)


    def get_logs(self, future, verbose=True):


        logclient = boto3.client('logs', region_name=self.config['account']['aws_region'])


        log_group_name = future.run_status['log_group_name']
        log_stream_name = future.run_status['log_stream_name']

        aws_request_id = future.run_status['aws_request_id']

        log_events = logclient.get_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,)
        if verbose: # FIXME use logger
            print("log events returned")
        this_events_logs = []
        in_this_event = False
        for event in log_events['events']:
            start_string = "START RequestId: {}".format(aws_request_id)
            end_string = "REPORT RequestId: {}".format(aws_request_id)

            message = event['message'].strip()
            timestamp = int(event['timestamp'])
            if verbose:
                print(timestamp, message)
            if start_string in message:
                in_this_event = True
            elif end_string in message:
                in_this_event = False
                this_events_logs.append((timestamp, message))

            if in_this_event:
                this_events_logs.append((timestamp, message))

        return this_events_logs
