from __future__ import absolute_import

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

logger = logging.getLogger(__name__)

from pywren.future import ResponseFuture, JobState


class SchedulerCommand(enum.Enum):
    NO_MORE_JOB = 1


class Scheduler(object):
    def __init__(self, queue, invoker, config, job_max_runtime,
                 invoke_pool_threads=64, use_cached_runtime=True):
        self.queue = queue
        self.config = config
        self.invoker = invoker
        self.job_max_runtime = job_max_runtime
        self.invoke_pool_threads = invoke_pool_threads
        self.use_cached_runtime = use_cached_runtime

        self.config = config
        self.storage_config = wrenconfig.extract_storage_config(self.config)
        self.storage = storage.Storage(self.storage_config)
        self.runtime_meta_info = runtime.get_runtime_info(config['runtime'], self.storage)

        if 'preinstalls' in self.runtime_meta_info:
            logger.info("using serializer with meta-supplied preinstalls")
            self.serializer = serialize.SerializeIndependent(self.runtime_meta_info['preinstalls'])
        else:
            self.serializer =  serialize.SerializeIndependent()

    def run(self):
        while True:
            if not self.queue.empty():
                job = self.queue.get()
                if job == SchedulerCommand.NO_MORE_JOB:
                    return
                self.run_job(job)


    def put_data(self, data_key, data_str,
                 callset_id, call_id):

        self.storage.put_object(data_key, data_str)
        logger.info("call_async {} {} data upload complete {}".format(callset_id, call_id,
                                                                      data_key))

    def invoke_with_keys(self, future, func_key, data_key, output_key,
                         status_key,
                         callset_id, call_id, attempt_id, extra_env,
                         extra_meta, data_byte_range, use_cached_runtime,
                         host_job_meta, job_max_runtime,
                         overwrite_invoke_args = None):

        # Pick a runtime url if we have shards.
        # If not the handler will construct it
        # TODO: we should always set the url, so our code here is S3-independent
        runtime_url = ""
        if ('urls' in self.runtime_meta_info and
                isinstance(self.runtime_meta_info['urls'], list) and
                    len(self.runtime_meta_info['urls']) > 1):
            num_shards = len(self.runtime_meta_info['urls'])
            logger.debug("Runtime is sharded, choosing from {} copies.".format(num_shards))
            random.seed()
            runtime_url = random.choice(self.runtime_meta_info['urls'])

        arg_dict = {'storage_info' : self.storage.get_storage_info(),
                    'func_key' : func_key,
                    'data_key' : data_key,
                    'output_key' : output_key,
                    'status_key' : status_key,
                    'callset_id': callset_id,
                    'job_max_runtime' : job_max_runtime,
                    'data_byte_range' : data_byte_range,
                    'call_id' : call_id,
                    'attempt_id': attempt_id,
                    'use_cached_runtime' : use_cached_runtime,
                    'runtime' : self.config['runtime'],
                    'pywren_version' : version.__version__,
                    'runtime_url' : runtime_url }

        if extra_env is not None:
            logger.debug("Extra environment vars {}".format(extra_env))
            arg_dict['extra_env'] = extra_env

        if extra_meta is not None:
            # sanity
            for k, v in extra_meta.iteritems():
                if k in arg_dict:
                    raise ValueError("Key {} already in dict".format(k))
                arg_dict[k] = v

        host_submit_time = time.time()
        arg_dict['host_submit_time'] = host_submit_time

        logger.info("call_async {} {} attempt {} lambda invoke"
                    .format(callset_id, call_id, attempt_id))
        lambda_invoke_time_start = time.time()

        # overwrite explicit args, mostly used for testing via injection
        if overwrite_invoke_args is not None:
            arg_dict.update(overwrite_invoke_args)

        # do the invocation
        self.invoker.invoke(arg_dict)

        logger.info("call_async {} {} attempt {} lambda invoke complete"
                    .format(callset_id, call_id, attempt_id))

        if attempt_id == 0: # return future if this is the first attempt
            host_job_meta['lambda_invoke_timestamp'] = lambda_invoke_time_start
            host_job_meta['lambda_invoke_time'] = time.time() - lambda_invoke_time_start
            host_job_meta.update(self.invoker.config())
            host_job_meta.update(arg_dict)

            future.host_job_meta = host_job_meta
            future._set_state(JobState.invoked)
            future.attempts_made = 1

            return future

    def agg_data(self, data_strs):
        ranges = []
        pos = 0
        for datum in data_strs:
            l = len(datum)
            ranges.append((pos, pos + l -1))
            pos += l
        return b"".join(data_strs), ranges

    def prepare(self, callset_id, func, iterdata, data_all_as_one=True):
        """
        # FIXME work with an actual iterable instead of just a list

        data_all_as_one : upload the data as a single object; fewer
        tcp transactions (good) but potentially higher latency for workers (bad)

        use_cached_runtime : if runtime has been cached, use that. When set
        to False, redownloads runtime.
        """

        host_job_meta = {}

        data = list(iterdata)

        ### pickle func and all data (to capture module dependencies
        func_and_data_ser, mod_paths = self.serializer([func] + data)

        func_str = func_and_data_ser[0]
        data_strs = func_and_data_ser[1:]
        data_size_bytes = sum(len(x) for x in data_strs)
        agg_data_key = None
        host_job_meta['agg_data'] = False
        host_job_meta['data_size_bytes'] =  data_size_bytes

        if data_size_bytes < wrenconfig.MAX_AGG_DATA_SIZE and data_all_as_one:
            agg_data_key = self.storage.create_agg_data_key(callset_id)
            agg_data_bytes, agg_data_ranges = self.agg_data(data_strs)
            agg_upload_time = time.time()
            self.storage.put_object(agg_data_key, agg_data_bytes)
            host_job_meta['agg_data'] = True
            host_job_meta['data_upload_time'] = time.time() - agg_upload_time
            host_job_meta['data_upload_timestamp'] = time.time()
        else:
            # FIXME add warning that you wanted data all as one but
            # it exceeded max data size
            pass


        module_data = create_mod_data(mod_paths)
        func_str_encoded = wrenutil.bytes_to_b64str(func_str)
        #debug_foo = {'func' : func_str_encoded,
        #             'module_data' : module_data}

        #pickle.dump(debug_foo, open("/tmp/py35.debug.pickle", 'wb'))
        ### Create func and upload
        func_module_str = json.dumps({'func' : func_str_encoded,
                                      'module_data' : module_data})
        host_job_meta['func_module_str_len'] = len(func_module_str)

        func_upload_time = time.time()
        func_key = self.storage.create_func_key(callset_id)
        self.storage.put_object(func_key, func_module_str)
        host_job_meta['func_upload_time'] = time.time() - func_upload_time
        host_job_meta['func_upload_timestamp'] = time.time()

        return callset_id, agg_data_key, func_key, data_strs, host_job_meta, agg_data_ranges


    def invoke_calls(self, job, callset_id, call_indices, agg_data_key, func_key, data_strs,
                        host_job_meta, agg_data_ranges, extra_env = None, extra_meta = None,
                        overwrite_invoke_args = None):

        def invoke(future, data_str, callset_id, call_id, attempt_id, func_key,
                   host_job_meta,
                   agg_data_key = None, data_byte_range=None ):
            data_key, output_key, status_key \
                = self.storage.create_keys(callset_id, call_id)

            host_job_meta['job_invoke_timestamp'] = time.time()

            if agg_data_key is None:
                data_upload_time = time.time()
                self.put_data(data_key, data_str,
                              callset_id, call_id)
                data_upload_time = time.time() - data_upload_time
                host_job_meta['data_upload_time'] = data_upload_time
                host_job_meta['data_upload_timestamp'] = time.time()

                data_key = data_key
            else:
                data_key = agg_data_key

            return self.invoke_with_keys(future, func_key, data_key,
                                         output_key,
                                         status_key,
                                         callset_id, call_id, attempt_id, extra_env,
                                         extra_meta, data_byte_range,
                                         self.use_cached_runtime, host_job_meta.copy(),
                                         self.job_max_runtime,
                                         overwrite_invoke_args = overwrite_invoke_args)

        poolsize = min(self.invoke_pool_threads, len(call_indices))
        pool = ThreadPool(poolsize)

        call_result_objs = []
        for (i, attempt_id) in call_indices:
            call_id = "{:05d}".format(i)
            future = job.futures[i]

            data_byte_range = None
            if agg_data_key is not None:
                data_byte_range = agg_data_ranges[i]

            cb = pool.apply_async(invoke, (future, data_strs[i], callset_id,
                                           call_id, attempt_id, func_key,
                                           host_job_meta.copy(),
                                           agg_data_key,
                                           data_byte_range))

            logger.info("map {} {} apply async".format(callset_id, call_id))

            call_result_objs.append(cb)

        res =  [c.get() for c in call_result_objs]
        pool.close()
        pool.join()
        logger.info("map invoked {} {} pool join".format(callset_id, call_id))

        # FIXME take advantage of the callset to return a lot of these

        # note these are just the invocation futures

        return res

    def run_job(self, job):
        job_desp = job.job_desp
        callset_id = job.callset_id
        assert job_desp.rate > 0

        calls_queue = [(id, 0, None) for id in range(len(list(job_desp.iterdata)))]
        num_available_workers = job_desp.rate
        fs_running = []
        res = []


        job_desp = job.job_desp
        callset_id, s3_agg_data_key, s3_func_key, data_strs, host_job_meta, agg_data_ranges \
            = self.prepare(callset_id, job_desp.func, job_desp.iterdata, job_desp.data_all_as_one)

        ever_failed = 0
        ever_suc = 0

        while len(calls_queue) > 0 or len(fs_running) > 0:
            print("queue length: " + str(len(calls_queue)) + " running: " + str(len(fs_running)))
            print("ever failed: " + str(ever_failed) + " ever suc: " + str(ever_suc))
            # invoking more calls
            if num_available_workers > 0 and len(calls_queue) > 0:
                num_calls_to_invoke = min(num_available_workers, len(calls_queue))
                # invoke according to the order
                custom_ids = [(c[0], c[1]) for c in calls_queue[:num_calls_to_invoke]]

                invoked = self.invoke_calls(job, callset_id, custom_ids, s3_agg_data_key,
                                               s3_func_key, data_strs, host_job_meta, agg_data_ranges,
                                               job_desp.extra_env, job_desp.extra_meta,
                                                job_desp.overwrite_invoke_args)
                new_fs = [fs for fs in invoked if fs]
                old_fs = [c[2] for c in calls_queue[:num_calls_to_invoke] if c[2]]
                for fs in old_fs:
                    fs.attempts_made += 1
                res += new_fs
                fs_running += new_fs + old_fs
                calls_queue = calls_queue[num_calls_to_invoke:]
                num_available_workers -= num_calls_to_invoke
            # wait for available slots
            else:
                fs_success, fs_running, fs_failed = wait(fs_running,
                                                            return_when=ANY_COMPLETED)
                print "after wait len of suc {} r {} f {} ".format(len(fs_success), len(fs_running), len(fs_failed))
                ever_failed += len(fs_failed)
                if ever_failed > 10000:
                    exit()
                ever_suc += len(fs_success)
                calls_queue += [(int(f.call_id), f.attempts_made, f) for f in fs_failed]
                num_available_workers += len(fs_success + fs_failed)

        # finally wait until all work finish
        wait(fs_running, return_when=ALL_COMPLETED)
        return res

