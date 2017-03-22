from __future__ import absolute_import

import boto3
import json
try:
    from six.moves import cPickle as pickle
except:
    import pickle
from multiprocessing.pool import ThreadPool
import time
import random
import logging
import botocore
import glob2
import os

import pywren.s3util as s3util
import pywren.version as version
import pywren.wrenconfig as wrenconfig
import pywren.wrenutil as wrenutil
import pywren.runtime as runtime
from pywren.serialize import cloudpickle, serialize
from pywren.serialize import create_mod_data
from pywren.future import ResponseFuture, JobState
from pywren.wait import *

logger = logging.getLogger(__name__)


class Executor(object):
    """
    Theoretically will allow for cross-AZ invocations
    """

    def __init__(self, aws_region, s3_bucket, s3_prefix,
                 invoker, runtime_s3_bucket, runtime_s3_key, job_max_runtime):
        self.aws_region = aws_region
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

        self.session = botocore.session.get_session()
        self.invoker = invoker
        self.s3client = self.session.create_client('s3', region_name = aws_region)
        self.job_max_runtime = job_max_runtime

        self.runtime_bucket = runtime_s3_bucket
        self.runtime_key = runtime_s3_key
        self.runtime_meta_info = runtime.get_runtime_info(runtime_s3_bucket, runtime_s3_key)
        if not runtime.runtime_key_valid(self.runtime_meta_info):
            raise Exception("The indicated runtime: s3://{}/{} is not approprite for this python version".format(runtime_s3_bucket, runtime_s3_key))

        if 'preinstalls' in self.runtime_meta_info:
            logger.info("using serializer with meta-supplied preinstalls")
            self.serializer = serialize.SerializeIndependent(self.runtime_meta_info['preinstalls'])
        else:
            self.serializer =  serialize.SerializeIndependent()

    def put_data(self, s3_data_key, data_str,
                 callset_id, call_id):

        # put on s3 -- FIXME right now this takes 2x as long

        self.s3client.put_object(Bucket = s3_data_key[0],
                                 Key = s3_data_key[1],
                                 Body = data_str)

        logger.info("call_async {} {} s3 upload complete {}".format(callset_id, call_id, s3_data_key))


    def invoke_with_keys(self, s3_func_key, s3_data_key, s3_output_key,
                         s3_status_key,
                         callset_id, call_id, extra_env,
                         extra_meta, data_byte_range, use_cached_runtime,
                         host_job_meta, job_max_runtime,
                         overwrite_invoke_args = None):

        # Pick a runtime url if we have shards. If not the handler will construct it
        # using s3_bucket and s3_key
        runtime_url = ""
        if ('urls' in self.runtime_meta_info and
                isinstance(self.runtime_meta_info['urls'], list) and
                    len(self.runtime_meta_info['urls']) > 1):
            num_shards = len(self.runtime_meta_info['urls'])
            logger.debug("Runtime is sharded, choosing from {} copies.".format(num_shards))
            random.seed()
            runtime_url = random.choice(self.runtime_meta_info['urls'])


        arg_dict = {'func_key' : s3_func_key,
                    'data_key' : s3_data_key,
                    'output_key' : s3_output_key,
                    'status_key' : s3_status_key,
                    'callset_id': callset_id,
                    'job_max_runtime' : job_max_runtime,
                    'data_byte_range' : data_byte_range,
                    'call_id' : call_id,
                    'use_cached_runtime' : use_cached_runtime,
                    'runtime_s3_bucket' : self.runtime_bucket,
                    'runtime_s3_key' : self.runtime_key,
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

        logger.info("call_async {} {} lambda invoke ".format(callset_id, call_id))
        lambda_invoke_time_start = time.time()

        # overwrite explicit args, mostly used for testing via injection
        if overwrite_invoke_args is not None:
            arg_dict.update(overwrite_invoke_args)

        # do the invocation
        self.invoker.invoke(arg_dict)

        host_job_meta['lambda_invoke_timestamp'] = lambda_invoke_time_start
        host_job_meta['lambda_invoke_time'] = time.time() - lambda_invoke_time_start


        host_job_meta.update(self.invoker.config())

        logger.info("call_async {} {} lambda invoke complete".format(callset_id, call_id))


        host_job_meta.update(arg_dict)

        fut = ResponseFuture(call_id, callset_id, host_job_meta,
                             self.s3_bucket, self.s3_prefix,
                             self.aws_region)

        fut._set_state(JobState.invoked)

        return fut

    def call_async(self, func, data, extra_env = None,
                   extra_meta=None):
        return self.map(func, [data],  extra_env, extra_meta)[0]

    def agg_data(self, data_strs):
        ranges = []
        pos = 0
        for datum in data_strs:
            l = len(datum)
            ranges.append((pos, pos + l -1))
            pos += l
        return b"".join(data_strs), ranges

    def map(self, func, iterdata, extra_env = None, extra_meta = None,
            invoke_pool_threads=64, data_all_as_one=True,
            use_cached_runtime=True, overwrite_invoke_args = None):
        """
        # FIXME work with an actual iterable instead of just a list

        data_all_as_one : upload the data as a single s3 object; fewer
        tcp transactions (good) but potentially higher latency for workers (bad)

        use_cached_runtime : if runtime has been cached, use that. When set
        to False, redownloads runtime.
        """

        host_job_meta = {}

        pool = ThreadPool(invoke_pool_threads)
        callset_id = s3util.create_callset_id()
        data = list(iterdata)

        ### pickle func and all data (to capture module dependencies
        func_and_data_ser, mod_paths = self.serializer([func] + data)

        func_str = func_and_data_ser[0]
        data_strs = func_and_data_ser[1:]
        data_size_bytes = sum(len(x) for x in data_strs)
        s3_agg_data_key = None
        host_job_meta['aggregated_data_in_s3'] = False
        host_job_meta['data_size_bytes'] =  data_size_bytes

        if data_size_bytes < wrenconfig.MAX_AGG_DATA_SIZE and data_all_as_one:
            s3_agg_data_key = s3util.create_agg_data_key(self.s3_bucket,
                                                         self.s3_prefix, callset_id)
            agg_data_bytes, agg_data_ranges = self.agg_data(data_strs)
            agg_upload_time = time.time()
            self.s3client.put_object(Bucket = s3_agg_data_key[0],
                                     Key = s3_agg_data_key[1],
                                     Body = agg_data_bytes)
            host_job_meta['agg_data_in_s3'] = True
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
        s3_func_key = s3util.create_func_key(self.s3_bucket, self.s3_prefix,
                                             callset_id)
        self.s3client.put_object(Bucket = s3_func_key[0],
                                 Key = s3_func_key[1],
                                 Body = func_module_str)
        host_job_meta['func_upload_time'] = time.time() - func_upload_time
        host_job_meta['func_upload_timestamp'] = time.time()
        def invoke(data_str, callset_id, call_id, s3_func_key,
                   host_job_meta,
                   s3_agg_data_key = None, data_byte_range=None ):
            s3_data_key, s3_output_key, s3_status_key \
                = s3util.create_keys(self.s3_bucket,
                                     self.s3_prefix,
                                     callset_id, call_id)

            host_job_meta['job_invoke_timestamp'] = time.time()

            if s3_agg_data_key is None:
                data_upload_time = time.time()
                self.put_data(s3_data_key, data_str,
                              callset_id, call_id)
                data_upload_time = time.time() - data_upload_time
                host_job_meta['data_upload_time'] = data_upload_time
                host_job_meta['data_upload_timestamp'] = time.time()

                data_key = s3_data_key
            else:
                data_key = s3_agg_data_key

            return self.invoke_with_keys(s3_func_key, data_key,
                                         s3_output_key,
                                         s3_status_key,
                                         callset_id, call_id, extra_env,
                                         extra_meta, data_byte_range,
                                         use_cached_runtime, host_job_meta.copy(),
                                         self.job_max_runtime,
                                         overwrite_invoke_args = overwrite_invoke_args)

        N = len(data)
        call_result_objs = []
        for i in range(N):
            call_id = "{:05d}".format(i)

            data_byte_range = None
            if s3_agg_data_key is not None:
                data_byte_range = agg_data_ranges[i]

            cb = pool.apply_async(invoke, (data_strs[i], callset_id,
                                           call_id, s3_func_key,
                                           host_job_meta.copy(),
                                           s3_agg_data_key,
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


        logclient = boto3.client('logs', region_name=self.aws_region)


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
