from __future__ import absolute_import
import boto3
import botocore
from six import reraise
import json
import base64
from threading import Thread
try:
    from six.moves import cPickle as pickle
except:
    import pickle
from pywren.wrenconfig import *
from pywren import wrenconfig, wrenutil, runtime
import enum
from multiprocessing.pool import ThreadPool
import time
from pywren import s3util, version
import logging
import botocore
import glob2
import os
from pywren.cloudpickle import serialize
from pywren import invokers
from tblib import pickling_support
pickling_support.install()

logger = logging.getLogger(__name__)

class JobState(enum.Enum):
    new = 1
    invoked = 2
    running = 3
    success = 4
    error = 5

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

standalone_executor = remote_executor

class Executor(object):
    """
    Theoretically will allow for cross-AZ invocations
    """

    def __init__(self, aws_region, s3_bucket, s3_prefix, 
                 invoker, config, job_max_runtime, shard_runtime=False):
        self.aws_region = aws_region
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.config = config

        self.session = botocore.session.get_session()
        self.invoker = invoker
        self.s3client = self.session.create_client('s3', region_name = aws_region)
        self.job_max_runtime = job_max_runtime
        self.shard_runtime = shard_runtime

        runtime_bucket = config['runtime']['s3_bucket']
        runtime_key =  config['runtime']['s3_key']
        if not runtime.runtime_key_valid(runtime_bucket, runtime_key):
            raise Exception("The indicated runtime: s3://{}/{} is not approprite for this python version".format(runtime_bucket, runtime_key))

    def create_mod_data(self, mod_paths):

        module_data = {}
        # load mod paths
        for m in mod_paths:
            if os.path.isdir(m):
                files = glob2.glob(os.path.join(m, "**/*.py"))
                pkg_root = os.path.dirname(m)
            else:
                pkg_root = os.path.dirname(m)
                files = [m]
            for f in files:
                dest_filename = f[len(pkg_root)+1:]
                mod_str = open(f, 'rb').read()
                module_data[f[len(pkg_root)+1:]] = mod_str.decode('utf-8')

        return module_data

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
    
        arg_dict = {'func_key' : s3_func_key, 
                    'data_key' : s3_data_key, 
                    'output_key' : s3_output_key, 
                    'status_key' : s3_status_key, 
                    'callset_id': callset_id, 
                    'job_max_runtime' : job_max_runtime, 
                    'data_byte_range' : data_byte_range, 
                    'call_id' : call_id, 
                    'use_cached_runtime' : use_cached_runtime, 
                    'runtime_s3_bucket' : self.config['runtime']['s3_bucket'], 
                    'runtime_s3_key' : self.config['runtime']['s3_key'], 
                    'pywren_version' : version.__version__, 
                    'shard_runtime_key' : self.shard_runtime}    
        
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
        serializer = serialize.SerializeIndependent()
        func_and_data_ser, mod_paths = serializer([func] + data)
        
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
            

        module_data = self.create_mod_data(mod_paths)
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

# this really should not be a global singleton FIXME
global_s3_client = boto3.client('s3') # , region_name = AWS_REGION)
    
def get_call_status(callset_id, call_id, 
                    AWS_S3_BUCKET = wrenconfig.AWS_S3_BUCKET, 
                    AWS_S3_PREFIX = wrenconfig.AWS_S3_PREFIX, 
                    AWS_REGION = wrenconfig.AWS_REGION, s3=None):
    s3_data_key, s3_output_key, s3_status_key = s3util.create_keys(AWS_S3_BUCKET, 
                                                                    AWS_S3_PREFIX, 
                                                                    callset_id, call_id)
    if s3 is None:
        s3 = global_s3_client
    
    try:
        r = s3.get_object(Bucket = s3_status_key[0], Key = s3_status_key[1])
        result_json = r['Body'].read()
        return json.loads(result_json.decode('ascii'))
    
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            return None
        else:
            raise e


def get_call_output(callset_id, call_id,
                    AWS_S3_BUCKET = wrenconfig.AWS_S3_BUCKET, 
                    AWS_S3_PREFIX = wrenconfig.AWS_S3_PREFIX, 
                    AWS_REGION = wrenconfig.AWS_REGION, s3=None):
    s3_data_key, s3_output_key, s3_status_key = s3util.create_keys(AWS_S3_BUCKET, 
                                                                    AWS_S3_PREFIX, 
                                                                    callset_id, call_id)
    
    if s3 is None:
        s3 = global_s3_client # boto3.client('s3', region_name = AWS_REGION)

    r = s3.get_object(Bucket = s3_output_key[0], Key = s3_output_key[1])
    return pickle.loads(r['Body'].read())
    

class ResponseFuture(object):

    """
    """
    GET_RESULT_SLEEP_SECS = 4
    def __init__(self, call_id, callset_id, invoke_metadata, 
                 s3_bucket, s3_prefix, aws_region):

        self.call_id = call_id
        self.callset_id = callset_id 
        self._state = JobState.new
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_region = aws_region

        self._invoke_metadata = invoke_metadata.copy()
        
        self.status_query_count = 0
        
    def _set_state(self, new_state):
        ## FIXME add state machine
        self._state = new_state

    def cancel(self):
        raise NotImplementedError("Cannot cancel dispatched jobs")

    def cancelled(self):
        raise NotImplementedError("Cannot cancel dispatched jobs")

    def running(self):
        raise NotImplementedError()
        
    def done(self):
        if self._state in [JobState.success, JobState.error]:
            return True
        if self.result(check_only = True) is None:
            return False
        return True


    def result(self, timeout=None, check_only=False, throw_except=True):
        """


        From the python docs:

        Return the value returned by the call. If the call hasn't yet
        completed then this method will wait up to timeout seconds. If
        the call hasn't completed in timeout seconds then a
        TimeoutError will be raised. timeout can be an int or float.If
        timeout is not specified or None then there is no limit to the
        wait time.
        
        If the future is cancelled before completing then CancelledError will be raised.
        
        If the call raised then this method will raise the same exception.

        """
        if self._state == JobState.new:
            raise ValueError("job not yet invoked")
        
        if self._state == JobState.success:
            return self._return_val
            
        if self._state == JobState.error:
            if throw_except:
                raise self._exception
            else:
                return None

        
        call_status = get_call_status(self.callset_id, self.call_id, 
                                      AWS_S3_BUCKET = self.s3_bucket, 
                                      AWS_S3_PREFIX = self.s3_prefix, 
                                      AWS_REGION = self.aws_region)

        self.status_query_count += 1

        ## FIXME implement timeout
        if timeout is not None : raise NotImplementedError()

        if check_only is True:
            if call_status is None:
                return None

        while call_status is None:
            time.sleep(self.GET_RESULT_SLEEP_SECS)
            call_status = get_call_status(self.callset_id, self.call_id, 
                                          AWS_S3_BUCKET = self.s3_bucket, 
                                          AWS_S3_PREFIX = self.s3_prefix, 
                                          AWS_REGION = self.aws_region)

            self.status_query_count += 1
        self._invoke_metadata['status_done_timestamp'] = time.time()
        self._invoke_metadata['status_query_count'] = self.status_query_count

        self.run_status = call_status # this is the remote status information
        self.invoke_status = self._invoke_metadata # local status information
            
        if call_status['exception'] is not None:
            # the wrenhandler had an exception
            exception_str = call_status['exception']
            print(call_status.keys())
            exception_args = call_status['exception_args']
            if exception_args[0] == "WRONGVERSION":
                if throw_except:
                    raise Exception("Pywren version mismatch: remove expected version {}, local library is version {}".format(exception_args[2], exception_args[3]))
                return None
            elif exception_args[0] == "OUTATIME":
                if throw_except:
                    raise Exception("process ran out of time")
                return None
            else:
                if throw_except:
                    raise Exception(exception_str, *exception_args)
                return None
        
        call_output_time = time.time()
        call_invoker_result = get_call_output(self.callset_id, self.call_id, 
                                              AWS_S3_BUCKET = self.s3_bucket, 
                                              AWS_S3_PREFIX = self.s3_prefix,
                                              AWS_REGION = self.aws_region)
        call_output_time_done = time.time()
        self._invoke_metadata['download_output_time'] = call_output_time_done - call_output_time_done
        
        self._invoke_metadata['download_output_timestamp'] = call_output_time_done
        call_success = call_invoker_result['success']
        logger.info("ResponseFuture.result() {} {} call_success {}".format(self.callset_id, 
                                                                           self.call_id, 
                                                                           call_success))
        


        self._call_invoker_result = call_invoker_result



        if call_success:

            self._return_val = call_invoker_result['result']
            self._state = JobState.success
            return self._return_val
        
        elif throw_except:
            
            self._exception = call_invoker_result['result']
            self._traceback = (call_invoker_result['exc_type'], 
                               call_invoker_result['exc_value'], 
                               call_invoker_result['exc_traceback'])

            self._state = JobState.error
            if call_invoker_result.get('pickle_fail', False):
                logging.warning("there was an error pickling. The original exception: {}\n The pickling exception: {}".format(call_invoker_result['exc_value'], str(call_invoker_result['pickle_exception'])))

                reraise(Exception, call_invoker_result['exc_value'], 
                        call_invoker_result['exc_traceback'])
            else:
                # reraise the exception
                reraise(*self._traceback)
        else:
            return None  # nothing, don't raise, no value
            
    def exception(self, timeout = None):
        raise NotImplementedError()

    def add_done_callback(self, fn):
        raise NotImplementedError()

    



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
