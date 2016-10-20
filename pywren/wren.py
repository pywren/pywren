import boto3
import botocore
import cloudpickle
import json
import base64
from six.moves import cPickle as pickle
import wrenconfig
import wrenutil
import enum
from multiprocessing.pool import ThreadPool
import time
import s3util
import logging
import botocore

logger = logging.getLogger('pywren')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.WARN)

# create formatter
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s', 
                              "%Y-%m-%d %H:%M:%S")

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

class JobState(enum.Enum):
    new = 1
    invoked = 2
    running = 3
    success = 4
    error = 5

def default_executor():
    config = wrenconfig.default()
    AWS_REGION = config['account']['aws_region']
    FUNCTION_NAME = config['lambda']['function_name']
    S3_BUCKET = config['s3']['bucket']
    S3_PREFIX = config['s3']['pywren_prefix']
    return Executor(AWS_REGION, S3_BUCKET, S3_PREFIX, FUNCTION_NAME, config)

class Executor(object):
    """
    Theoretically will allow for cross-AZ invocations
    """

    def __init__(self, aws_region, s3_bucket, s3_prefix, function_name, 
                 config):
        self.aws_region = aws_region
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.config = config
        self.lambda_function_name = function_name

        self.session = botocore.session.get_session()
        self.lambclient = self.session.create_client('lambda', 
                                                     region_name = aws_region)
        self.s3client = self.session.create_client('s3', region_name = aws_region)

    
    def call_async(self, func, data, callset_id=None, extra_env = None, 
                   extra_meta=None, call_id = None):
        """
        Returns a future

        callset is just a handle to refer to a bunch of calls simultaneously
        """

        if callset_id is None:
            callset_id = s3util.create_callset_id()
        if call_id is None:
            call_id = s3util.create_call_id()

        logger.info("call_async {} {} ".format(callset_id, call_id))

        # FIXME someday we can optimize this


        func_str = cloudpickle.dumps({'func' : func, 
                                      'data' : data})
        logger.info("call_async {} {} dumps complete size={} ".format(callset_id, call_id, len(func_str)))

        s3_input_key, s3_output_key, s3_status_key = s3util.create_keys(self.s3_bucket,
                                                                        self.s3_prefix, 
                                                                        callset_id, call_id)

        arg_dict = {'input_key' : s3_input_key, 
                    'output_key' : s3_output_key, 
                    'status_key' : s3_status_key, 
                    'callset_id': callset_id, 
                    'call_id' : call_id, 
                    'runtime_s3_bucket' : self.config['runtime']['s3_bucket'], 
                    'runtime_s3_key' : self.config['runtime']['s3_key']}    



        if extra_env is not None:
            arg_dict['extra_env'] = extra_env
        if extra_meta is not None:
            # sanity 
            for k, v in extra_meta.iteritems():
                if k in arg_dict:
                    raise ValueError("Key {} already in dict".format(k))
                arg_dict[k] = v

        # put on s3 
        logger.info("call_async {} {} s3 upload".format(callset_id, call_id))
        self.s3client.put_object(Bucket = s3_input_key[0], 
                            Key = s3_input_key[1], 
                            Body = func_str)
        logger.info("call_async {} {} s3 upload complete {}".format(callset_id, call_id, s3_input_key))

        arg_dict['host_submit_time'] =  time.time()
        
        json_arg = json.dumps(arg_dict)

        logger.info("call_async {} {} lambda invoke ".format(callset_id, call_id))
        res = self.lambclient.invoke(FunctionName=self.lambda_function_name, 
                                     Payload = json.dumps(arg_dict), 
                                     InvocationType='Event')
        logger.info("call_async {} {} lambda invoke complete".format(callset_id, call_id))

        fut = ResponseFuture(call_id, callset_id, self)

        fut._set_state(JobState.invoked)

        return fut



    def map(self, func, iterdata, extra_meta = None, extra_env = None, 
            invoke_pool_threads=64):
        """
        Optionally use threadpool for faster invocation

        # FIXME work with an actual iterable instead of just a list

        """

        pool = ThreadPool(invoke_pool_threads)
        callset_id = s3util.create_callset_id()

        N = len(iterdata)
        call_result_objs = []
        for i in range(N):
            call_id = "{:05d}".format(i)

            cb = pool.apply_async(self.call_async, (func, iterdata[i]), 
                                  dict(callset_id = callset_id,
                                       call_id = call_id, 
                                       extra_env=extra_env))

            logger.info("map {} {} apply async".format(callset_id, call_id))

            call_result_objs.append(cb)

        res =  [c.get() for c in call_result_objs]
        pool.close()
        pool.join()
        logger.info("map invoked {} {} pool join".format(callset_id, call_id))

        # FIXME take advantage of the callset to return a lot of these 

        # note these are just the invocation futures

        return res
    
    
def get_call_status(callset_id, call_id, 
                    AWS_S3_BUCKET = wrenconfig.AWS_S3_BUCKET, 
                    AWS_S3_PREFIX = wrenconfig.AWS_S3_PREFIX, 
                    AWS_REGION = wrenconfig.AWS_REGION, s3=None):
    s3_input_key, s3_output_key, s3_status_key = s3util.create_keys(AWS_S3_BUCKET, 
                                                                    AWS_S3_PREFIX, 
                                                                    callset_id, call_id)
    if s3 is None:
        s3 = global_s3_client
    
    try:
        r = s3.get_object(Bucket = s3_status_key[0], Key = s3_status_key[1])
        result_json = r['Body'].read()
        return json.loads(result_json)
    
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            return None
        else:
            raise e


def get_call_output(callset_id, call_id,
                    AWS_S3_BUCKET = wrenconfig.AWS_S3_BUCKET, 
                    AWS_S3_PREFIX = wrenconfig.AWS_S3_PREFIX, 
                    AWS_REGION = wrenconfig.AWS_REGION, s3=None):
    s3_input_key, s3_output_key, s3_status_key = s3util.create_keys(AWS_S3_BUCKET, 
                                                                    AWS_S3_PREFIX, 
                                                                    callset_id, call_id)
    
    if s3 is None:
        s3 = global_s3_client # boto3.client('s3', region_name = AWS_REGION)

    r = s3.get_object(Bucket = s3_output_key[0], Key = s3_output_key[1])
    return pickle.loads(r['Body'].read())
    

class ResponseFuture(object):

    """
    """
    def __init__(self, call_id, callset_id, executor):

        self.call_id = call_id
        self.callset_id = callset_id 
        self._state = JobState.new
        self.executor = executor
        
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
            raise self._exception

        
        status = get_call_status(self.callset_id, self.call_id, 
                                 AWS_S3_BUCKET = self.executor.s3_bucket, 
                                 AWS_S3_PREFIX = self.executor.s3_prefix, 
                                 AWS_REGION = self.executor.aws_region, 
                                 s3 = self.executor.s3client)


        ## FIXME implement timeout
        if timeout is not None : raise NotImplementedError()

        if check_only is True:
            if status is None:
                return None

        while status is None:
            time.sleep(4)
            status = get_call_status(self.callset_id, self.call_id, 
                                     AWS_S3_BUCKET = self.executor.s3_bucket, 
                                     AWS_S3_PREFIX = self.executor.s3_prefix, 
                                     AWS_REGION = self.executor.aws_region, 
                                     s3 = self.executor.s3client)
            
        # FIXME check if it actually worked all the way through 

        call_invoker_result = get_call_output(self.callset_id, self.call_id, 
                                              AWS_S3_BUCKET = self.executor.s3_bucket, 
                                              AWS_S3_PREFIX = self.executor.s3_prefix,
                                              AWS_REGION = self.executor.aws_region, 
                                              s3 = self.executor.s3client)
        call_success = call_invoker_result['success']
        logger.info("ResponseFuture.result() {} {} call_success {}".format(self.callset_id, 
                                                                           self.call_id, 
                                                                           call_success))

        self._run_status = status
        
        if call_success:
            
            self._return_val = call_invoker_result['result']
            self._state = JobState.success
        else:
            self._exception = call_invoker_result['result']
            self._state = JobState.error

        if call_success:
            return self._return_val
        elif call_success == False and throw_except:
            raise self._exception
        return None
            
    def exception(self, timeout = None):
        raise NotImplementedError()

    def add_done_callback(self, fn):
        raise NotImplementedError()

    



ALL_COMPLETED = 1
ANY_COMPLETED = 2
ALWAYS = 3

def wait(fs, return_when=ALWAYS):
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


    # get all the futures that are not yet done
    not_done_futures =  [f for f in fs if f._state not in [JobState.success, 
                                                       JobState.error]]

    # check if the not-done ones have the same callset_id
    present_callsets = set([f.callset_id for f in not_done_futures])
    if len(present_callsets) > 1:
        raise NotImplementedError()

    # get the list of all objects in this callset
    callset_id = present_callsets.pop() # FIXME assume only one
    f0 = not_done_futures[0] # This is a hack too 

    callids_done = s3util.get_callset_done(f0.executor.s3_bucket, 
                                           f0.executor.s3_prefix,
                                           callset_id)
    callids_done = set(callids_done)

    fs_dones = []
    fs_notdones = []

    pool = ThreadPool(64)

    for f in fs:
        if f._state in [JobState.success, JobState.error]:
            # done, don't need to do anything
            fs_dones.append(f)
        else:
            if f.call_id in callids_done:
                pool.apply_async(f.result, None, dict(throw_except=False))

                fs_dones.append(f)
            else:
                fs_notdones.append(f)
    pool.close()
    pool.join()
    return fs_dones, fs_notdones

    
