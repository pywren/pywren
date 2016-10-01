import boto3
import botocore
import cloudpickle
import json
import base64
import cPickle as pickle
import wrenconfig
import wrenutil
import enum
from multiprocessing.pool import ThreadPool
import time
import s3util

class JobState(enum.Enum):
    new = 1
    invoked = 2
    running = 3
    success = 4
    error = 5

def get_call_status(callset_id, call_id):
    s3_input_key, s3_output_key, s3_status_key = s3util.create_keys(wrenconfig.AWS_S3_BUCKET, 
                                                                    wrenconfig.AWS_S3_PREFIX, 
                                                                    callset_id, call_id)
    

    s3 = boto3.client('s3', region_name=wrenconfig.AWS_REGION)

    try:
        r = s3.get_object(Bucket = s3_status_key[0], Key = s3_status_key[1])
        result_json = r['Body'].read()
        return json.loads(result_json)
    
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            return None
        else:
            raise e




def get_call_output(callset_id, call_id):
    s3_input_key, s3_output_key, s3_status_key = s3util.create_keys(wrenconfig.AWS_S3_BUCKET, 
                                                                    wrenconfig.AWS_S3_PREFIX, 
                                                                    callset_id, call_id)
    s3 = boto3.client('s3', region_name=wrenconfig.AWS_REGION)
    r = s3.get_object(Bucket = s3_output_key[0], Key = s3_output_key[1])
    return pickle.loads(r['Body'].read())
    
class ResponseFuture(object):

    """
    """
    def __init__(self, call_id, callset_id):
        self.call_id = call_id
        self.callset_id = callset_id 
        self._state = JobState.new

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
        sdbclient = boto3.client('sdb', region_name=wrenconfig.AWS_REGION)
        

        raise NotImplementedError()

    def result(self, timeout=None):
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

        ## FIXME implement timeout
        if timeout is not None : raise NotImplementedError()
        
        status = get_call_status(self.callset_id, self.call_id) 
        while status is None:
            time.sleep(4)
            status = get_call_status(self.callset_id, self.call_id) 

        # FIXME check if it actually worked all the way through 

        call_invoker_result = get_call_output(self.callset_id, self.call_id)
        call_success = call_invoker_result['success']
        
        if call_success:
            
            self._return_val = call_invoker_result['result']
            self._state = JobState.success
        else:
            self._exception = call_invoker_result['result']
            self._state = JobState.error

        self._run_status = status
        return self._return_val
            
    def exception(self, timeout = None):
        raise NotImplementedError()

    def add_done_callback(self, fn):
        raise NotImplementedError()

    
def call_async(func, data, callset_id=None, extra_env = None, extra_meta=None):
    """
    Returns a future

    callset is just a handle to refer to a bunch of calls simultaneously
    """

    if callset_id is None:
        callset_id = s3util.create_callset_id()
    call_id = s3util.create_call_id()




    session = boto3.session.Session()
    lambclient = session.client('lambda', region_name=wrenconfig.AWS_REGION)
    s3client = session.client('s3', region_name=wrenconfig.AWS_REGION)

    # FIXME someday we can optimize this
    
    
    func_str = cloudpickle.dumps({'func' : func, 
                                  'data' : data})

    s3_input_key, s3_output_key, s3_status_key = s3util.create_keys(wrenconfig.AWS_S3_BUCKET, 
                                                     wrenconfig.AWS_S3_PREFIX, 
                                                     callset_id, call_id)

    arg_dict = {'input_key' : s3_input_key, 
                'output_key' : s3_output_key, 
                'status_key' : s3_status_key, 
                'callset_id': callset_id, 
                'call_id' : call_id}    
    
    

    if extra_env is not None:
        arg_dict['extra_env'] = extra_env
    if extra_meta is not None:
        # sanity 
        for k, v in extra_meta.iteritems():
            if k in arg_dict:
                raise ValueError("Key {} already in dict".format(k))
            arg_dict[k] = v

    # put on s3 
    s3client.put_object(Bucket = s3_input_key[0], 
                  Key = s3_input_key[1], 
                  Body = func_str)

    json_arg = json.dumps(arg_dict)

    res = lambclient.invoke(FunctionName=wrenconfig.FUNCTION_NAME, 
                            Payload = json.dumps(arg_dict), 
                            InvocationType='Event')
    fut = ResponseFuture(call_id, callset_id)

    fut._set_state(JobState.invoked)

    return fut

def map(func, iterdata, extra_meta = None, extra_env = None, 
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
        def f():
            return call_async(func, iterdata[i], callset_id = callset_id,
                              extra_env=extra_env)
        cb = pool.apply_async(f)
        call_result_objs.append(cb)
    # invocation_done = False
    # while not invocation_done:
    #     invocation_done = True
    #     for result_obj in call_result_objs:
    #         if not result_obj.ready() :
    #             invocation_done = False
    #             time.sleep(1)

    # for result_obj in call_result_objs:
    #     print "was successful?", result_obj.successful()
    res =  [c.get() for c in call_result_objs]
    pool.close()
    pool.join()

    # FIXME take advantage of the callset to return a lot of these 

    # note these are just the invocation futures

    return res
    
ALL_COMPLETED = 1


def wait(fs, return_when=ALL_COMPLETED):
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


