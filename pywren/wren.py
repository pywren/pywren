import boto3
import cloudpickle
import json
import base64
import cPickle as pickle
import wrenconfig
import wrenutil
import enum
from multiprocessing.pool import ThreadPool
import time


class JobState(enum.Enum):
    new = 1
    invoked = 2
    running = 3
    success = 4
    error = 5

def get_call_status(call_id):
    sdbclient = boto3.client('sdb', region_name=wrenconfig.AWS_REGION)

    r = sdbclient.select(SelectExpression="select * from {} where call_id='{}'".format(wrenconfig.AWS_SDB_DOMAIN, call_id))

    # Fixme this might not work due to eventual consistency 
    if 'Items' in r and len(r['Items']) > 0:
        return wrenutil.sdb_to_dict(r['Items'][0]) 
    else:
        return None
        
class ResponseFuture(object):

    """
    """
    def __init__(self):
        self.call_id = wrenutil.uuid_str()
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
        
        sdb_dict = get_call_status(self.call_id) 
        while sdb_dict is None:
            time.sleep(4)
            sdb_dict = get_call_status(self.call_id) 

        s = base64.b64decode(sdb_dict['func_output'])
        call_invoker_result = pickle.loads(s)
        call_success = call_invoker_result['success']
        
        if call_success:
            
            self._return_val = call_invoker_result['result']
            self._state = JobState.success
        else:
            self._exception = call_invoker_result['result']
            self._state = JobState.error

        self._run_dict = sdb_dict
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

    session = boto3.session.Session()
    lambclient = session.client('lambda', region_name=wrenconfig.AWS_REGION)

    func_str = cloudpickle.dumps(func)
    data_str = cloudpickle.dumps(data)
    arg_dict = {'func_pickle_string' : base64.b64encode(func_str), 
                'data_pickle_string' : base64.b64encode(data_str), 
                'callset_id': callset_id}
    
    

    if extra_env is not None:
        arg_dict['extra_env'] = extra_env
    if extra_meta is not None:
        # sanity 
        for k, v in extra_meta.iteritems():
            if k in arg_dict:
                raise ValueError("Key {} already in dict".format(k))
            arg_dict[k] = v

    fut = ResponseFuture()
    arg_dict['call_id'] = fut.call_id
    json_arg = json.dumps(arg_dict)

    res = lambclient.invoke(FunctionName=wrenconfig.FUNCTION_NAME, 
                            Payload = json.dumps(arg_dict), 
                            InvocationType='Event')
    fut._set_state(JobState.invoked)

    return fut

def map(func, iterdata, extra_meta = None, extra_env = None, 
        invoke_pool_threads=64):
    """
    Optionally use threadpool for faster invocation
    
    # FIXME work with an actual iterable instead of just a list

    """
    
    pool = ThreadPool(invoke_pool_threads)
    callset_id = wrenutil.uuid_str()
    
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
    sets. The first set, named “done”, contains the futures that completed
    (finished or were cancelled) before the wait completed. The second
    set, named “not_done”, contains uncompleted futures.


    http://pythonhosted.org/futures/#concurrent.futures.wait

    """


