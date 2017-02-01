import boto3
import botocore
import json
import shutil
import glob2
import os
from pywren import wrenhandler


SOURCE_DIR = os.path.dirname(os.path.abspath(__file__)) 


class LambdaInvoker(object):
    def __init__(self, region_name, lambda_function_name):

        self.session = botocore.session.get_session()

        self.region_name = region_name
        self.lambda_function_name = lambda_function_name
        self.lambclient = self.session.create_client('lambda', 
                                                     region_name = region_name)

    def invoke(self, payload):
        """
        Invoke -- return information about this invocation
        """
        res = self.lambclient.invoke(FunctionName=self.lambda_function_name, 
                                     Payload = json.dumps(payload), 
                                     InvocationType='Event')
        # FIXME check response
        return {}

    def config(self):
        """
        Return config dict
        """
        return {'lambda_function_name' : self.lambda_function_name, 
                'region_name' : self.region_name}


class DummyInvoker(object):
    """
    A mock invoker that simply appends payloads to a list. You must then
    call run()

    does not delete left-behind jobs

    """

    def __init__(self):
        self.payloads = []

    def invoke(self, payload):
        self.payloads.append(payload)

    def config(self):
        return {}

    def copy_runtime(self, tgt_dir):
        files = glob2.glob(os.path.join(SOURCE_DIR, "./*.py"))
        for f in files:
            shutil.copy(f, os.path.join(tgt_dir, os.path.basename(f)))

    def run_jobs(self, MAXJOBS=-1, run_dir="/tmp/task"):
        """
        run MAXJOBS in the queue
        MAXJOBS = -1  to run all

        # FIXME not multithreaded safe
        """

        jobn = len(self.payloads)
        if MAXJOBS != -1:
            jobn = MAXJOBS
        original_dir = os.getcwd()

        for i in range(jobn):
            task_run_dir = os.path.join(run_dir, str(i))
            shutil.rmtree(task_run_dir, True) # delete old modules
            os.makedirs(task_run_dir)
            self.copy_runtime(task_run_dir)

            job = self.payloads.pop(0)
            context = {'invoker' : 'DummyInvoker', 
                       'jobnum' : i}
            os.chdir(task_run_dir)
            wrenhandler.generic_handler(job, context)
            
            os.chdir(original_dir)
    
class StandaloneInvoker(object):
    """
    """

    pass
