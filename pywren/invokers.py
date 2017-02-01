import boto3
import botocore
import json
from pywren import wrenhandler

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

    """

    def __init__(self):
        self.payloads = []

    def invoke(self, payload):
        self.payloads.append(payload)

    def config(self):
        return {}

    def run_jobs(self, MAXJOBS=-1):
        """
        run MAXJOBS in the queue
        MAXJOBS = -1  to run all
        """

        jobn = len(self.payloads)
        if MAXJOBS != -1:
            jobn = MAXJOBS

        for i in range(jobn):
            job = self.payloads.pop(0)
            context = {'invoker' : 'DummyInvoker', 
                       'jobnum' : i}
            wrenhandler.generic_handler(job, context)
        
    
class StandaloneInvoker(object):
    """
    """

    pass
