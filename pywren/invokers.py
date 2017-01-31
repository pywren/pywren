import boto3
import json

class LambdaInvoker(object):
    def __init__(self, session, region_name, lambda_function_name):
        self.region_name = region_name
        self.lambda_function_name = lambda_function_name
        self.lambclient = session.create_client('lambda', 
                                                region_name = region_name)

    def invoke(self, payload):
        res = self.lambclient.invoke(FunctionName=self.lambda_function_name, 
                                     Payload = json.dumps(arg_dict), 
                                     InvocationType='Event')
        # FIXME check response

class DummyInvoker(object):
    """
    a placeholder to then attempt to manually run jobs
    """

    def __init__(self):
        self.payloads = []

    def invoke(self, payload):
        self.payloads.append(payload)
