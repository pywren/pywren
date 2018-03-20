from __future__ import absolute_import

import json
import os
import socket
import ssl

import botocore
import botocore.session
from pywren import local
from pywren.wrenutil import create_request_string

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))


class LambdaInvoker(object):
    def __init__(self, region_name, lambda_function_name):

        self.session = botocore.session.get_session()

        self.region_name = region_name
        self.lambda_function_name = lambda_function_name
        self.lambclient = self.session.create_client('lambda',
                                                     region_name=region_name)
        self.TIME_LIMIT = True

    def invoke(self, payload):
        """
        Invoke -- return information about this invocation
        """
        self.lambclient.invoke(FunctionName=self.lambda_function_name,
                               Payload=json.dumps(payload),
                               InvocationType='Event')
        # FIXME check response
        return {}

    def config(self):
        """
        Return config dict
        """
        return {'lambda_function_name' : self.lambda_function_name,
                'region_name' : self.region_name}


class LambdaSyncInvoker(object):
    def __init__(self, region_name, lambda_function_name):
        self.region_name = region_name
        self.lambda_function_name = lambda_function_name
        self.host = 'lambda.us-west-2.amazonaws.com'.replace('us-west-2', self.region_name)
        self.host_addr = socket.gethostbyname(self.host)

    def invoke(self, payload):
        """
        Invoke -- return information about this invocation
        """
        request_str = create_request_string(self.region_name,
                                            self.lambda_function_name,
                                            json.dumps(payload))

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        wrapped_socket = ssl.wrap_socket(s, ssl_version=ssl.PROTOCOL_SSLv23)
        wrapped_socket.connect((self.host_addr , 443))
        wrapped_socket.sendall(request_str)

        return wrapped_socket

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
        self.TIME_LIMIT = False

    def invoke(self, payload):
        self.payloads.append(payload)

    def config(self): # pylint: disable=no-self-use
        return {}


    def run_jobs(self, MAXJOBS=-1, run_dir="/tmp/task"):
        """
        run MAXJOBS in the queue
        MAXJOBS = -1  to run all

        # FIXME not multithreaded safe
        """

        jobn = len(self.payloads)
        if MAXJOBS != -1:
            jobn = MAXJOBS
        jobs = self.payloads[:jobn]

        local.local_handler(jobs, run_dir,
                            {'invoker' : 'DummyInvoker'})

        self.payloads = self.payloads[jobn:]
