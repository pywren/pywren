from __future__ import absolute_import

import json
import os
import sys
import threading
import botocore
import botocore.session
from pywren import local
from six.moves import queue




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

class LocalInvoker(object):
    """
    An invoker which spawns a thread that then waits for jobs on a queue
    """

    def __init__(self, run_dir="/tmp/task"):


        if not sys.platform.startswith('linux'):
            raise RuntimeError("LocalInvoker can only be run under linux")

        self.running = True

        self.queue = queue.Queue()
        self.thread = threading.Thread(target=self._thread_runner)
        self.thread.start()
        self.run_dir = run_dir


    def quit(self):
        self.running = False
        self.thread.join()

    def _thread_runner(self):
        BLOCK_SEC_MAX = 10
        while self.running:
            try:
                res = self.queue.get(True, BLOCK_SEC_MAX)
                jobs = [res]

                local.local_handler(jobs, self.run_dir,
                                    {'invoker' : 'LocalInvoker'})
                self.queue.task_done()

            except queue.Empty:
                pass

    def invoke(self, payload):
        self.queue.put(payload)

    def config(self): # pylint: disable=no-self-use
        return {}
