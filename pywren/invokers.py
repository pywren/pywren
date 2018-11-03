#
# Copyright 2018 PyWren Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import

import json
import os
import threading
import sys
import shutil
import tempfile
import multiprocessing
import botocore
import botocore.session
from pywren import local


SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))

LOCAL_DIR = tempfile.gettempdir()
LOCAL_RUN_DIR = os.path.join(LOCAL_DIR, "task")


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
    """

    def __init__(self):
        self.payloads = []
        self.TIME_LIMIT = False
        self.thread = None

    def invoke(self, payload):
        self.payloads.append(payload)

    def config(self): # pylint: disable=no-self-use
        return {}


    def run_jobs(self, MAXJOBS=-1, run_dir=LOCAL_RUN_DIR):
        """
        run MAXJOBS in the queue
        MAXJOBS = -1  to run all

        # FIXME not multithreaded safe
        """

        jobn = len(self.payloads)
        if MAXJOBS != -1:
            jobn = MAXJOBS
        jobs = self.payloads[:jobn]

        local.dummy_handler(jobs, run_dir,
                            {'invoker' : 'DummyInvoker'})

        self.payloads = self.payloads[jobn:]

    def run_jobs_threaded(self, MAXJOBS=-1, run_dir=LOCAL_RUN_DIR):
        """
        Just like run_jobs but in a separate thread
        (so it's non-blocking)
        """

        self.thread = threading.Thread(target=self.run_jobs,
                                       args=(MAXJOBS, run_dir))
        self.thread.start()

class LocalInvoker(object):
    """
    An invoker which spawns a thread that then waits
    for jobs on a queue. This is a more self-contained invoker in that
    it doesn't require the run_jobs() of the dummy invoker.
    """

    def __init__(self, run_dir=LOCAL_RUN_DIR):
        # When Windows/OSX runtimes are made available, local invoker should be
        # ready to run on them as well
        if not sys.platform.startswith('linux'):
            raise RuntimeError("LocalInvoker can only be run under linux")

        self.queue = multiprocessing.Queue()
        shutil.rmtree(run_dir, True)
        self.run_dir = run_dir
        for _ in range(multiprocessing.cpu_count()):
            p = multiprocessing.Process(target=self._process_runner)
            p.daemon = True
            p.start()


    def _process_runner(self):
        while True:
            res = self.queue.get(block=True)
            local.local_handler(res, self.run_dir,
                                {'invoker' : 'LocalInvoker'})

    def invoke(self, payload):
        self.queue.put(payload)

    def config(self): # pylint: disable=no-self-use
        return {}
