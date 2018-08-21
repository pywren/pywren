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

import botocore
import botocore.session
from pywren import local

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))


class LambdaInvoker:
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


class DummyInvoker:
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
