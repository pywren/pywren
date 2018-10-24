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

import glob
import os
import shutil

import pywren
from . import wrenhandler


def run_generic_handler(job, job_id, original_dir, run_dir, extra_context):

    def copy_runtime(tgt_dir):
        files = glob.glob(os.path.join(pywren.SOURCE_DIR, "./*.py"))
        files = glob.glob(os.path.join(pywren.SOURCE_DIR, "jobrunner/*.py"))
        for f in files:
            shutil.copy(f, os.path.join(tgt_dir, os.path.basename(f)))

    local_task_run_dir = os.path.join(run_dir, job_id)
    if not os.path.exists(local_task_run_dir):
        os.makedirs(local_task_run_dir)
        copy_runtime(local_task_run_dir)


    context = {'jobnum' : job_id}
    if extra_context is not None:
        context.update(extra_context)

    os.chdir(local_task_run_dir)
    # FIXME debug
    wrenhandler.generic_handler(job, context)

    os.chdir(original_dir)

def dummy_handler(jobs, run_dir, extra_context=None):
    """
    Run a list of (deserialized) jobs locally inside of
    run_dir, used by dummy invoker

    Just for debugging
    """
    original_dir = os.getcwd()
    for job_i, job in enumerate(jobs):
        run_generic_handler(job, str(job_i), original_dir, run_dir, extra_context)

def local_handler(job, run_dir, extra_context=None):
    """
    Runs a job locally inside of run_dir, used by local invoker
    """
    run_generic_handler(job, str(os.getpid()), os.getcwd(), run_dir, extra_context)
