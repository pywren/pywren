from __future__ import absolute_import

import glob
import os
import shutil

import pywren
from . import wrenhandler


def local_handler(jobs, run_dir, extra_context=None):
    """
    Run a list of (deserialized) jobs locally inside of
    run_dir

    Just for debugging
    """

    def copy_runtime(tgt_dir):
        files = glob.glob(os.path.join(pywren.SOURCE_DIR, "./*.py"))
        for f in files:
            shutil.copy(f, os.path.join(tgt_dir, os.path.basename(f)))

    original_dir = os.getcwd()
    jobn = len(jobs)

    for job_i, job in enumerate(jobs):
        task_run_dir = os.path.join(run_dir, str(job_i))
        shutil.rmtree(task_run_dir, True) # delete old modules
        os.makedirs(task_run_dir)
        copy_runtime(task_run_dir)


        context = {'jobnum' : job_i}
        if extra_context is not None:
            context.update(extra_context)

        os.chdir(task_run_dir)
        # FIXME debug
        wrenhandler.generic_handler(job, context)

        os.chdir(original_dir)
