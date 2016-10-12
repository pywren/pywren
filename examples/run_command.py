#from gevent import monkey

#monkey.patch_socket()
#monkey.patch_ssl()

import time
import boto3 
import uuid
import numpy as np
import time
import pywren
import subprocess
import logging
import sys

def run_command(x):
    return subprocess.check_output(x, shell=True)

if __name__ == "__main__":
    cmd = " ".join(sys.argv[1:])

    wrenexec = pywren.default_executor()
    fut = wrenexec.call_async(run_command, cmd)
    print fut.callset_id

    res = fut.result() 
    print res
