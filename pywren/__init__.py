from __future__ import absolute_import
from pywren.wren import default_executor, wait, lambda_executor, dummy_executor, remote_executor, get_all_results
from pywren import wrenlogging
from pywren.version import __version__
import logging
import os
if "PYWREN_LOGLEVEL" in os.environ:
    log_level = os.environ['PYWREN_LOGLEVEL']
    wrenlogging.default_config(log_level)
    # FIXME there has to be a better way to disable noisy boto logs
    logging.getLogger('boto').setLevel(logging.CRITICAL)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)



SOURCE_DIR = os.path.dirname(os.path.abspath(__file__)) 
