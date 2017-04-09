import boto3
import json
import sys

import pywren.storage as storage

# FIXME separate runtime code with S3

def get_runtime_info(config):
    """
    Download runtime information from S3 at deserialize
    """
    runtime_meta = storage.Storage(config).get_runtime_info()

    if not runtime_valid(runtime_meta):
        raise Exception("The indicated runtime: {} "
                        + "is not approprite for this python version"
                        .format(config['runtime']))

    return runtime_meta


def version_str(version_info):
    return "{}.{}".format(version_info[0], version_info[1])


def runtime_valid(runtime_meta):
    """
    Basic checks
    """
    # FIXME at some point we should attempt to match modules
    # more closely 
    this_version_str = version_str(sys.version_info)

    return this_version_str == runtime_meta['python_ver']

