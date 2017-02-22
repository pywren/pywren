import boto3
import json
import sys

def get_runtime_info(bucket, key):
    """
    Download runtime information from S3 at deserialize
    """
    s3 = boto3.resource('s3')

    runtime_meta_key = key.replace(".tar.gz", ".meta.json")
    
    json_str = s3.meta.client.get_object(Bucket=bucket, Key=runtime_meta_key)['Body'].read()
    runtime_meta = json.loads(json_str.decode("ascii"))

    return runtime_meta

def version_str(version_info):
    return "{}.{}".format(version_info[0], version_info[1])

def runtime_key_valid(bucket, key):
    runtime_meta = get_runtime_info(bucket, key)
    return runtime_valid(runtime_meta)

def runtime_valid(runtime_meta):
    """
    Basic checks
    """
    # FIXME at some point we should attempt to match modules
    # more closely 
    this_version_str = version_str(sys.version_info)

    return this_version_str == runtime_meta['python_ver']

