import boto3
import wrenconfig
import wrenutil
import os

def create_callset_id():
    return wrenutil.uuid_str()

def create_call_id():
    return wrenutil.uuid_str()

def create_keys(bucket, prefix, callset_id, call_id):
    input_key = (bucket, os.path.join(prefix, callset_id, call_id, "input.pickle"))
    output_key = (bucket, os.path.join(prefix, callset_id, call_id, "output.pickle"))
    status_key = (bucket, os.path.join(prefix, callset_id, call_id, "status.json"))
    return input_key, output_key, status_key
