import boto3
import wrenconfig
import wrenutil
import os
import botocore

def create_callset_id():
    return wrenutil.uuid_str()

def create_call_id():
    return wrenutil.uuid_str()

def create_keys(bucket, prefix, callset_id, call_id):
    input_key = (bucket, os.path.join(prefix, callset_id, call_id, "input.pickle"))
    output_key = (bucket, os.path.join(prefix, callset_id, call_id, "output.pickle"))
    status_key = (bucket, os.path.join(prefix, callset_id, call_id, "status.json"))
    return input_key, output_key, status_key


def key_size(bucket, key):
    try:

        s3 = boto3.resource('s3')
        a = s3.meta.client.head_object(Bucket=bucket, Key=key)
        return a['ContentLength']
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return None
        else:
            raise e


def get_callset_done(bucket, prefix, callset_id):
    key_prefix = os.path.join(prefix, callset_id)
    s3 = boto3.resource('s3', region_name=wrenconfig.AWS_REGION)
    s3res = s3.meta.client.list_objects_v2(Bucket=bucket, Prefix=key_prefix, 
                                           MaxKeys=1000)
    
    status_keys = []

    while True:
        for k in s3res['Contents']:
            if "status.json" in k['Key']:
                status_keys.append(k['Key'])

        if 'NextContinuationToken' in s3res:
            continuation_token = s3res['NextContinuationToken']
            s3res = s3.meta.client.list_objects_v2(Bucket=bucket, Prefix=key_prefix, 
                                                   MaxKeys=1000, 
                                                   ContinuationToken = continuation_token)
        else:
            break

    call_ids = [k[len(key_prefix)+1:].split("/")[0] for k in status_keys]
    return call_ids
        
        
