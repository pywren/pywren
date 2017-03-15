import boto3
import os
import botocore
import json
import sys

if (sys.version_info > (3, 0)):
    from . import wrenutil
    from . import wrenconfig
else:
    import wrenutil
    import wrenconfig

def create_callset_id():
    return wrenutil.uuid_str()

def create_call_id():
    return wrenutil.uuid_str()

def create_keys(bucket, prefix, callset_id, call_id):
    data_key = (bucket, os.path.join(prefix, callset_id, call_id, "data.pickle"))
    output_key = (bucket, os.path.join(prefix, callset_id, call_id, "output.pickle"))
    status_key = (bucket, os.path.join(prefix, callset_id, call_id, "status.json"))
    return data_key, output_key, status_key

def create_func_key(bucket, prefix, callset_id):
    func_key = (bucket, os.path.join(prefix, callset_id, "func.json"))
    return func_key

def create_agg_data_key(bucket, prefix, callset_id):
    func_key = (bucket, os.path.join(prefix, callset_id, "aggdata.pickle"))
    return func_key


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
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket,
                            'Prefix': key_prefix}
    page_iterator = paginator.paginate(**operation_parameters)

    status_keys = []
    for page in page_iterator:
        for item in page['Contents']:
            object_name = item['Key']
            if "status.json" in object_name:
                status_keys.append(object_name)

    call_ids = [k[len(key_prefix)+1:].split("/")[0] for k in status_keys]
    return call_ids

def get_call_status(callset_id, call_id,
                    AWS_S3_BUCKET = wrenconfig.AWS_S3_BUCKET_DEFAULT,
                    AWS_S3_PREFIX = wrenconfig.AWS_S3_PREFIX_DEFAULT):
    s3_data_key, s3_output_key, s3_status_key = create_keys(AWS_S3_BUCKET,
                                                            AWS_S3_PREFIX,
                                                            callset_id, call_id)
    s3_client = boto3.client('s3')

    try:
        r = s3_client.get_object(Bucket = s3_status_key[0], Key = s3_status_key[1])
        result_json = r['Body'].read()
        return json.loads(result_json.decode('ascii'))

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            return None
        else:
            raise e


def get_call_output(callset_id, call_id,
                    AWS_S3_BUCKET = wrenconfig.AWS_S3_BUCKET_DEFAULT,
                    AWS_S3_PREFIX = wrenconfig.AWS_S3_PREFIX_DEFAULT):
    s3_data_key, s3_output_key, s3_status_key = create_keys(AWS_S3_BUCKET,
                                                                   AWS_S3_PREFIX,
                                                                   callset_id, call_id)

    s3_client = boto3.client('s3')

    r = s3_client.get_object(Bucket = s3_output_key[0], Key = s3_output_key[1])
    return r['Body'].read()
