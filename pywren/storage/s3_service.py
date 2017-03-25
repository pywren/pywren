import boto3
import os
import botocore
import json


class S3Service(object):

    def __init__(self, config):
        self.config = config
        self.aws_region = config['account']['aws_region']
        FUNCTION_NAME = config['lambda']['function_name']

        self.s3_bucket = config['s3']['bucket']
        self.s3_prefix = config['s3']['pywren_prefix']

        self.runtime_bucket = config['runtime']['s3_bucket']
        self.runtime_key = config['runtime']['s3_key']

        self.session = botocore.session.get_session()
        self.s3client = self.session.create_client('s3', region_name=self.aws_region)

    def put_object(self, key, body):
        self.s3client.put_object(Bucket=self.s3_bucket, Key=key, Body=body)

    def create_keys(self, callset_id, call_id):
        data_key = os.path.join(self.prefix, callset_id, call_id, "data.pickle")
        output_key = os.path.join(self.prefix, callset_id, call_id, "output.pickle")
        status_key = os.path.join(self.prefix, callset_id, call_id, "status.json")
        return data_key, output_key, status_key

    def create_func_key(self, callset_id):
        func_key = os.path.join(self.prefix, callset_id, "func.json")
        return func_key

    def create_agg_data_key(self, callset_id):
        func_key = os.path.join(self.prefix, callset_id, "aggdata.pickle")
        return func_key

    def get_callset_done(self, callset_id):
        key_prefix = os.path.join(self.prefix, callset_id)
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.s3_bucket,
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

    def get_call_status(self, callset_id, call_id):
        s3_data_key, s3_output_key, s3_status_key = self.create_keys(callset_id, call_id)

        s3_client = boto3.client('s3')

        try:
            r = s3_client.get_object(Bucket = self.s3_bucket, Key = s3_status_key)
            result_json = r['Body'].read()
            return json.loads(result_json.decode('ascii'))

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return None
            else:
                raise e

    def get_call_output(self, callset_id, call_id):
        s3_data_key, s3_output_key, s3_status_key = self.create_keys(callset_id, call_id)

        s3_client = boto3.client('s3')

        r = s3_client.get_object(Bucket = self.s3_bucket, Key = s3_output_key)
        return r['Body'].read()

    def get_runtime_info(self):
        runtime_meta_key = self.runtime_key.replace(".tar.gz", ".meta.json")

        json_str = self.s3_client.get_object(Bucket=self.runtime_bucket,
                                             Key=runtime_meta_key)['Body'].read()

        runtime_meta = json.loads(json_str.decode("ascii"))

        return runtime_meta

