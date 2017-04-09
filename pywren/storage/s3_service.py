import boto3
import os
import botocore
import json


class S3Service(object):

    def __init__(self, s3config):
        self.s3_bucket = s3config['bucket']
        self.session = botocore.session.get_session()
        self.s3client = self.session.create_client('s3')

    def get_storage_location(self):
        return self.s3_bucket

    def put_object(self, key, body):
        self.s3client.put_object(Bucket=self.s3_bucket, Key=key, Body=body)

    def get_object(self, key):
        r = self.s3client.get_object(Bucket = self.s3_bucket, Key = key)
        data = r['Body'].read()
        return data

    def get_callset_status(self, callset_prefix, status_suffix):
            paginator = self.s3client.get_paginator('list_objects_v2')
            operation_parameters = {'Bucket': self.s3_bucket,
                                    'Prefix': callset_prefix}
            page_iterator = paginator.paginate(**operation_parameters)

            status_keys = []
            for page in page_iterator:
                for item in page['Contents']:
                    object_name = item['Key']
                    if status_suffix in object_name:
                        status_keys.append(object_name)

            call_ids = [k[len(callset_prefix)+1:].split("/")[0] for k in status_keys]
            return call_ids

    def get_call_status(self, s3_status_key):
        try:
            data = self.get_object(s3_status_key)
            return json.loads(data.decode('ascii'))
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return None
            else:
                raise e

    def get_call_output(self, s3_output_key):
        return self.get_object(s3_output_key)
