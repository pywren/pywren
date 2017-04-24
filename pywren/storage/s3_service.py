import boto3
import os
import botocore
import json


class S3Service(object):
    """
    A wrap-up around S3 boto3 APIs.
    """

    def __init__(self, s3config):
        self.s3_bucket = s3config['bucket']
        self.session = botocore.session.get_session()
        self.s3client = self.session.create_client('s3',
                            config=botocore.client.Config(max_pool_connections=200))

    def get_storage_location(self):
        """
        Get storage location for this S3 service.
        :return: S3 bucket name
        """
        return self.s3_bucket

    def put_object(self, key, data):
        """
        Put an object in S3.
        :param key: key of the object.
        :param data: data of the object
        :type data: str/bytes
        :return: None
        """
        self.s3client.put_object(Bucket=self.s3_bucket, Key=key, Body=data)

    def get_object(self, key):
        """
        Get object from S3 with a key.
        :param key: key of the object
        :return: Data of the object
        :rtype: str/bytes
        """
        r = self.s3client.get_object(Bucket = self.s3_bucket, Key = key)
        data = r['Body'].read()
        return data

    def get_callset_status(self, callset_prefix, status_suffix = "status.json"):
        """
        Get status for a prefix.
        :param callset_prefix: A prefix for the callset.
        :param status_suffix: Suffix used for status files. By default, "status.json"
        :return: A list of call IDs
        """
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
        """
        Get the status for a call.
        :param s3_status_key: status key
        :return: Updated status if status key exists, otherwise None.
        """
        try:
            data = self.get_object(s3_status_key)
            return json.loads(data.decode('ascii'))
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return None
            else:
                raise e

    def get_call_output(self, s3_output_key):
        """
        Get the output for a call.
        :param s3_output_key: output key
        :return: output for a call, throws exception if output key does not exist
        """
        return self.get_object(s3_output_key)
