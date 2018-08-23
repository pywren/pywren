#
# Copyright 2018 PyWren Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import botocore

from .exceptions import StorageNoSuchKeyError


class S3Backend(object):
    """
    A wrap-up around S3 boto3 APIs.
    """

    def __init__(self, s3config):
        self.s3_bucket = s3config['bucket']
        self.session = botocore.session.get_session()
        self.s3client = self.session.create_client(
            's3', config=botocore.client.Config(max_pool_connections=200))

    def put_object(self, key, data):
        """
        Put an object in S3. Override the object if the key already exists.
        :param key: key of the object.
        :param data: data of the object
        :type data: str/bytes
        :return: None
        """
        self.s3client.put_object(Bucket=self.s3_bucket, Key=key, Body=data)

    def get_object(self, key):
        """
        Get object from S3 with a key. Throws StorageNoSuchKeyError if the given key does not exist.
        :param key: key of the object
        :return: Data of the object
        :rtype: str/bytes
        """
        try:
            r = self.s3client.get_object(Bucket=self.s3_bucket, Key=key)
            data = r['Body'].read()
            return data
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                raise StorageNoSuchKeyError(key)
            else:
                raise e

    def key_exists(self, key):
        """
        Check if a key exists in S3.
        :param key: key of the object
        :return: True if key exists, False if not exists
        :rtype: boolean
        """
        try:
            self.s3client.head_object(Bucket=self.s3_bucket, Key=key)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return False
            else:
                raise e

    def list_keys_with_prefix(self, prefix):
        """
        Return a list of keys for the given prefix.
        :param prefix: Prefix to filter object names.
        :return: List of keys in bucket that match the given prefix.
        :rtype: list of str
        """
        paginator = self.s3client.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.s3_bucket,
                                'Prefix': prefix}
        page_iterator = paginator.paginate(**operation_parameters)

        key_list = []
        for page in page_iterator:
            if 'Contents' in page:
                for item in page['Contents']:
                    key_list.append(item['Key'])

        return key_list
