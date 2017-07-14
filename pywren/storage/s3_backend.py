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
        :return: Objects of the bucket that match prefix
        :rtype: A list of keys
        """
        paginator = self.s3client.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.s3_bucket,
                                'Prefix': prefix}
        page_iterator = paginator.paginate(**operation_parameters)

        key_list = []
        for page in page_iterator:
            for item in page['Contents']:
                key_list.append(item['Key'])

        return key_list

