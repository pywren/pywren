from __future__ import absolute_import

import json
import os

from  .exceptions import StorageNoSuchKeyError, StorageOutputNotFoundError
from .s3_backend import S3Backend
from .storage_utils import create_status_key, create_output_key, status_key_suffix


class Storage(object):
    """
    A Storage object is used by executors and other components to access underlying storage backend
    without exposing the the implementation details.
    Currently we only support S3 as the underlying backend.
    We plan to support other storage backends in the future.
    """

    def __init__(self, config):
        self.storage_config = config
        self.prefix = config['storage_prefix']
        self.backend_type = config['storage_backend']
        if config['storage_backend'] == 's3':
            self.backend_handler = S3Backend(config['backend_config'])
        else:
            raise NotImplementedError(("Using {} as storage backend is" +
                                       "not supported yet").format(config['storage_backend']))

    def get_storage_config(self):
        """
        Retrieves the configuration of this storage handler.
        :return: storage configuration
        """
        return self.storage_config

    def put_data(self, key, data):
        """
        Put input data into storage.
        :param key: data key
        :param data: data content
        :return: None
        """
        return self.backend_handler.put_object(key, data)

    def put_func(self, key, func):
        """
        Put serialized function into storage.
        :param key: function key
        :param func: serialized function
        :return: None
        """
        return self.backend_handler.put_object(key, func)

    def get_callset_status(self, callset_id):
        """
        Get the status of a callset.
        :param callset_id: callset's ID
        :return: A list of call IDs that have updated status.
        """
        # TODO: a better API for this is to return status for all calls in the callset. We'll fix
        #  this in scheduler refactoring.
        callset_prefix = os.path.join(self.prefix, callset_id)
        keys = self.backend_handler.list_keys_with_prefix(callset_prefix)
        suffix = status_key_suffix
        status_keys = [k for k in keys if suffix in k]
        call_ids = [k[len(callset_prefix)+1:].split("/")[0] for k in status_keys]
        return call_ids

    def get_call_status(self, callset_id, call_id):
        """
        Get status of a call.
        :param callset_id: callset ID of the call
        :param call_id: call ID of the call
        :return: A dictionary containing call's status, or None if no updated status
        """
        status_key = create_status_key(self.prefix, callset_id, call_id)
        try:
            data = self.backend_handler.get_object(status_key)
            return json.loads(data.decode('ascii'))
        except StorageNoSuchKeyError:
            return None

    def get_call_output(self, callset_id, call_id):
        """
        Get the output of a call.
        :param callset_id: callset ID of the call
        :param call_id: call ID of the call
        :return: Output of the call.
        """
        output_key = create_output_key(self.prefix, callset_id, call_id)
        try:
            return self.backend_handler.get_object(output_key)
        except StorageNoSuchKeyError:
            raise StorageOutputNotFoundError(callset_id, call_id)


def get_runtime_info(runtime_config):
    """
    Get the metadata given a runtime config.
    :param runtime_config: configuration of runtime (dictionary)
    :return: runtime metadata
    """
    if runtime_config['runtime_storage'] != 's3':
        raise NotImplementedError(("Storing runtime in non-S3 storage is not " +
                                   "supported yet").format(runtime_config['runtime_storage']))
    config = dict()
    config['bucket'] = runtime_config['s3_bucket']
    handler = S3Backend(config)

    key = runtime_config['s3_key'].replace(".tar.gz", ".meta.json")
    json_str = handler.get_object(key)
    runtime_meta = json.loads(json_str.decode("ascii"))
    return runtime_meta
