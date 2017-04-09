import os
import json
import copy

from s3_service import S3Service


class Storage(object):
    """
    A Storage object is used by executors and other components to access underlying storage service
    without exposing the the implementation details.
    Currently we only support S3 as the underlying service.
    We plan to support other services in the future.
    """

    def __init__(self, config):
        self.storage_config = config
        self.prefix = config['storage_prefix']
        self.service = config['storage_service']
        if config['storage_service'] == 's3':
            self.service_handler = S3Service(config['s3'])
        else:
            raise NotImplementedError(("Using {} as storage service is" +
                                       "not supported yet").format(config['storage_service']))

    def get_storage_config(self):
        """
        Retrieves the configuration of this storage handler.
        :return: storage configuration
        """
        return self.storage_config

    def get_storage_info(self):
        """
        Get the information of underlying storage service.
        :return:
        """
        info = dict()
        info['service'] = self.service
        info['location'] = self.service_handler.get_storage_location()
        return info

    def put_object(self, key, data):
        """
        Put an object into storage.
        :param key: object key
        :param data: object data
        :return: None
        """
        return self.service_handler.put_object(key, data)

    def get_object(self, key):
        """
        Get an object with key.
        :param key: object key
        :return: data
        """
        return self.service_handler.get_object(key)

    def create_keys(self, callset_id, call_id):
        """
        Create keys for data, output and status given callset and call IDs.
        :param callset_id: callset's ID
        :param call_id: call's ID
        :return: data_key, output_key, status_key
        """
        data_key = os.path.join(self.prefix, callset_id, call_id, "data.pickle")
        output_key = os.path.join(self.prefix, callset_id, call_id, "output.pickle")
        status_key = os.path.join(self.prefix, callset_id, call_id, "status.json")
        return data_key, output_key, status_key

    def create_func_key(self, callset_id):
        """
        Create function key
        :param callset_id: callset's ID
        :return: function key
        """
        func_key = os.path.join(self.prefix, callset_id, "func.json")
        return func_key

    def create_agg_data_key(self, callset_id):
        """
        Create aggregate data key
        :param callset_id: callset's ID
        :return: a key for aggregate data
        """
        agg_data_key = os.path.join(self.prefix, callset_id, "aggdata.pickle")
        return agg_data_key

    def get_callset_status(self, callset_id):
        """
        Get the status of a callset.
        :param callset_id: callset's ID
        :return: A list of call IDs that have updated status.
        """
        callset_prefix = os.path.join(self.prefix, callset_id)
        status_suffix = "status.json"
        return self.service_handler.get_callset_status(callset_prefix, status_suffix)

    def get_call_status(self, callset_id, call_id):
        """
        Get status of a call.
        :param callset_id: callset ID of the call
        :param call_id: call ID of the call
        :return: A dictionary containing call's status, or None if no updated status
        """
        _, _, status_key = self.create_keys(callset_id, call_id)
        return self.service_handler.get_call_status(status_key)

    def get_call_output(self, callset_id, call_id):
        """
        Get the output of a call.
        :param callset_id: callset ID of the call
        :param call_id: call ID of the call
        :return: Output of the call.
        """
        _, output_key, _ = self.create_keys(callset_id, call_id)
        return self.service_handler.get_call_output(output_key)

    def get_runtime_info(self, runtime_config):
        """
        Get the metadata given a runtime config.
        :param runtime_config: configuration of runtime (dictionary)
        :return: runtime metadata
        """
        if runtime_config['runtime_storage'] != 's3':
            raise NotImplementedError(("Storing runtime in non-S3 storage is not " +
                                       "supported yet").format(runtime_config['runtime_storage']))
        config = copy.deepcopy(self.storage_config['s3'])
        config['bucket'] = runtime_config['s3_bucket']
        handler = S3Service(config)

        key = runtime_config['s3_key'].replace(".tar.gz", ".meta.json")
        json_str = handler.get_object(key)
        runtime_meta = json.loads(json_str.decode("ascii"))
        return runtime_meta
