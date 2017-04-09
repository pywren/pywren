import os
import json
import copy

from s3_service import S3Service

class Storage(object):

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
        return self.storage_config

    def get_storage_info(self):
        info = dict()
        info['service'] = self.service
        info['location'] = self.service_handler.get_storage_location()
        return info

    def put_object(self, key, data):
        return self.service_handler.put_object(key, data)

    def get_object(self, key):
        return self.service_handler.get_object(key)

    def create_keys(self, callset_id, call_id):
        data_key = os.path.join(self.prefix, callset_id, call_id, "data.pickle")
        output_key = os.path.join(self.prefix, callset_id, call_id, "output.pickle")
        status_key = os.path.join(self.prefix, callset_id, call_id, "status.json")
        return data_key, output_key, status_key

    def create_func_key(self, callset_id):
        func_key = os.path.join(self.prefix, callset_id, "func.json")
        return func_key

    def create_agg_data_key(self, callset_id):
        agg_data_key = os.path.join(self.prefix, callset_id, "aggdata.pickle")
        return agg_data_key

    def get_callset_status(self, callset_id):
        callset_prefix = os.path.join(self.prefix, callset_id)
        status_suffix = "status.json"
        return self.service_handler.get_callset_status(callset_prefix, status_suffix)

    def get_call_status(self, callset_id, call_id):
        _, _, status_key = self.create_keys(callset_id, call_id)
        return self.service_handler.get_call_status(status_key)

    def get_call_output(self, callset_id, call_id):
        _, output_key, _ = self.create_keys(callset_id, call_id)
        return self.service_handler.get_call_output(output_key)

    def get_runtime_info(self, runtime_config):
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
