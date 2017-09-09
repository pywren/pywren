import os

from .exceptions import StorageConfigMismatchError

func_key_suffix = "func.json"
agg_data_key_suffix = "aggdata.pickle"
data_key_suffix = "data.pickle"
output_key_suffix = "output.pickle"
status_key_suffix = "status.json"

def create_func_key(prefix, callset_id):
    """
    Create function key
    :param prefix: prefix
    :param callset_id: callset's ID
    :return: function key
    """
    func_key = os.path.join(prefix, callset_id, func_key_suffix)
    return func_key


def create_agg_data_key(prefix, callset_id):
    """
    Create aggregate data key
    :param prefix: prefix
    :param callset_id: callset's ID
    :return: a key for aggregate data
    """
    agg_data_key = os.path.join(prefix, callset_id, agg_data_key_suffix)
    return agg_data_key


def create_data_key(prefix, callset_id, call_id):
    """
    Create data key
    :param prefix: prefix
    :param callset_id: callset's ID
    :param call_id: call's ID
    :return: data key
    """
    return os.path.join(prefix, callset_id, call_id, data_key_suffix)


def create_output_key(prefix, callset_id, call_id):
    """
    Create output key
    :param prefix: prefix
    :param callset_id: callset's ID
    :param call_id: call's ID
    :return: output key
    """
    return os.path.join(prefix, callset_id, call_id, output_key_suffix)


def create_status_key(prefix, callset_id, call_id):
    """
    Create status key
    :param prefix: prefix
    :param callset_id: callset's ID
    :param call_id: call's ID
    :return: status key
    """
    return os.path.join(prefix, callset_id, call_id, status_key_suffix)


def create_keys(prefix, callset_id, call_id):
    """
    Create keys for data, output and status given callset and call IDs.
    :param prefix: prefix
    :param callset_id: callset's ID
    :param call_id: call's ID
    :return: data_key, output_key, status_key
    """
    data_key = create_data_key(prefix, callset_id, call_id)
    output_key = create_output_key(prefix, callset_id, call_id)
    status_key = create_status_key(prefix, callset_id, call_id)
    return data_key, output_key, status_key


def get_storage_path(config):
    if config['storage_backend'] != 's3':
        raise NotImplementedError(
            ("Using {} as storage backend is not supported yet").format(
                config['storage_backend']))
    return [config['storage_backend'], config['backend_config']['bucket'], config['storage_prefix']]


def check_storage_path(config, prev_path):
    current_path = get_storage_path(config)
    if current_path != prev_path:
        raise StorageConfigMismatchError(current_path, prev_path)
