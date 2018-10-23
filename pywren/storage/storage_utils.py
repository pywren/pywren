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

import posixpath

from .exceptions import StorageConfigMismatchError

func_key_suffix = "func.pickle"
agg_data_key_suffix = "aggdata.pickle"
data_key_suffix = "data.pickle"
output_key_suffix = "output.pickle"
status_key_suffix = "status.json"
cancel_key_suffix = "cancel"

def create_func_key(prefix, callset_id):
    """
    Create function key
    :param prefix: prefix
    :param callset_id: callset's ID
    :return: function key
    """
    func_key = posixpath.join(prefix, callset_id, func_key_suffix)
    return func_key


def create_agg_data_key(prefix, callset_id):
    """
    Create aggregate data key
    :param prefix: prefix
    :param callset_id: callset's ID
    :return: a key for aggregate data
    """
    agg_data_key = posixpath.join(prefix, callset_id, agg_data_key_suffix)
    return agg_data_key


def create_data_key(prefix, callset_id, call_id):
    """
    Create data key
    :param prefix: prefix
    :param callset_id: callset's ID
    :param call_id: call's ID
    :return: data key
    """
    return posixpath.join(prefix, callset_id, call_id, data_key_suffix)


def create_output_key(prefix, callset_id, call_id):
    """
    Create output key
    :param prefix: prefix
    :param callset_id: callset's ID
    :param call_id: call's ID
    :return: output key
    """
    return posixpath.join(prefix, callset_id, call_id, output_key_suffix)


def create_status_key(prefix, callset_id, call_id):
    """
    Create status key
    :param prefix: prefix
    :param callset_id: callset's ID
    :param call_id: call's ID
    :return: status key
    """
    return posixpath.join(prefix, callset_id, call_id, status_key_suffix)

def create_cancel_key(prefix, callset_id, call_id):
    """
    Create cancel key
    :param prefix: prefix
    :param callset_id: callset's ID
    :param call_id: call's ID
    :return: status key
    """
    return posixpath.join(prefix, callset_id, call_id, cancel_key_suffix)


def create_keys(prefix, callset_id, call_id):
    """
    Create keys for data, output and status given callset and call IDs.
    :param prefix: prefix
    :param callset_id: callset's ID
    :param call_id: call's ID
    :return: data_key, output_key, status_key, cancel_key
    """
    data_key = create_data_key(prefix, callset_id, call_id)
    output_key = create_output_key(prefix, callset_id, call_id)
    status_key = create_status_key(prefix, callset_id, call_id)
    cancel_key = create_cancel_key(prefix, callset_id, call_id)
    return data_key, output_key, status_key, cancel_key


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
