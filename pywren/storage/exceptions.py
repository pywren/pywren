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

class StorageNoSuchKeyError(Exception):
    def __init__(self, key):
        msg = "No such key {} found in storage.".format(key)
        super(StorageNoSuchKeyError, self).__init__(msg)

class StorageOutputNotFoundError(Exception):
    def __init__(self, callset_id, call_id):
        msg = "Output for {} {} not found in storage.".format(callset_id, call_id)
        super(StorageOutputNotFoundError, self).__init__(msg)

class StorageConfigMismatchError(Exception):
    def __init__(self, current_path, prev_path):
        msg = "The data is stored at {}, but current storage is configured at {}.".format(
            prev_path, current_path)
        super(StorageConfigMismatchError, self).__init__(msg)
