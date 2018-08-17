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

import sys

import pywren.storage as storage


def get_runtime_info(runtime_config):
    """
    Download runtime information from storage at deserialize
    """
    runtime_meta = storage.get_runtime_info(runtime_config)

    if not runtime_valid(runtime_meta):
        raise Exception(("The indicated runtime: {} "
                         + "is not approprite for this python version.")
                        .format(runtime_config))

    return runtime_meta


def version_str(version_info):
    return "{}.{}".format(version_info[0], version_info[1])


def runtime_valid(runtime_meta):
    """
    Basic checks
    """
    # FIXME at some point we should attempt to match modules
    # more closely
    this_version_str = version_str(sys.version_info)

    return this_version_str == runtime_meta['python_ver']
