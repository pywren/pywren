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
