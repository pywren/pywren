"""
Generic utility functions for serialization
"""

import base64
import os

import glob2


def bytes_to_b64str(byte_data):
    byte_data_64 = base64.b64encode(byte_data)
    byte_data_64_ascii = byte_data_64.decode('ascii')
    return byte_data_64_ascii

def create_mod_data(mod_paths):

    module_data = {}
    # load mod paths
    for m in mod_paths:
        if os.path.isdir(m):
            files = glob2.glob(os.path.join(m, "**/*.py"))
            pkg_root = os.path.abspath(os.path.dirname(m))
        else:
            pkg_root = os.path.abspath(os.path.dirname(m))
            files = [m]
        for f in files:
            f = os.path.abspath(f)
            mod_str = open(f, 'rb').read()

            dest_filename = f[len(pkg_root)+1:]
            module_data[dest_filename] = bytes_to_b64str(mod_str)

    return module_data
