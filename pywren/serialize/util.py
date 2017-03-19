"""
Generic utility functions for serialization
"""

import os
import glob2

def create_mod_data(mod_paths):

    module_data = {}
    # load mod paths
    for m in mod_paths:
        if os.path.isdir(m):
            files = glob2.glob(os.path.join(m, "**/*.py"))
            pkg_root = os.path.dirname(m)
        else:
            pkg_root = os.path.dirname(m)
            files = [m]
        for f in files:
            dest_filename = f[len(pkg_root)+1:]
            mod_str = open(f, 'rb').read()
            module_data[f[len(pkg_root)+1:]] = mod_str.decode('utf-8')

    return module_data
