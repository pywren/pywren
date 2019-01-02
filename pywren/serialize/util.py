#
# Copyright 2018 PyWren Team
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
# WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
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
            if m.split("/")[-1] == 'pywren':
                files = glob2.glob(os.path.join(m, "serialize/**/*.py"))
                files.append(os.path.join(m, "version.py"))
                init_path = os.path.join(m, "__init__.py")
                pkg_root = os.path.abspath(os.path.dirname(m))
                dest_filename = os.path.abspath(init_path)[len(pkg_root)+1:]
                module_data[dest_filename] = bytes_to_b64str("")
            else:
                files = glob2.glob(os.path.join(m, "**/*.py"))
            pkg_root = os.path.abspath(os.path.dirname(m))
        else:
            pkg_root = os.path.abspath(os.path.dirname(m))
            files = [m]
        for f in files:
            f = os.path.abspath(f)
            mod_str = open(f, 'rb').read()

            dest_filename = f[len(pkg_root)+1:].replace(os.sep, "/")
            module_data[dest_filename] = bytes_to_b64str(mod_str)

    return module_data
