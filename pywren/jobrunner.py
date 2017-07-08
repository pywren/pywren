from __future__ import print_function

import base64
import json
import sys
import traceback

from six.moves import cPickle as pickle
from tblib import pickling_support

pickling_support.install()

def b64str_to_bytes(str_data):
    str_ascii = str_data.encode('ascii')
    byte_data = base64.b64decode(str_ascii)
    return byte_data

try:
    func_filename = sys.argv[1]
    data_filename = sys.argv[2]
    out_filename = sys.argv[3]
    # initial output file in case job fails
    pickle.dump({'result' : None,
                 'success' : False},
                open(out_filename, 'wb'), -1)

    print("loading", func_filename, data_filename, out_filename)
    func_b64 = b64str_to_bytes(json.load(open(func_filename, 'r'))['func'])
    loaded_func = pickle.loads(func_b64)
    loaded_data = pickle.load(open(data_filename, 'rb'))
    print("loaded")
    y = loaded_func(loaded_data)
    print("success")
    pickle.dump({'result' : y,
                 'success' : True,
                 'sys.path' : sys.path},
                open(out_filename, 'wb'), -1)


except Exception as e:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_tb(exc_traceback)

    # Shockingly often, modules like subprocess don't properly
    # call the base Exception.__init__, which results in them
    # being unpickleable. As a result, we actually wrap this in a try/catch block
    # and more-carefully handle the exceptions if any part of this save / test-reload
    # fails

    try:
        with  open(out_filename, 'wb') as fid:
            pickle.dump({'result' : e,
                         'exc_type' : exc_type,
                         'exc_value' : exc_value,
                         'exc_traceback' : exc_traceback,
                         'sys.path' : sys.path,
                         'success' : False}, fid, -1)

        # this is just to make sure they can be unpickled
        pickle.load(open(out_filename, 'rb'))

    except Exception as pickle_exception:
        pickle.dump({'result' : str(e),
                     'exc_type' : str(exc_type),
                     'exc_value' : str(exc_value),
                     'exc_traceback' : exc_traceback,
                     'exc_traceback_str' : str(exc_traceback),
                     'sys.path' : sys.path,
                     'pickle_fail' : True,
                     'pickle_exception' : pickle_exception,
                     'success' : False},
                    open(out_filename, 'wb'), -1)


