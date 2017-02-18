from __future__ import print_function
from six.moves import cPickle as pickle
import sys
import traceback
import boto3
import json
import base64
from tblib import pickling_support
pickling_support.install()

def b64str_to_bytes(str_data):
    str_ascii = str_data.encode('ascii')
    byte_data= base64.b64decode(str_ascii)
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
                 'sys.path' : sys.path} , 
                open(out_filename, 'wb'), -1)
    

except Exception as e:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_tb(exc_traceback)
    pickle.dump({'result' : e, 
                 'exc_type' : exc_type, 
                 'exc_value' : exc_value, 
                 'exc_traceback' : exc_traceback, 
                 'sys.path' : sys.path, 
                 'success' : False}, 
                open(out_filename, 'wb'), -1)
    
