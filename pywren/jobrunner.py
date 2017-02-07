import cPickle as pickle
import sys
import traceback
import boto3
from tblib import pickling_support
pickling_support.install()

try:
    func_filename = sys.argv[1]
    data_filename = sys.argv[2]
    out_filename = sys.argv[3]
    print "loading", func_filename, data_filename, out_filename
    loaded_func = pickle.loads(pickle.load(open(func_filename, 'r'))['func'])
    loaded_data = pickle.load(open(data_filename, 'r'))
    print "loaded"
    y = loaded_func(loaded_data)
    print "success"
    pickle.dump({'result' : y, 
                 'success' : True, 
                 'sys.path' : sys.path} , 
                open(out_filename, 'w'), -1)
    

except Exception as e:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_tb(exc_traceback)
    pickle.dump({'result' : e, 
                 'exc_type' : exc_type, 
                 'exc_value' : exc_value, 
                 'exc_traceback' : exc_traceback, 
                 'sys.path' : sys.path, 
                 'success' : False}, 
                open(out_filename, 'w'), -1)
    


    
