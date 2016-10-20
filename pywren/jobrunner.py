import cPickle as pickle
import sys
import traceback
import boto3

try:
    func_and_data_filename = sys.argv[1]
    out_filename = sys.argv[2]
    print "loading", func_and_data_filename, out_filename
    loaded_data = pickle.load(open(func_and_data_filename, 'r'))
    print len(loaded_data)
    func, args, kwargs = loaded_data
    print "loaded" 
    y = func(*args, **kwargs)
    
    pickle.dump({'result' : y, 
                 'success' : True, 
                 'sys.path' : sys.path} , 
                open(out_filename, 'w'), -1)
    

except Exception as e:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_tb(exc_traceback)
    pickle.dump({'result' : e, 
                 'exc_type' : exc_type, 
                 'sys.path' : sys.path, 
                 'success' : False}, 
                open(out_filename, 'w'), -1)
    


    
