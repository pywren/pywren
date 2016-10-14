import cPickle as pickle
import sys
import boto3

try:
    func_and_data_filename = sys.argv[1]
    out_filename = sys.argv[2]
    
    print "THIS IS WHERE WE ARE" 
    

    d = pickle.load(open(func_and_data_filename, 'r'))
    func = d['func']
    data = d['data']
    y = func(data)
    
    pickle.dump({'result' : y, 
                 'success' : True} , 
                open(out_filename, 'w'), -1)
    

except Exception as e:
    exc_type, exc_value, exc_traceback = sys.exc_info()

    pickle.dump({'result' : e, 
                 'exc_type' : exc_type, 
                 'exc_value' : exc_value, 
                 'exc_traceback' : exc_traceback, 
                 'success' : False}, 
                open(out_filename, 'w'), -1)
    


    
