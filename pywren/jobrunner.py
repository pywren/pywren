import cPickle as pickle
import sys
import boto3

try:
    func_and_data_filename = sys.argv[1]
    out_filename = sys.argv[2]
    
    print "THIS IS WHERE WE ARE" 
    

    d = pickle.load(open(func_and_data_filename, 'r'))
    func_and_data = d['func_and_data']
    module_data = d['module_data']
    d = pickle.loads(func_and_data)
    func = d['func']
    data = d['data']
    
    y = func(data)
    
    pickle.dump({'result' : y, 
                 'success' : True} , 
                open(out_filename, 'w'), -1)
    

except Exception as e:
    pickle.dump({'result' : e, 
                 'success' : False}, 
                open(out_filename, 'w'), -1)
    


    
