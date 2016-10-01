import cPickle as pickle
import sys
import boto3

try:
    func_filename = sys.argv[1]
    arg_filename = sys.argv[2]
    out_filename = sys.argv[3]
    
    print "THIS IS WHERE WE ARE" 
    


    func = pickle.load(open(func_filename, 'r'))
    data = pickle.load(open(arg_filename, 'r'))
    
    
    y = func(data)
    
    pickle.dump({'result' : y, 
                 'success' : True} , 
                open(out_filename, 'w'), -1)


    

except Exception as e:
    pickle.dump({'result' : e, 
                 'success' : False}, 
                open(out_filename, 'w'), -1)
    


    
