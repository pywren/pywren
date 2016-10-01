import cPickle as pickle
import boto3
import tarfile
import subprocess
import os
import time
import base64
import logging
import uuid
import wrenutil


def handler(event, context):

    print "invocation started"
    print subprocess.check_output("df -h", shell=True)
    start_time = time.time()

    s3 = boto3.resource('s3')
    res = s3.meta.client.get_object(Bucket='ericmjonas-public', Key='condaruntime.tar.gz')
    condatar = tarfile.open(mode= "r:gz", 
                            fileobj = wrenutil.WrappedStreamingBody(res['Body'], 
                                                                    res['ContentLength']))
    condatar.extractall('/tmp/')
    print "download and untar of conda runtime complete"
    print subprocess.check_output("df -h", shell=True)

    cwd = os.getcwd()
    jobrunner_path = os.path.join(cwd, "jobrunner.py")

    func_filename = "/tmp/func.pickle"
    data_filename = "/tmp/data.pickle"
    output_filename = "/tmp/output.pickle"
    
    func_pickle_string = base64.b64decode(event['func_pickle_string'])
    data_pickle_string = base64.b64decode(event['data_pickle_string'])
    extra_env = event.get('extra_env', {})

    call_id = event['call_id']
    callset_id = event['callset_id']
    
    func_fid = open(func_filename, 'w')
    func_fid.write(func_pickle_string)
    func_fid.close()

    
    data_fid = open(data_filename, 'w')
    data_fid.write(data_pickle_string)
    data_fid.close()

    print "state written to disk" 

    CONDA_PYTHON_RUNTIME = "/tmp/condaruntime/bin/python"
    
    cmdstr = "{} {} {} {} {}".format(CONDA_PYTHON_RUNTIME, 
                                     jobrunner_path, 
                                     func_filename, 
                                     data_filename, 
                                     output_filename)

    setup_time = time.time()
    

    local_env = os.environ.copy()

    local_env["OMP_NUM_THREADS"] = "1"
    local_env.update(extra_env)

    print "command str=", cmdstr
    message = subprocess.check_output(cmdstr, shell=True, env=local_env)
    print "command executed, message=", message

    func_output = base64.b64encode(open(output_filename, 'r').read())
    
    end_time = time.time()

    d = { 
        'message' : message,
        'call_id' : call_id, 
        'start_time' : start_time, 
        'setup_time' : setup_time - start_time, 
        'exec_time' : time.time() - setup_time, 
        'func_output' : func_output,
        'end_time' : end_time, 
        'callset_id' : callset_id, 
        'aws_request_id' : context.aws_request_id, 
        'log_group_name' : context.log_group_name, 
        'log_stream_name' : context.log_stream_name, 
    }  


    try:
        uuid_str = str(uuid.uuid1())
        
        sdbclient = boto3.client('sdb', region_name='us-west-2')
        
        attributes = [{'Name' : k, 'Value' : str(v)} for k, v in d.iteritems()]
        sdbclient.put_attributes(DomainName='test_two', 
                                 ItemName=uuid_str, 
                                 Attributes=attributes)
    except Exception as e:
        d['exception'] = str(e)
    
    return d

if __name__ == "__main__":
    s3 = boto3.resource('s3')
    #s3.meta.client.download_file('ericmjonas-public', 'condaruntime.tar.gz', '/tmp/condaruntime.tar.gz')
    res = s3.meta.client.get_object(Bucket='ericmjonas-public', Key='condaruntime.tar.gz')

    condatar = tarfile.open(mode= "r:gz", 
                            fileobj = WrappedStreamingBody(res['Body'], res['ContentLength']))
    condatar.extractall('/tmp/test1/')
