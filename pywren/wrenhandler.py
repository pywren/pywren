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
import json
import s3util

PYTHON_MODULE_PATH = "/tmp/pymodules"

def handler(event, context):
    s3 = boto3.resource('s3')

    start_time = time.time()

    all_input_filename = "/tmp/all_input.pickle"
    func_and_data_filename = "/tmp/input.pickle"
    output_filename = "/tmp/output.pickle"
    # cleanup previous invocations
    subprocess.check_output("rm -Rf /tmp/*", shell=True)


    server_info = {'/proc/cpuinfo': open("/proc/cpuinfo", 'r').read(), 
                   '/proc/meminfo': open("/proc/meminfo", 'r').read(), 
                   '/proc/self/cgroup': open("/proc/meminfo", 'r').read(), 
                   '/proc/cgroups': open("/proc/cgroups", 'r').read() }
                   
                 

    print "invocation started"

    # download the input 
    input_key = event['input_key']
    output_key = event['output_key']
    status_key = event['status_key']
    runtime_s3_bucket = event['runtime_s3_bucket']
    runtime_s3_key = event['runtime_s3_key']

    b, k = input_key
    KS =  s3util.key_size(b, k)
    print "bucket=", b, "key=", k,  "status: ", KS, "bytes" 
    while KS is None:
        print "WARNING COULD NOT GET FIRST KEY" 

        KS =  s3util.key_size(b, k)

    # get the input and save to disk 
    # FIXME here is we where we would attach the "canceled" metadata
    s3.meta.client.download_file(input_key[0], input_key[1], all_input_filename)
    input_download_time = time.time()

    print "input data download complete"
    
    # now split
    d = pickle.load(open(all_input_filename, 'r'))
    fdfid = open(func_and_data_filename, 'w')
    fdfid.write(d['func_and_data'])
    fdfid.close()
    
    # get modules
    for m_filename, m_text in d['module_data'].iteritems():
        m_path = os.path.dirname(m_filename)
        if m_path[0] == "/":
            m_path = m_path[1:]
        to_make = os.path.join(PYTHON_MODULE_PATH, m_path)
        print "to_make=", to_make, "m_path=", m_path
        try:
            os.makedirs(to_make)
        except OSError as e:
            if e.errno == 17:
                pass
            else:
                raise e
        full_filename = os.path.join(to_make, os.path.basename(m_filename))
        print "creating", full_filename
        fid = open(full_filename, 'w')
        fid.write(m_text)
        fid.close()
        

    ## Now get the runtime

    res = s3.meta.client.get_object(Bucket=runtime_s3_bucket, 
                                    Key=runtime_s3_key)

    condatar = tarfile.open(mode= "r:gz", 
                            fileobj = wrenutil.WrappedStreamingBody(res['Body'], 
                                                                    res['ContentLength']))
    condatar.extractall('/tmp/')
    print "download and untar of conda runtime complete"

    cwd = os.getcwd()
    jobrunner_path = os.path.join(cwd, "jobrunner.py")
    
    print event
    extra_env = event.get('extra_env', {})
    extra_env['PYTHONPATH'] = PYTHON_MODULE_PATH

    call_id = event['call_id']
    callset_id = event['callset_id']

    print "state written to disk" 

    CONDA_PYTHON_RUNTIME = "/tmp/condaruntime/bin/python"
    
    cmdstr = "{} {} {} {}".format(CONDA_PYTHON_RUNTIME, 
                                     jobrunner_path, 
                                     func_and_data_filename, 
                                     output_filename)

    setup_time = time.time()
    

    local_env = os.environ.copy()

    local_env["OMP_NUM_THREADS"] = "1"
    local_env.update(extra_env)

    print "command str=", cmdstr
    stdout = subprocess.check_output(cmdstr, shell=True, env=local_env)
    print "command executed, stdout=", stdout

    s3.meta.client.upload_file(output_filename, output_key[0], 
                               output_key[1])
    
    end_time = time.time()

    d = { 
        'stdout' : stdout, 
        'call_id' : call_id, 
        'callset_id' : callset_id, 
        'start_time' : start_time, 
        'setup_time' : setup_time - start_time, 
        'exec_time' : time.time() - setup_time, 
        'input_key' : input_key, 
        'output_key' : output_key, 
        'status_key' : status_key, 
        'end_time' : end_time, 
        'host_submit_time' : event['host_submit_time'],  
        'aws_request_id' : context.aws_request_id, 
        'log_group_name' : context.log_group_name, 
        'log_stream_name' : context.log_stream_name, 
        'server_info' : server_info, 
    }  


    s3.meta.client.put_object(Bucket=status_key[0], Key=status_key[1], 
                              Body=json.dumps(d))
    
    return d

if __name__ == "__main__":
    s3 = boto3.resource('s3')
    #s3.meta.client.download_file('ericmjonas-public', 'condaruntime.tar.gz', '/tmp/condaruntime.tar.gz')
    res = s3.meta.client.get_object(Bucket='ericmjonas-public', Key='condaruntime.tar.gz')

    condatar = tarfile.open(mode= "r:gz", 
                            fileobj = WrappedStreamingBody(res['Body'], res['ContentLength']))
    condatar.extractall('/tmp/test1/')
