from six.moves import cPickle as pickle
import boto3
import tarfile
import subprocess
import os
import time
import base64
import logging
import uuid
import json
import shutil
import sys
from threading import Thread
import signal
import random

if (sys.version_info > (3, 0)):
    from . import wrenutil
    from . import s3util
    from . import version
    from . import wrenconfig
    from queue import Queue, Empty

else:
    import wrenutil
    import s3util
    import version
    import wrenconfig
    from Queue import Queue, Empty


PYTHON_MODULE_PATH = "/tmp/pymodules"
CONDA_RUNTIME_DIR = "/tmp/condaruntime"
RUNTIME_LOC = "/tmp/runtimes"
logger = logging.getLogger(__name__)

PROCESS_STDOUT_SLEEP_SECS = 2

def key_size(bucket, key):
    try:

        s3 = boto3.resource('s3')
        a = s3.meta.client.head_object(Bucket=bucket, Key=key)
        return a['ContentLength']
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return None
        else:
            raise e


def download_runtime_if_necessary(s3conn, runtime_s3_bucket, runtime_s3_key):
    """
    Download the runtime if necessary

    return True if cached, False if not (download occured)

    """

    # get runtime etag
    runtime_meta = s3conn.meta.client.head_object(Bucket=runtime_s3_bucket, 
                                                  Key=runtime_s3_key)
    # etags have strings (double quotes) on each end, so we strip those
    ETag = str(runtime_meta['ETag'])[1:-1]
    logger.debug("The etag is ={}".format(ETag))
    runtime_etag_dir = os.path.join(RUNTIME_LOC, ETag)
    logger.debug("Runtime etag dir={}".format(runtime_etag_dir))
    expected_target = os.path.join(runtime_etag_dir, 'condaruntime')    
    logger.debug("Expected target={}".format(expected_target))
    # check if dir is linked to correct runtime
    if os.path.exists(RUNTIME_LOC):
        if os.path.exists(CONDA_RUNTIME_DIR) :
            if not os.path.islink(CONDA_RUNTIME_DIR):
                raise Exception("{} is not a symbolic link, your runtime config is broken".format(CONDA_RUNTIME_DIR))

            existing_link = os.readlink(CONDA_RUNTIME_DIR)
            if existing_link == expected_target:
                logger.debug("found existing {}, not re-downloading".format(ETag))
                return True

    logger.debug("{} not cached, downloading".format(ETag))
    # didn't cache, so we start over
    if os.path.islink(CONDA_RUNTIME_DIR):
        os.unlink(CONDA_RUNTIME_DIR)

    shutil.rmtree(RUNTIME_LOC, True)
    
    os.makedirs(runtime_etag_dir)
    
    res = s3conn.meta.client.get_object(Bucket=runtime_s3_bucket, 
                                    Key=runtime_s3_key)

    condatar = tarfile.open(mode= "r:gz", 
                            fileobj = wrenutil.WrappedStreamingBody(res['Body'], 
                                                                    res['ContentLength']))


    condatar.extractall(runtime_etag_dir)

    # final operation 
    os.symlink(expected_target, CONDA_RUNTIME_DIR)
    return False

def aws_lambda_handler(event, context):
    logger.setLevel(logging.INFO)
    context_dict = {
        'aws_request_id' : context.aws_request_id, 
        'log_group_name' : context.log_group_name, 
        'log_stream_name' : context.log_stream_name, 
    }
    return generic_handler(event, context_dict)

def get_server_info():

    server_info = {'uname' : subprocess.check_output("uname -a", shell=True).decode("ascii") }
    if os.path.exists("/proc"):
        server_info.update({'/proc/cpuinfo': open("/proc/cpuinfo", 'r').read(), 
                            '/proc/meminfo': open("/proc/meminfo", 'r').read(), 
                            '/proc/self/cgroup': open("/proc/meminfo", 'r').read(), 
                            '/proc/cgroups': open("/proc/cgroups", 'r').read()})


    return server_info

def generic_handler(event, context_dict):
    """
    context_dict is generic infromation about the context
    that we are running in, provided by the scheduler
    """

    try:
        response_status = {'exception' : None}
        s3 = boto3.resource('s3')
        
        logger.info("invocation started")

        # download the input 
        status_key = event['status_key']
        func_key = event['func_key']
        data_key = event['data_key']
        data_byte_range = event['data_byte_range']
        output_key = event['output_key']

        if version.__version__ != event['pywren_version']:
            raise Exception("WRONGVERSION", "Pywren version mismatch", 
                            version.__version__, event['pywren_version'])

        start_time = time.time()
        response_status['start_time'] = start_time

        func_filename = "/tmp/func.pickle"
        data_filename = "/tmp/data.pickle"
        output_filename = "/tmp/output.pickle"

        runtime_s3_bucket = event['runtime_s3_bucket']
        runtime_s3_key = event['runtime_s3_key']
        if event.get('shard_runtime_key', False):
            random.seed()
            shard = random.randrange(wrenconfig.MAX_S3_RUNTIME_SHARDS)
            key_shard = wrenutil.get_s3_shard(runtime_s3_key, shard)
            runtime_s3_key_used = wrenutil.hash_s3_key(key_shard)
        else:
            runtime_s3_key_used = runtime_s3_key

        job_max_runtime = event.get("job_max_runtime", 290) # default for lambda

        response_status['func_key'] = func_key
        response_status['data_key'] = data_key 
        response_status['output_key'] = output_key
        response_status['status_key'] = status_key 

        b, k = data_key
        KS =  s3util.key_size(b, k)
        #logger.info("bucket=", b, "key=", k,  "status: ", KS, "bytes" )
        while KS is None:
            logger.warn("WARNING COULD NOT GET FIRST KEY" )

            KS =  s3util.key_size(b, k)
        if not event['use_cached_runtime'] :
            subprocess.check_output("rm -Rf {}/*".format(RUNTIME_LOC), shell=True)




        # get the input and save to disk 
        # FIXME here is we where we would attach the "canceled" metadata
        s3.meta.client.download_file(func_key[0], func_key[1], func_filename)
        func_download_time = time.time() - start_time
        response_status['func_download_time'] = func_download_time

        logger.info("func download complete, took {:3.2f} sec".format(func_download_time))

        if data_byte_range is None:
            s3.meta.client.download_file(data_key[0], data_key[1], data_filename)
        else:
            range_str = 'bytes={}-{}'.format(*data_byte_range)
            dres = s3.meta.client.get_object(Bucket=data_key[0], Key=data_key[1], 
                                             Range=range_str)
            data_fid = open(data_filename, 'wb')
            data_fid.write(dres['Body'].read())
            data_fid.close()

        data_download_time = time.time() - start_time
        logger.info("data data download complete, took {:3.2f} sec".format(data_download_time))
        response_status['data_download_time'] = data_download_time

        # now split
        d = json.load(open(func_filename, 'r'))
        shutil.rmtree(PYTHON_MODULE_PATH, True) # delete old modules
        os.mkdir(PYTHON_MODULE_PATH)
        # get modules and save
        for m_filename, m_text in d['module_data'].items():
            m_path = os.path.dirname(m_filename)

            if len(m_path) > 0 and m_path[0] == "/":
                m_path = m_path[1:]
            to_make = os.path.join(PYTHON_MODULE_PATH, m_path)
            #print "to_make=", to_make, "m_path=", m_path
            try:
                os.makedirs(to_make)
            except OSError as e:
                if e.errno == 17:
                    pass
                else:
                    raise e
            full_filename = os.path.join(to_make, os.path.basename(m_filename))
            #print "creating", full_filename
            fid = open(full_filename, 'wb')
            fid.write(m_text.encode('utf-8'))
            fid.close()
        logger.info("Finished writing {} module files".format(len(d['module_data'])))
        logger.debug(subprocess.check_output("find {}".format(PYTHON_MODULE_PATH), shell=True))
        logger.debug(subprocess.check_output("find {}".format(os.getcwd()), shell=True))

        response_status['runtime_s3_key_used'] = runtime_s3_key_used
        
        runtime_cached = download_runtime_if_necessary(s3, runtime_s3_bucket, 
                                                       runtime_s3_key_used)
        logger.info("Runtime ready, cached={}".format(runtime_cached))
        response_status['runtime_cached'] = runtime_cached

        cwd = os.getcwd()
        jobrunner_path = os.path.join(cwd, "jobrunner.py")

        extra_env = event.get('extra_env', {})
        extra_env['PYTHONPATH'] = "{}:{}".format(os.getcwd(), PYTHON_MODULE_PATH)

        call_id = event['call_id']
        callset_id = event['callset_id']
        response_status['call_id'] = call_id
        response_status['callset_id'] = callset_id

        CONDA_PYTHON_RUNTIME = "/tmp/condaruntime/bin/python"

        cmdstr = "{} {} {} {} {}".format(CONDA_PYTHON_RUNTIME, 
                                         jobrunner_path, 
                                         func_filename, 
                                         data_filename, 
                                         output_filename)

        setup_time = time.time()
        response_status['setup_time'] = setup_time - start_time

        local_env = os.environ.copy()

        local_env["OMP_NUM_THREADS"] = "1"
        local_env.update(extra_env)

        logger.debug("command str=%s", cmdstr)
        # This is copied from http://stackoverflow.com/a/17698359/4577954
        # reasons for setting process group: http://stackoverflow.com/a/4791612
        process = subprocess.Popen(cmdstr, shell=True, env=local_env, bufsize=1, 
                                   stdout=subprocess.PIPE, preexec_fn=os.setsid)

        logger.info("launched process")
        def consume_stdout(stdout, queue):
            with stdout:
                for line in iter(stdout.readline, b''):
                    queue.put(line)

        q = Queue()

        t = Thread(target=consume_stdout, args=(process.stdout, q))
        t.daemon = True
        t.start()

        stdout = b""
        while t.isAlive():
            try: 
                line = q.get_nowait()
                stdout += line
                logger.info(line)
            except Empty:
                time.sleep(PROCESS_STDOUT_SLEEP_SECS )
            total_runtime = time.time() - start_time
            if total_runtime > job_max_runtime:
                logger.warn("Process exceeded maximum runtime of {} sec".format(job_max_runtime))
                # Send the signal to all the process groups
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)  
                raise Exception("OUTATIME", 
                                "Process executed for too long and was killed")


        logger.info("command execution finished")

        s3.meta.client.upload_file(output_filename, output_key[0], 
                                   output_key[1])
        logger.debug("output uploaded to %s %s", output_key[0], output_key[1])

        end_time = time.time()

        response_status['stdout'] = stdout.decode("ascii")


        response_status['exec_time'] = time.time() - setup_time
        response_status['end_time'] = end_time

        response_status['host_submit_time'] = event['host_submit_time']
        response_status['server_info'] = get_server_info()

        response_status.update(context_dict) 
    except Exception as e:
        # internal runtime exceptions
        response_status['exception'] = str(e)
        response_status['exception_args'] = e.args
    finally:

        s3.meta.client.put_object(Bucket=status_key[0], Key=status_key[1], 
                                  Body=json.dumps(response_status))
    

if __name__ == "__main__":
    s3 = boto3.resource('s3')
    #s3.meta.client.download_file('ericmjonas-public', 'condaruntime.tar.gz', '/tmp/condaruntime.tar.gz')
    res = s3.meta.client.get_object(Bucket='ericmjonas-public', Key='condaruntime.tar.gz')

    condatar = tarfile.open(mode= "r:gz", 
                            fileobj = WrappedStreamingBody(res['Body'], res['ContentLength']))
    condatar.extractall('/tmp/test1/')
