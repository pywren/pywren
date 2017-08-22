import base64
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import traceback
import platform

from threading import Thread

if sys.version_info > (3, 0):
    from queue import Queue, Empty # pylint: disable=import-error
    from . import wrenutil # pylint: disable=relative-import
    from . import version  # pylint: disable=relative-import

else:
    from Queue import Queue, Empty # pylint: disable=import-error
    import wrenutil # pylint: disable=relative-import
    import version  # pylint: disable=relative-import
    from storage.storage import Storage

if sys.platform == 'win32':
    TEMP = "D:\local\Temp"
    PATH_DELIMETER = ";"

else:
    TEMP = "/tmp"
    PATH_DELIMETER = ":"
    import boto3
    import botocore


PYTHON_MODULE_PATH = os.path.join(TEMP, "pymodules")
CONDA_RUNTIME_DIR = os.path.join(TEMP, "condaruntime")
RUNTIME_LOC = os.path.join(TEMP, "runtimes")

logger = logging.getLogger(__name__)

PROCESS_STDOUT_SLEEP_SECS = 2


def get_key_size(storage_client, key):
    try:
        a = storage_client.head_object(key)
        return a['ContentLength']
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return None
        else:
            raise e

def download_runtime_if_necessary(runtime_bucket, runtime_key):
    """
    Download the runtime if necessary

    return True if cached, False if not (download occured)

    """
    s3_client = boto3.client("s3")

    # get runtime etag
    runtime_meta = s3_client.head_object(Bucket=runtime_bucket,
                                         Key=runtime_key)
    # etags have strings (double quotes) on each end, so we strip those
    ETag = str(runtime_meta['ETag'])[1:-1]
    logger.debug("The etag is ={}".format(ETag))
    runtime_etag_dir = os.path.join(RUNTIME_LOC, ETag)
    logger.debug("Runtime etag dir={}".format(runtime_etag_dir))
    expected_target = os.path.join(runtime_etag_dir, 'condaruntime')
    logger.debug("Expected target={}".format(expected_target))
    # check if dir is linked to correct runtime
    if os.path.exists(RUNTIME_LOC):
        if os.path.exists(CONDA_RUNTIME_DIR):
            if not os.path.islink(CONDA_RUNTIME_DIR):
                raise Exception("{} is not a symbolic link, your runtime config is broken".format(
                    CONDA_RUNTIME_DIR))

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

    res = s3_client.get_object(Bucket=runtime_bucket,
                               Key=runtime_key)

    condatar = tarfile.open(
        mode="r:gz",
        fileobj=wrenutil.WrappedStreamingBody(res['Body'], res['ContentLength']))

    condatar.extractall(runtime_etag_dir)

    # final operation
    os.symlink(expected_target, CONDA_RUNTIME_DIR)
    return False


def b64str_to_bytes(str_data):
    str_ascii = str_data.encode('ascii')
    byte_data = base64.b64decode(str_ascii)
    return byte_data


def aws_lambda_handler(event, context):
    logger.setLevel(logging.INFO)
    context_dict = {
        'aws_request_id' : context.aws_request_id,
        'log_group_name' : context.log_group_name,
        'log_stream_name' : context.log_stream_name,
    }

    #construct storage handler based on s3 backend
    backend_config = {
        'bucket': event['storage_config']['backend_config']['bucket']
    }

    storage_config = {
        'storage_prefix' : event['storage_config']['storage_prefix'],
        'storage_backend' : 's3',
        'backend_config': backend_config
    }

    storage_handler = Storage(storage_config)
    return generic_handler(event, context_dict, storage_handler)

def get_server_info():

    server_info = {'uname' : " ".join(platform.uname()),
                   'cpuinfo': platform.processor()}

    if os.path.exists("/proc"):
        server_info.update({'/proc/meminfo': open("/proc/meminfo", 'r').read(),
                            '/proc/self/cgroup': open("/proc/meminfo", 'r').read(),
                            '/proc/cgroups': open("/proc/cgroups", 'r').read()})

    return server_info

def generic_handler(event, context_dict, storage_client):
    """
    context_dict is generic infromation about the context
    that we are running in, provided by the scheduler
    """

    response_status = {'exception': None}
    try:
        storage_backend = event['storage_config']['storage_backend']
        if storage_backend != 's3':
            raise NotImplementedError(("Using {} as storage backend is not supported " +
                                       "yet.").format(event['storage_config']['storage_backend']))
#        s3_transfer = boto3.s3.transfer.S3Transfer(s3_client)

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

        func_filename = os.path.join(TEMP, "func.pickle")
        data_filename = os.path.join(TEMP, "data.pickle")
        output_filename = os.path.join(TEMP, "output.pickle")

        if storage_backend == 's3':
            runtime_bucket = event['runtime']['s3_bucket']
            runtime_key = event['runtime']['s3_key']
        if event.get('runtime_url'):
            # NOTE(shivaram): Right now we only support S3 urls.
            runtime_bucket_used, runtime_key_used = wrenutil.split_s3_url(
                event['runtime_url'])
        else:
            runtime_bucket_used = runtime_bucket
            runtime_key_used = runtime_key

        job_max_runtime = event.get("job_max_runtime", 290) # default for lambda

        response_status['func_key'] = func_key
        response_status['data_key'] = data_key
        response_status['output_key'] = output_key
        response_status['status_key'] = status_key

        KS = get_key_size(storage_client, data_key)

        while KS is None:
            logger.warning("WARNING COULD NOT GET FIRST KEY")

            KS = get_key_size(storage_client, data_key)
        if not event['use_cached_runtime']:
            shutil.rmtree(RUNTIME_LOC, True)
            os.mkdir(RUNTIME_LOC)

        # get the input and save to disk
        # FIXME here is we where we would attach the "canceled" metadata
        
#        s3_transfer.download_file(s3_bucket, func_key, func_filename)
        func_data = storage_client.get_object(func_key)
        with open(func_filename, 'wb') as f:
            f.write(func_data)
        func_download_time = time.time() - start_time
        response_status['func_download_time'] = func_download_time

        logger.info("func download complete, took {:3.2f} sec".format(func_download_time))

        with open(data_filename, 'wb') as data_fid:
            data_data = storage_client.get_object(data_key, data_byte_range)
            data_fid.write(data_data)

        data_download_time = time.time() - start_time
        logger.info("data download complete, took {:3.2f} sec".format(data_download_time))
        response_status['data_download_time'] = data_download_time

        # now split
        d = json.load(open(func_filename, 'r'))
        shutil.rmtree(PYTHON_MODULE_PATH, True) # delete old modules
        os.mkdir(PYTHON_MODULE_PATH)
        # get modules and save
        for m_filename, m_data in d['module_data'].items():
            m_path = os.path.dirname(m_filename)

            if len(m_path) > 0 and m_path[0] == "/":
                m_path = m_path[1:]

            if sys.platform == 'win32':
                #change backslash to forward slash. 
                m_path = os.path.join(*filter(lambda x: len(x) > 0, m_path.split("/")))

            to_make = os.path.join(PYTHON_MODULE_PATH, m_path)

            try:
                os.makedirs(to_make)
            except OSError as e:
                if e.errno == 17:
                    pass
                else:
                    raise e
            full_filename = os.path.join(to_make, os.path.basename(m_filename))
            fid = open(full_filename, 'wb')
            fid.write(b64str_to_bytes(m_data))
            fid.close()

        logger.info("Finished writing {} module files".format(len(d['module_data'])))
        response_status['runtime_key_used'] = runtime_key_used
        response_status['runtime_bucket_used'] = runtime_bucket_used

        runtime_cached = download_runtime_if_necessary(runtime_bucket_used,
                                                       runtime_key_used)
        logger.info("Runtime ready, cached={}".format(runtime_cached))
        response_status['runtime_cached'] = runtime_cached

        cwd = os.getcwd()
        jobrunner_path = os.path.join(cwd, "jobrunner.py")

        extra_env = event.get('extra_env', {})
        extra_env['PYTHONPATH'] = "{}{}{}".format(os.getcwd(), PATH_DELIMETER, PYTHON_MODULE_PATH)

        call_id = event['call_id']
        callset_id = event['callset_id']
        response_status['call_id'] = call_id
        response_status['callset_id'] = callset_id

        CONDA_PYTHON_PATH = os.path.join(CONDA_RUNTIME_DIR, "bin")
        CONDA_PYTHON_RUNTIME = os.path.join(CONDA_PYTHON_PATH, "python")

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

        local_env['PATH'] = "{}{}{}".format(CONDA_PYTHON_PATH, PATH_DELIMETER, local_env.get("PATH", ""))

        logger.debug("command str=%s", cmdstr)
        # This is copied from http://stackoverflow.com/a/17698359/4577954
        # reasons for setting process group: http://stackoverflow.com/a/4791612

        # os.setsid doesn't work in windows
        if sys.platform == 'win32':
           preexec = None
        else:
            preexec = os.setsid
        process = subprocess.Popen(cmdstr, shell=True, env=local_env, bufsize=1,
                                   stdout=subprocess.PIPE, preexec_fn=preexec)

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
                time.sleep(PROCESS_STDOUT_SLEEP_SECS)
            total_runtime = time.time() - start_time
            if total_runtime > job_max_runtime:
                logger.warning("Process exceeded maximum runtime of {} sec".format(job_max_runtime))
                # Send the signal to all the process groups
                if sys.platform.startswith('linux'):
                    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                else:
                    pass
                raise Exception("OUTATIME",
                                "Process executed for too long and was killed")


        logger.info("command execution finished")

#        s3_transfer.upload_file(output_filename, s3_bucket,
#                                output_key)
        output_d = open(output_filename).read()
        storage_client.put_data(output_key, output_d)
        logger.debug("output uploaded to %s %s", storage_client.storage_config['backend_config']['bucket'], output_key)

        end_time = time.time()
        response_status['stdout'] = stdout.decode("ascii")
        response_status['exec_time'] = time.time() - setup_time
        response_status['end_time'] = end_time
        response_status['host_submit_time'] = event['host_submit_time']

        response_status.update(context_dict)
    except Exception as e:
        # internal runtime exceptions
        response_status['exception'] = str(e)
        response_status['exception_args'] = e.args
        response_status['server_info'] = get_server_info()
        response_status['exception_traceback'] = traceback.format_exc()
    finally:
        storage_client.put_data(status_key, json.dumps(response_status))


if __name__ == "__main__":
    s3 = boto3.resource('s3')
    # s3.meta.client.download_file('ericmjonas-public', 'condaruntime.tar.gz',
    #                              '/tmp/condaruntime.tar.gz')
    s3_res = s3.meta.client.get_object(Bucket='ericmjonas-public', Key='condaruntime.tar.gz')

    condatar_test = tarfile.open(
        mode="r:gz",
        fileobj=wrenutil.WrappedStreamingBody(s3_res['Body'], s3_res['ContentLength']))
    condatar_test.extractall('/tmp/test1/')
