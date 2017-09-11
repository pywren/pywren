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
from threading import Thread

import boto3
import botocore

if sys.version_info > (3, 0):
    from queue import Queue, Empty # pylint: disable=import-error
    from . import wrenutil # pylint: disable=relative-import
    from . import version  # pylint: disable=relative-import

else:
    from Queue import Queue, Empty # pylint: disable=import-error
    import wrenutil # pylint: disable=relative-import
    import version  # pylint: disable=relative-import

PYTHON_MODULE_PATH = "/tmp/pymodules"
CONDA_RUNTIME_DIR = "/tmp/condaruntime"
RUNTIME_LOC = "/tmp/runtimes"
JOBRUNNER_CONFIG_FILENAME = "/tmp/jobrunner.config.json"
JOBRUNNER_STATS_FILENAME = "/tmp/jobrunner.stats.txt"

logger = logging.getLogger(__name__)

PROCESS_STDOUT_SLEEP_SECS = 2

def get_key_size(s3client, bucket, key):
    try:
        a = s3client.head_object(Bucket=bucket, Key=key)
        return a['ContentLength']
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return None
        else:
            raise e

def free_disk_space(dirname):
    """
    Returns the number of free bytes on the mount point containing DIRNAME
    """
    s = os.statvfs(dirname)
    return s.f_bsize * s.f_bavail

def download_runtime_if_necessary(s3_client, runtime_s3_bucket, runtime_s3_key):
    """
    Download the runtime if necessary

    return True if cached, False if not (download occured)

    """

    # get runtime etag
    runtime_meta = s3_client.head_object(Bucket=runtime_s3_bucket,
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

    res = s3_client.get_object(Bucket=runtime_s3_bucket,
                               Key=runtime_s3_key)

    try:

        condatar = tarfile.open(
            mode="r:gz",
            fileobj=wrenutil.WrappedStreamingBody(res['Body'],
                                                  res['ContentLength']))
        condatar.extractall(runtime_etag_dir)
    except (OSError, IOError) as e:
        # no difference, see https://stackoverflow.com/q/29347790/1073963
        # do the cleanup
        shutil.rmtree(runtime_etag_dir, True)
        if e.args[0] == 28:

            raise Exception("RUNTIME_TOO_BIG",
                            "Ran out of space when untarring runtime")
        else:
            raise Exception("RUNTIME_ERROR", str(e))
    except tarfile.ReadError as e:
        # do the cleanup
        shutil.rmtree(runtime_etag_dir, True)
        raise Exception("RUNTIME_READ_ERROR", str(e))
    except:
        shutil.rmtree(runtime_etag_dir, True)
        raise

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
    return generic_handler(event, context_dict)

def get_server_info():

    server_info = {'uname' : subprocess.check_output("uname -a", shell=True).decode("ascii")}
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

    response_status = {'exception': None}
    try:
        if event['storage_config']['storage_backend'] != 's3':
            raise NotImplementedError(("Using {} as storage backend is not supported " +
                                       "yet.").format(event['storage_config']['storage_backend']))
        s3_client = boto3.client("s3")
        s3_bucket = event['storage_config']['backend_config']['bucket']

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

        runtime_s3_bucket = event['runtime']['s3_bucket']
        runtime_s3_key = event['runtime']['s3_key']
        if event.get('runtime_url'):
            # NOTE(shivaram): Right now we only support S3 urls.
            runtime_s3_bucket_used, runtime_s3_key_used = wrenutil.split_s3_url(
                event['runtime_url'])
        else:
            runtime_s3_bucket_used = runtime_s3_bucket
            runtime_s3_key_used = runtime_s3_key

        job_max_runtime = event.get("job_max_runtime", 290) # default for lambda

        response_status['func_key'] = func_key
        response_status['data_key'] = data_key
        response_status['output_key'] = output_key
        response_status['status_key'] = status_key

        data_key_size = get_key_size(s3_client, s3_bucket, data_key)
        #logger.info("bucket=", s3_bucket, "key=", data_key,  "status: ", data_key_size, "bytes" )
        while data_key_size is None:
            logger.warning("WARNING COULD NOT GET FIRST KEY")

            data_key_size = get_key_size(s3_client, s3_bucket, data_key)
        if not event['use_cached_runtime']:
            subprocess.check_output("rm -Rf {}/*".format(RUNTIME_LOC), shell=True)


        free_disk_bytes = free_disk_space("/tmp")
        response_status['free_disk_bytes'] = free_disk_bytes

        response_status['runtime_s3_key_used'] = runtime_s3_key_used
        response_status['runtime_s3_bucket_used'] = runtime_s3_bucket_used

        runtime_cached = download_runtime_if_necessary(s3_client, runtime_s3_bucket_used,
                                                       runtime_s3_key_used)
        logger.info("Runtime ready, cached={}".format(runtime_cached))
        response_status['runtime_cached'] = runtime_cached

        cwd = os.getcwd()
        jobrunner_path = os.path.join(cwd, "jobrunner.py")

        extra_env = event.get('extra_env', {})
        extra_env['PYTHONPATH'] = "{}".format(os.getcwd())

        call_id = event['call_id']
        callset_id = event['callset_id']
        response_status['call_id'] = call_id
        response_status['callset_id'] = callset_id

        CONDA_PYTHON_PATH = "/tmp/condaruntime/bin"
        CONDA_PYTHON_RUNTIME = os.path.join(CONDA_PYTHON_PATH, "python")

        # pass a full json blob

        jobrunner_config = {'func_bucket' : s3_bucket,
                            'func_key' : func_key,
                            'data_bucket' : s3_bucket,
                            'data_key' : data_key,
                            'data_byte_range' : data_byte_range,
                            'python_module_path' : PYTHON_MODULE_PATH,
                            'output_bucket' : s3_bucket,
                            'output_key' : output_key,
                            'stats_filename' : JOBRUNNER_STATS_FILENAME}

        with open(JOBRUNNER_CONFIG_FILENAME, 'w') as jobrunner_fid:
            json.dump(jobrunner_config, jobrunner_fid)

        if os.path.exists(JOBRUNNER_STATS_FILENAME):
            os.remove(JOBRUNNER_STATS_FILENAME)

        cmdstr = "{} {} {}".format(CONDA_PYTHON_RUNTIME,
                                   jobrunner_path,
                                   JOBRUNNER_CONFIG_FILENAME)

        setup_time = time.time()
        response_status['setup_time'] = setup_time - start_time

        local_env = os.environ.copy()

        local_env["OMP_NUM_THREADS"] = "1"
        local_env.update(extra_env)

        local_env['PATH'] = "{}:{}".format(CONDA_PYTHON_PATH, local_env.get("PATH", ""))

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
                time.sleep(PROCESS_STDOUT_SLEEP_SECS)
            total_runtime = time.time() - start_time
            if total_runtime > job_max_runtime:
                logger.warning("Process exceeded maximum runtime of {} sec".format(job_max_runtime))
                # Send the signal to all the process groups
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                raise Exception("OUTATIME",
                                "Process executed for too long and was killed")


        logger.info("command execution finished")

        if os.path.exists(JOBRUNNER_STATS_FILENAME):
            with open(JOBRUNNER_STATS_FILENAME, 'r') as fid:
                for l in fid.readlines():
                    key, value = l.strip().split(" ")
                    float_value = float(value)
                    response_status[key] = float_value

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
        response_status['exception_traceback'] = traceback.format_exc()
    finally:
        # creating new client in case the client has not been created
        boto3.client("s3").put_object(Bucket=s3_bucket, Key=status_key,
                                      Body=json.dumps(response_status))
