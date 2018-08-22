#
# Copyright 2018 PyWren Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#!/usr/bin/env python

from __future__ import print_function

import json
import time
import logging
import math
import os
import shutil
import subprocess
import sys
import random

from threading import Thread

import boto3
import click
from glob2 import glob

try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

import watchtower

import pywren
from pywren import wrenhandler

logger = logging.getLogger(__name__)

# Creating cloudwatch logstreams is rate-limited so we have
# to jitter the startup a bit so we dont' hammer the
# cloudwatch endpoint when starting > 200 instances.
STARTUP_JITTER_SEC = 60


SQS_VISIBILITY_SEC = 10
PROCESS_SLEEP_DUR_SEC = 2
AWS_REGION_DEBUG = 'us-west-2'
QUEUE_SLEEP_DUR_SEC = 2
EXP_BACKOFF_FACTOR = 5

IDLE_TERMINATE_THRESHOLD = 0.95

INSTANCE_ID_URL = "http://169.254.169.254/latest/meta-data/instance-id"
def get_my_ec2_instance(aws_region):

    ec2 = boto3.resource('ec2', region_name=aws_region)

    instance_id = urlopen(INSTANCE_ID_URL).read()
    instances = ec2.instances.filter(InstanceIds=[instance_id])


    for instance in instances:
        return instance

def tags_to_dict(d):
    if d is None:
        return {}
    return {a['Key'] : a['Value'] for a in d}


def get_my_ec2_meta(instance):

    tags = tags_to_dict(instance.tags)

    r = {'public_dns_name' : instance.public_dns_name,
         'public_ip_address' : instance.public_ip_address,
         'instance_id': instance.id}
    r.update(tags)
    return r

def get_my_uptime():
    with open('/proc/uptime', 'r') as f:
        uptime_seconds = float(f.readline().split()[0])
        return uptime_seconds

def check_is_ec2():
    """
    last-minute check to make sure we are on EC2.

    """
    try:
        urlopen(INSTANCE_ID_URL, timeout=3).read()
        return True
    except:
        return False

def ec2_self_terminate(idle_time, uptime, message_count, in_minutes=0):
    if check_is_ec2():
        logger.info("self-terminating after idle for " + \
            "{:.0f} sec ({:.0f} s uptime), processed {:d} messages".format(
                idle_time, uptime, message_count))
        for h in logger.handlers:
            h.flush()

        subprocess.call("sudo shutdown -h +{:d}".format(in_minutes), shell=True) # slight delay
    else:
        logger.warning("attempted to self-terminate on non-EC2 instance. Check config")


def idle_granularity_valid(idle_terminate_granularity,
                           queue_receive_message_timeout):
    return ((1.0 - IDLE_TERMINATE_THRESHOLD)*idle_terminate_granularity >
            queue_receive_message_timeout * 1.1)

def server_runner(aws_region, sqs_queue_name,
                  max_run_time, run_dir,
                  max_idle_time=None,
                  idle_terminate_granularity=None,
                  queue_receive_message_timeout=10):
    """
    Extract messages from queue and pass them off
    """

    sqs = boto3.resource('sqs', region_name=aws_region)

    # Get the queue
    queue = sqs.get_queue_by_name(QueueName=sqs_queue_name)
    local_message_i = 0
    last_processed_timestamp = time.time()

    terminate_thold_sec = (IDLE_TERMINATE_THRESHOLD * idle_terminate_granularity)
    terminate_window_sec = idle_terminate_granularity - terminate_thold_sec
    queue_receive_message_timeout = min(math.floor(terminate_window_sec/1.2),
                                        queue_receive_message_timeout)
    queue_receive_message_timeout = int(max(queue_receive_message_timeout, 1))
    if not idle_granularity_valid(idle_terminate_granularity, queue_receive_message_timeout):
        raise Exception("Idle time granularity window smaller than queue receive " + \
                        "message timeout with headroom, instance will not self-terminate")
    message_count = 0
    idle_time = 0
    while True:
        logger.debug("reading queue")
        response = queue.receive_messages(WaitTimeSeconds=queue_receive_message_timeout)
        if len(response) > 0:
            m = response[0]
            logger.info("Dispatching message_id={}".format(m.message_id))

            process_message(m, local_message_i, max_run_time, run_dir)
            message_count += 1
            last_processed_timestamp = time.time()
            idle_time = 0
        else:
            idle_time = time.time() - last_processed_timestamp

            logger.debug("no message, idle for {:3.0f} sec".format(idle_time))

        # this is EC2_only
        if max_idle_time is not None and \
           idle_terminate_granularity is not None:
            if idle_time > max_idle_time:
                my_uptime = get_my_uptime()
                time_frac = (my_uptime % idle_terminate_granularity)

                logger.debug("Instance has been up for " + \
                    "{:.0f} and inactive for {:.0f} time_frac={:.0f} terminate_thold={:.0f}".format(
                        my_uptime, idle_time, time_frac, terminate_thold_sec))

                if time_frac > terminate_thold_sec:
                    logger.info("Instance has been up for {:.0f}"
                                "and inactive for {:.0f}, terminating".format(my_uptime,
                                                                              idle_time))
                    ec2_self_terminate(idle_time, my_uptime,
                                       message_count, in_minutes=1)

                    sys.exit(0)

def process_message(m, local_message_i, max_run_time, run_dir):

    event = json.loads(m.body)

    call_id = event['call_id']
    callset_id = event['callset_id']

    extra_env_debug = event.get('extra_env', {})

    logger.info("processing message_id={} "
                "callset_id={} call_id={}".format(m.message_id, callset_id,
                                                  call_id))

    # FIXME this is all for debugging
    if 'DEBUG_THROW_EXCEPTION' in extra_env_debug:
        m.delete()
        raise Exception("Debug exception")
    message_id = m.message_id

    # id this in a thread: pywren.wrenhandler.generic_handler(event)
    p = Thread(target=job_handler, args=(event, local_message_i,
                                         run_dir))
    # is thread done
    p.start()

    start_time = time.time()

    run_time = time.time() - start_time
    last_visibility_update_time = time.time()
    while run_time < max_run_time:
        time_since_visibility_update = time.time() - last_visibility_update_time
        est_visibility_left = SQS_VISIBILITY_SEC - time_since_visibility_update
        if est_visibility_left < (PROCESS_SLEEP_DUR_SEC*1.5):
            logger.debug("{} - {:3.1f}s since last visibility update, "
                         "setting to {:3.1f} sec".format(message_id,
                                                         time_since_visibility_update,
                                                         SQS_VISIBILITY_SEC))
            last_visibility_update_time = time.time()
            _ = m.change_visibility(VisibilityTimeout=SQS_VISIBILITY_SEC)


        if not p.is_alive():
            logger.debug("{} - attempting to join process".format(message_id))
            # FIXME will this join ever hang?
            p.join()
            break
        else:
            logger.debug("{} - {:3.1f}s since visibility update, "
                         "sleeping".format(message_id, time_since_visibility_update))
            time.sleep(PROCESS_SLEEP_DUR_SEC)

        run_time = time.time() - start_time

    logger.info("deleting message_id={} "
                "callset_id={} call_id={}".format(m.message_id, callset_id, call_id))


    m.delete()

def copy_runtime(tgt_dir):
    files = glob(os.path.join(pywren.SOURCE_DIR, "jobrunner/*.py"))
    for f in files:
        shutil.copy(f, os.path.join(tgt_dir, os.path.basename(f)))

def job_handler(event, job_i, run_dir,
                extra_context=None,
                delete_taskdir=True):
    """
    Run a deserialized job in run_dir

    Just for debugging
    """

    debug_pid = open("/tmp/pywren.scripts.standalone.{}.{}.log".format(os.getpid(),
                                                                       time.time()), 'w')

    call_id = event['call_id']
    callset_id = event['callset_id']
    logger.info("jobhandler_thread callset_id={} call_id={}".format(callset_id, call_id))

    original_dir = os.getcwd()

    task_run_dir = os.path.join(run_dir, str(job_i))
    shutil.rmtree(task_run_dir, True) # delete old modules
    os.makedirs(task_run_dir)
    copy_runtime(task_run_dir)


    context = {'jobnum' : job_i}
    if extra_context is not None:
        context.update(extra_context)

    os.chdir(task_run_dir)

    try:
        debug_pid.write("invoking generic_handler\n")
        logger.debug("jobhandler_thread callset_id={} call_id={} invoking".format(callset_id,
                                                                                  call_id))

        wrenhandler.generic_handler(event, context)
    except Exception as e:
        logger.warning("jobhandler_thread callset_id={} "
                       "call_id={} exception={}".format(callset_id,
                                                        call_id, str(e)))

    finally:
        debug_pid.write("generic handler finally\n")

        if delete_taskdir:
            shutil.rmtree(task_run_dir)
        os.chdir(original_dir)

    debug_pid.write("done and returning\n")
    logger.debug("jobhandler_thread callset_id={} call_id={} returning".format(callset_id, call_id))




@click.command()
@click.option('--max_run_time', default=3600,
              help='max run time for a job', type=int)
@click.option('--run_dir', default="/tmp/pywren.rundir",
              help='directory to hold intermediate output')
@click.option('--aws_region', default="us-west-2",
              help='aws region')
@click.option('--sqs_queue_name', default="pywren-queue",
              help='queue')
@click.option('--max_idle_time', default=None, type=int,
              help='maximum time for queue to remine idle before we try to self-terminate (sec)')
@click.option('--idle_terminate_granularity', default=None, type=int,
              help="only terminate if we have been up for an integral number of this")
@click.option('--queue_receive_message_timeout', default=10, type=int,
              help="longpoll timeout for getting sqs messages")
def server(aws_region, max_run_time, run_dir, sqs_queue_name, max_idle_time,
           idle_terminate_granularity, queue_receive_message_timeout):


    rand_sleep = random.random() * STARTUP_JITTER_SEC
    time.sleep(rand_sleep)

    session = boto3.session.Session(region_name=aws_region)
    # make boto quiet locally FIXME is there a better way of doing this?
    logging.getLogger('boto').setLevel(logging.CRITICAL)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)

    def async_log_setup():
        ''' None of this stuff should be on the critical path to launching an
            * instance. Instances should start dequeuing from SQS queue as soon
            * as possible and shouldn't have to wait for rest of spot cluster
            * to come up so they have a valid ec2_metadata['Name']
            * If there are any exceptions in this function,
            * we should exponentially backoff and try again until we succeed,
            * this is critical because if this doesn't happen we end up
            * clogging all EC2 resources
            * This function is called once per pywren executor process
        '''
        success = False
        backoff_time = EXP_BACKOFF_FACTOR
        while (not success):
            try:
                time.sleep(backoff_time)
                instance = get_my_ec2_instance(aws_region)
                ec2_metadata = get_my_ec2_meta(instance)
                server_name = ec2_metadata['Name']
                log_stream_prefix = ec2_metadata['instance_id']

                log_format_str = '{} %(asctime)s - %(name)s- %(levelname)s - %(message)s'\
                                 .format(server_name)

                formatter = logging.Formatter(log_format_str, "%Y-%m-%d %H:%M:%S")
                stream_name = log_stream_prefix + "-{logger_name}"
                handler = watchtower.CloudWatchLogHandler(send_interval=20,
                                                          log_group="pywren.standalone",
                                                          stream_name=stream_name,
                                                          boto3_session=session,
                                                          max_batch_count=10)

                debug_stream_handler = logging.StreamHandler()
                handler.setFormatter(formatter)
                logger.addHandler(handler)
                logger.setLevel(logging.DEBUG)
                wren_log = pywren.wrenhandler.logger
                wren_log.addHandler(handler)
                wren_log.addHandler(debug_stream_handler)
                success = True
            except Exception as e:
                logger.error('Logging setup error: '+ str(e))


                backoff_time *= 2

    log_setup = Thread(target=async_log_setup)
    log_setup.start()
    pid = os.getpid()
    run_dir = run_dir + "_" + str(pid)

    #config = pywren.wrenconfig.default()
    server_runner(aws_region, sqs_queue_name,
                  max_run_time, os.path.abspath(run_dir),
                  #server_name, log_stream_prefix,
                  max_idle_time,
                  idle_terminate_granularity,
                  queue_receive_message_timeout)
