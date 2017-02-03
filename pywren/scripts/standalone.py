#!/usr/bin/env python

import pywren
import boto3
import click
import shutil
import os
import json
import zipfile
from glob2 import glob
import io
import time 
import botocore
import boto
from multiprocess import Process
from pywren import wrenhandler



SQS_VISIBILITY_INCREMENT_SEC = 10
SLEEP_DUR_SEC=2

def get_my_uptime():

    ec2 = boto3.resource('ec2') # , region_name=AWS_REGION)

    instance_id =  boto.utils.get_instance_metadata()['instance-id']
    instances = ec2.instances.filter(InstanceIds=[instance_id])


    for instance in instances:
        launch_time = instance.launch_time
        time_delta =  datetime.datetime.now(launch_time.tzinfo) - launch_time
        print launch_time, time_delta
        #hour_frac = (time_delta.total_seconds() % 3600) / 3600

        return time_delta.total_seconds()

def server_runner(config, max_run_time, run_dir):
    """
    Extract messages from queue and pass them off
    """
    AWS_REGION = config['account']['aws_region']
    SQS_QUEUE_NAME = config['standalone']['sqs_queue_name']

    sqs = boto3.resource('sqs', region_name=AWS_REGION)
    
    # Get the queue
    queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_NAME)
    local_message_i = 0

    while(True):
        print "reading queue" 
        response = queue.receive_messages(WaitTimeSeconds=10)
        if len(response) > 0:
            print "Dispatching"
            m = response[0]
            
            process_message(m, local_message_i, max_run_time, run_dir)
        else:
            print "no message, sleeping"
            time.sleep(1)

def process_message(m, local_message_i, max_run_time, run_dir):
    event = json.loads(m.body)
    
    # run this in a thread: pywren.wrenhandler.generic_handler(event)
    p =  Process(target=job_handler, args=(event, local_message_i, run_dir))
    # is thread done
    p.start()
    start_time = time.time()

    response = m.change_visibility(
        VisibilityTimeout=SQS_VISIBILITY_INCREMENT_SEC)

    # add 10s to visibility 
    run_time = time.time() - start_time
    last_visibility_update_time = time.time()
    while run_time < max_run_time:
        if (time.time() - last_visibility_update_time) > (SQS_VISIBILITY_INCREMENT_SEC*0.9):
            response = m.change_visibility(
                VisibilityTimeout=SQS_VISIBILITY_INCREMENT_SEC)
            last_visibility_update_time = time.time()

        if p.exitcode is not None:
            print "attempting to join"
            # FIXME will this join ever hang? 
            p.join()
            break
        else:
            print "sleeping"
            time.sleep(SLEEP_DUR_SEC)

        run_time = time.time() - start_time

    if p.exitcode is None:
        p.terminate()  # PRINT LOTS OF ERRORS HERE

    m.delete()

def copy_runtime(tgt_dir):
    files = glob(os.path.join(pywren.SOURCE_DIR, "./*.py"))
    for f in files:
        shutil.copy(f, os.path.join(tgt_dir, os.path.basename(f)))

def job_handler(job, job_i, run_dir, extra_context = None, 
                  delete_taskdir=True):
    """
    Run a deserialized job in run_dir

    Just for debugging
    """

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
        wrenhandler.generic_handler(job, context)
    finally:
        if delete_taskdir:
            shutil.rmtree(task_run_dir)
        os.chdir(original_dir)




@click.command()
@click.option('--max_run_time', default=3600, 
              help='max run time for a job', type=int)
@click.option('--run_dir', default="/tmp/pywren.rundir", 
              help='directory to hold intermediate output')
def server(max_run_time, run_dir):
    config = pywren.wrenconfig.default()
    server_runner(config, max_run_time, os.path.abspath(run_dir))

