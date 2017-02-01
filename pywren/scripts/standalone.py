#!/usr/bin/env python

import pywren
import boto3
import click
import shutil
import os
import json
import zipfile
import glob2
import io
import time 
import botocore


SOURCE_DIR = os.path.dirname(os.path.abspath(__file__)) 

def process_message(m):
    event = json.loads(m.body)
    
    # run this in a thread: pywren.wrenhandler.generic_handler(event)
    
    # is thread done
    # add 10s to visibility 
    while True:
        response = message.change_visibility(
            VisibilityTimeout=100
        )
        for i in range(10):
            time.sleep(10)
            # check if thread is done
            
            # thread is done, delete the message
            m.delete()
        break
    
    # sleep 10s

    # 
    

def server_runner(config):

    AWS_REGION = config['account']['aws_region']
    SQS_QUEUE_NAME = config['standalone']['sqs_queue_name']

    sqs = boto3.resource('sqs', region_name=AWS_REGION)
    
    # Get the queue
    queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_NAME)

    while(True):
        print "reading queue" 
        response = queue.receive_messages(WaitTimeSeconds=10)

        if len(response) > 0:
            print "Dispatching"
            #pool.apply_async(
            process_message(response[0])
        else:
            print "no message, sleeping"
            time.sleep(1)


def server():
    config = pywren.wrenconfig.default()
    server_runner(config)
