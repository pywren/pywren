
from fabric.api import local, env, run, put, cd, task, sudo, get, settings, warn_only, lcd
from fabric.contrib import project
import boto3
import cloudpickle
import json
import base64
import cPickle as pickle
from pywren.wrenconfig import * 
import time

"""
conda notes

be sure to call conda clean --all before compressing



"""

env.roledefs['m'] = ['jonas@c65']


@task
def create_zip():
    with lcd("pywren"):
        
        local("zip ../deploy.zip *.py")


@task
def get_condaruntime():
    pass

@task
def put_condaruntime():
    local("scp -r c65:/data/jonas/chicken/condaruntime.tar.gz .")
    #local("tar czvf condaruntime.tar.gz condaruntime")
    local("aws s3 cp condaruntime.tar.gz s3://ericmjonas-public/condaruntime.tar.gz")



@task 
def create_function():
    lambclient = boto3.client('lambda', region_name=AWS_REGION)

    lambclient.create_function(FunctionName = FUNCTION_NAME, 
                               Handler = HANDLER_NAME, 
                               Runtime = "python2.7", 
                               MemorySize = MEMORY, 
                               Timeout = TIMEOUT, 
                               Role = ROLE, 
                               Code = {'ZipFile' : open(PACKAGE_FILE, 'r').read()})

@task
def update_function():

    lambclient = boto3.client('lambda', region_name=AWS_REGION)

    response = lambclient.update_function_code(FunctionName=FUNCTION_NAME,
                                               ZipFile=open(PACKAGE_FILE, 'r').read())



@task
def deploy():
        local('git ls-tree --full-tree --name-only -r HEAD > .git-files-list')
    
        project.rsync_project("/data/jonas/pywren/", local_dir="./",
                              exclude=['*.npy', "*.ipynb", 'data', "*.mp4", 
                                       "*.pdf", "*.png"],
                              extra_opts='--files-from=.git-files-list')

        # copy the notebooks from remote to local

        project.rsync_project("/data/jonas/pywren/", local_dir="./",
                              extra_opts="--include '*.ipynb' --include '*.pdf' --include '*.png'  --include='*/' --exclude='*' ", 
                              upload=False)
        

QUEUE_NAME = 'pywren-queue'
#MESSAGE_GROUP_ID = 'hello.world'
@task
def create_queue():

    sqs = boto3.resource('sqs',  region_name=AWS_REGION)

    queue = sqs.create_queue(QueueName=QUEUE_NAME, 
                             Attributes={'VisibilityTimeout' : "20"})

@task 
def put_message(): # MessageBody="hello world"):
    # Get the service resource
    sqs = boto3.resource('sqs', region_name=AWS_REGION)
    
    
    # Get the queue
    queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)
    MessageBody = "{}".format(time.time())
    response = queue.send_message(MessageBody=MessageBody)

@task 
def get_message(delete=False):
    # Get the service resource
    sqs = boto3.resource('sqs', region_name=AWS_REGION)
    
    # Get the queue
    queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)

    response = queue.receive_messages()
    if len(response) > 0 :
        print response[0].body
        if delete:
            response[0].delete()

@task
def sqs_worker(number=1):
    from multiprocessing.pool import ThreadPool, Pool

    number = int(number)

    sqs = boto3.resource('sqs', region_name=AWS_REGION)
    
    # Get the queue
    queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)

    LOG_FILE = "sqs.log"
    def process_message(m):
        fid = open(LOG_FILE, 'a')
        fid.write("sent {} received {}\n".format(m.body, time.time()))
        m.delete()
        fid.close()

    pool = ThreadPool(10)
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

    
    
@task 
def get_message(delete=False):
    # Get the service resource
    sqs = boto3.resource('sqs', region_name=AWS_REGION)
    
    # Get the queue
    queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)

    response = queue.receive_messages()
    if len(response) > 0 :
        print response[0].body
        if delete:
            response[0].delete()
@task 
def get_message(delete=False):
    # Get the service resource
    sqs = boto3.resource('sqs', region_name=AWS_REGION)
    
    # Get the queue
    queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)

@task
def sqs_purge_queue():
    sqs = boto3.resource('sqs', region_name=AWS_REGION)
    
    # Get the queue
    queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)
    queue.purge()
