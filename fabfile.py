
from fabric.api import local, env, run, put, cd, task, sudo, get, settings, warn_only, lcd
from fabric.contrib import project
import boto3
import cloudpickle
import json
import base64
from six.moves import cPickle as pickle
from pywren.wrenconfig import * 
import pywren
import time

"""
conda notes

be sure to call conda clean --all before compressing



"""

env.roledefs['m'] = ['jonas@c65']

AWS_INSTANCE_NAME = "test_instance"
    
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

INSTANCE_PROFILE_NAME = "pywren_standalone"
@task
def create_instance_profile():
    iam = boto3.resource('iam')
    #iam.create_instance_profile(InstanceProfileName=INSTANCE_PROFILE_NAME)

    instance_profile = iam.InstanceProfile(INSTANCE_PROFILE_NAME)
    #instance_profile.add_role(RoleName='pywren_exec_role_refactor8')
    print instance_profile.name

@task 
def launch_instance():
    
    tgt_ami = 'ami-b04e92d0'
    AWS_REGION = 'us-west-2'
    my_aws_key = 'ec2-us-west-2'

    
    INSTANCE_TYPE = 'm3.xlarge'
    instance_name = AWS_INSTANCE_NAME

    ec2 = boto3.resource('ec2', region_name=AWS_REGION)

    BlockDeviceMappings=[
        {
            'DeviceName': '/dev/xvda',
            'Ebs': {
                'VolumeSize': 100,
                'DeleteOnTermination': True,
                'VolumeType': 'standard',
                'SnapshotId' : 'snap-c87f35ec'
            },
        },
    ]

    user_data = """
    #cloud-config
    repo_update: true
    repo_upgrade: all
    
    packages:
     - tmux
     - emacs
     - gcc
     - g++
     - git 
     - htop

    runcmd:
     - [ sh, -c, 'echo "hello world" > /tmp/hello.txt' ]
     - pip install supervisor 
     - [ sudo, -Hu, ec2-user, sh, -c, "wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh -O /tmp/miniconda.sh"]
     - [ sudo, -Hu, ec2-user, sh, -c, "chmod +x /tmp/miniconda.sh"]
     - [ sudo, -Hu, ec2-user, sh, -c, "/tmp/miniconda.sh -b -p  /home/ec2-user/anaconda"]
     - [ sudo, -Hu, ec2-user, sh, -c, "/home/ec2-user/anaconda/bin/conda install -q -y numpy boto3"]
     - [ sudo, -Hu, ec2-user, sh, -c, "git clone -b standalone-worker https://github.com/ericmjonas/pywren.git /home/ec2-user/pywren"]
     - [ sudo, -Hu, ec2-user, sh, -c, "/home/ec2-user/anaconda/bin/pip install -e /home/ec2-user/pywren"]
    """
    #      - [
    # - /home/ec2-user/anaconda/bin/conda install -q -y numpy boto3

    #      - git clone -b standalone-worker "https://github.com/ericmjonas/pywren.git" /home/ec2-user/pywren
    #  - /home/ec2-user/anaconda/bin/pip install -e 
    # - [ ./Miniconda2-latest-Linux-x86_64.sh -b -p /home/ec2-user/anaconda] 
    #  - [ /home/ec2-user/anaconda/bin/conda install numpy boto3] 

    iam = boto3.resource('iam')
    instance_profile = iam.InstanceProfile(INSTANCE_PROFILE_NAME)
    instance_profile_dict =  {
                              'Name' : instance_profile.name}
    instances = ec2.create_instances(ImageId=tgt_ami, MinCount=1, MaxCount=1,
                                     KeyName=my_aws_key, 
                                     InstanceType=INSTANCE_TYPE, 
                                     BlockDeviceMappings = BlockDeviceMappings,
                                     InstanceInitiatedShutdownBehavior='terminate',
                                     EbsOptimized=True, 
                                     IamInstanceProfile = instance_profile_dict, 
                                     UserData=user_data)

    for inst in instances:


        inst.wait_until_running()
        inst.reload()
        inst.create_tags(
            Resources=[
                inst.instance_id
            ],
            Tags=[
                {
                    'Key': 'Name',
                    'Value': instance_name
                },
            ]
        )
        print inst.public_dns_name

def tags_to_dict(d):
    return {a['Key'] : a['Value'] for a in d}

@task
def terminate_instance():
    instance_name = "test_instance"

    ec2 = boto3.resource('ec2', region_name=AWS_REGION)

    insts = []
    for i in ec2.instances.all():
        if i.state['Name'] == 'running':
            d = tags_to_dict(i.tags)
            if d['Name'] == instance_name:
                i.terminate()
                insts.append(i)


@task
def delete_log_groups(prefix):
    config = pywren.wrenconfig.default()

    logclient = boto3.client('logs', region_name=config['account']['aws_region'])
    lg = logclient.describe_log_groups(logGroupNamePrefix=prefix)
    for l in lg['logGroups']:
        logGroupName = l['logGroupName']
        print 'deleting', logGroupName
        logclient.delete_log_group(logGroupName = logGroupName)
        

