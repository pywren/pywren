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
from pywren import ec2standalone

@click.group()
def cli():
    pass

@click.group("standalone")
def standalone():
    """
    Standalone commands to control groups of servers
    """


# FIXME use pywren main module SOURCE_DIR
SOURCE_DIR = os.path.dirname(os.path.abspath(__file__)) 

@click.command()
@click.option('--filename', default=pywren.wrenconfig.get_default_home_filename(), 
              help='create a default config and populate with sane values')
@click.option('--lambda_role', default='pywren_exec_role', 
              help='name of the IAM role we are creating')
@click.option('--function_name', default='pywren1', 
              help='lambda function name')
@click.option('--bucket_name', default='BUCKET_NAME', 
              help='s3 bucket name for intermediates')
@click.option('--sqs_queue', default='pywren-queue', 
              help='sqs queue name for standalone execution')
@click.option('--standalone_name', default='pywren-standalone', 
              help='ec2 standalone server name and profile name')
@click.option('--force', is_flag=True, default=False, 
              help='force overwrite an existing file')
def create_config(filename, force, lambda_role, function_name, bucket_name, 
                  sqs_queue, standalone_name):
    """
    Create a config file initialized with the defaults, and
    put it in your ~/.pywren_config

    """
    
    # copy default config file

    default_yaml = open(os.path.join(SOURCE_DIR, "../default_config.yaml")).read()
    
    client = boto3.client("sts")
    account_id = client.get_caller_identity()["Account"]

    # perform substitutions -- get your AWS account ID and auto-populate

    default_yaml = default_yaml.replace('AWS_ACCOUNT_ID', account_id)
    default_yaml = default_yaml.replace('pywren_exec_role', lambda_role)
    default_yaml = default_yaml.replace('pywren1', function_name)
    default_yaml = default_yaml.replace('BUCKET_NAME', bucket_name)
    default_yaml = default_yaml.replace('pywren-queue', sqs_queue)
    default_yaml = default_yaml.replace('pywren-standalone', standalone_name)

    # print out message about the stuff you need to do 
    if os.path.exists(filename) and not force:
        raise ValueError("{} already exists; not overwriting (did you need --force?)".format(filename))
        
    open(filename, 'w').write(default_yaml)
    click.echo("new default file created in {}".format(filename))
    click.echo("lambda role is {}".format(lambda_role))
    click.echo("remember to set your s3 bucket and preferred AWS region")


@click.command()
def test_config():
    """
    Test that you have properly filled in the necessary
    aws fields, your boto install is working correctly, your s3 bucket is
    readable and writable (also by your indicated role), etc. 
    """

    client = boto3.client("sts")
    account_id = client.get_caller_identity()["Account"]
    print "The accountID is ", account_id
    # make sure the bucket exists
    #config = pywren.wrenconfig.default()

@click.command()
def create_role():
    """
    
    """
    config = pywren.wrenconfig.default()

    iamclient = boto3.resource('iam')
    json_policy = json.dumps(pywren.wrenconfig.basic_role_policy)
    role_name = config['account']['aws_lambda_role']
    role = iamclient.create_role(RoleName=role_name, 
                                 AssumeRolePolicyDocument=json_policy)
    more_json_policy = json.dumps(pywren.wrenconfig.more_permissions_policy)
    
    AWS_ACCOUNT_ID = config['account']['aws_account_id']
    AWS_REGION = config['account']['aws_region']
    more_json_policy = more_json_policy.replace("AWS_ACCOUNT_ID", str(AWS_ACCOUNT_ID))
    more_json_policy = more_json_policy.replace("AWS_REGION", AWS_REGION)

    iamclient.RolePolicy(role_name, '{}-more-permissions'.format(role_name)).put(
        PolicyDocument=more_json_policy)


@click.command()
def create_instance_profile():
    config = pywren.wrenconfig.default()
    role_name = config['account']['aws_lambda_role']
    instance_profile_name = config['standalone']['instance_profile_name']

    iam = boto3.resource('iam')
    iam.create_instance_profile(InstanceProfileName=instance_profile_name)
    
    instance_profile = iam.InstanceProfile(instance_profile_name)
    instance_profile.add_role(RoleName=role_name)


@click.command()    
def deploy_lambda(update_if_exists = True):
    """
    Package up the source code and deploy to aws. Only creates the new
    function if it doesn't already exist
    """
    config = pywren.wrenconfig.default()
    AWS_REGION = config['account']['aws_region']
    FUNCTION_NAME = config['lambda']['function_name']
    MEMORY = config['lambda']['memory']
    TIMEOUT = config['lambda']['timeout']
    AWS_LAMBDA_ROLE = config['account']['aws_lambda_role'] 
    AWS_ACCOUNT_ID = config['account']['aws_account_id']


    file_like_object = io.BytesIO()
    zipfile_obj = zipfile.ZipFile(file_like_object, mode='w')

    # FIXME see if role exists
    files = glob2.glob(os.path.join(SOURCE_DIR, "../**/*.py"))
    for f in files:
        a = os.path.relpath(f, SOURCE_DIR + "/..") 
                            
        zipfile_obj.write(f, arcname=a)
    zipfile_obj.close()
    #open("/tmp/deploy.zip", 'w').write(file_like_object.getvalue())
        
    lambclient = boto3.client('lambda', region_name=AWS_REGION)

    ROLE = "arn:aws:iam::{}:role/{}".format(AWS_ACCOUNT_ID, AWS_LAMBDA_ROLE)

    b = lambclient.list_functions()
    function_exists = False

    if FUNCTION_NAME in [f['FunctionName'] for f in b['Functions']]:
        function_exists = True

    retries = 0
    while retries < 10:
        try:
            if function_exists:
                print "function exists, updating"
                if update_if_exists:

                    response = lambclient.update_function_code(FunctionName=FUNCTION_NAME,
                                                               ZipFile=file_like_object.getvalue())
                    return True
                else:
                    raise Exception() # FIXME will this work? 
            else:

                lambclient.create_function(FunctionName = FUNCTION_NAME, 
                                           Handler = pywren.wrenconfig.AWS_LAMBDA_HANDLER_NAME, 
                                           Runtime = "python2.7", 
                                           MemorySize = MEMORY, 
                                           Timeout = TIMEOUT, 
                                           Role = ROLE, 
                                           Code = {'ZipFile' : file_like_object.getvalue()})
                print "Create successful" 
                break
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "InvalidParameterValueException":

                print "attempt", retries
                retries += 1

                # FIXME actually check for "botocore.exceptions.ClientError: An error occurred (InvalidParameterValueException) when calling the CreateFunction operation: The role defined for the function cannot be assumed by Lambda."
                print "sleeping for 5"
                time.sleep(5)
                print "done"
                continue
            else:
                raise e
    if retries == 10:
        raise ValueError("could not register funciton after 10 tries")
        
                
@click.command()    
def delete_lambda():
    config = pywren.wrenconfig.default()
    AWS_REGION = config['account']['aws_region']
    FUNCTION_NAME = config['lambda']['function_name']

    lambclient = boto3.client('lambda', region_name=AWS_REGION)

    b = lambclient.list_functions()

    if FUNCTION_NAME not in [f['FunctionName'] for f in b['Functions']]:
        raise Exception()
    lambclient.delete_function(FunctionName = FUNCTION_NAME)


@click.command()
def delete_role():
    """
    
    """

    config = pywren.wrenconfig.default()
    iamclient = boto3.client('iam')
    role_name = config['account']['aws_lambda_role']
    
    iamclient.delete_role_policy(RoleName = role_name, 
                                 PolicyName = '{}-more-permissions'.format(role_name))
    iamclient.delete_role(RoleName = role_name)
    

@click.command()
def create_queue():
    """
    Create the SQS queue
    """
    config = pywren.wrenconfig.default()
    AWS_REGION = config['account']['aws_region']
    SQS_QUEUE_NAME = config['standalone']['sqs_queue_name']

    sqs = boto3.resource('sqs',  region_name=AWS_REGION)

    queue = sqs.create_queue(QueueName=SQS_QUEUE_NAME, 
                             Attributes={'VisibilityTimeout' : "20"})


@click.command()
def delete_queue():
    """
    Delete the SQS queue
    """
    config = pywren.wrenconfig.default()
    AWS_REGION = config['account']['aws_region']
    SQS_QUEUE_NAME = config['standalone']['sqs_queue_name']

    sqs = boto3.resource('sqs',  region_name=AWS_REGION)
    queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_NAME)
    queue.delete()


def test_lambda():
    """
    Simple single-function test
    """

    config = pywren.wrenconfig.default()


@click.command()
def print_latest_logs():
    """
    Print the latest log group and log stream. 

    Note this does not contain support for going back further in history, 
    use the CloudWatch Logs web GUI for that. 
    """


    config = pywren.wrenconfig.default()

    logclient = boto3.client('logs', region_name=config['account']['aws_region'])

    logGroupName = "/aws/lambda/{}".format(config['lambda']['function_name'])

    response = logclient.describe_log_streams(
        logGroupName=logGroupName,
        orderBy='LastEventTime',
        descending=True)

    latest_logStreamName = response['logStreams'][0]['logStreamName']


    response = logclient.get_log_events(
        logGroupName=logGroupName,
        logStreamName=latest_logStreamName,)

    for event in response['events']:
        print "{} : {}".format(event['timestamp'], event['message'].strip())


@click.command()
def log_url():
    """
    return the cloudwatch log URL
    """
    config = pywren.wrenconfig.default()
    function_name = config['lambda']['function_name']
    aws_region = config['account']['aws_region']
    url = "https://{}.console.aws.amazon.com/cloudwatch/home?region={}#logStream:group=/aws/lambda/{}".format(aws_region, aws_region, function_name)
    print url


@standalone.command('launch_instances')
@click.argument('number', default=1, type=int)
@click.option('--max_idle_time', default=None, type=int, 
              help='instance queue idle time before checking self-termination')
@click.option('--idle_terminate_granularity', default=None, type=int, 
              help='granularity of billing (sec)')
@click.option('--pywren_git_branch', default='master', type=str, 
              help='which branch to use on the stand-alone')
def standalone_launch_instances(number, max_idle_time, idle_terminate_granularity, 
                                pywren_git_branch):
    config = pywren.wrenconfig.default()
    sc= config['standalone']
    aws_region = config['account']['aws_region']

    if max_idle_time is not None:
        sc['max_idle_time'] = max_idle_time
    if idle_terminate_granularity is not None:
        sc['idle_terminate_granularity'] = idle_terminate_granularity
            
    inst_list = ec2standalone.launch_instances(number, 
                                               sc['target_ami'], aws_region, 
                                               sc['ec2_ssh_key'], 
                                               sc['ec2_instance_type'], 
                                               sc['instance_name'],
                                               sc['instance_profile_name'], 
                                               sc['sqs_queue_name'], 
                                               sc['max_idle_time'], 
                                               idle_terminate_granularity = sc['idle_terminate_granularity'], pywren_git_branch=pywren_git_branch )
    
    print "launched:"
    ec2standalone.prettyprint_instances(inst_list)


@standalone.command("list_instances")
def standalone_list_instances():
    config = pywren.wrenconfig.default()
    aws_region = config['account']['aws_region']
    sc= config['standalone']
    
    inst_list = ec2standalone.list_instances(aws_region, sc['instance_name'])
    ec2standalone.prettyprint_instances(inst_list)

@standalone.command("instance_uptime")
def standalone_instance_uptime():
    pass

@standalone.command("terminate_instances")
def standalone_terminate_instances():
    config = pywren.wrenconfig.default()
    aws_region = config['account']['aws_region']
    sc= config['standalone']
    
    inst_list = ec2standalone.list_instances(aws_region, sc['instance_name'])
    print "terminate"
    ec2standalone.prettyprint_instances(inst_list)
    ec2standalone.terminate_instances(inst_list)


cli.add_command(create_config)
cli.add_command(test_config)
cli.add_command(create_role)
cli.add_command(create_instance_profile)
cli.add_command(deploy_lambda)
cli.add_command(delete_lambda)
cli.add_command(delete_role)
cli.add_command(create_queue)
cli.add_command(delete_queue)
cli.add_command(print_latest_logs)
cli.add_command(log_url)
cli.add_command(standalone)
