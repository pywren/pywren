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

import io
import json
import os
import sys
import time
import zipfile

import boto3
import botocore
import botocore.exceptions
import click
import pywren
import pywren.runtime
from pywren import ec2standalone


@click.group()
@click.option('--filename', default=pywren.wrenconfig.get_default_config_filename())
@click.pass_context
def cli(ctx, filename):
    ctx.obj = {'config_filename' : filename}

@click.group("standalone")
def standalone():
    """
    Standalone commands to control groups of servers
    """


# FIXME use pywren main module SOURCE_DIR
SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))

@click.command("get_aws_account_id")
def get_aws_account_id(verbose=True):
    """
    Check to make sure boto is working and get the AWS ACCONT ID
    """
    client = boto3.client("sts")
    account_id = client.get_caller_identity()["Account"]
    if verbose:
        click.echo("Your AWS account ID is {}".format(account_id))
    return account_id


@click.command("create_config")
@click.pass_context
@click.option('--aws_region', default=pywren.wrenconfig.AWS_REGION_DEFAULT,
              help='aws region to run in')
@click.option('--bucket_name', default=pywren.wrenconfig.AWS_S3_BUCKET_DEFAULT,
              help='s3 bucket name for intermediates')
@click.option('--bucket_prefix', default=pywren.wrenconfig.AWS_S3_PREFIX_DEFAULT,
              help='prefix for S3 keys used for input and output')
@click.option('--lambda_role',
              default=pywren.wrenconfig.AWS_LAMBDA_ROLE_DEFAULT,
              help='name of the IAM role we are creating')
@click.option('--function_name',
              default=pywren.wrenconfig.AWS_LAMBDA_FUNCTION_NAME_DEFAULT,
              help='lambda function name')
@click.option('--sqs_queue', default=pywren.wrenconfig.AWS_SQS_QUEUE_DEFAULT,
              help='sqs queue name for standalone execution')
@click.option('--standalone_name', default='pywren-standalone',
              help='ec2 standalone server name and profile name')
@click.option('--force', is_flag=True, default=False,
              help='force overwrite an existing file')
@click.option('--pythonver', default=pywren.runtime.version_str(sys.version_info),
              help="Python version to use for runtime")
def create_config(ctx, force, aws_region, lambda_role, function_name, bucket_name,
                  bucket_prefix,
                  sqs_queue, standalone_name, pythonver):
    """
    Create a config file initialized with the defaults, and
    put it in your ~/.pywren_config

    """
    filename = ctx.obj['config_filename']
    # copy default config file

    # FIXME check if it exists
    default_yaml = open(os.path.join(SOURCE_DIR, "../default_config.yaml")).read()

    client = boto3.client("sts")
    account_id = client.get_caller_identity()["Account"]

    # perform substitutions -- get your AWS account ID and auto-populate

    default_yaml = default_yaml.replace('AWS_ACCOUNT_ID', account_id)
    default_yaml = default_yaml.replace('AWS_REGION', aws_region)
    default_yaml = default_yaml.replace('pywren_exec_role', lambda_role)
    default_yaml = default_yaml.replace('pywren1', function_name)
    default_yaml = default_yaml.replace('BUCKET_NAME', bucket_name)
    default_yaml = default_yaml.replace('pywren.jobs', bucket_prefix)
    default_yaml = default_yaml.replace('pywren-queue', sqs_queue)
    default_yaml = default_yaml.replace('pywren-standalone', standalone_name)
    if pythonver not in pywren.wrenconfig.default_runtime:
        print('No matching runtime package for python version ', pythonver)
        print('Python 2.7 runtime will be used for remote.')
        pythonver = '2.7'

    if aws_region in pywren.wrenconfig.RUNTIME_BUCKET_REGION:
        runtime_bucket = pywren.wrenconfig.RUNTIME_BUCKET_REGION[aws_region]
        target_ami = pywren.wrenconfig.TARGET_AMI_REGION[aws_region]
    else:
        print("WARNING: Runtime not deployed for your region")
        print("using runtime from us-west-2.")
        print("Performance may be impacted")
        runtime_bucket = pywren.wrenconfig.RUNTIME_BUCKET_REGION['us-west-2']
        target_ami = pywren.wrenconfig.TARGET_AMI_REGION['us-west-2']

    default_yaml = default_yaml.replace("RUNTIME_BUCKET",
                                        runtime_bucket)
    k = pywren.wrenconfig.default_runtime[pythonver]

    default_yaml = default_yaml.replace("RUNTIME_KEY", k)

    default_yaml = default_yaml.replace("TARGET_AMI", target_ami)

    # print out message about the stuff you need to do
    if os.path.exists(filename) and not force:
        raise ValueError("{} already exists; not overwriting (did you need --force?)".format(
            filename))

    open(filename, 'w').write(default_yaml)
    click.echo("new default file created in {}".format(filename))
    click.echo("lambda role is {}".format(lambda_role))


@click.command("test_config")
@click.pass_context
def test_config(ctx): # pylint: disable=unused-argument
    """
    Test that you have properly filled in the necessary
    aws fields, your boto install is working correctly, your s3 bucket is
    readable and writable (also by your indicated role), etc.
    """

    client = boto3.client("sts")
    account_id = client.get_caller_identity()["Account"]
    print("The accountID is ", account_id)
    # make sure the bucket exists
    # config = pywren.wrenconfig.default()

@click.command("create_role")
@click.pass_context
def create_role(ctx):
    """
    Creates the IAM profile used by PyWren.
    """
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    iam = boto3.resource('iam')
    iamclient = boto3.client('iam')
    json_policy = json.dumps(pywren.wrenconfig.basic_role_policy)
    role_name = config['account']['aws_lambda_role']
    roles = [x for x in iamclient.list_roles()["Roles"] if x["RoleName"] == role_name]
    if (len(roles) == 0):
        iam.create_role(RoleName=role_name,
                        AssumeRolePolicyDocument=json_policy)
        more_json_policy = json.dumps(pywren.wrenconfig.more_permissions_policy)

        AWS_ACCOUNT_ID = config['account']['aws_account_id']
        AWS_REGION = config['account']['aws_region']
        more_json_policy = more_json_policy.replace("AWS_ACCOUNT_ID", str(AWS_ACCOUNT_ID))
        more_json_policy = more_json_policy.replace("AWS_REGION", AWS_REGION)

        iam.RolePolicy(role_name, '{}-more-permissions'.format(role_name)).put(
            PolicyDocument=more_json_policy)
    else:
        print("Using existing IAM role...")


@click.command("create_bucket")
@click.pass_context
def create_bucket(ctx):
    """
    Creates S3 buckets used by PyWren.
    """
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    s3 = boto3.client("s3")
    region = config['account']['aws_region']
    kwargs = {'CreateBucketConfiguration': {'LocationConstraint': region}}
    if region == 'us-east-1':
        kwargs = {}
    s3.create_bucket(Bucket=config['s3']['bucket'], **kwargs)

@click.command("create_instance_profile")
@click.pass_context
def create_instance_profile(ctx):
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    role_name = config['account']['aws_lambda_role']
    instance_profile_name = config['standalone']['instance_profile_name']

    iam = boto3.resource('iam')
    iamclient = boto3.client('iam')
    response = iamclient.list_instance_profiles()
    instance_profiles = [profile for profile in response['InstanceProfiles']
                         if profile['InstanceProfileName'] == instance_profile_name]

    if len(instance_profiles) == 0:
        iam.create_instance_profile(InstanceProfileName=instance_profile_name)
        iam.InstanceProfile(instance_profile_name).add_role(RoleName=role_name)
    else:
        print("Using existing instance profile...")


@click.command("create_ec2_ssh_key")
@click.pass_context
def create_ec2_ssh_key(ctx):
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    click.echo("EC2 instances require an SSH key pair to be specified in order" + \
               " to connect to them.")
    if not click.confirm("Do you want to be able to connect to launched standalone instances?",
                         default=False):
        return
    key_name = click.prompt("Name of ssh key:",
                            default="ec2-{}".format(config['account']['aws_region']))

    ec2client = boto3.client('ec2')
    key_pairs = ec2client.describe_key_pairs()
    key_pairs = [key_pair for key_pair in key_pairs['KeyPairs'] if key_pair['KeyName'] == key_name]
    create_key = len(key_pairs) == 0
    if create_key and not click.confirm(
            "{} does not exist, would you like to create a new key pair?".format(key_name)):
        return
    if create_key:
        ec2 = boto3.resource('ec2')
        key_pair = ec2.create_key_pair(KeyName=key_name)
        key_location = click.prompt("Where should I put the private key?",
                                    default=os.path.expanduser("~/.ssh/"))
        if not os.path.exists(key_location):
            os.makedirs(key_location)
        key_path = os.path.join(key_location, "{}.pem".format(key_name))
        with open(key_path, "w+") as key_file:
            key_file.write(key_pair.key_material)
        click.echo("Wrote private key to {}".format(key_path))

    config_yaml = open(config_filename).read()
    config_yaml = config_yaml.replace('PYWREN_DEFAULT_KEY', key_name)
    with open(config_filename, "w") as f:
        f.write(config_yaml)


def list_all_funcs(lambclient):
    return lambclient.get_paginator('list_functions').paginate().build_full_result()

@click.command("deploy_lambda")
@click.pass_context
def deploy_lambda(ctx, update_if_exists=True):
    """
    Package up the source code and deploy to aws. Only creates the new
    function if it doesn't already exist
    """
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    AWS_REGION = config['account']['aws_region']
    FUNCTION_NAME = config['lambda']['function_name']
    MEMORY = config['lambda']['memory']
    TIMEOUT = config['lambda']['timeout']
    AWS_LAMBDA_ROLE = config['account']['aws_lambda_role']
    AWS_ACCOUNT_ID = config['account']['aws_account_id']


    file_like_object = io.BytesIO()
    zipfile_obj = zipfile.ZipFile(file_like_object, mode='w')

    # FIXME see if role exists
    module_dir = os.path.join(SOURCE_DIR, "../")

    for f in ['wrenutil.py', 'wrenconfig.py', 'wrenhandler.py',
              'version.py', 'jobrunner/jobrunner.py', 'wren.py']:
        f = os.path.abspath(os.path.join(module_dir, f))
        a = os.path.basename(f) # , SOURCE_DIR + "/..")
        zipfile_obj.write(f, arcname=a)
    zipfile_obj.close()
    #open("/tmp/deploy.zip", 'w').write(file_like_object.getvalue())

    lambclient = boto3.client('lambda', region_name=AWS_REGION)

    ROLE = "arn:aws:iam::{}:role/{}".format(AWS_ACCOUNT_ID, AWS_LAMBDA_ROLE)

    b = list_all_funcs(lambclient)

    function_exists = False

    function_name_list = [f['FunctionName'] for f in b['Functions']]

    if FUNCTION_NAME in function_name_list:
        function_exists = True

    retries = 0
    while retries < 10:
        try:
            if function_exists:
                print("function exists, updating")
                if update_if_exists:
                    lambclient.update_function_code(FunctionName=FUNCTION_NAME,
                                                    ZipFile=file_like_object.getvalue())
                    return True
                else:
                    raise Exception() # FIXME will this work?
            else:

                lambclient.create_function(FunctionName=FUNCTION_NAME,
                                           Handler=pywren.wrenconfig.AWS_LAMBDA_HANDLER_NAME,
                                           Runtime="python2.7",
                                           MemorySize=MEMORY,
                                           Timeout=TIMEOUT,
                                           Role=ROLE,
                                           Code={'ZipFile' : file_like_object.getvalue()})
                print("Successfully created function.")
                break
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "InvalidParameterValueException":

                retries += 1

                # FIXME actually check for "botocore.exceptions.ClientError: An error occurred
                # (InvalidParameterValueException) when calling the CreateFunction operation:
                # The role defined for the function cannot be assumed by Lambda."
                print("Pausing for 5 seconds for changes to propagate.")
                time.sleep(5)
                continue
            else:
                raise e
    if retries == 10:
        raise ValueError("could not register funciton after 10 tries")


@click.command("delete_lambda")
@click.pass_context
def delete_lambda(ctx):
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    AWS_REGION = config['account']['aws_region']
    FUNCTION_NAME = config['lambda']['function_name']

    lambclient = boto3.client('lambda', region_name=AWS_REGION)

    b = list_all_funcs(lambclient)

    if FUNCTION_NAME not in [f['FunctionName'] for f in b['Functions']]:
        raise Exception()
    lambclient.delete_function(FunctionName=FUNCTION_NAME)


@click.command("delete_role")
@click.pass_context
def delete_role(ctx):
    """
    Deletes IAM roles specified in PyWren config.
    """
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    iamclient = boto3.client('iam')
    role_name = config['account']['aws_lambda_role']

    iamclient.delete_role_policy(RoleName=role_name,
                                 PolicyName='{}-more-permissions'.format(role_name))
    iamclient.delete_role(RoleName=role_name)
    print("deleted role{}".format(role_name))


@click.command("delete_instance_profile")
@click.pass_context
@click.argument('name', default="", type=str)
def delete_instance_profile(ctx, name):
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    instance_profile_name = config['standalone']['instance_profile_name']

    if name != "":
        instance_profile_name = name
    iam = boto3.resource('iam')
    profile = iam.InstanceProfile(instance_profile_name)
    roles = profile.roles
    for r in roles:
        profile.remove_role(RoleName=r.name)
    profile.delete()


@click.command("create_queue")
@click.pass_context
def create_queue(ctx):
    """
    Create the SQS queue
    """
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)
    AWS_REGION = config['account']['aws_region']
    SQS_QUEUE_NAME = config['standalone']['sqs_queue_name']

    sqs = boto3.resource('sqs', region_name=AWS_REGION)
    print("creating queue {}".format(SQS_QUEUE_NAME))
    sqs.create_queue(QueueName=SQS_QUEUE_NAME,
                     Attributes={'VisibilityTimeout' : "20"})


@click.command("delete_queue")
@click.pass_context
def delete_queue(ctx):
    """
    Delete the SQS queue
    """
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    AWS_REGION = config['account']['aws_region']
    SQS_QUEUE_NAME = config['standalone']['sqs_queue_name']

    sqs = boto3.resource('sqs', region_name=AWS_REGION)
    queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_NAME)
    queue.delete()

@click.command("test_function")
@click.pass_context
def test_function(ctx):
    """
    Simple single-function test
    """
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    wrenexec = pywren.default_executor(config=config)
    def hello_world(_):
        return "Hello world"

    fut = wrenexec.call_async(hello_world, None)
    res = fut.result(storage_handler=wrenexec.storage)

    click.echo("function returned: {}".format(res))

@click.command("print_latest_logs")
@click.pass_context
def print_latest_logs(ctx):
    """
    Print the latest log group and log stream.

    Note this does not contain support for going back further in history,
    use the CloudWatch Logs web GUI for that.
    """
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

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
        print("{} : {}".format(event['timestamp'], event['message'].strip()))


@click.command("log_url")
@click.pass_context
def log_url(ctx):
    """
    return the cloudwatch log URL
    """
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    function_name = config['lambda']['function_name']
    aws_region = config['account']['aws_region']
    url = "https://" + \
        "{}.console.aws.amazon.com/cloudwatch/home?region={}#logStream:group=/aws/lambda/{}".format(
            aws_region, aws_region, function_name)
    print(url)


@standalone.command('launch_instances')
@click.pass_context
@click.argument('number', default=1, type=int)
@click.option('--max_idle_time', default=None, type=int,
              help='instance queue idle time before checking self-termination')
@click.option('--idle_terminate_granularity', default=None, type=int,
              help='granularity of billing (sec)')
@click.option('--parallelism', default=1, type=int,
              help='Number of workers per machine')
@click.option('--pywren_git_branch', default='master', type=str,
              help='which branch to use on the stand-alone')
@click.option('--pywren_git_commit', default=None,
              help='which git to use on the stand-alone (supercedes pywren_git_branch)')
@click.option('--spot_price', default=None, type=float,
              help='use spot instances, at this reserve price')
@click.option('--instance_type', default=None, type=str,
              help='launch this instance type')
def standalone_launch_instances(ctx, number, max_idle_time, parallelism,
                                idle_terminate_granularity,
                                pywren_git_branch, pywren_git_commit,
                                spot_price, instance_type):
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    sc = config['standalone']
    aws_region = config['account']['aws_region']

    if max_idle_time is not None:
        sc['max_idle_time'] = max_idle_time
    if idle_terminate_granularity is not None:
        sc['idle_terminate_granularity'] = idle_terminate_granularity
    if instance_type is not None:
        sc['instance_type'] = instance_type

    use_fast_io = sc.get("fast_io", False)

    availability_zone = sc.get("availability_zone", None)

    inst_list = ec2standalone.launch_instances(number,
                                               sc['target_ami'], aws_region,
                                               sc['ec2_ssh_key'],
                                               sc['ec2_instance_type'],
                                               sc['instance_name'],
                                               sc['instance_profile_name'],
                                               sc['sqs_queue_name'],
                                               max_idle_time=sc['max_idle_time'],
                                               idle_terminate_granularity=\
                                               sc['idle_terminate_granularity'],
                                               pywren_git_branch=pywren_git_branch,
                                               pywren_git_commit=pywren_git_commit,
                                               availability_zone=availability_zone,
                                               fast_io=use_fast_io,
                                               parallelism=parallelism,
                                               spot_price=spot_price)

    print("launched:")
    ec2standalone.prettyprint_instances(inst_list)


@standalone.command("list_instances")
@click.pass_context
def standalone_list_instances(ctx):
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    aws_region = config['account']['aws_region']
    sc = config['standalone']

    inst_list = ec2standalone.list_instances(aws_region, sc['instance_name'])
    ec2standalone.prettyprint_instances(inst_list)

@standalone.command("instance_uptime")
@click.pass_context
def standalone_instance_uptime(ctx):
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    aws_region = config['account']['aws_region']
    sc = config['standalone']

    inst_list = ec2standalone.list_instances(aws_region, sc['instance_name'])
    ec2standalone.prettyprint_instance_uptimes(inst_list)


@standalone.command("terminate_instances")
@click.pass_context
def standalone_terminate_instances(ctx):
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    aws_region = config['account']['aws_region']
    sc = config['standalone']

    inst_list = ec2standalone.list_instances(aws_region, sc['instance_name'])
    print("terminate")
    ec2standalone.prettyprint_instances(inst_list)
    ec2standalone.terminate_instances(inst_list)

@standalone.command("queue_size")
@click.pass_context
def standalone_queue_size(ctx):
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)
    AWS_REGION = config['account']['aws_region']
    SQS_QUEUE_NAME = config['standalone']['sqs_queue_name']

    sqs = boto3.resource('sqs', region_name=AWS_REGION)
    queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_NAME)
    click.echo("Approximate number of jobs in flight: {}".format(
        queue.attributes['ApproximateNumberOfMessagesNotVisible']))
    click.echo("Approximate number of jobs in queue: {}".format(
        queue.attributes['ApproximateNumberOfMessages']))

@standalone.command("purge_queue")
@click.pass_context
def standalone_purge_queue(ctx):
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)
    AWS_REGION = config['account']['aws_region']
    SQS_QUEUE_NAME = config['standalone']['sqs_queue_name']

    sqs = boto3.resource('sqs', region_name=AWS_REGION)
    queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_NAME)
    click.echo("purge queue")
    queue.purge()

@click.command("delete_bucket")
@click.pass_context
def delete_bucket(ctx):
    """
    Warning this will also delete all keys inside a bucket

    """
    config_filename = ctx.obj['config_filename']
    config = pywren.wrenconfig.load(config_filename)

    s3 = boto3.resource('s3')
    client = boto3.client('s3')
    bucket = s3.Bucket(config['s3']['bucket'])
    while True:
        response = client.list_objects_v2(Bucket=bucket.name,
                                          MaxKeys=1000)
        if response['KeyCount'] > 0:
            keys = [c['Key'] for c in response['Contents']]
            objects = [{'Key' : k} for k in keys]
            print("deleting", len(keys), "keys")
            client.delete_objects(Bucket=bucket.name,
                                  Delete={'Objects' : objects})
        else:
            break
    #for obj in bucket.objects.all():

    print("deleting", bucket.name)
    bucket.delete()

@click.command("cleanup_all")
@click.option('--force', is_flag=True, default=False,
              help='dont error')
@click.pass_context
def cleanup_all(ctx, force):
    """
    Delete every service and object listed in the indicated
    config file.

    """
    for func in [delete_queue,
                 delete_lambda,
                 delete_instance_profile,
                 delete_role,
                 delete_bucket]:
        try:
            ctx.invoke(func)
        except Exception as e:
            if force:
                print("{} was raised, ignoring".format(e))
            else:
                raise



cli.add_command(create_config)
cli.add_command(test_config)
cli.add_command(test_function)
cli.add_command(get_aws_account_id)
cli.add_command(create_role)
cli.add_command(create_bucket)
cli.add_command(create_instance_profile)
cli.add_command(delete_instance_profile)
cli.add_command(deploy_lambda)
cli.add_command(delete_lambda)
cli.add_command(delete_role)
cli.add_command(create_queue)
cli.add_command(delete_queue)
cli.add_command(delete_bucket)
cli.add_command(cleanup_all)
cli.add_command(print_latest_logs)
cli.add_command(log_url)
cli.add_command(standalone)


def main():
    return cli() # pylint: disable=no-value-for-parameter
