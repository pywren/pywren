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

@click.group()
def cli():
    pass

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__)) 
@cli.command()
@click.option('--filename', default=pywren.wrenconfig.get_default_home_filename(), 
              help='create a default config and populate with sane values')
@click.option('--lambda_role', default='pywren_exec_role', 
              help='name of the IAM role we are creating')
@click.option('--function_name', default='pywren1', 
              help='lambda function name')
@click.option('--bucket_name', default='BUCKET_NAME', 
              help='s3 bucket name for intermediates')
@click.option('--force', is_flag=True, default=False, 
              help='force overwrite an existing file')
def create_config(filename, force, lambda_role, function_name, bucket_name):
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
    
    # print out message about the stuff you need to do 
    if os.path.exists(filename) and not force:
        raise ValueError("{} already exists; not overwriting (did you need --force?)".format(filename))
        
    open(filename, 'w').write(default_yaml)
    click.echo("new default file created in {}".format(filename))
    click.echo("lambda role is {}".format(lambda_role))
    click.echo("remember to set your s3 bucket and preferred AWS region")

@cli.command()
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

@cli.command()
def create_role():
    """
    
    """

    config = pywren.wrenconfig.default()
    print "config=", config
    iamclient = boto3.resource('iam')
    json_policy = json.dumps(pywren.wrenconfig.basic_role_policy)
    role_name = config['account']['aws_lambda_role']
    role = iamclient.create_role(RoleName=role_name, 
                                 AssumeRolePolicyDocument=json_policy)
    more_json_policy = json.dumps(pywren.wrenconfig.more_permissions_policy)

    iamclient.RolePolicy(role_name, '{}-more-permissions'.format(role_name)).put(
        PolicyDocument=more_json_policy)

@cli.command()    
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
                                           Handler = pywren.wrenconfig.HANDLER_NAME, 
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
        
                
@cli.command()    
def delete_lambda():
    config = pywren.wrenconfig.default()
    AWS_REGION = config['account']['aws_region']
    FUNCTION_NAME = config['lambda']['function_name']

    lambclient = boto3.client('lambda', region_name=AWS_REGION)

    b = lambclient.list_functions()

    if FUNCTION_NAME not in [f['FunctionName'] for f in b['Functions']]:
        raise Exception()
    lambclient.delete_function(FunctionName = FUNCTION_NAME)

@cli.command()
def delete_role():
    """
    
    """

    config = pywren.wrenconfig.default()
    iamclient = boto3.client('iam')
    role_name = config['account']['aws_lambda_role']
    
    iamclient.delete_role_policy(RoleName = role_name, 
                                 PolicyName = '{}-more-permissions'.format(role_name))
    iamclient.delete_role(RoleName = role_name)
    

def test_lambda():
    """
    Simple single-function test
    """

    config = pywren.wrenconfig.default()
