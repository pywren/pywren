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

import os
import copy


GENERIC_HANDLER_NAME = "wrenhandler.generic_handler"
AWS_LAMBDA_HANDLER_NAME = "wrenhandler.aws_lambda_handler"

PACKAGE_FILE = "deploy.zip"

MEMORY = 512 * 3
TIMEOUT = 300
AWS_ACCOUNT_ID = 783175685819
AWS_ROLE = "helloworld_exec_role"

ROLE = "arn:aws:iam::{}:role/{}".format(AWS_ACCOUNT_ID, AWS_ROLE)

AWS_REGION_DEFAULT = 'us-west-2'
AWS_S3_BUCKET_DEFAULT = "pywren.data"
AWS_S3_PREFIX_DEFAULT = "pywren.jobs"
AWS_LAMBDA_ROLE_DEFAULT = 'pywren_exec_role_1'
AWS_LAMBDA_FUNCTION_NAME_DEFAULT = 'pywren_1'
AWS_SQS_QUEUE_DEFAULT = 'pywren-jobs-1'

MAX_AGG_DATA_SIZE = 4e6

DEFAULT_KEY_NAME = "PYWREN_DEFAULT_KEY"

default_runtime = {'2.7' : "pywren.runtimes/default_2.7.meta.json",
                   '3.4' : "pywren.runtimes/default_3.4.meta.json",
                   '3.5' : "pywren.runtimes/default_3.5.meta.json",
                   '3.6' : "pywren.runtimes/default_3.6.meta.json",
                   '3.7' : "pywren.runtimes/default_3.7.meta.json"}

def load(config_filename):
    import yaml
    res = yaml.safe_load(open(config_filename, 'r'))
    # sanity check
    if res['s3']['bucket'] == 'BUCKET_NAME':
        raise Exception(
            "{} has bucket name as {} -- make sure you change the default bucket".format(
                config_filename, res['s3']['bucket']))
    if 'storage_backend' not in res:
        res = patch_storage_config(res)
    if res['standalone']['ec2_ssh_key'] == DEFAULT_KEY_NAME:
        res['standalone']['ec2_ssh_key'] = None

    return res

def get_default_home_filename():
    default_home_filename = os.path.join(os.path.expanduser("~/.pywren_config"))
    return default_home_filename


def get_default_config_filename():
    """
    First checks .pywren_config
    then checks PYWREN_CONFIG_FILE environment variable
    then ~/.pywren_config
    """
    if 'PYWREN_CONFIG_FILE' in os.environ:
        config_filename = os.environ['PYWREN_CONFIG_FILE']
        # FIXME log this

    elif os.path.exists(".pywren_config"):
        config_filename = os.path.abspath('.pywren_config')

    else:
        config_filename = get_default_home_filename()

    return config_filename

def patch_storage_config(config_data):
    if 'storage_backend' in config_data:
        raise Exception("storage_backend is already in the config")

    patched_config = copy.deepcopy(config_data)
    patched_config['storage_backend'] = 's3'
    patched_config['storage_prefix'] = config_data['s3']['pywren_prefix']
    patched_config['runtime']['runtime_storage'] = 's3'
    return patched_config


def default():
    """
    First checks .pywren_config
    then checks PYWREN_CONFIG_FILE environment variable
    then ~/.pywren_config
    """
    config_filename = get_default_config_filename()
    if config_filename is None:
        raise ValueError("could not find configuration file")

    config_data = load(config_filename)
    return config_data


def extract_storage_config(config):
    storage_config = dict()
    storage_config['storage_backend'] = config['storage_backend']
    storage_config['storage_prefix'] = config['storage_prefix']
    if storage_config['storage_backend'] == 's3':
        storage_config['backend_config'] = {}
        storage_config['backend_config']['bucket'] = config['s3']['bucket']
        storage_config['backend_config']['region'] = config['account']['aws_region']
    return storage_config

basic_role_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        },
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {"Service": "ec2.amazonaws.com"},
            "Action": "sts:AssumeRole"
        },
    ]
}

# FIXME make these permissions more curtailed, esp. w.r.t. the target
# bucket and the target sqs queue and the target logs

more_permissions_policy = {
    "Version": "2012-10-17",
    'Statement': [
        {
            'Effect':'Allow',
            'Action': [
                's3:ListBucket',
                's3:Put*',
                's3:Get*',
                's3:*MultipartUpload*'
            ],
            'Resource': '*'
        },
        {
            "Effect": "Allow",
            "Action": "logs:CreateLogGroup",
            "Resource": "arn:aws:logs:AWS_REGION:AWS_ACCOUNT_ID:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:AWS_REGION:AWS_ACCOUNT_ID:log-group:/aws/lambda/*:*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": "arn:aws:iam::AWS_ACCOUNT_ID:role/*"
        },
        {
            "Effect": "Allow",
            "Action": "ec2:Describe*",
            "Resource": "*"
        },
        {
            "Action": "sqs:*",
            "Resource": "*",
            "Effect": "Allow"
        },
        {
            "Action": [
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:AWS_REGION:AWS_ACCOUNT_ID:log-group:*:*"
            ],
            "Effect": "Allow"
        }
    ]}


RUNTIME_BUCKET_REGION = {'us-west-1' : 'pywren-runtimes-public-us-west-1',
                         'us-west-2' : 'pywren-runtimes-public-us-west-2',
                         'us-east-1' : 'pywren-runtimes-public-us-east-1',
                         'us-east-2' : 'pywren-runtimes-public-us-east-2',
                         'ap-northeast-2' : 'pywren-runtimes-public-ap-northeast-2',
                         'eu-central-1' : 'pywren-runtimes-public-eu-central-1',
                         'eu-west-1' : 'pywren-runtimes-public-eu-west-1'}

TARGET_AMI_REGION = {'us-west-1' : 'ami-0bdb828fd58c52235',
                     'us-west-2' : 'ami-a0cfeed8',
                     'us-east-1' : 'ami-0ff8a91507f77f867',
                     'us-east-2' : 'ami-0303c7b2e7066b60d',
                     'ap-northeast-2' : 'ami-01f7db8de071cd2fc',
                     'eu-central-1' : 'ami-0233214e13e500f77',
                     'eu-west-1' : 'ami-047bb4163c506cd98'}
