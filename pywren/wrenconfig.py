import os


FUNCTION_NAME = "pywren1"
HANDLER_NAME = "wrenhandler.handler"

PACKAGE_FILE = "deploy.zip"

MEMORY = 512 * 3
TIMEOUT = 300
AWS_ACCOUNT_ID = 783175685819
AWS_ROLE = "helloworld_exec_role"

ROLE = "arn:aws:iam::{}:role/{}".format(AWS_ACCOUNT_ID, AWS_ROLE)

AWS_REGION ='us-west-2'
AWS_S3_BUCKET = "jonas-testbucket2"
AWS_S3_PREFIX = "pywren.jobs"
FUNCTION_NAME = "pywren1"

MAX_AGG_DATA_SIZE = 4e6

def load(config_filename):
    import yaml
    res =  yaml.safe_load(open(config_filename, 'r'))    
    # sanity check
    if res['s3']['bucket'] == 'BUCKET_NAME':
        raise Exception("{} has bucket name as {} -- make sure you change the default bucket".format(config_filename, res['s3']['bucket']))
    return res

def get_default_home_filename():
    default_home_filename = os.path.join(os.path.expanduser("~/.pywren_config"))
    return default_home_filename


def default():
    """
    First checks .pywren_config
    then checks PYWREN_CONFIG_FILE environment variable
    then ~/.pywren_config
    """
    default_home_filename = get_default_home_filename()

    if 'PYWREN_CONFIG_FILE' in os.environ:
        config_filename = os.environ['PYWREN_CONFIG_FILE']
        # FIXME log this

    elif os.path.exists(".pywren_config"):
        config_filename = os.path.abspath('.pywren_config')

    elif os.path.exists(default_home_filename):
        config_filename = default_home_filename
    else:
        raise ValueError("could not find configuration file")

    config_data = load(config_filename)
    return config_data

basic_role_policy = {
    "Version": "2012-10-17",
    "Statement": [{
        "Sid": "",
        "Effect": "Allow",
        "Principal": {
            "Service": "lambda.amazonaws.com"
        },
        "Action": "sts:AssumeRole"}, 


    ]
}

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
            "Resource": "arn:aws:logs:us-west-2:783175685819:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:us-west-2:783175685819:log-group:/aws/lambda/*:*"
            ]
        }


]}
