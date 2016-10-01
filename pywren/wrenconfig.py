
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

