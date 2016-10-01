
FUNCTION_NAME = "pywren1"
HANDLER_NAME = "wrenhandler.handler"

PACKAGE_FILE = "deploy.zip"
AWS_REGION ='us-west-2'
MEMORY = 512 * 3
TIMEOUT = 300
AWS_ACCOUNT_ID = 783175685819
AWS_ROLE = "helloworld_exec_role"

ROLE = "arn:aws:iam::{}:role/{}".format(AWS_ACCOUNT_ID, AWS_ROLE)

AWS_SDB_DOMAIN = "test_two"
