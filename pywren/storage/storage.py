from s3_service import S3Service

def get_storage(config):
    '''
    Get the function service based on configuration.
    Currently we only support AWS Lambda.
    Google Cloud Functions and Azure are to be supported in the future.
    :return: A handler for the function service.
    '''
    return S3Service(config)


