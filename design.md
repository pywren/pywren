

# Basic design for new S3 version

1. invoke a lambda function 
2. Download our environment and run there
3. Check if object exists in s3 and if it does, download and unpickle. If
it has been canceled, then abort 
4. INvoke the job
5. Take the results and put them back in S3
6. Retrun 




