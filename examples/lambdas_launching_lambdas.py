import pywren
import time
import cPickle as pickle
import boto3

"""
Error log

Exception: An error occurred (AccessDeniedException) when calling the Invoke operation: User: arn:aws:sts::783175685819:assumed-role/pywren_exec_role_1/pywren_1 is not authorized to perform: lambda:InvokeFunction on resource: arn:aws:lambda:us-west-2:783175685819:function:pywren_1


"""

if __name__ == "__main__":

    SLEEP_DURATION=20

    N = 10

    config = pywren.wrenconfig.default()

    def invoke_test(number_to_invoke):
        lexec = pywren.lambda_executor(config)
        def bar(y):
            return y + 20

        response_futures = lexec.map(bar, range(number_to_invoke))
        
        return response_futures


    wrenexec = pywren.default_executor()
    futures = wrenexec.map(invoke_test, [N])

    s3_client = boto3.client('s3', 'us-west-2')

    pywren.wait(futures, s3_client=s3_client)

    results = [f.result() for f in futures]
    run_statuses = [f.run_status for f in futures]
    invoke_statuses = [f.invoke_status for f in futures]
    
    remote_results = []
    remote_run_statuses = []
    remote_invoke_statuses = []
    remote_result_local_future_list = []
    # each result should be a future! 
    for fi, local_future in enumerate(futures):
        remote_futures = local_future.result()
        for f in remote_futures:
            remote_results.append(f.result())
            remote_result_local_future_list.append(fi)

            remote_run_statuses.append(f.run_status)
            remote_invoke_statuses.append(f.invoke_status)

    
    outdict = {'results' : results, 
               'run_statuses' : run_statuses, 
               'invoke_statuses' : invoke_statuses, 
               'remote_invoke_statuses' : remote_invoke_statuses, 
               'remote_results' : remote_results, 
               'remote_run_statuses' : remote_run_statuses, 
               'N' : N}

    
    pickle.dump(outdict, open("lll.pickle", 'w'))


