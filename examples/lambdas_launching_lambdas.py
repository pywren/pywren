import pywren
import time
import cPickle as pickle
import boto3
import re
import subprocess

"""
Error log

Exception: An error occurred (AccessDeniedException) when calling the Invoke operation: User: arn:aws:sts::783175685819:assumed-role/pywren_exec_role_1/pywren_1 is not authorized to perform: lambda:InvokeFunction on resource: arn:aws:lambda:us-west-2:783175685819:function:pywren_1


"""

def parse_ifconfig_hwaddr(s):

    a = re.search(r'.+?(HWaddr\s+(?P<hardware_address>\S+))', s, re.MULTILINE).groupdict('')
    return a['hardware_address']

def get_hwaddr():
    ifconfig_data = subprocess.check_output("/sbin/ifconfig")
    hwaddr = parse_ifconfig_hwaddr(ifconfig_data)
    return hwaddr


if __name__ == "__main__":

    SLEEP_DURATION=50
    SOURCE_SLEEP_DURATION=50


    AMPLIFICATION_FACTOR=0
    LAUNCH_NUMBER = 2800

    config = pywren.wrenconfig.default()

    def invoke_test(number_to_invoke):
        lexec = pywren.lambda_executor(config)
        invoke_addr = get_hwaddr()

        def bar(y):
            run_addr = get_hwaddr()
            time.sleep(SLEEP_DURATION)
            return y + 20, invoke_addr, run_addr
        
        if number_to_invoke > 0:
            response_futures = lexec.map(bar, range(number_to_invoke))
        else:
            response_futures = []

        time.sleep(SOURCE_SLEEP_DURATION)

        return response_futures, invoke_addr
    

    wrenexec = pywren.default_executor()
    t1 = time.time()
    futures = wrenexec.map(invoke_test, [AMPLIFICATION_FACTOR] * LAUNCH_NUMBER)

    s3_client = boto3.client('s3', 'us-west-2')

    pywren.wait(futures, s3_client=s3_client)

    results = [f.result() for f in futures]
    run_statuses = [f.run_status for f in futures]
    invoke_statuses = [f.invoke_status for f in futures]
    t2 = time.time()

    remote_results = []
    remote_run_statuses = []
    remote_invoke_statuses = []
    remote_result_local_future_list = []
    # each result should be a future! 
    for fi, local_future in enumerate(futures):
        remote_futures, invoke_addr = local_future.result()
        pywren.wait(remote_futures, s3_client = s3_client)
        for f in remote_futures:
            remote_results.append(f.result())
            remote_result_local_future_list.append(fi)

            remote_run_statuses.append(f.run_status)
            remote_invoke_statuses.append(f.invoke_status)
    t3 = time.time()
    
    outdict = {'results' : results, 
               'run_statuses' : run_statuses, 
               'invoke_statuses' : invoke_statuses, 
               'remote_invoke_statuses' : remote_invoke_statuses, 
               'remote_results' : remote_results, 
               'remote_run_statuses' : remote_run_statuses, 
               'remote_results_local_invoker' : remote_result_local_future_list, 
               'LAUNCH_NUMBER' : LAUNCH_NUMBER, 
               'AMPLIFICATION_FACTOR' : AMPLIFICATION_FACTOR, 
               'launch_time' : t2-t1, 
               'retrieve_time' : t3-t2}

    
    pickle.dump(outdict, open("lll.pickle", 'w'))


