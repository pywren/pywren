import pywren
import subprocess
import sys

def run_command(x):
    return subprocess.check_output(x, shell=True)

if __name__ == "__main__":
    cmd = " ".join(sys.argv[1:])

    #wrenexec = pywren.dummy_executor()
    wrenexec = pywren.default_executor()
    #wrenexec = pywren.remote_executor()
    fut = wrenexec.call_async(run_command, cmd)
    print fut.callset_id
    #wrenexec.invoker.run_jobs()

    res = fut.result() 
    print res
