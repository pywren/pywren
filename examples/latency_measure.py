import pywren
import time

if __name__ == "__main__":

    def test_add(x):
        return x + 7

    wrenexec = pywren.default_executor()
    t1 = time.time()
    fut = wrenexec.map(test_add, [10], use_cached_runtime=False)[0]
    res = fut.result() 
    
    #print "cached=", fut._run_status['runtime_cached']
    assert res == 17
    t2 = time.time()
    print "runtime was cached: ", fut._run_status['runtime_cached']
    print "no cache, invoke latency= {:3.2f}s".format(t2-t1)



    t1 = time.time()
    fut = wrenexec.map(test_add, [10], use_cached_runtime=True)[0]
    res = fut.result() 
    

    assert res == 17
    t2 = time.time()
    print "runtime was cached: ", fut._run_status['runtime_cached']
    print "with cache, invoke latency= {:3.2f}s".format(t2-t1)



