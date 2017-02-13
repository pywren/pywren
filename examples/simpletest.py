#from gevent import monkey

#monkey.patch_socket()
#monkey.patch_ssl()

import pywren

if __name__ == "__main__":
    import logging
    #logging.basicConfig(level=logging.DEBUG)

    # fh = logging.FileHandler('simpletest.log')
    # fh.setLevel(logging.DEBUG)
    # fh.setFormatter(pywren.wren.formatter)
    # pywren.wren.logger.addHandler(fh)

    def test_add(x):
        return x + 7

    wrenexec = pywren.default_executor()
    fut = wrenexec.call_async(test_add, 10)
    print(fut.callset_id)
    
    res = fut.result() 
    print("cached=", fut.run_status['runtime_cached'])
    print(res)


