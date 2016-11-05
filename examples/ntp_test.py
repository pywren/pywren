import pywren
import subprocess
import sys
import ntplib
import cPickle as pickle
import time
import re

def get_ifconfig_hwaddr(s):
    a = re.search(r'.+?(HWaddr\s+(?P<hardware_address>\S+))', s, re.MULTILINE).groupdict('')
    return a['hardware_address']


def get_offset(x):

    hwaddr = exputil.get_hwaddr()

    server_list = ['{}.amazon.pool.ntp.org'.format(i) for i in range(4)]
    server_list.append('time.mit.edu')
    server_list.append('ntp1.net.berkeley.edu')
    server_list.append('ntp2.net.berkeley.edu')

    c = ntplib.NTPClient()
    
    responses = []
    for i in range(8):
        for s in server_list:
            offset = None
            delay = None
            try:
                r = c.request(s, version=3)
                offset = r.offset
                delay = r.delay
            except ntplib.NTPException:
                pass
            
            responses.append({'iter' : i, 
                              'server' : s, 
                              'offset' : offset, 
                              'delay' : delay, 
                              'localtime' : time.time(), 
                              'hwaddr' : hwaddr})
                             
            time.sleep(1)


    return responses


if __name__ == "__main__":

    wrenexec = pywren.default_executor()
    futures = wrenexec.map(get_offset, range(100))
    res = [f.result() for f in futures]
    pickle.dump(res, open('ntp_test.pickle', 'w'))

    #print res[1]
