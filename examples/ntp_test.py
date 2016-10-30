import pywren
import subprocess
import sys
import ntplib
import cPickle as pickle


def get_offset(x):
    r = subprocess.check_output("/sbin/ifconfig")



    c = ntplib.NTPClient()

    responses = [c.request('{}.amazon.pool.ntp.org'.format(i), 
                          version=3).offset for i in range(4)]

    return responses, r


if __name__ == "__main__":

    wrenexec = pywren.default_executor()
    futures = wrenexec.map(get_offset, range(20))
    res = [f.result() for f in futures]
    pickle.dump(res, open('ntp_test.pickle', 'w'))

    #print res[1]
