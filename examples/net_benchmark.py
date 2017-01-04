"""
Benchmark microtransactions for s3 -- each thread tries to read and write
a ton of little objects and logging what values are read. 

"""

import time
import numpy as np
import time
import pywren
import subprocess
import logging
import sys
import boto3
import hashlib
import cPickle as pickle
import uuid
import click
import exampleutils
import ntplib
import exampleutils
import requests
import socket

import SocketServer

class MyTCPHandler(SocketServer.BaseRequestHandler):
    """
    The request handler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def handle(self):
        # self.request is the TCP socket connected to the client
        self.data = self.request.recv(16).strip()
        print "{} wrote:".format(self.client_address[0])
        print self.data
        # just send back the same data, but upper-cased
        self.request.sendall(self.data.upper())

NTP_SERVER = 'ntp1.net.berkeley.edu'

OBJ_METADATA_KEY = "benchmark_metadata"
domain_name = "test-domain" 

PORT = 9999

@click.command()
@click.option('--bucket_name', default=None,  help='bucket to save files in')
@click.option('--key_prefix', default='', help='S3 key prefix')
@click.option('--workers', default=10, help='how many workers', type=int)
@click.option('--outfile', default='net_benchmark.results.pickle', 
              help='filename to save results in')
@click.option('--region', default='us-west-2', help="AWS Region")
@click.option('--begin_delay', default=0, help="start delay ")
def benchmark(bucket_name, key_prefix, workers, 
              outfile, region, begin_delay):

    start_time = time.time()
    print "bucket_name =", bucket_name

    host_start_time = time.time()
    wait_until = host_start_time + begin_delay



    item_name = "net_benchmark_test_" + str(uuid.uuid4().get_hex().upper())

    def run_command(isServer):
        # get timing offset
        timing_offsets = exampleutils.get_time_offset(NTP_SERVER, 4)

        # first pause (for sync)
        sleep_duration = wait_until - time.time()
        if sleep_duration > 0:
            time.sleep(sleep_duration)

        # start the job
        job_start = time.time()
        sdb_client = boto3.client('sdb', region)

        
        #response2 = requests.get("http://169.254.169.254/latest/meta-data/local-hostname")
        # s = socket.socket(
        #     socket.AF_INET, socket.SOCK_STREAM)
        # #now connect to the web server on port 80
        # # - the normal http port
        # s.connect(("www.berkeley.edu", 80))
        # s.send('GET /')
        # response = s.recv(4096)
        if isServer:

            hw_addr, ip_addrs = exampleutils.get_ifconfig()
            
            write_dict = {'hw_addr' : hw_addr, 
                          'ip' : ip_addrs[1]}
            
    
            write_attr = exampleutils.dict_to_sdb_attr(write_dict, True)
            response = sdb_client.put_attributes(DomainName = domain_name, 
                                                 ItemName = item_name, 
                                                 Attributes = write_attr)
            
            server = SocketServer.TCPServer((ip_addrs[1], PORT), MyTCPHandler)
            server.handle_request() # I think this just handles one 
            response =  "handled one?" 
            
        else:
            server_ip = None
            response = ""
            
            while server_ip is None:
                resp = sdb_client.get_attributes(
                    DomainName=domain_name,
                    ItemName=item_name,
                    AttributeNames=['hw_addr', 'ip'],
                    ConsistentRead=True)

                if 'Attributes' in resp:
                    a = exampleutils.sdb_attr_to_dict(resp['Attributes'])        
                    server_ip = a['ip']
                    response = "server IP is {}".format(server_ip)
            time.sleep(4)
            s = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            s.connect((server_ip, PORT))
            s.send("hello world this is a bunch of text") 
            response = s.recv(16)

        job_end = time.time()


        return {'response' : response}


    print "starting transactions"


    wrenexec = pywren.default_executor()

    fut = wrenexec.map(run_command, [False, True]) # range(workers))
    print "launch took {:3.2f} sec".format(time.time()-host_start_time)
    
    # local_sdb_client =  boto3.client('sdb', region)

    # for i in range(1):
    #     resp = local_sdb_client.get_attributes(
    #         DomainName=domain_name,
    #         ItemName=item_name,
    #         AttributeNames=['hw_addr', 'ip'],
    #         ConsistentRead=True)
    #     print resp
    #     if 'Attributes' in resp:
    #         print "WHEEE", resp['Attributes']
    #     time.sleep(1)



    res = [f.result(throw_except=False) for f in fut]
    print res
    pickle.dump({
        'host_start_time' : host_start_time, 
        'begin_delay' : begin_delay, 
        'workers' : workers, 
        'res' : res}, 
                open(outfile, 'w'))

if __name__ == '__main__':
    benchmark()


