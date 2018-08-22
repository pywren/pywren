#
# Copyright 2018 PyWren Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import base64
import datetime
import hashlib
import hmac
import os
import requests
import struct
import sys
import uuid


def uuid_str():
    return str(uuid.uuid4())


def create_callset_id():
    return uuid_str()


def create_call_id():
    return uuid_str()

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))

class WrappedStreamingBody(object):
    """
    Wrap boto3's StreamingBody object to provide enough Python fileobj functionality
    so that tar/gz can happen in memory

    from https://gist.github.com/debedb/2e5cbeb54e43f031eaf0

    """
    def __init__(self, sb, size):
        # The StreamingBody we're wrapping
        self.sb = sb
        # Initial position

        self.pos = 0
        # Size of the object

        self.size = size

    def tell(self):
        # print("In tell()")

        return self.pos

    def readline(self):
        # print("Calling readline()")

        try:
            retval = self.sb.readline()
        except struct.error:
            raise EOFError()
        self.pos += len(retval)
        return retval


    def read(self, n=None):
        retval = self.sb.read(n)
        if retval == "":
            raise EOFError()
        self.pos += len(retval)
        return retval

    def seek(self, offset, whence=0):
        # print("Calling seek()")
        retval = self.pos
        if whence == 2:
            if offset == 0:
                retval = self.size
            else:
                raise Exception("Unsupported")
        else:
            if whence == 1:
                offset = self.pos + offset
                if offset > self.size:
                    retval = self.size
                else:
                    retval = offset
        # print("In seek(%s, %s): %s, size is %s" % (offset, whence, retval, self.size))

        self.pos = retval
        return retval

    def __str__(self):
        return "WrappedBody"

    def __getattr__(self, attr):
        # print("Calling %s"  % attr)

        if attr == 'tell':
            return self.tell
        elif attr == 'seek':
            return self.seek
        elif attr == 'read':
            return self.read
        elif attr == 'readline':
            return self.readline
        elif attr == '__str__':
            return self.__str__
        else:
            return getattr(self.sb, attr)



def sdb_to_dict(item):
    attr = item['Attributes']
    return {c['Name'] : c['Value'] for c in attr}

def bytes_to_b64str(byte_data):
    byte_data_64 = base64.b64encode(byte_data)
    byte_data_64_ascii = byte_data_64.decode('ascii')
    return byte_data_64_ascii


def b64str_to_bytes(str_data):
    str_ascii = str_data.encode('ascii')
    byte_data = base64.b64decode(str_ascii)
    return byte_data

def split_s3_url(s3_url):
    if s3_url[:5] != "s3://":
        raise ValueError("URL {} is not valid".format(s3_url))


    splits = s3_url[5:].split("/")
    bucket_name = splits[0]
    key = "/".join(splits[1:])
    return bucket_name, key

def create_request_string(region, function_name, request_parameters):

    #
    # https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
    #
    # ************* TASK 1: CREATE THE REQUEST*************
    method = 'POST'
    service = 'lambda'
    host = 'lambda.us-west-2.amazonaws.com'.replace('us-west-2', region)
    endpoint = 'https://' + host
    path = '/2015-03-31/functions/some-function/invocations'.replace('some-function', function_name)
    request_url = endpoint + path

    # the content is JSON.
    content_type = 'application/x-amz-json-1.0'

    # Read AWS access key from env. variables or configuration file. Best practice is NOT
    # to embed credentials in code.

    access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    if access_key is None or secret_key is None:
        print 'No access key is available.'
        sys.exit()

    # Create a date for headers and the credential string
    t = datetime.datetime.utcnow()
    amz_date = t.strftime('%Y%m%dT%H%M%SZ')
    date_stamp = t.strftime('%Y%m%d') # Date w/o time, used in credential scope

    canonical_uri = path
    canonical_querystring = ''
    canonical_headers = 'content-type:' + content_type + '\n' \
                        + 'host:' + host + '\n' \
                        + 'x-amz-date:' + amz_date + '\n'
    signed_headers = 'content-type;host;x-amz-date'


    # Create payload hash. In this example, the payload (body of
    # the request) contains the request parameters.
    payload_hash = hashlib.sha256(request_parameters).hexdigest()

    # Combine elements to create create canonical request
    canonical_request = method + '\n' \
                        + canonical_uri + '\n' \
                        + canonical_querystring + '\n' \
                        + canonical_headers + '\n' \
                        + signed_headers + '\n' \
                        + payload_hash

    # ************* TASK 2: CREATE THE STRING TO SIGN*************
    # Match the algorithm to the hashing algorithm you use, either SHA-1 or
    # SHA-256 (recommended)
    algorithm = 'AWS4-HMAC-SHA256'
    credential_scope = date_stamp + '/' + region + '/' + service + '/' + 'aws4_request'
    string_to_sign = algorithm + '\n' +  amz_date + '\n' +  credential_scope + '\n' +  hashlib.sha256(canonical_request).hexdigest()


    # ************* TASK 3: CALCULATE THE SIGNATURE *************
    # Create the signing key using the function defined above.


    # Key derivation functions. See:
    # http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-python
    def sign(key, msg):
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    def getSignatureKey(key, date_stamp, regionName, serviceName):
        kDate = sign(('AWS4' + key).encode('utf-8'), date_stamp)
        kRegion = sign(kDate, regionName)
        kService = sign(kRegion, serviceName)
        kSigning = sign(kService, 'aws4_request')
        return kSigning


    signing_key = getSignatureKey(secret_key, date_stamp, region, service)

    # Sign the string_to_sign using the signing_key
    signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()


    # ************* TASK 4: ADD SIGNING INFORMATION TO THE REQUEST *************
    # Put the signature information in a header named Authorization.
    authorization_header = algorithm + ' ' \
                           + 'Credential=' + access_key + '/' \
                           + credential_scope + ', ' \
                           +  'SignedHeaders=' + signed_headers + ', ' \
                           + 'Signature=' + signature

    headers = {'authorization':authorization_header,
               'content-type':content_type,
               'host':host,
               'x-amz-date':amz_date}


    # ************* TASK 5: CONVERT REQUEST TO STRING *************
    # Put the signature information in a header named Authorization.

    req = requests.Request('POST', request_url, data=request_parameters, headers=headers)
    prepped = req.prepare()

    request_str = ""
    request_str += method + " " + path + "?" + " " + "HTTP/1.1" + "\r\n"
    for header in sorted(prepped.headers.keys()):
        #print header
        request_str += header + ":" + prepped.headers[header] + "\r\n"
    request_str += "\r\n"
    request_str += request_parameters

    return request_str
