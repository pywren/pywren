import uuid
import os
import shutil
import base64
import glob
import hashlib

def uuid_str():
    return str(uuid.uuid4())


SOURCE_DIR = os.path.dirname(os.path.abspath(__file__)) 

class WrappedStreamingBody:
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
        #print("In tell()")
        
        return self.pos

    def readline(self):
        #print("Calling readline()")
        
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
        #print("Calling seek()")                          
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
    return {c['Name'] : c['Value'] for c in attr }

def bytes_to_b64str(byte_data):
    byte_data_64 = base64.b64encode(byte_data)
    byte_data_64_ascii = byte_data_64.decode('ascii')
    return byte_data_64_ascii


def b64str_to_bytes(str_data):
    str_ascii = str_data.encode('ascii')
    byte_data= base64.b64decode(str_ascii)
    return byte_data

def split_s3_url(s3_url):
    if s3_url[:5] != "s3://":
        raise ValueError("URL {} is not valid".format(s3_url))
    
    
    splits = s3_url[5:].split("/")
    bucket_name = splits[0]
    key = "/".join(splits[1:])
    return bucket_name, key

def hash_s3_key(s):
    """
    MD5-hash the contents of an S3 key to enable good partitioning. 
    used for sharding the runtimes
    """
    DIGEST_LEN = 6
    m = hashlib.md5()
    m.update(s.encode('ascii'))
    digest = m.hexdigest()
    return "{}-{}".format(digest[:DIGEST_LEN], s)

def get_s3_shard(key, shard_num):
    return "{}.{:04d}".format(key, shard_num)
