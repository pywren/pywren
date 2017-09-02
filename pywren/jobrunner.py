from __future__ import print_function
import os
import base64
import json
import sys
import traceback
import boto3


from six.moves import cPickle as pickle
from tblib import pickling_support

pickling_support.install()

# def s3_url_parse(url):
#     if url[:5] != "s3://":
#         raise Exception("improperly formatted s3 url: {}".format(url))
#     bucket, key = url[5:].split("/", 1)
#     return bucket, key

def b64str_to_bytes(str_data):
    str_ascii = str_data.encode('ascii')
    byte_data = base64.b64decode(str_ascii)
    return byte_data

try:
    jobrunner_config_filename = sys.argv[1]
    out_filename = sys.argv[2]


    # initial output file in case job fails
    pickle.dump({'result' : None,
                 'success' : False},
                open(out_filename, 'wb'), -1)
    jobrunner_config = json.load(open(jobrunner_config_filename,
                                      'r'))


    # FIXME someday switch to storage handler
    # download the func data into memory
    s3_client = boto3.client("s3")
    func_bucket, func_key = jobrunner_config['func_bucket'], jobrunner_config['func_key']
    data_bucket, data_key = jobrunner_config['data_bucket'], jobrunner_config['data_key']

    data_byte_range = jobrunner_config['data_byte_range']
    func_obj_stream = s3_client.get_object(Bucket=func_bucket, Key=func_key)
    loaded_func_all = pickle.loads(func_obj_stream['Body'].read())

    # save modules, before we unpickle actual function
    PYTHON_MODULE_PATH = jobrunner_config['python_module_path']
    for m_filename, m_data in loaded_func_all['module_data'].items():
        m_path = os.path.dirname(m_filename)

        if len(m_path) > 0 and m_path[0] == "/":
            m_path = m_path[1:]
        to_make = os.path.join(PYTHON_MODULE_PATH, m_path)
        try:
            os.makedirs(to_make)
        except OSError as e:
            if e.errno == 17:
                pass
            else:
                raise e
        full_filename = os.path.join(to_make, os.path.basename(m_filename))
        #print "creating", full_filename
        with open(full_filename, 'wb') as fid:
            fid.write(b64str_to_bytes(m_data))

    # logger.info("Finished writing {} module files".format(len(d['module_data'])))
    # logger.debug(subprocess.check_output("find {}".format(PYTHON_MODULE_PATH), shell=True))
    # logger.debug(subprocess.check_output("find {}".format(os.getcwd()), shell=True))


    # now unpickle function; it will expect modules to be there
    loaded_func = pickle.loads(loaded_func_all['func'])

    extra_get_args = {}
    if data_byte_range is not None:
        range_str = 'bytes={}-{}'.format(*data_byte_range)
        extra_get_args['Range'] = range_str
    data_obj_stream = s3_client.get_object(Bucket=data_bucket,
                                           Key=data_key, **extra_get_args)
    # FIXME make this streaming
    loaded_data = pickle.loads(data_obj_stream['Body'].read())

    print("loaded")
    y = loaded_func(loaded_data)
    print("success")
    pickle.dump({'result' : y,
                 'success' : True,
                 'sys.path' : sys.path},
                open(out_filename, 'wb'), -1)


except Exception as e:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_tb(exc_traceback)

    # Shockingly often, modules like subprocess don't properly
    # call the base Exception.__init__, which results in them
    # being unpickleable. As a result, we actually wrap this in a try/catch block
    # and more-carefully handle the exceptions if any part of this save / test-reload
    # fails

    try:
        with  open(out_filename, 'wb') as fid:
            pickle.dump({'result' : e,
                         'exc_type' : exc_type,
                         'exc_value' : exc_value,
                         'exc_traceback' : exc_traceback,
                         'sys.path' : sys.path,
                         'success' : False}, fid, -1)

        # this is just to make sure they can be unpickled
        pickle.load(open(out_filename, 'rb'))

    except Exception as pickle_exception:
        pickle.dump({'result' : str(e),
                     'exc_type' : str(exc_type),
                     'exc_value' : str(exc_value),
                     'exc_traceback' : exc_traceback,
                     'exc_traceback_str' : str(exc_traceback),
                     'sys.path' : sys.path,
                     'pickle_fail' : True,
                     'pickle_exception' : pickle_exception,
                     'success' : False},
                    open(out_filename, 'wb'), -1)
