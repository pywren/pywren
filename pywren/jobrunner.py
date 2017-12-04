from __future__ import print_function
import os
import base64
import shutil
import json
import sys
import time
from storage.storage import Storage # pylint: disable=relative-import


from six.moves import cPickle as pickle
from tblib import pickling_support

pickling_support.install()


def b64str_to_bytes(str_data):
    str_ascii = str_data.encode('ascii')
    byte_data = base64.b64decode(str_ascii)
    return byte_data

# initial output file in case job fails
output_dict = {'result' : None,
               'success' : False}

pickled_output = pickle.dumps(output_dict)
jobrunner_config_filename = sys.argv[1]

jobrunner_config = json.load(open(jobrunner_config_filename, 'r'))

# FIXME someday switch to storage handler
# download the func data into memory
storage_handler = Storage(jobrunner_config['storage_config'])

func_bucket = jobrunner_config['func_bucket']
func_key = jobrunner_config['func_key']

data_bucket = jobrunner_config['data_bucket']
data_key = jobrunner_config['data_key']
data_byte_range = jobrunner_config['data_byte_range']

output_bucket = jobrunner_config['output_bucket']
output_key = jobrunner_config['output_key']

## Jobrunner stats are fieldname float
jobrunner_stats_filename = jobrunner_config['stats_filename']
# open the stats filename
stats_fid = open(jobrunner_stats_filename, 'w')

def write_stat(stat, val):
    stats_fid.write("{} {:f}\n".format(stat, val))
    stats_fid.flush()

try:
    func_download_time_t1 = time.time()
    func_obj = storage_handler.get_object(func_key)
    loaded_func_all = pickle.loads(func_obj)
    func_download_time_t2 = time.time()
    write_stat('func_download_time',
               func_download_time_t2-func_download_time_t1)

    # save modules, before we unpickle actual function
    PYTHON_MODULE_PATH = jobrunner_config['python_module_path']

    shutil.rmtree(PYTHON_MODULE_PATH, True) # delete old modules
    os.mkdir(PYTHON_MODULE_PATH)
    sys.path.append(PYTHON_MODULE_PATH)

    for m_filename, m_data in loaded_func_all['module_data'].items():
        m_path = os.path.dirname(m_filename)
        if sys.platform == 'win32':
            if len(m_path) > 0 and m_path[0] == "\\":
                m_path = m_path[1:]
            # fix windows forward slash delimeter
            m_path = os.path.join([x for x in m_path.split("/") if len(x) > 0])

        else:
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

    # logger.info("Finished wrting {} module files".format(len(d['module_data'])))
    # logger.debug(subprocess.check_output("find {}".format(PYTHON_MODULE_PATH), shell=True))
    # logger.debug(subprocess.check_output("find {}".format(os.getcwd()), shell=True))


    # now unpickle function; it will expect modules to be there
    loaded_func = pickle.loads(loaded_func_all['func'])

    data_download_time_t1 = time.time()
    data_obj = storage_handler.get_object(data_key, data_byte_range)
    # FIXME make this streaming
    loaded_data = pickle.loads(data_obj)
    data_download_time_t2 = time.time()
    write_stat('data_download_time',
               data_download_time_t2-data_download_time_t1)

    #print("loaded")
    y = loaded_func(loaded_data)
    #print("success")
    output_dict = {'result' : y,
                   'success' : True,
                   'sys.path' : sys.path}
    pickled_output = pickle.dumps(output_dict)

except Exception as e:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    #traceback.print_tb(exc_traceback)

    # Shockingly often, modules like subprocess don't properly
    # call the base Exception.__init__, which results in them
    # being unpickleable. As a result, we actually wrap this in a try/catch block
    # and more-carefully handle the exceptions if any part of this save / test-reload
    # fails

    try:
        pickled_output = pickle.dumps({'result' : e,
                                       'exc_type' : exc_type,
                                       'exc_value' : exc_value,
                                       'exc_traceback' : exc_traceback,
                                       'sys.path' : sys.path,
                                       'success' : False})

        # this is just to make sure they can be unpickled
        pickle.loads(pickled_output)

    except Exception as pickle_exception:
        pickled_output = pickle.dumps({'result' : str(e),
                                       'exc_type' : str(exc_type),
                                       'exc_value' : str(exc_value),
                                       'exc_traceback' : exc_traceback,
                                       'exc_traceback_str' : str(exc_traceback),
                                       'sys.path' : sys.path,
                                       'pickle_fail' : True,
                                       'pickle_exception' : pickle_exception,
                                       'success' : False})
finally:
    output_upload_timestamp_t1 = time.time()
    storage_handler.put_data(output_key, pickled_output)
    output_upload_timestamp_t2 = time.time()
    write_stat("output_upload_time",
               output_upload_timestamp_t2 - output_upload_timestamp_t1)
