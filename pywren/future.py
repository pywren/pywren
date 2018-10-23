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

from __future__ import absolute_import
from __future__ import print_function

import logging
import time

import enum
from tblib import pickling_support

try:
    from six import reraise
    from six.moves import cPickle as pickle
except:
    import pickle

from pywren import wrenconfig
from pywren.storage import storage, storage_utils

pickling_support.install()

logger = logging.getLogger(__name__)

class JobState(enum.Enum):
    new = 1
    invoked = 2
    running = 3
    success = 4
    error = 5

class ResponseFuture:

    """
    Object representing the result of a PyWren invocation. Returns the status of the
    execution and the result when available.
    """
    GET_RESULT_SLEEP_SECS = 4
    def __init__(self, call_id, callset_id, invoke_metadata, storage_path):

        self.call_id = call_id
        self.callset_id = callset_id
        self._state = JobState.new
        self._exception = Exception()
        self._return_val = None
        self._traceback = None
        self._call_invoker_result = None

        self._invoke_metadata = invoke_metadata.copy()

        self.run_status = None
        self.invoke_status = None

        self.status_query_count = 0

        self.storage_path = storage_path

    def _set_state(self, new_state):
        ## FIXME add state machine
        self._state = new_state

    def cancel(self, storage_handler=None):
        # TODO Figure out a better way for this function to have
        # access to a custom storage handler

        if storage_handler is None:
            storage_config = wrenconfig.extract_storage_config(wrenconfig.default())
            storage_handler = storage.Storage(storage_config)

        storage_handler.put_cancelled(self.callset_id,
                                      self.call_id, "CANCEL")

    def cancelled(self):
        raise NotImplementedError("Cannot cancel dispatched jobs")

    def running(self):
        raise NotImplementedError()

    def done(self):
        if self._state in [JobState.success, JobState.error]:
            return True
        return self.result(check_only=True)

    def succeeded(self):
        return self._state == JobState.success

    def errored(self):
        return self._state == JobState.error

    def result(self, timeout=None, check_only=False,
               throw_except=True, storage_handler=None):
        """

        check_only = True implies we only check if the job is completed.

        # FIXME check_only is the worst API and should be refactored
        # out to be part of done()

        From the python docs:

        Return the value returned by the call. If the call hasn't yet
        completed then this method will wait up to timeout seconds. If
        the call hasn't completed in timeout seconds then a
        TimeoutError will be raised. timeout can be an int or float.If
        timeout is not specified or None then there is no limit to the
        wait time.

        Return the value returned by the call.
        If the call raised an exception, this method will raise the same exception
        If the future is cancelled before completing then CancelledError will be raised.

        :param timeout: This method will wait up to timeout seconds before raising
            a TimeoutError if function hasn't completed. If None, wait indefinitely. Default None.
        :param check_only: Return None immediately if job is not complete. Default False.
        :param throw_except: Reraise exception if call raised. Default true.
        :param storage_handler: Storage handler to poll cloud storage. Default None.
        :return: Result of the call.
        :raises CancelledError: If the job is cancelled before completed.
        :raises TimeoutError: If job is not complete after `timeout` seconds.

        """
        if self._state == JobState.new:
            raise ValueError("job not yet invoked")

        if check_only:
            if self._state == JobState.success or self._state == JobState.error:
                return True

        if self._state == JobState.success:
            return self._return_val

        if self._state == JobState.error:
            if throw_except:
                raise self._exception
            else:
                return None

        if storage_handler is None:
            storage_config = wrenconfig.extract_storage_config(wrenconfig.default())
            storage_handler = storage.Storage(storage_config)

        storage_utils.check_storage_path(storage_handler.get_storage_config(), self.storage_path)


        call_status = storage_handler.get_call_status(self.callset_id, self.call_id)

        self.status_query_count += 1

        ## FIXME implement timeout
        if timeout is not None:
            raise NotImplementedError()

        if check_only:
            if call_status is None:
                return False
            else:
                return True

        while call_status is None:
            time.sleep(self.GET_RESULT_SLEEP_SECS)
            call_status = storage_handler.get_call_status(self.callset_id, self.call_id)

            self.status_query_count += 1
        self._invoke_metadata['status_done_timestamp'] = time.time()
        self._invoke_metadata['status_query_count'] = self.status_query_count

        self.run_status = call_status # this is the remote status information
        self.invoke_status = self._invoke_metadata # local status information

        if call_status['exception'] is not None:
            # the wrenhandler had an exception
            exception_str = call_status['exception']

            exception_args = call_status['exception_args']
            if exception_args[0] == "WRONGVERSION":
                if throw_except:
                    raise Exception("Pywren version mismatch: remote " + \
                        "expected version {}, local library is version {}".format(
                            exception_args[2], exception_args[3]))
                return None
            elif exception_args[0] == "OUTATIME":
                if throw_except:
                    raise Exception("process ran out of time")
                return None
            elif exception_args[0] == "CANCELLED":
                if throw_except:
                    raise Exception("job was cancelled")
            elif exception_args[0] == "RETCODE":
                if throw_except:
                    raise Exception("python process failed, returned a non-zero return code"
                                    "(check stdout for information)")
                return None
            else:
                if throw_except:
                    if 'exception_traceback' in call_status:
                        logger.error(call_status['exception_traceback'])
                    raise Exception(exception_str, *exception_args)
                return None

        # FIXME this shouldn't be called if check_only is True
        call_output_time = time.time()
        call_invoker_result = pickle.loads(storage_handler.get_call_output(
            self.callset_id, self.call_id))

        call_output_time_done = time.time()
        self._invoke_metadata['download_output_time'] = call_output_time_done - call_output_time

        self._invoke_metadata['download_output_timestamp'] = call_output_time_done
        call_success = call_invoker_result['success']
        logger.info("ResponseFuture.result() {} {} call_success {}".format(self.callset_id,
                                                                           self.call_id,
                                                                           call_success))



        self._call_invoker_result = call_invoker_result

        if call_success:

            self._return_val = call_invoker_result['result']
            self._set_state(JobState.success)
            return self._return_val
        else:
            self._set_state(JobState.error)
            self._exception = call_invoker_result['result']
            self._traceback = (call_invoker_result['exc_type'],
                               call_invoker_result['exc_value'],
                               call_invoker_result['exc_traceback'])

            if throw_except:

                if call_invoker_result.get('pickle_fail', False):
                    logging.warning(
                        "there was an error pickling. The original exception: " + \
                            "{}\nThe pickling exception: {}".format(
                                call_invoker_result['exc_value'],
                                str(call_invoker_result['pickle_exception'])))

                    reraise(Exception, call_invoker_result['exc_value'],
                            call_invoker_result['exc_traceback'])
                else:
                    # reraise the exception
                    reraise(*self._traceback)
            else:
                return None  # nothing, don't raise, no value

    def exception(self, timeout=None):
        raise NotImplementedError()

    def add_done_callback(self, fn):
        raise NotImplementedError()
