from __future__ import absolute_import

import time
from multiprocessing.pool import ThreadPool

from pywren.future import JobState
import pywren.storage as storage
import pywren.wrenconfig as wrenconfig

ALL_COMPLETED = 1
ANY_COMPLETED = 2
ALWAYS = 3

def wait(fs, return_when=ALL_COMPLETED, THREADPOOL_SIZE=64,
         WAIT_DUR_SEC=5):
    """
    Wait for the Future instances `fs` to complete. Returns a 2-tuple of
    lists. The first list contains the futures that completed
    (finished or cancelled) before the wait completed. The second
    contains uncompleted futures.

    :param fs: A list of futures.
    :param return_when: One of `ALL_COMPLETED`, `ANY_COMPLETED`, `ALWAYS`
    :param THREADPOOL_SIZE: Number of threads to use. Default 64
    :param WAIT_DUR_SEC: Time interval between each check.
    :return: `(fs_dones, fs_notdones)` where `fs_dones` is a list of futures that have completed, and `fs_notdones` is a list of futures that have not completed.
    :rtype: 2-tuple of lists
    
    Usage 
      >>> futures = pwex.map(foo, data)
      >>> dones, not_dones = wait(futures, ALL_COMPLETED)
      >>> # not_dones should be an empty list.
      >>> results = [f.result() for f in dones]

    """

    #FIXME:  this will eventually provide an optimization for checking if a large
    # number of futures have completed without too much network traffic
    # by exploiting the callset

    N = len(fs)

    if return_when == ALL_COMPLETED:
        result_count = 0
        while result_count < N:

            fs_dones, fs_notdones = _wait(fs, THREADPOOL_SIZE)
            result_count = len(fs_dones)

            if result_count == N:
                return fs_dones, fs_notdones
            else:
                time.sleep(WAIT_DUR_SEC)

    elif return_when == ANY_COMPLETED:
        while True:
            fs_dones, fs_notdones = _wait(fs, THREADPOOL_SIZE)

            if len(fs_dones) != 0:
                return fs_dones, fs_notdones
            else:
                time.sleep(WAIT_DUR_SEC)

    elif return_when == ALWAYS:
        return _wait(fs, THREADPOOL_SIZE)
    else:
        raise ValueError()

def _wait(fs, THREADPOOL_SIZE):
    """
    internal function that performs the majority of the WAIT task
    work.
    """


    # get all the futures that are not yet done
    not_done_futures = [f for f in fs if f._state not in [JobState.success,
                                                          JobState.error]]
    if len(not_done_futures) == 0:
        return fs, []

    # check if the not-done ones have the same callset_id
    present_callsets = set([f.callset_id for f in not_done_futures])
    if len(present_callsets) > 1:
        raise NotImplementedError()

    # get the list of all objects in this callset
    callset_id = present_callsets.pop() # FIXME assume only one

    storage_config = wrenconfig.extract_storage_config(wrenconfig.default())
    storage_handler = storage.Storage(storage_config)
    callids_done = storage_handler.get_callset_status(callset_id)

    callids_done = set(callids_done)

    fs_dones = []
    fs_notdones = []

    f_to_wait_on = []
    for f in fs:
        if f._state in [JobState.success, JobState.error]:
            # done, don't need to do anything
            fs_dones.append(f)
        else:
            if f.call_id in callids_done:
                f_to_wait_on.append(f)
                fs_dones.append(f)
            else:
                fs_notdones.append(f)
    def test(f):
        f.result(throw_except=False, storage_handler=storage_handler)
    pool = ThreadPool(THREADPOOL_SIZE)
    pool.map(test, f_to_wait_on)

    pool.close()
    pool.join()

    return fs_dones, fs_notdones
