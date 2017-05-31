from __future__ import absolute_import

try:
    from six.moves import cPickle as pickle
except:
    import pickle
from tblib import pickling_support
import time
from multiprocessing.pool import ThreadPool
pickling_support.install()

from pywren.future import JobState
import pywren.storage as storage

ALL_COMPLETED = 1
ANY_COMPLETED = 2
ALWAYS = 3

def wait(fs, return_when=ALL_COMPLETED, THREADPOOL_SIZE=64,
            WAIT_DUR_SEC=5):
    """
    this will eventually provide an optimization for checking if a large
    number of futures have completed without too much network traffic
    by exploiting the callset

    From python docs:

    Wait for the Future instances (possibly created by different Executor
    instances) given by fs to complete. Returns a named 2-tuple of
    sets. The first set, named "done", contains the futures that completed
    (finished or were cancelled) before the wait completed. The second
    set, named "not_done", contains uncompleted futures.


    http://pythonhosted.org/futures/#concurrent.futures.wait

    """
    N = len(fs)

    if return_when==ALL_COMPLETED:
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
            fs_success, fs_running, fs_failed = _wait_status(fs, THREADPOOL_SIZE)

            if len(fs_success + fs_failed) != 0:
                return fs_success, fs_running, fs_failed
            else:
                time.sleep(WAIT_DUR_SEC)

    elif return_when == ALWAYS:
        return _wait(fs, THREADPOOL_SIZE)
    else:
        raise ValueError()


def _wait(fs, THREADPOOL_SIZE):
    fs_success, fs_running, fs_failed = _wait_status(fs, THREADPOOL_SIZE)
    return fs_success + fs_failed, fs_running

def _wait_status(fs, THREADPOOL_SIZE):
    """
    internal function that performs the majority of the WAIT task
    work.
    """


    # get all the futures that are not yet done
    not_done_futures =  [f for f in fs if f._state not in [JobState.success,
                                                           JobState.error]]
    if len(not_done_futures) == 0:
        return fs, [], []

    # check if the not-done ones have the same callset_id
    present_callsets = set([f.callset_id for f in not_done_futures])
    if len(present_callsets) > 1:
        raise NotImplementedError()

    # get the list of all objects in this callset
    callset_id = present_callsets.pop() # FIXME assume only one
    f0 = not_done_futures[0] # This is a hack too

    storage_handler = storage.Storage(f0.storage_config)
    succeded_calls, other_calls_attempts = storage_handler.get_callset_status(callset_id)

    succeded_calls = set(succeded_calls)

    fs_success = []
    fs_failed = []
    fs_running = []

    f_to_wait_on = []
    for f in fs:
        if f._state == JobState.success:
            # done, don't need to do anything
            if f._state == JobState.success:
                fs_success.append(f)
        else: # not checked by results yet
            if f.call_id in succeded_calls:
                f_to_wait_on.append(f)
                fs_success.append(f)
            elif f.call_id in other_calls_attempts:
                n_done = other_calls_attempts[f.call_id]
                if f.attempts_made <= n_done:
                    fs_failed.append(f)
                else:
                    fs_running.append(f)
            else: # not found
                fs_running.append(f)
    def test(f):
        f.result(throw_except=False, storage_handler=storage_handler)
    pool = ThreadPool(THREADPOOL_SIZE)
    ids = [f.call_id for f in f_to_wait_on]
    #print "ids: ", ids
    pool.map(test, f_to_wait_on)

    pool.close()
    pool.join()

    # print "start"
    # print fs_success
    # print fs_running
    # print fs_failed
    # print "end"
    assert(len(fs_success + fs_running + fs_failed) == len(fs))

    return fs_success, fs_running, fs_failed

