from __future__ import absolute_import

import random
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


    # These are performance-related settings that we may eventually
    # want to expose to end users:
    MAX_DIRECT_QUERY_N = 16
    RETURN_EARLY_N = 16

    if return_when == ALL_COMPLETED:
        result_count = 0
        while result_count < N:

            fs_dones, fs_notdones = _wait(fs, RETURN_EARLY_N,
                                          MAX_DIRECT_QUERY_N,
                                          THREADPOOL_SIZE)
            result_count = len(fs_dones)

            if result_count == N:
                return fs_dones, fs_notdones
            else:
                time.sleep(WAIT_DUR_SEC)

    elif return_when == ANY_COMPLETED:
        while True:
            fs_dones, fs_notdones = _wait(fs, RETURN_EARLY_N,
                                          MAX_DIRECT_QUERY_N,
                                          THREADPOOL_SIZE)

            if len(fs_dones) != 0:
                return fs_dones, fs_notdones
            else:
                time.sleep(WAIT_DUR_SEC)

    elif return_when == ALWAYS:
        return _wait(fs, RETURN_EARLY_N,
                     MAX_DIRECT_QUERY_N,
                     THREADPOOL_SIZE)
    else:
        raise ValueError()

def _wait(fs, return_early_n, max_direct_query_n,
          random_query=False, THREADPOOL_SIZE=16):
    """
    internal function that performs the majority of the WAIT task
    work.

    For the list of futures fn, we will check at a minimum `max_direct_query_n`
    futures at least once. Internally we :
    1. use list() to quickly get a list of which ones are done (but
    list can be behind due to eventual consistency issues)
    2. then individually call get_status on at most `max_direct_query_n` returning
       early if we have found at least `return_early_n`

    This can mitigate the stragglers.

    random_query decides whether we get the fs in the order they are presented
    or in a random order.
    """


    # get all the futures that are not yet done
    not_done_futures = [f for f in fs if f._state not in [JobState.success,
                                                          JobState.error]]
    if len(not_done_futures) == 0:
        return fs, []


    storage_config = wrenconfig.extract_storage_config(wrenconfig.default())
    storage_handler = storage.Storage(storage_config)

    ### Callset optimization via object store convenience functions:
    # check if the not-done ones have the same callset_id
    present_callsets = set([f.callset_id for f in not_done_futures])
    if len(present_callsets) > 1:
        raise NotImplementedError()

    # get the list of all objects in this callset
    callset_id = present_callsets.pop() # FIXME assume only one

    # note this returns everything done, so we have to figure out
    # the intersection of those that are done
    callids_done_in_callset = set(storage_handler.get_callset_status(callset_id))

    not_done_call_ids = set([f.call_id for f in not_done_futures])

    done_call_ids = not_done_call_ids.intersection(callids_done_in_callset)
    not_done_call_ids = not_done_call_ids - done_call_ids

    still_not_done_futures = [f for f in not_done_futures if (f.call_id in not_done_call_ids)]

    def fetch_future_status(f):
        return storage_handler.get_call_status(f.callset_id, f.call_id)


    pool = ThreadPool(THREADPOOL_SIZE)

    # now try up to max_direct_query_n direct status queries, quitting once
    # we have return_n done.
    query_count = 0
    max_queries = min(max_direct_query_n, len(still_not_done_futures))

    if random_query:
        random.shuffle(still_not_done_futures)

    while query_count < max_queries:

        if len(done_call_ids) >= return_early_n:
            break
        num_to_query_at_once = THREADPOOL_SIZE
        fs_to_query = still_not_done_futures[query_count:query_count + num_to_query_at_once]

        fs_statuses = pool.map(fetch_future_status, fs_to_query)

        callids_found = [fs_to_query[i].call_id for i in range(len(fs_to_query))
                         if (fs_statuses[i] is not None)]
        done_call_ids = done_call_ids.union(set(callids_found))

        # # update done call_ids
        # callids_done.update(callids_found)

        # # break if not all N tasks completed
        # if (len(callids_found) < len(fs_samples)):
        #     break
        # # calculate new still_not_done_futures
        # still_not_done_futures = [f for f in not_done_futures if (f.call_id not in callids_done)]
        query_count += len(fs_to_query)


    # now we walk through all the original queries and get
    # the ones that are actually done.
    fs_dones = []
    fs_notdones = []

    f_to_wait_on = []
    for f in fs:
        if f._state in [JobState.success, JobState.error]:
            # done, don't need to do anything
            fs_dones.append(f)
        else:
            if f.call_id in done_call_ids:
                f_to_wait_on.append(f)
                fs_dones.append(f)
            else:
                fs_notdones.append(f)
    def get_result(f):
        f.result(throw_except=False, storage_handler=storage_handler)

    pool.map(get_result, f_to_wait_on)

    pool.close()
    pool.join()

    return fs_dones, fs_notdones
