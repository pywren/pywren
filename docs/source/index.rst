Documentation
=============

Executor
---------
The primary object in pywren is an `executor`. The standard way to get everything set up is to import pywren, and call the `default_executor` function.

.. automethod:: pywren.wren.default_executor

`default_executor()` reads your `pywren_config` and returns `executor` object that's ready to go.
We can use this `executor` to run `map`, which applies a function to a list of data in the cloud.

.. automethod:: pywren.executor.Executor.map

`map` returns a list of `futures`, and which can return their `result` when the task is complete. 

.. autoclass:: pywren.future.ResponseFuture

    .. automethod:: pywren.future.ResponseFuture.result

Waiting for the results
--------------------------

.. autofunction:: pywren.wait.wait

If you want to get all of the results, you can simply call `get_all_results`

.. autofunction:: pywren.wren.get_all_results


