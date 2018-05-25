Documentation
=============

Executor
---------
The primary object in pywren is an `executor`. The standard way to get everything set up is to import pywren, and call the `default_executor` function.

.. autofunction:: pywren.wren.default_executor

`default_executor()` reads your `pywren_config` and returns `executor` object that's ready to go.
We can use this `executor` to run `map`, which applies a function to a list of data in the cloud.

.. automethod:: pywren.executor.Executor.map

`map` returns a list of `futures`, and which can return their `result` when the task is complete. 

.. autoclass:: pywren.future.ResponseFuture

    .. automethod:: pywren.future.ResponseFuture.result

Waiting for the results
--------------------------

.. autofunction:: pywren.wait.wait

Alternatively, if you want to wait for everything to finish and then get all of the results, you can simply call `get_all_results`

.. autofunction:: pywren.wren.get_all_results


Standalone Mode
---------------

To run pywren in standalone mode, run

.. code-block:: bash

  pywren standalone launch_instances 2

This launches EC2 instances to run pywren. Once the instances are ready, you can run pywren as usual. The instance type is set in the `pywren_config` file, under the `standalone` key. By default, the instance type is set to `m4.4xlarge`.

.. code-block:: python

  >>> import pywren
  >>> pwex = pywren.standalone_executor()
  >>> futures = pwex.map(func, data)

If you set `max_idle_time` when launching, the ec2 instances will terminate themselves. Otherwise, you need to explicitly shut them down.


.. code-block:: bash

  pywren standalone terminate_instances


Standalone Commands
-------------------

.. code-block:: bash

  # Launch EC2 instances.
  > pywren standalone launch_instances --help

    Usage: pywren standalone launch_instances [OPTIONS] [NUMBER]

    Options:
      --max_idle_time INTEGER         instance queue idle time before checking
                                      self-termination
      --idle_terminate_granularity INTEGER
                                      granularity of billing (sec)
      --pywren_git_branch TEXT        which branch to use on the stand-alone
      --pywren_git_commit TEXT        which git to use on the stand-alone
                                      (supercedes pywren_git_branch)
      --spot_price FLOAT              use spot instances, at this reserve price

  # List all running EC2 instances.
  > pywren standalone list_instances

  # Shut down all running EC2 instances.
  > pywren standalone terminate_instances 

