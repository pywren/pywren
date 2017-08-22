.. Everything in this block
   will be commented.

.. role:: bash(code)
   :language: bash

Getting Started
==============

Setting up aws
--------------
Make sure you have an AWS account set up, and have the credentials set up in ``~/.aws``.



Installing pywren
------------------

Install pywren from pypi.

.. code-block:: bash

  pip install pywren

This should recursively install all of the dependencies, including those needed to interact with AWS.

You can launch an interactive setup script with

.. code-block:: bash

   pywren-setup

This will help you set up your buckets and deploy the lambda function.


The test function is broken, so verify that everything works by...
