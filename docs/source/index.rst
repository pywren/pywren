.. pywren documentation master file, created by
   sphinx-quickstart on Mon Aug 21 13:11:55 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pywren documentation!
==================================

foo bar bas blah blah blah blah

.. code-block:: python

  def my_function(b):
    x = np.random.normal(0, b, 1024)
    A = np.random.normal(0, b, (1024, 1024))
    return np.dot(A, x)

  pwex = pywren.default_executor()
  res = pwex.map(my_function, np.linspace(0.1, 100, 1000))


.. toctree::
   :caption: Contents:
   :maxdepth: 1

   getting-started.rst
   pywren.rst
   examples.rst
   design.rst
   help.rst
   



