import sys

if sys.version_info > (3, 0):
    from .cloudpickle import *
else:
    from cloudpickle import *

__version__ = '0.2.2'
