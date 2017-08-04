import sys

if sys.version_info > (3, 0):
    from pywren.serialize.cloudpickle.cloudpickle import CloudPickler
else:
    from pywren.serialize.cloudpickle.cloudpickle import CloudPickler

__version__ = '0.2.2'
