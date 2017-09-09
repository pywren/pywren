import sys

if sys.version_info > (3, 0):
    from pywren.storage.storage import Storage, get_runtime_info
else:
    from pywren.storage.storage import Storage, get_runtime_info
