import sys

if sys.version_info > (3, 0):
    from .storage import *
else:
    from storage import *
