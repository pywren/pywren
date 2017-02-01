import wren
from wren import default_executor, wait, dummy_executor
import wrenlogging

import os
if "PYWREN_LOGLEVEL" in os.environ:
    log_level = os.environ['PYWREN_LOGLEVEL']
    wrenlogging.default_config(log_level)

