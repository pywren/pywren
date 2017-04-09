#!/bin/bash
set -e
set -x
if [ "$RUN_COMMANDLINE" != "true" ]; then
    exit 0;
fi
export PYWREN_SETUP_INTERACTIVE_DEBUG_SUFFIX=$BUILD_GUID
pytest -v tests/test_cmdline.py --runcmdtest
RESULT=$?
exit $RESULT

