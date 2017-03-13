#!/bin/bash
set -e
set -x
if [ "$RUN_COMMANDLINE" != "true" ]; then
    exit 0;
fi

pytest -v tests/test_cmdline.py
RESULT=$?
exit $RESULT

