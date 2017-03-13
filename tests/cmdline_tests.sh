#!/bin/bash
set -e
set -x
if [ "$RUN_COMMANDLINE" != "true" ]; then
    exit 0;
fi

pytest -v tests/cmdline_tests.py
RESULT=$?
exit $RESULT

