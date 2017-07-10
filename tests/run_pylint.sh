#!/bin/bash
set -e
set -x
if [ "$RUN_PYLINT" != "true" ]; then
    exit 0;
fi

pylint pywren
RESULT=$?
exit $RESULT
