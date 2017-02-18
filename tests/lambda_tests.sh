#!/bin/bash
set -e
set -x
if [ "$RUN_LAMBDA" != "true" ]; then
    exit 0;
fi

pytest -v tests --runlambda
RESULT=$?
exit $RESULT

