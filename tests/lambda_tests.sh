#!/bin/bash
set -e
set -x
if [ "$RUN_LAMBDA" != "true" ]; then
    exit 0;
fi

pytest -v tests
pytest -v tests/test_lambda.py --runlambda
RESULT=$?
exit $RESULT

