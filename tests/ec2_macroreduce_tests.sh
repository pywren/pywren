#!/bin/bash
set -e
set -x
if [ "$RUN_MACROREDUCE" != "true" ]; then
    exit 0
fi
pywren standalone launch_instances 1 --max_idle_time=10 --idle_terminate_granularity=600 --pywren_git_commit=$TRAVIS_COMMIT
sleep 20
pytest -v tests/test_macroreduce.py --runmacro
RESULT=$?
pywren standalone terminate_instances
exit $RESULT
