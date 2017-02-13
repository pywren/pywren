#!/bin/bash
set -e
set -x
if [ "$RUN_STANDALONE" != "true" ]; then
    exit 0
fi

pywren standalone launch_instances 1 --max_idle_time=10 --idle_terminate_granularity=600 --pywren_git_branch=$TRAVIS_BRANCH
sleep 20
export PYWREN_EXECUTOR=remote
pytest -v tests/test_simple.py
RESULT=$?
pywren standalone terminate_instances
exit $RESULT
