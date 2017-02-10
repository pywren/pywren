#!/bin/bash
set -e
set -x
pywren standalone launch_instances 1 --max_idle_time=10 --idle_terminate_granularity=600 --pywren_git_branch=$TRAVIS_BRANCH
sleep 20
pytest -v tests/test_macroreduce.py --runmacro
pywren standalone terminate_instances
