#!/bin/bash
set -e
set -x
if [ "$RUN_STANDALONE" != "true" ]; then
    exit 0
fi
# sometimes the instance profile isn't visible yet to the function.
# this results in erratic test behavior and tests sometimes failing

n=0
until [ $n -ge 5 ]
do
    aws iam get-instance-profile --instance-profile-name pywren_travis_$BUILD_GUID > /dev/null && break
    echo "instance profile was not available, retrying"
    n=$[$n+1]
    sleep 10
done
   
pywren standalone launch_instances 1 --max_idle_time=10 --idle_terminate_granularity=600 --pywren_git_commit=$TRAVIS_COMMIT
sleep 20
export PYWREN_EXECUTOR=remote
pytest -v tests/test_simple.py
RESULT0=$?
pywren standalone terminate_instances


export EXECUTOR_PARALLELISM=16
export INSTANCE_TYPE=m4.4xlarge
pywren standalone launch_instances 1 --max_idle_time=10 --idle_terminate_granularity=600 --pywren_git_commit=$TRAVIS_COMMIT  --parallelism $EXECUTOR_PARALLELISM --instance_type $INSTANCE_TYPE
sleep 20
export PYWREN_EXECUTOR=remote
pytest -v tests/test_simple.py
RESULT1=$?
pytest -v tests/test_standalone_many.py
RESULT2=$?
pywren standalone terminate_instances
exit $(($RESULT0 + $RESULT1 + RESULT2))
