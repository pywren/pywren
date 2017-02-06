#!/bin/bash
set -e
set -x
pywren standalone launch_instances 1 --max_idle_time=10 --idle_terminate_granularity=600
sleep 20
PYWREN_EXECUTOR=remote pytest tests/test_simple.py::SimpleAsync::test_simple  tests/test_simple.py::SimpleAsync::test_simple2 tests/test_simple.py::SimpleAsync::test_exception
pywren standalone terminate_instances
