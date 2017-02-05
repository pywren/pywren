#!/bin/bash
set -e
pywren standalone launch_instances 1 --max_idle_time=10 --idle_terminate_granularity=600
sleep 10
PYWREN_EXECUTOR=lambda pytest tests/test_simple.py
pywren standalone terminate_instances
