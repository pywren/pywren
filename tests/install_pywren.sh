#!/bin/bash

set -ev
# this is a script that either installs the local pywren from source
# or downloads from pypy depending on the branch name
if [ ${TRAVIS_TAG} = "pypitest-build" ]; then
    rm -Rf pywren
    pip install -i https://testpypi.python.org/pypi pywren
else
    python setup.py install
fi

exit 0;
