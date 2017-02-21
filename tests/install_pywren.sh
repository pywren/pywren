#!/bin/bash

set -ev
# this is a script that either installs the local pywren from source
# or downloads from pypy depending on the branch name
if [ "$TRAVIS_TAG" = "pypitest-build" ]; then
    echo "installing from pypitest";
    cp pywren/version.py print_version.py; 
    rm -Rf pywren; # make sure we aren't touching git code
    pip install --extra-index-url https://testpypi.python.org/pypi pywren==`python print_version.py`
else
    echo "installing from git"; 
    python setup.py install; 
fi

exit 0;
