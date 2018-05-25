#
# Copyright 2018 PyWren Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pytest
import time
import boto3 
import uuid
import numpy as np
import time
import pywren
import pywren.runtime
import subprocess
import logging
from six.moves import cPickle as pickle
import os
import unittest
import numpy as np
from flaky import flaky
import sys
from click.testing import CliRunner
from pywren.scripts.setupscript import interactive_setup
import random
from pywren.scripts import pywrencli
from six import string_types


questions = {'region' : None, 
             'config_file' : None, 
             'bucket_name' : None, 
             'prefix' : None, 
             'advanced' : None, 
             'standalone' : None}


cmdtest = pytest.mark.skipif(
    not pytest.config.getoption("--runcmdtest"),
    reason="need --runcmdtest option to run"
)

def default_questions():
    return questions.copy()

def questions_to_string(q):
    """
    compile the questions in the indicated order. This way
    we can add a question later and not break everything
    """
    order = ['region', 'config_file', 'bucket_name', 'prefix', 
             'advanced', 'standalone']
    out_str = ""
    for o in order:
        action = q[o]
        if action is None:
            # default
            append_str = "\n"
        else:
            if isinstance(action, string_types):
                append_str = action + "\n"
            else:
                # assume action is a list of strings
                append_str = "\n".join(action) + "\n"
        out_str += append_str
    return out_str

def config_exists(filename):
    return os.path.exists(os.path.expanduser(filename))
    

SUFFIX = os.environ.get("PYWREN_SETUP_INTERACTIVE_DEBUG_SUFFIX", "")

@cmdtest    
class InteractiveCMDDryrun(unittest.TestCase):
    """
    Dryrun tests to make sure that we catch common input
    errors. Does not execute anything with AWS
    """
    def setUp(self):
        self.config_filename = "~/.config_test_dryrun"
        self.runner = CliRunner()
        try:
            os.remove(os.path.expanduser(self.config_filename))
        except OSError:
            pass

    def tearDown(self):
        try:
            os.remove(os.path.expanduser(self.config_filename))
        except OSError:
            pass
        
    def test_basic_defaults_dryrun(self):


        qs = default_questions()
        qs['config_file'] = self.config_filename

        cmd_str = questions_to_string(qs)
        result = self.runner.invoke(interactive_setup, 
                                    ['--dryrun', '--suffix', SUFFIX], 
                                    input=cmd_str)
        print(result.output)
        assert not result.exception
        assert config_exists(self.config_filename)

    def test_bad_region(self):
        qs = default_questions()
        qs['config_file'] = self.config_filename
        qs['region'] = ['us-foo-bar', 'us-baz-2', 
                        'ap-northeast-2']

        cmd_str = questions_to_string(qs)
        result = self.runner.invoke(interactive_setup, 
                                    ['--dryrun', '--suffix', SUFFIX], 
                                    input=cmd_str)
        print(result.output)
        assert not result.exception
        assert config_exists(self.config_filename)

    @pytest.mark.skip()
    def test_overwrite_existing_file(self):
        pass

    @pytest.mark.skip()
    def test_existing_bucket(self):
        pass

    @pytest.mark.skip()
    def test_invalid_s3_prefix(self):
        pass

@cmdtest    
class InteractiveCMD(unittest.TestCase):
    """
    Tests that actually check things with AWS. Executes everything
    inside of region TARGET_REGION
    """
    def setUp(self):
        self.config_filename = "~/.config_test"
        self.runner = CliRunner()
        try:
            os.remove(os.path.expanduser(self.config_filename))
        except OSError:
            pass

    def tearDown(self):
        args = ['--filename', os.path.expanduser(self.config_filename), 
                'cleanup_all', '--force']
        res = self.runner.invoke(pywrencli.cli, args=args)

    def test_basic_defaults(self):
        
        qs = default_questions()
        qs['config_file'] = self.config_filename

        cmd_str = questions_to_string(qs)
        result = self.runner.invoke(interactive_setup,
                                    args=['--suffix', SUFFIX],
                                    input=cmd_str)
        print(result.output)
        assert "Hello world" in result.output
        assert not result.exception
        assert config_exists(self.config_filename)
