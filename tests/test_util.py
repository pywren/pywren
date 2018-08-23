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

import unittest
import pytest
import pywren
import pywren.wrenutil


class S3HashingTest(unittest.TestCase):
    def test_s3_split(self):

        good_s3_url = "s3://bucket_name/and/the/key"
        bucket, key = pywren.wrenutil.split_s3_url(good_s3_url)

        self.assertEqual(bucket, "bucket_name")
        self.assertEqual(key, "and/the/key")

        with pytest.raises(ValueError):
            bad_s3_url = "notS3://foo/bar"
            bucket, key = pywren.wrenutil.split_s3_url(bad_s3_url)

def test_version():
    """
    test that __version__ exists
    """
    assert pywren.__version__ is not None
