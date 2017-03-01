import pytest
import numpy as np
import time
import pywren.wrenutil
import unittest

class S3HashingTest(unittest.TestCase):
    def test_s3_split(self):
        
        good_s3_url = "s3://bucket_name/and/the/key"
        bucket, key = pywren.wrenutil.split_s3_url(good_s3_url)
        
        self.assertEqual(bucket, "bucket_name")
        self.assertEqual(key, "and/the/key")

        with pytest.raises(ValueError) as excinfo:
            bad_s3_url = "notS3://foo/bar"
            bucket, key = pywren.wrenutil.split_s3_url(bad_s3_url)
            
    def test_hash(self):
        
        key = 'testkey'
        hashed_key = pywren.wrenutil.hash_s3_key(key)
        print(hashed_key)
        self.assertEqual(hashed_key[-len(key):], key)
        self.assertNotEqual(hashed_key, key)
