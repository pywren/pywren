import pytest
def pytest_addoption(parser):
    parser.addoption("--runmacro", action="store_true",
        help="run macroreduce tests that require both lambda and stand-alone instances")

    parser.addoption("--runlambda", action="store_true",
        help="run tests that can only be run on lambda")
