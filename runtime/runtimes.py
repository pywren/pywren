
CONDA_DEFAULT_LIST = ["tblib", "numpy", "pytest", "Click", "numba", "boto3", "PyYAML", "cython", "boto", "scipy", "pillow", "cvxopt", "scikit-learn"]

PIP_DEFAULT_LIST = ['cvxpy', 'redis', 'glob2']
PIP_DEFAULT_UPGRADE_LIST = ['cloudpickle', 'enum34']

RUNTIMES = {'keyname' : (3, CONDA_DEFAULT_LIST, 
                         PIP_DEFAULT_LIST, 
                         PIP_DEFAULT_UPGRADE_LIST)}
