
CONDA_DEFAULT_LIST = ["tblib", "numpy", "pytest", "Click", "numba", "boto3", "PyYAML", "cython", "boto"]

PIP_DEFAULT_LIST = ['glob2']
PIP_DEFAULT_UPGRADE_LIST = ['cloudpickle', 'enum34']

CONDA_ML_SET = ['scipy', 'pillow', 'cvxopt', 'sklearn']
PIP_ML_SET = ['cvxpy', 'redis']

RUNTIMES = {'minimal_2' : (2, CONDA_DEFAULT_LIST, 
                         PIP_DEFAULT_LIST, 
                         PIP_DEFAULT_UPGRADE_LIST),
            'minimal_3' : (3, CONDA_DEFAULT_LIST, 
                           PIP_DEFAULT_LIST, 
                           PIP_DEFAULT_UPGRADE_LIST), 
            'ml_2' : (2, CONDA_DEFAULT_LIST  + ML_SET, 
                      PIP_DEFAULT_LIST, 
                      PIP_DEFAULT_UPGRADE_LIST),
            'ml_3' : (3, CONDA_DEFAULT_LIST + ML_SET, 
                           PIP_DEFAULT_LIST, 
                           PIP_DEFAULT_UPGRADE_LIST), 

}

