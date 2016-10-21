"""
examples:

fab -f fabfile_builer.py -R builder conda_setup_mkl conda_clean package_all


"""
from fabric.api import local, env, run, put, cd, task, sudo, settings, warn_only, lcd, path, get
from fabric.contrib import project
import boto3
import cloudpickle
import json
import base64
import cPickle as pickle
from pywren.wrenconfig import * 



tgt_ami = 'ami-7172b611'
region = 'us-west-2'
unique_instance_name = 'pywren_builder'
#s3url = "s3://ericmjonas-public/condaruntime.nomkl_sklearn.tar.gz"
#s3url = "s3://ericmjonas-public/condaruntime.mkl.avx.tar.gz"
#s3url = "s3://ericmjonas-public/condaruntime.nomkl.tar.gz"
s3url = "s3://ericmjonas-public/condaruntime.mkl.avx2.tar.gz"

def tags_to_dict(d):
    return {a['Key'] : a['Value'] for a in d}

def get_target_instance():
    res = []
    ec2 = boto3.resource('ec2', region_name=region)

    for i in ec2.instances.all():
        if i.state['Name'] == 'running':
            d = tags_to_dict(i.tags)
            if d['Name'] == unique_instance_name:
                res.append('ec2-user@{}'.format(i.public_dns_name))
    print "found", res
    return {'builder' : res}

env.roledefs.update(get_target_instance())

@task
def launch():

    ec2 = boto3.resource('ec2', region_name=region)

    instances = ec2.create_instances(ImageId=tgt_ami, MinCount=1, MaxCount=1, 
                         KeyName='ec2-us-west-2', InstanceType='m4.large')
    inst = instances[0]

    inst.wait_until_running()
    inst.reload()
    inst.create_tags(
        Resources=[
            inst.instance_id
        ],
        Tags=[
            {
                'Key': 'Name',
                'Value': unique_instance_name
            },
        ]
    )

@task        
def ssh():
    local("ssh -A " + env.host_string)

@task 
def openblas():
    sudo("sudo yum install -q -y git gcc g++ gfortran libgfortran")
    with path("/tmp/conda/condaruntime/bin", behavior="prepend"):
        with cd("/tmp/conda"):
            run("rm -Rf openblas-build")
            run("mkdir openblas-build")
            with cd('openblas-build'):
                run('git clone https://github.com/xianyi/OpenBLAS.git')
                with cd('OpenBLAS'):
                    run('make -j4')
                    run('make install PREFIX=/tmp/conda/condaruntime/openblas-install')

@task
def conda_setup_mkl():
    run("rm -Rf /tmp/conda")
    run("mkdir -p /tmp/conda")
    with cd("/tmp/conda"):
        run("wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh -O miniconda.sh ")
        run("bash miniconda.sh -b -p /tmp/conda/condaruntime")
        with path("/tmp/conda/condaruntime/bin", behavior="prepend"):
            run("conda install -q -y numpy enum34 pytest Click numba boto3 PyYAML cython")
            run("conda list")
            run("conda clean -y -i -t -p")
            run("pip install --upgrade cloudpickle")
            run("rm -Rf /tmp/conda/condaruntime/pkgs/mkl-11.3.3-0/*")
            with cd("/tmp/conda/condaruntime/lib"):
                run("rm *_mc.so *_mc2.so *_mc3.so *_avx512* *_avx2*")
            
@task
def conda_setup_mkl_avx2():
    run("rm -Rf /tmp/conda")
    run("mkdir -p /tmp/conda")
    with cd("/tmp/conda"):
        run("wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh -O miniconda.sh ")
        run("bash miniconda.sh -b -p /tmp/conda/condaruntime")
        with path("/tmp/conda/condaruntime/bin", behavior="prepend"):
            run("conda install -q -y numpy enum34 pytest Click numba boto3 PyYAML cython")
            run("conda list")
            run("conda clean -y -i -t -p")
            run("pip install --upgrade cloudpickle")
            run("rm -Rf /tmp/conda/condaruntime/pkgs/mkl-11.3.3-0/*")
            with cd("/tmp/conda/condaruntime/lib"):
                run("rm *_mc.so *_mc2.so *_mc3.so *_avx512* *_avx.*")
            
@task
def conda_setup_nomkl():
    run("rm -Rf /tmp/conda")
    run("mkdir -p /tmp/conda")
    with cd("/tmp/conda"):
        run("wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh -O miniconda.sh ")
        run("bash miniconda.sh -b -p /tmp/conda/condaruntime")
        with path("/tmp/conda/condaruntime/bin", behavior="prepend"):
            run("conda install -q -y nomkl numpy enum34 pytest Click numba boto3 PyYAML cython")
            run("conda list")
            run("conda clean -y -i -t -p")
            run("pip install --upgrade cloudpickle")
            

@task
def package_all():
    with cd("/tmp/conda"):
         run("tar czf condaruntime.tar.gz condaruntime")
         get("condaruntime.tar.gz", local_path="/tmp/condaruntime.tar.gz")
         local("aws s3 cp /tmp/condaruntime.tar.gz {}".format(s3url))


@task
def numpy():
    """
    http://stackoverflow.com/questions/11443302/compiling-numpy-with-openblas-integration

    """
    with path("/tmp/conda/condaruntime/bin", behavior="prepend"):
        # git clone
        run("rm -Rf /tmp/conda/numpy")
        with cd("/tmp/conda"):
            run("git clone https://github.com/numpy/numpy")
            with cd("numpy"):
                run("cp site.cfg.example site.cfg")
        
                config = """
                [openblas]
                libraries = openblas
                library_dirs = /tmp/conda/condaruntime/openblas-install/lib
                include_dirs = /tmp/conda/condaruntime/openblas-install/install
                runtime_library_dirs = /tmp/conda/condaruntime/openblas-install/lib
                """
                for l in config.split("\n"):
                    run("echo '{}' >> {}".format(l.strip(), 'site.cfg'))
                run("python setup.py config") # check this output
                run("pip install .")

@task
def conda_clean():    
    # for some reason gcc leaves detritus around in pkg that is 200 MB
    run("conda remove gcc cython clog gmp isl mpc")
    run("conda clean --all")
    with cd("/tmp/conda/condaruntime/"):
        run("rm -Rf pkgs/gcc-* pkgs/cython*/")
            
