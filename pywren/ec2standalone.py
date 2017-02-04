"""
Code to handle EC2 stand-alone instances
"""
import boto3
import os


def create_instance_profile(instance_profile_name):
    iam = boto3.resource('iam')
    #iam.create_instance_profile(InstanceProfileName=INSTANCE_PROFILE_NAME)

    instance_profile = iam.InstanceProfile(instance_profile_name)
    #instance_profile.add_role(RoleName='pywren_exec_role_refactor8')


def launch_instances(tgt_ami, aws_region, my_aws_key, instance_type, 
                     instance_name, 
                     instance_profile_name, default_volume_size=100, ):

    # tgt_ami = 'ami-b04e92d0'
    # AWS_REGION = 'us-west-2'
    # my_aws_key = 'ec2-us-west-2'

    
    # INSTANCE_TYPE = 'm3.xlarge'
    # instance_name = AWS_INSTANCE_NAME

    ec2 = boto3.resource('ec2', region_name=aws_region)

    BlockDeviceMappings=[
        {
            'DeviceName': '/dev/xvda',
            'Ebs': {
                'VolumeSize': default_volume_size,
                'DeleteOnTermination': True,
                'VolumeType': 'standard',
                'SnapshotId' : 'snap-c87f35ec'
            },
        },
    ]

    user_data = """
    #cloud-config
    repo_update: true
    repo_upgrade: all
    
    packages:
     - tmux
     - emacs
     - gcc
     - g++
     - git 
     - htop

    runcmd:
     - [ sh, -c, 'echo "hello world" > /tmp/hello.txt' ]
     - pip install supervisor 
     - [ sudo, -Hu, ec2-user, sh, -c, "wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh -O /tmp/miniconda.sh"]
     - [ sudo, -Hu, ec2-user, sh, -c, "chmod +x /tmp/miniconda.sh"]
     - [ sudo, -Hu, ec2-user, sh, -c, "/tmp/miniconda.sh -b -p  /home/ec2-user/anaconda"]
     - [ sudo, -Hu, ec2-user, sh, -c, "/home/ec2-user/anaconda/bin/conda install -q -y numpy boto3"]
     - [ sudo, -Hu, ec2-user, sh, -c, "git clone -b standalone-worker https://github.com/ericmjonas/pywren.git /home/ec2-user/pywren"]
     - [ sudo, -Hu, ec2-user, sh, -c, "/home/ec2-user/anaconda/bin/pip install -e /home/ec2-user/pywren"]
    """
    #      - [
    # - /home/ec2-user/anaconda/bin/conda install -q -y numpy boto3

    #      - git clone -b standalone-worker "https://github.com/ericmjonas/pywren.git" /home/ec2-user/pywren
    #  - /home/ec2-user/anaconda/bin/pip install -e 
    # - [ ./Miniconda2-latest-Linux-x86_64.sh -b -p /home/ec2-user/anaconda] 
    #  - [ /home/ec2-user/anaconda/bin/conda install numpy boto3] 

    iam = boto3.resource('iam')
    instance_profile = iam.InstanceProfile(instance_profile_name)
    instance_profile_dict =  {
                              'Name' : instance_profile.name}
    instances = ec2.create_instances(ImageId=tgt_ami, MinCount=1, MaxCount=1,
                                     KeyName=my_aws_key, 
                                     InstanceType=instance_type, 
                                     BlockDeviceMappings = BlockDeviceMappings,
                                     InstanceInitiatedShutdownBehavior='terminate',
                                     EbsOptimized=True, 
                                     IamInstanceProfile = instance_profile_dict, 
                                     UserData=user_data)
    
    # FIXME there's a race condition where we could end up with two 
    # instances with the same name but that's ok
    existing_instance_names = [a[0] for a in list_instances(aws_region, 
                                                            instance_name)]

    new_instances_with_names = []
    
    def generate_unique_instance_name():
        inst_pos = 0
        while True:
            name_string = "{}-{}".format(instance_name, inst_pos)
            if (name_string not in [a[0] for a in new_instances_with_names]) and \
               (name_string not in existing_instance_names):
                return name_string

    for inst in instances:
        
        unique_instance_name = generate_unique_instance_name()
        print "setting instance name to", unique_instance_name

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
        new_instances_with_names.append((unique_instance_name, inst))
    for inst in instances:
        inst.wait_until_running()

    return new_instances_with_names

def tags_to_dict(d):
    if d is None:
        return {}
    return {a['Key'] : a['Value'] for a in d}

def list_instances(aws_region, instance_name):
    """
    List all instances whose names match the main

    Returns [(name, instance_object)]
    """

    ec2 = boto3.resource('ec2', region_name=aws_region)
    insts = []

    for i in ec2.instances.all():
        if i.state['Name'] == 'running':
            d = tags_to_dict(i.tags)
            if 'Name' in d and instance_name in d['Name']:
                insts.append((d['Name'], i))
    return insts

def terminate_instances(instance_list):
    """
    # FIXME delete individuals
    """
    for instance_name, instance_obj in instance_list:
        instance_obj.terminate()

    
def prettyprint_instances(inst_list):
    for instance_name, instance_obj in inst_list:
        print instance_name, instance_obj.public_dns_name

