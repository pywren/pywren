"""
Code to handle EC2 stand-alone instances
"""
import boto3
import os
import pywren
import base64
import logging

logger = logging.getLogger(__name__)


def create_instance_profile(instance_profile_name):
    iam = boto3.resource('iam')
    #iam.create_instance_profile(InstanceProfileName=INSTANCE_PROFILE_NAME)

    instance_profile = iam.InstanceProfile(instance_profile_name)
    #instance_profile.add_role(RoleName='pywren_exec_role_refactor8')


def launch_instances(number, tgt_ami, aws_region, my_aws_key, instance_type, 
                     instance_name, instance_profile_name, sqs_queue_name, 
                     default_volume_size=100, 
                     max_idle_time=60, idle_terminate_granularity=600, 
                     pywren_git_branch='master'):


    logger.info("launching {} {} instances in {}".format(number, instance_type, 
                                                         aws_region))
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
    template_file = os.path.join(pywren.SOURCE_DIR, 
                                 'ec2standalone.cloudinit.template')

    user_data = open(template_file, 'r').read()

    supervisord_init_script = open(os.path.join(pywren.SOURCE_DIR, 
                                                'supervisord.init'), 'r').read()
    supervisord_init_script_64 = base64.b64encode(supervisord_init_script)

    supervisord_conf = open(os.path.join(pywren.SOURCE_DIR, 
                                         'supervisord.conf'), 'r').read()
    logger.info("Running with idle_terminate_granularity={}".format(idle_terminate_granularity))
    supervisord_conf = supervisord_conf.format(run_dir = "/tmp/pywren.runner", 
                                               sqs_queue_name=sqs_queue_name, 
                                               aws_region=aws_region, 
                                               max_idle_time=max_idle_time,
                                               idle_terminate_granularity=idle_terminate_granularity)
    supervisord_conf_64 = base64.b64encode(supervisord_conf)

    cloud_agent_conf = open(os.path.join(pywren.SOURCE_DIR, "cloudwatch-agent.config"), 
                            'r').read()
    cloud_agent_conf_64 = base64.b64encode(cloud_agent_conf)

    user_data = user_data.format(supervisord_init_script = supervisord_init_script_64, 
                                 supervisord_conf = supervisord_conf_64, 
                                 pywren_git_branch=pywren_git_branch, 
                                 aws_region = aws_region, 
                                 cloud_agent_conf = cloud_agent_conf_64)


    open("/tmp/user_data", 'w').write(user_data)

    iam = boto3.resource('iam')
    instance_profile = iam.InstanceProfile(instance_profile_name)
    instance_profile_dict =  {
                              'Name' : instance_profile.name}
    instances = ec2.create_instances(ImageId=tgt_ami, MinCount=number, 
                                     MaxCount=number,
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
            inst_pos += 1

    for inst in instances:
        
        unique_instance_name = generate_unique_instance_name()
        logger.info("setting instance name to {}".format(unique_instance_name))

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

