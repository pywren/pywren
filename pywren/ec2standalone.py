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

"""
Code to handle EC2 stand-alone instances
"""
import base64
import logging
import os
import time
import datetime
import boto3
import pywren

logger = logging.getLogger(__name__)

def b64s(string):
    """
    Base-64 encode a string and return a string
    """
    return base64.b64encode(string.encode('utf-8')).decode('ascii')

def sd(filename):
    """
    get the file in the standalone dir
    """
    return os.path.join(pywren.SOURCE_DIR,
                        'ec2_standalone_files', filename)

def create_instance_profile(instance_profile_name):
    iam = boto3.resource('iam')
    #iam.create_instance_profile(InstanceProfileName=INSTANCE_PROFILE_NAME)

    iam.InstanceProfile(instance_profile_name)
    #instance_profile.add_role(RoleName='pywren_exec_role_refactor8')


def launch_instances(number, tgt_ami, aws_region, my_aws_key, instance_type,
                     instance_name, instance_profile_name, sqs_queue_name,
                     default_volume_size=100,
                     max_idle_time=60, idle_terminate_granularity=600,
                     pywren_git_branch='master',
                     spot_price=None,
                     availability_zone=None,
                     fast_io=False,
                     parallelism=1,
                     pywren_git_commit=None):


    logger.info("launching {} {} instances in {} (zone {}) ".format(number,
                                                                    instance_type,
                                                                    aws_region,
                                                                    availability_zone))

    if fast_io:
        BlockDeviceMappings = [
            {
                'DeviceName': '/dev/xvda',
                'Ebs': {
                    'VolumeSize': default_volume_size,
                    'DeleteOnTermination': True,
                    'VolumeType': 'gp2',
                    #'Iops' : 10000,
                },
            },
        ]
    else:
        BlockDeviceMappings = None
    template_file = sd('ec2standalone.cloudinit.template')

    user_data = open(template_file, 'r').read()

    supervisord_init_script = open(sd('supervisord.init'), 'r').read()
    supervisord_init_script_64 = b64s(supervisord_init_script)

    supervisord_conf = open(sd('supervisord.conf'), 'r').read()
    logger.info("Running with idle_terminate_granularity={}".format(idle_terminate_granularity))
    supervisord_conf = supervisord_conf.format(
        run_dir="/tmp/pywren.runner",
        sqs_queue_name=sqs_queue_name,
        aws_region=aws_region,
        max_idle_time=max_idle_time,
        idle_terminate_granularity=idle_terminate_granularity,
        num_procs=parallelism)
    supervisord_conf_64 = b64s(supervisord_conf)

    cloud_agent_conf = open(sd("cloudwatch-agent.config"),
                            'r').read()
    cloud_agent_conf_64 = b64s(cloud_agent_conf)

    if pywren_git_commit is not None:
        # use a git commit
        git_checkout_string = str(pywren_git_commit)
    else:
        git_checkout_string = " {}".format(pywren_git_branch)

    user_data = user_data.format(supervisord_init_script=supervisord_init_script_64,
                                 supervisord_conf=supervisord_conf_64,
                                 git_checkout_string=git_checkout_string,
                                 aws_region=aws_region,
                                 cloud_agent_conf=cloud_agent_conf_64)

    # FIXME debug
    open("/tmp/user_data", 'w').write(user_data)

    iam = boto3.resource('iam')
    instance_profile = iam.InstanceProfile(instance_profile_name)

    instance_profile_dict = {'Name' : instance_profile.name}

    instances = _create_instances(number, aws_region,
                                  spot_price, ami=tgt_ami,
                                  key_name=my_aws_key,
                                  instance_type=instance_type,
                                  block_device_mappings=BlockDeviceMappings,
                                  security_group_ids=[],
                                  ebs_optimized=True,
                                  instance_profile=instance_profile_dict,
                                  availability_zone=availability_zone,
                                  user_data=user_data) ###FIXME DEBUG DEBUG


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





def _create_instances(num_instances,
                      region,
                      spot_price,
                      ami,
                      key_name,
                      instance_type,
                      block_device_mappings,
                      security_group_ids,
                      ebs_optimized,
                      instance_profile,
                      availability_zone,
                      user_data):

    ''' Function graciously borrowed from Flintrock ec2 wrapper
        https://raw.githubusercontent.com/nchammas/flintrock/00cce5fe9d9f741f5999fddf2c7931d2cb1bdbe8/flintrock/ec2.py
    '''

    ec2 = boto3.resource(service_name='ec2', region_name=region)
    spot_requests = []
    try:
        if spot_price is not None:
            if spot_price > 0:
                print("Requesting {c} spot instances at a max price of ${p}...".format(
                    c=num_instances, p=spot_price))
            else:
                print("Requesting {c} spot instances at the on-demand price...".format(
                    c=num_instances))
            client = ec2.meta.client


            LaunchSpecification = {
                'ImageId': ami,
                'InstanceType': instance_type,
                'SecurityGroupIds': security_group_ids,
                'EbsOptimized': ebs_optimized,
                'IamInstanceProfile' : instance_profile,
                'UserData' : b64s(user_data)}
            if availability_zone is not None:
                LaunchSpecification['Placement'] = {"AvailabilityZone":availability_zone}
            if block_device_mappings is not None:
                LaunchSpecification['BlockDeviceMappings'] = block_device_mappings
            if key_name is not None:
                LaunchSpecification['KeyName'] = key_name

            if spot_price > 0:
                spot_requests = client.request_spot_instances(
                    SpotPrice=str(spot_price),
                    InstanceCount=num_instances,
                    LaunchSpecification=LaunchSpecification)['SpotInstanceRequests']
            else:
                spot_requests = client.request_spot_instances(
                    InstanceCount=num_instances,
                    LaunchSpecification=LaunchSpecification)['SpotInstanceRequests']


            request_ids = [r['SpotInstanceRequestId'] for r in spot_requests]
            pending_request_ids = request_ids

            time.sleep(5)

            while pending_request_ids:
                spot_requests = client.describe_spot_instance_requests(
                    SpotInstanceRequestIds=request_ids)['SpotInstanceRequests']

                failed_requests = [r for r in spot_requests if r['State'] == 'failed']
                if failed_requests:
                    failure_reasons = {r['Status']['Code'] for r in failed_requests}
                    raise Exception(
                        "The spot request failed for the following reason{s}: {reasons}"
                        .format(
                            s='' if len(failure_reasons) == 1 else 's',
                            reasons=', '.join(failure_reasons)))

                pending_request_ids = [
                    r['SpotInstanceRequestId'] for r in spot_requests
                    if r['State'] == 'open']

                if pending_request_ids:
                    print("{grant} of {req} instances granted. Waiting...".format(
                        grant=num_instances - len(pending_request_ids),
                        req=num_instances))
                    time.sleep(30)

            print("All {c} instances granted.".format(c=num_instances))

            cluster_instances = list(
                ec2.instances.filter(
                    Filters=[
                        {'Name': 'instance-id', 'Values': [r['InstanceId'] for r in spot_requests]}
                    ]))
        else:
            # Move this to flintrock.py?
            print("Launching {c} instance{s}...".format(
                c=num_instances,
                s='' if num_instances == 1 else 's'))

            # TODO: If an exception is raised in here, some instances may be
            #       left stranded.

            LaunchSpecification = {
                "MinCount" : num_instances,
                "MaxCount" : num_instances,
                "ImageId" : ami,
                "InstanceType" : instance_type,
                "SecurityGroupIds" : security_group_ids,
                "EbsOptimized" : ebs_optimized,
                "IamInstanceProfile" :   instance_profile,
                "InstanceInitiatedShutdownBehavior" :  'terminate',

                "UserData" :  user_data}
            if block_device_mappings is not None:
                LaunchSpecification['BlockDeviceMappings'] = block_device_mappings
            if key_name is not None:
                LaunchSpecification['KeyName'] = key_name

            cluster_instances = ec2.create_instances(**LaunchSpecification)

        time.sleep(10)  # AWS metadata eventual consistency tax.
        return cluster_instances
    except (Exception, KeyboardInterrupt) as e:
        if not isinstance(e, KeyboardInterrupt):
            print(e)
        if spot_requests:
            request_ids = [r['SpotInstanceRequestId'] for r in spot_requests]
            if any([r['State'] != 'active' for r in spot_requests]):
                print("Canceling spot instance requests...")
                client.cancel_spot_instance_requests(
                    SpotInstanceRequestIds=request_ids)
            # Make sure we have the latest information on any launched spot instances.
            spot_requests = client.describe_spot_instance_requests(
                SpotInstanceRequestIds=request_ids)['SpotInstanceRequests']
            instance_ids = [
                r['InstanceId'] for r in spot_requests
                if 'InstanceId' in r]
            if instance_ids:
                cluster_instances = list(
                    ec2.instances.filter(
                        Filters=[
                            {'Name': 'instance-id', 'Values': instance_ids}
                        ]))
        raise Exception("Launch failure")


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
        logger.debug('Terminating instance %s', instance_name)
        instance_obj.terminate()


def prettyprint_instances(inst_list):
    for instance_name, instance_obj in inst_list:
        print(instance_name, instance_obj.public_dns_name)

def prettyprint_instance_uptimes(inst_list):
    for instance_name, instance_obj in inst_list:
        launch_time = instance_obj.launch_time
        delta = str(datetime.datetime.now(launch_time.tzinfo) - launch_time).split('.')[0]
        print(instance_name, delta)
