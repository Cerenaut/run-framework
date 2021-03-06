import boto3
import os
import botocore
import logging

from agief_experiment import utils


class Cloud:

    # EC2 instances will be launched into this subnet (in a vpc)
    subnet_id = 'subnet-0b1a206e'

    # For ECS, which cluster to use
    cluster = 'default'

    # When creating EC2 instances, the root ssh key to use
    mainkeyname = 'nextpair'

    # For compute hosts, which the security group to use
    ec2_compute_securitygroup_id = 'sg-98d574fc'

    # AZ for all EC2 instances
    availability_zone = 'ap-southeast-2a'

    # Placement group for EC2 instances
    placement_group = 'MNIST-PGroup'

    # Unique, case-sensitive identifier you provide to ensure
    # client_token = 'this_is_the_client_token_la_la_34'

    # The idempotency of the request.
    network_interface_id = 'eni - b2acd4d4'

    def __init__(self):
        pass

    def sync_experiment(self, remote):
        """
        Sync experiment from this machine to remote machine
        """

        print("\n....... Use remote-sync-experiment.sh to "
              "rsync relevant folders.")

        cmd = ("../remote/remote-sync-experiment.sh " +
               remote.host_key_user_variables())
        utils.run_bashscript_repeat(cmd, 15, 6)

    def remote_download_output(self, prefix, host_node):
        """ Download /output/prefix folder from remote storage (s3) to remote machine.
        :param host_node:
        :param prefix:
        :type host_node: RemoteNode
        """

        print("\n....... Use remote-download-output.sh to copy /output files "
              "from s3 (typically input and data files) with "
              "prefix = " + prefix + ", to remote machine.")

        cmd = ("../remote/remote-download-output.sh " + " " + prefix +
               " " + host_node.host_key_user_variables())
        utils.run_bashscript_repeat(cmd, 15, 6)

    def remote_docker_launch_compute(self, host_node):
        """
        Assumes there exists a private key for the given
        ec2 instance, at keypath
        """

        print("\n....... Launch compute node in a docker container "
              "on a remote host.")

        commands = '''
            export VARIABLES_FILE={0}
            source {0}
            cd $AGI_HOME/bin/node_coordinator
            ./run-in-docker.sh -d
        '''.format(host_node.remote_variables_file)

        return utils.remote_run(host_node, commands)

    def ecs_run_task(self, task_name):
        """ Run task 'task_name' and return the Task ARN """

        print("\n....... Running task on ecs ")
        client = boto3.client('ecs')
        response = client.run_task(
            cluster=self.cluster,
            taskDefinition=task_name,
            count=1,
            startedBy='pyScript'
        )

        logging.debug("LOG: " + response)

        length = len(response['failures'])
        if length > 0:
            logging.error("Could not initiate task on AWS.")
            logging.error("reason = " + response['failures'][0]['reason'])
            logging.error("arn = " + response['failures'][0]['arn'])
            logging.error(" ----- exiting -------")
            exit(1)

        if len(response['tasks']) <= 0:
            logging.error("could not retrieve task arn when initiating task "
                          "on AWS - something has gone wrong.")
            exit(1)

        task_arn = response['tasks'][0]['taskArn']
        return task_arn

    def ecs_stop_task(self, task_arn):

        print("\n....... Stopping task on ecs ")
        client = boto3.client('ecs')

        response = client.stop_task(
            cluster=self.cluster,
            task=task_arn,
            reason='pyScript said so!'
        )

        logging.debug("LOG: " + response)

    def ec2_start_from_instanceid(self, instance_id):
        """
        Run the chosen instance specified by instance_id
        :return: the instance AWS public and private ip addresses
        """

        print("\n....... Starting ec2 (instance id " + instance_id + ")")
        ec2 = boto3.resource('ec2')
        instance = ec2.Instance(instance_id)
        response = instance.start()

        print("LOG: Start response: " + response)

        instance_id = instance.instance_id

        ips = self.ec2_wait_till_running(instance_id)
        return ips

    def ec2_start_from_ami(self, name, ami_id, min_ram):
        """
        :param name:
        :param ami_id: ami id
        :param min_ram: (integer), minimum ram to allocate to ec2 instance
        :return: ip addresses: public and private, and instance id
        """

        print("\n....... Launching ec2 from AMI (AMI id " + ami_id +
              ", with minimum " + str(min_ram) + "GB RAM)")

        # minimum size, 15GB on machine, leaves 13GB for compute
        instance_type = None
        ram_allocated = 8
        if min_ram < 6:
            instance_type = 'm4.large'      # 8
            ram_allocated = 8
        elif min_ram < 13:
            instance_type = 'r3.large'      # 15.25
            ram_allocated = 15.25
        elif min_ram < 28:
            instance_type = 'r3.xlarge'     # 30.5
            ram_allocated = 30.5
        else:
            logging.error("cannot create an ec2 instance with that much RAM")
            exit(1)

        print("\n............. RAM to be allocated: " + str(ram_allocated) +
              " GB RAM")

        ec2 = boto3.resource('ec2')
        subnet = ec2.Subnet(self.subnet_id)

        # Set the correct Logz.io token in EC2
        logzio_token = os.getenv("AGI_LOGZIO_TOKEN")
        user_data = '''
        #!/bin/sh
        echo export AGI_LOGZIO_TOKEN=%s >> /etc/environment
        ''' % (logzio_token)

        instance = subnet.create_instances(
            DryRun=False,
            ImageId=ami_id,
            MinCount=1,
            MaxCount=1,
            KeyName=self.mainkeyname,

            SecurityGroupIds=[
                self.ec2_compute_securitygroup_id,
            ],
            InstanceType=instance_type,
            Placement={
                'AvailabilityZone': self.availability_zone,
                # 'GroupName': self.placement_group,
                'Tenancy': 'default'                # | 'dedicated' | 'host',
            },
            Monitoring={
                'Enabled': False
            },
            DisableApiTermination=False,
            InstanceInitiatedShutdownBehavior='terminate',      # | 'stop'
            # ClientToken=self.client_token,
            AdditionalInfo='started by run-framework.py',
            # IamInstanceProfile={
            #     'Arn': 'string',
            #     'Name': 'string'
            # },
            EbsOptimized=False,
            UserData=user_data
        )

        instance_id = instance[0].instance_id

        logging.debug("Instance launched %s", instance_id)

        # set name
        response = ec2.create_tags(
            DryRun=False,
            Resources=[
                instance_id,
            ],
            Tags=[
                {
                    'Key': 'Name',
                    'Value': name
                },
            ]
        )

        logging.debug("Set Name tag on instanceid: %s", instance_id)
        logging.debug("Response is: %s", response)

        ips = self.ec2_wait_till_running(instance_id)
        return ips, instance_id

    def ec2_wait_till_running(self, instance_id):
        """
        :return: the instance AWS public and private ip addresses
        """

        ec2 = boto3.resource('ec2')
        instance = ec2.Instance(instance_id)

        print("wait_till_running for instance: ", instance)

        instance.wait_until_running()

        ip_public = instance.public_ip_address
        ip_private = instance.private_ip_address

        print("Instance is up and running ...")
        self.print_ec2_info(instance)

        return {'ip_public': ip_public, 'ip_private': ip_private}

    def ec2_stop(self, instance_id):
        print("\n...... Closing ec2 instance (instance id " +
              str(instance_id) + ")")
        ec2 = boto3.resource('ec2')
        instance = ec2.Instance(instance_id)

        self.print_ec2_info(instance)

        response = instance.stop()

        print("stop ec2: ", response)

    def remote_upload_runfilename_s3(self, host_node, prefix, dest_name):
        cmd = ("../remote/remote-upload-runfilename.sh " + " " + prefix +
               " " + dest_name +
               host_node.host_key_user_variables())
        try:
            utils.run_bashscript_repeat(cmd, 3, 3)
        except Exception as e:
            logging.error("Remote Upload Failed for this file")
            logging.error("Exception: %s", e)

    def remote_upload_output_s3(self, host_node, prefix, no_compress,
                                csv_output):
        cmd = "../remote/remote-upload-output.sh " + prefix + " "
        cmd += host_node.host_key_user_variables() + " "
        cmd += str(no_compress) + " " + str(csv_output)
        utils.run_bashscript_repeat(cmd, 3, 3)

    def upload_folder_s3(self, bucket_name, key, source_folderpath):

        if not os.path.exists(source_folderpath):
            logging.warning("folder does not exist, cannot upload: " +
                            source_folderpath)
            return

        if not os.path.isdir(source_folderpath):
            logging.warning("path is not a folder, cannot upload: " +
                            source_folderpath)
            return

        for root, dirs, files in os.walk(source_folderpath):
            for f in files:
                filepath = os.path.join(source_folderpath, f)
                filekey = os.path.join(key, f)

                self.upload_file_s3(bucket_name, filekey, filepath)

    @staticmethod
    def upload_file_s3(bucket_name, key, source_filepath):

        try:
            if os.stat(source_filepath).st_size == 0:
                logging.warning("file is empty, cannot upload: " +
                                source_filepath)
                return
        except OSError:
            logging.warning("file does not exist, cannot upload: " +
                            source_filepath)
            return

        s3 = boto3.resource('s3')

        exists = True
        try:
            s3.meta.client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                exists = False

        if not exists:
            logging.warning("s3 bucket " + bucket_name +
                            " does not exist, creating it now.")
            s3.create_bucket(Bucket=bucket_name)

        print(" ... file = " + source_filepath + ", to bucket = " +
              bucket_name + ", key = " + key)
        response = s3.Object(bucket_name=bucket_name,
                             key=key).put(Body=open(source_filepath, 'rb'))

        logging.debug("Response = : ", response)

    @staticmethod
    def print_ec2_info(instance):
        print("Instance details.")
        print(" -- Public IP address is: ", instance.public_ip_address)
        print(" -- Private IP address is: ", instance.private_ip_address)
        print(" -- id is: ", str(instance.instance_id))
