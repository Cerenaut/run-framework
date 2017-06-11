from __future__ import print_function

import os
import logging
from datetime import datetime

from agief_experiment.host_node import HostNode
from agief_experiment.compute import Compute
from agief_experiment.cloud import Cloud
from agief_experiment.experiment import Experiment
from agief_experiment.launchmode import LaunchMode
from agief_experiment import utils

help_generic = """
run-framework.py allows you to run each step of the AGIEF (AGI Experimental Framework), locally and on AWS.
Each step can be toggled with a parameter prefixed with 'step'. See parameter list for description of parameters.
As with all scripts that are part of AGIEF, the environment variables in VARIABLES_FILES are used.
The main ones being $AGI_HOME (code) and $AGI_RUN_HOME (experiment definitions).

Note that script runs the experiment by updating the experiment Entity until termination.
The script imports input files to set up the experiment, and exports experimental results for archive.

See README.md for installation instructions and longer explanation of the end-to-end AGIEF system.

Assumptions:
- experiment entity exists, with 'terminated' field
- The VARIABLES_FILE is used for env variables
"""


def setup_arg_parsing():
    import argparse
    from argparse import RawTextHelpFormatter

    parser = argparse.ArgumentParser(description=help_generic, formatter_class=RawTextHelpFormatter)

    # generate input files from the java experiment description
    parser.add_argument('--step_gen_input', dest='main_class', required=False,
                        help='Generate input files for experiments, then exit. '
                             'The value is the Main class to run, that defines the experiment, '
                             'before exporting the experimental input files entities.json and data.json. ')

    # main program flow
    parser.add_argument('--step_remote', dest='remote_type',
                        help='Run Compute on remote machine. This parameter can specify "simple" or "aws". '
                             'If "AWS", then launch remote machine on AWS '
                             '(then --instanceid or --amiid needs to be specified).'
                             'Requires setting key path with --ssh_keypath'
                             '(default= % (default)s)')
    parser.add_argument('--exps_file', dest='exps_file', required=False,
                        help='Run experiments, defined in the file that is set with this parameter.'
                             'Filename is within AGI_RUN_HOME that defines the '
                             'experiments to run (with parameter sweeps) in json format (default=%(default)s).')
    parser.add_argument('--step_sync', dest='sync', action='store_true',
                        help='Sync the code and run folder. Copy from local machine to remote. '
                             'Requires setting --step_remote and key path with --ssh_keypath')
    parser.add_argument('--step_sync_s3_prefix', dest='sync_s3_prefix', required=False,
                        help='Sync output files. Download relevant output '
                             'files from a previous phase determined by prefix, to the remote machine.'
                             'Requires setting --step_remote and key path with --ssh_keypath')
    parser.add_argument('--step_compute', dest='launch_compute', action='store_true',
                        help='Launch the Compute node.')
    parser.add_argument('--step_shutdown', dest='shutdown', action='store_true',
                        help='Shutdown instances and Compute (if --launch_per_session) after other stages.')
    parser.add_argument('--step_export', dest='export', action='store_true',
                        help='Export entity tree and data at the end of each experiment.')
    parser.add_argument('--step_export_compute', dest='export_compute', action='store_true',
                        help='Compute should export entity tree and data at the end of each experiment '
                             '- i.e. saved on the Compute node.')
    parser.add_argument('--step_upload', dest='upload', action='store_true',
                        help='Upload exported entity tree and data at the end of each experiment.')
    parser.add_argument('--DEBUG-NO-RUN', dest='debug_no_run', action='store_true',
                        help='Do everything except actually run experiment - used for debugging.')

    # how to reach the Compute node
    parser.add_argument('--host', dest='host', required=False,
                        help='Host where the Compute node will be running (default=%(default)s). '
                             'THIS IS IGNORED IF RUNNING ON AWS (in which case the IP of the instance '
                             'specified by the Instance ID is used)')
    parser.add_argument('--port', dest='port', required=False,
                        help='Port where the Compute node will be running (default=%(default)s).')
    parser.add_argument('--user', dest='user', required=False,
                        help='If remote, the "user" on the remote Compute node (default=%(default)s).')
    parser.add_argument('--ssh_port', dest='ssh_port', required=False,
                        help='Which port to use for ssh when communicating with remote machine (default=%(default)s).')
    parser.add_argument('--remote_variables_file', dest='remote_variables_file', required=False,
                        help='If remote, the path to the remote VARIABLES_FILE to use on the remote Compute node '
                             '(default=%(default)s).')

    # launch mode
    parser.add_argument('--launch_per_session', dest='launch_per_session', action='store_true',
                        help='Compute node is launched once at the start (and shutdown at the end if you use '
                             '--step_shutdown. Otherwise, it is launched and shut per experiment.')

    parser.add_argument('--no_docker', dest='no_docker', action='store_true',
                        help='If set, then DO NOT launch in a docker container. Applies to LOCAL usage only. '
                             '(default=%(default)s). ')

    # aws/remote details
    parser.add_argument('--instanceid', dest='instanceid', required=False,
                        help='Instance ID of the ec2 container instance - to start an ec2 instance, use this OR ami id,'
                             ' not both.')
    parser.add_argument('--amiid', dest='amiid', required=False,
                        help='AMI ID for new ec2 instances - to start an ec2 instance, use this OR instance id, not'
                             ' both.')
    parser.add_argument('--ami_ram', dest='ami_ram', required=False,
                        help='If launching ec2 via AMI, use this to specify how much minimum RAM you want '
                             '(default=%(default)s).')
    parser.add_argument('--task_name', dest='task_name', required=False,
                        help='The name of the ecs task (default=%(default)s).')
    parser.add_argument('--ssh_keypath', dest='ssh_keypath', required=False,
                        help='Path to the private key for the remote machine, '
                             'used for comms over ssh (default=%(default)s).')
    parser.add_argument('--pg_instance', dest='pg_instance', required=False,
                        help='Instance ID of the Postgres ec2 instance (default=%(default)s). '
                             'If you want to use a running postgres instance, just specify the host (e.g. localhost). '
                             'WARNING: assumes that if the string starts with "i-", then it is an Instance ID')

    parser.add_argument('--logging', dest='logging', action='store_true', help='Turn logging on.')

    parser.set_defaults(remote_type="local")  # i.e. not remote
    parser.set_defaults(host="localhost")
    parser.set_defaults(port="8491")
    parser.set_defaults(ssh_port="22")
    parser.set_defaults(user="ec2-user")
    parser.set_defaults(remote_variables_file="/home/ec2-user/agief-project/variables/variables-ec2.sh")
    parser.set_defaults(pg_instance="localhost")
    parser.set_defaults(task_name="mnist-spatial-task:10")
    parser.set_defaults(ssh_keypath=utils.filepath_from_env_variable(".ssh/ecs-key.pem", "HOME"))
    parser.set_defaults(ami_ram='6')
    parser.set_defaults(no_docker=False)

    return parser.parse_args()


def check_args(args, compute_node):
    if args.amiid and args.instanceid:
        print("ERROR: Both the AMI ID and EC2 Instance ID have been specified. Use just one to specify how to get "
              "a running ec2 instance")
        exit(1)

    if not args.remote_type == "aws" and (args.amiid or args.instanceid):
        print("ERROR: amiid or instanceid was specified, but AWS has not been set, so they have no effect.")
        exit(1)

    if args.ssh_keypath and not compute_node.remote():
        print("WARNING: a keypath has been set, but we're not running on a remote machine (arg: step_remote). "
              "It will have no effect.")

    if args.sync and not compute_node.remote():
        print("ERROR: Syncing experiment is meaningless unless you're running on a "
              "remote machine (use param --step_remote)")
        exit(1)

    if args.exps_file and not args.launch_compute:
        print("WARNING: You have elected to run experiment without launching a Compute node. For success, you'll "
              "have to have one running already, or use param --step_compute)")


def main():
    print("------------------------------------------")
    print("----          run-framework           ----")
    print("------------------------------------------")

    # setup logging
    logger = logging.getLogger('root')
    log_format = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
    logging.basicConfig(format=log_format)
    logger.setLevel(logging.WARNING)

    # Record experiment start time
    exp_start_time = datetime.now()

    args = setup_arg_parsing()
    if args.logging:
        logging.debug("Arguments: ", args)

    exps_file = args.exps_file if args.exps_file else ""
    experiment = Experiment(args.logging, args.debug_no_run, LaunchMode.from_args(args), exps_file)

    # 1) Generate input files
    if args.main_class:
        compute_node = Compute(host_node=HostNode(), port=args.port)
        compute_node.launch(experiment, main_class=args.main_class, no_local_docker=args.no_docker)
        experiment.generate_input_files_locally(compute_node)
        compute_node.terminate()
        return

    # *) all other use cases (non Generate input files)

    cloud = Cloud(args.logging)

    if args.upload and not (args.export or args.export_compute):
        logging.warning("Uploading experiment to S3 is enabled, but 'export experiment' is not, so the most "
                        "important files (output entity.json and data.json) will be missing")

    if args.remote_type != "local":
        host_node = HostNode(args.host, args.user, args.ssh_keypath, args.remote_variables_file, args.ssh_port)
    else:
        host_node = HostNode(args.host, args.user)

    compute_node = Compute(host_node, args.port)

    check_args(args, compute_node)

    # 2) Setup infrastructure (on AWS or nothing to do locally)
    ips = {'ip_public': args.host, 'ip_private': None}
    ips_pg = {'ip_public': None, 'ip_private': None}
    instance_id = None

    is_pg_ec2 = args.pg_instance and args.pg_instance[:2] == 'i-'
    if args.remote_type == "aws":
        # start Compute ec2 either from instanceid or amiid
        if args.instanceid:
            ips = cloud.ec2_start_from_instanceid(args.instanceid)
            instance_id = args.instanceid
        else:
            ips, instance_id = cloud.ec2_start_from_ami('run-fwk auto', args.amiid, int(args.ami_ram))

        # start DB ec2, from instanceid
        if args.pg_instance and is_pg_ec2:
            ips_pg = cloud.ec2_start_from_instanceid(args.pg_instance)
        else:
            ips_pg = {'ip_private': args.pg_instance}

    elif args.pg_instance:
        if is_pg_ec2:
            logging.error("The pg instance is set to an ec2 instance id, but you are not running AWS.")
            exit(1)

        ips_pg = {'ip_public': args.pg_instance, 'ip_private': args.pg_instance}

    # infrastructure has been started
    # try to run experiment, and if fails with exception, still shut down infrastructure
    failed = False
    try:
        compute_node.host_node.host = ips['ip_public']
        compute_node.port = args.port

        # TEMPORARY HACK for ECS
        # Set the DB_HOST environment variable
        if args.pg_instance:
            os.putenv("DB_HOST", ips_pg['ip_private'])

        # 3) Sync code and run-home
        if args.sync:
            cloud.sync_experiment(compute_node.host_node)

        # 3.5) Sync data from S3 (typically used to download output files from a prev. experiment to be used as input)
        if args.sync_s3_prefix:
            cloud.remote_download_output(args.sync_s3_prefix, compute_node.host_node)

        # 4) Launch Compute (remote or local) - *** IF Mode == 'Per Session' ***
        if (LaunchMode.from_args(args) is LaunchMode.per_session) and args.launch_compute:
            compute_node.launch(experiment, cloud=cloud, main_class=args.main_class, no_local_docker=args.no_docker)

        # 5) Run experiments (includes per experiment 'export results' and 'upload results')
        if args.exps_file:
            experiment.run_sweeps(compute_node, cloud, args)
            experiment.persist_prefix_history()

    except Exception as e:
        failed = True

        logging.error("Something failed running sweeps generally. "
                      "If the error occurred in a specific parameter set it should have been caught there. "
                      "Attempt to shut down infrastructure if running, and exit.")
        logging.error(e)

        # TODO It may be running locally, and NOT in docker, so need to check for this scenario.
        # TODO see the compute.launch() method for checking for this scenario

        # Shutdown the Docker container
        print("Attempting to shutdown Docker container...")
        if host_node.remote() and compute_node.container_id:
            utils.remote_run(host_node, 'docker stop ' + compute_node.container_id, True)
        elif not host_node.remote():
            utils.docker_stop()

    # 6) Shutdown framework
    if args.shutdown:
        if LaunchMode.from_args(args) is LaunchMode.per_session:
            compute_node.terminate()

        # Shutdown infrastructure
        if args.remote_type == "aws":
            cloud.ec2_stop(instance_id)

            if is_pg_ec2:
                cloud.ec2_stop(args.pg_instance)

    # Record experiment end time
    exp_end_time = datetime.now()

    # Print the experiment runtime in d:h:m:s:ms format
    exp_runtime = utils.format_timedelta(exp_end_time - exp_start_time)
    print("Experiment finished in %d days, %d hr, %d min, %d s, %d ms." % tuple(exp_runtime))

    if failed:
        exit(1)


if __name__ == '__main__':
    main()
