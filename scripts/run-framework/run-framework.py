from __future__ import print_function
import functools
import json
import os
import subprocess

import dpath
from enum import Enum

from agief_experiment.host_node import HostNode
from agief_experiment.compute import Compute
from agief_experiment.cloud import Cloud
from agief_experiment.experiment import Experiment
from agief_experiment import utils
from agief_experiment.valueseries import ValueSeries

help_generic = """
run-framework.py allows you to run each step of the AGIEF (AGI Experimental Framework), locally and on AWS.
Each step can be toggled with a parameter prefixed with 'step'. See parameter list for description of parameters.
As with all scripts that are part of AGIEF, the environment variables in VARIABLES_FILES are used.
The main ones being $AGI_HOME (code) and $AGI_RUN_HOME (experiment definitions).

Note that script runs the experiment by updating the Experiment Entity until termination.
The script imports input files to set up the experiment, and exports experimental results for archive.

See README.md for installation instructions and longer explanation of the end-to-end AGIEF system.

Assumptions:
- Experiment entity exists, with 'terminated' field
- The VARIABLES_FILE is used for env variables
"""

DISABLE_RUN_FOR_DEBUG = False
TEMPLATE_PREFIX = "SPAGHETTI"
PREFIX_DELIMITER = "--"


def log_results_config(experiment, compute_node):
    config_exp = compute_node.get_entity_config(experiment.entity_with_prefix("experiment"))

    reporting_name_key = "reportingEntityName"
    reporting_path_key = "reportingEntityConfigPath"
    if 'value' in config_exp and reporting_name_key in config_exp['value']:
        # get the reporting entity's config
        entity_name = config_exp['value'][reporting_name_key]
        config = compute_node.get_entity_config(entity_name)

        # get the relevant param, or if not there, show the whole config
        report = None
        param_path = None
        try:
            if reporting_path_key in config_exp['value']:
                param_path = config_exp['value'][reporting_path_key]
                report = dpath.util.get(config, 'value.' + param_path, '.')
            else:
                print("WARNING: No reporting entity config path found in experiment config.")
        except KeyError:
            print("KeyError Exception")
            print("WARNING: trying to access path '" + param_path + "' at config.value, but it DOES NOT exist!")
        if report is None:
            print("\n================================================")
            print("Reporting Entity Config:")
            print(json.dumps(config, indent=4))
            print("================================================\n")
        else:
            print("\n================================================")
            print("Reporting Entity Config Path (" + entity_name + "-Config.value." + param_path + "):")
            print(report)
            print("================================================\n")
    else:
        print("WARNING: No reportingEntityName has been specified in Experiment config.")


def run_parameterset(experiment, compute_node, cloud, args, entity_filepath, data_filepaths, compute_data_filepaths,
                     sweep_param_vals=''):
    """
    Import input files
    Run Experiment and Export experiment
    The input files specified by params ('entity_file' and 'data_file')
    have parameters modified, which are described in parameters 'param_description'

    :param experiment:
    :param compute_node:
    :param cloud:
    :param args:
    :param entity_filepath:
    :param data_filepaths:
    :param compute_data_filepaths:      data files on the compute machine, relative to run folder
    :param sweep_param_vals:
    :return:
    """

    print("........ Run parameter set.")

    # print and save experiment info
    info = experiment.info(sweep_param_vals)
    print(info)

    info_filepath = experiment.outputfile("experiment-info.txt")
    utils.create_folder(info_filepath)
    with open(info_filepath, 'w') as data:
        data.write(info)

    failed = False
    task_arn = None
    try:
        is_valid = utils.check_validity([entity_filepath]) and utils.check_validity(data_filepaths)

        if not is_valid:
            msg = "ERROR: One of the input files are not valid:\n"
            msg += entity_filepath + "\n"
            msg += json.dumps(data_filepaths)
            raise Exception(msg)

        if (LaunchMode.from_args(args) is LaunchMode.per_experiment) and args.launch_compute:
            task_arn = launch_compute(experiment, compute_node, cloud, args)

        compute_node.import_experiment(entity_filepath, data_filepaths)
        compute_node.import_compute_experiment(compute_data_filepaths, is_data=True)

        set_dataset(experiment, compute_node)

        if not DISABLE_RUN_FOR_DEBUG:
            compute_node.run_experiment(experiment)

        experiment.remember_prefix()

        # log results expressed in the appropriate entity config
        log_results_config(experiment, compute_node)

        if args.export:
            out_entity_file_path, out_data_file_path = experiment.output_names_from_input_names(entity_filepath,
                                                                                                data_filepaths)
            compute_node.export_subtree(experiment.entity_with_prefix("experiment"),
                                        out_entity_file_path,
                                        out_data_file_path)

        if args.export_compute:
            compute_node.export_subtree(experiment.entity_with_prefix("experiment"),
                                        experiment.outputfile_remote(),
                                        experiment.outputfile_remote(),
                                        True)
    except Exception as e:
        failed = True
        print("ERROR: Experiment failed for some reason, shut down Compute and continue.")
        print(e)

    if task_arn:
        shutdown_compute(compute_node, cloud, args, task_arn)

    if not failed and args.upload:
        experiment.upload_results(cloud, compute_node, args.export_compute)


def setup_parameter_sweepers(param_sweep):
    """
    For each 'param' in a set, get details and setup counter
    The result is an array of counters
    Each counter represents one parameter
    """
    val_sweepers = []
    for param in param_sweep['parameter-set']:  # set of params for one 'sweep'
        if 'val-series' in param:
            value_series = ValueSeries(param['val-series'])
        else:
            value_series = ValueSeries.from_range(minv=param['val-begin'],
                                                  maxv=param['val-end'],
                                                  deltav=param['val-inc'])
        val_sweepers.append({'value-series': value_series,
                             'entity-name': param['entity-name'],
                             'param-path': param['parameter-path']})
    return val_sweepers


def inc_parameter_set(experiment, compute_node, args, entity_filepath, val_sweepers):
    """
    Iterate through counters, incrementing each parameter in the set
    Set the new values in the input file, and then run the experiment
    First counter to reset, return False

    :param experiment:
    :param compute_node:
    :param args:
    :param entity_filepath:
    :param val_sweepers:
    :return: reset (True if any counter has reached above max), description of parameters (string)
                            If reset is False, there MUST be a description of the parameters that have been set
    """

    if len(val_sweepers) == 0:
        print("WARNING: in_parameter_set: there are no counters to use to increment the parameter set.")
        print("         Returning without any action. This may have undesirable consequences.")
        return True, ""

    # inc all counters, and set parameter in entity file
    sweep_param_vals = []
    reset = False
    for val_sweeper in val_sweepers:
        val_series = val_sweeper['value-series']

        # check if it overflowed last time it was incremented
        overflowed = val_series.overflowed()

        if overflowed:
            if args.logging:
                print("LOG: Sweeping has concluded for this sweep-set, due to the parameter: " +
                      val_sweeper['entity-name'] + '.' + val_sweeper['param-path'])
            reset = True
            break

        set_param = compute_node.set_parameter_inputfile(entity_filepath,
                                                         experiment.entity_with_prefix(val_sweeper['entity-name']),
                                                         val_sweeper['param-path'],
                                                         val_series.value())
        sweep_param_vals.append(set_param)
        val_series.next_val()

    if len(sweep_param_vals) == 0:
        print("WARNING: no parameters were changed.")

    if args.logging:
        if len(sweep_param_vals):
            print("LOG: Parameter sweep: " + str(sweep_param_vals))

    if reset is False and len(sweep_param_vals) == 0:
        print("Error: inc_parameter_set() indeterminate state, reset is False, but parameter_description indicates " 
              "no parameters have been modified. If there is no sweep to conduct, reset should be True.")
        exit(1)

    return reset, sweep_param_vals


def run_sweeps(experiment, compute_node, cloud, args):
    """ Perform parameter sweep steps, and run experiment for each step. """

    print("\n........ Run Sweeps")

    exps_filename = experiment.experiment_def_file()

    if not os.path.exists(exps_filename):
        msg = "ERROR: Experiment file does not exist at: " + exps_filename
        raise Exception(msg)

    with open(exps_filename) as exps_file:
        filedata = json.load(exps_file)

    for exp_i in filedata['experiments']:
        import_files = exp_i['import-files']  # import files dictionary

        if args.logging:
            print("LOG: Import Files Dictionary = ")
            print("LOG: ", json.dumps(import_files, indent=4))

        base_entity_filename = import_files['file-entities']
        base_data_filenames = import_files['file-data']

        exp_ll_data_filepaths = []
        if 'load-local-files' in exp_i:
            load_local_files = exp_i['load-local-files']
            if 'file-data' in load_local_files:
                exp_ll_data_filepaths = list(map(experiment.runpath, load_local_files['file-data']))

        exp_entity_filepath = experiment.create_input_files(TEMPLATE_PREFIX, [base_entity_filename])[0]
        exp_data_filepaths = experiment.create_input_files(TEMPLATE_PREFIX, base_data_filenames)
        run_parameterset_partial = functools.partial(run_parameterset,
                                                     experiment=experiment,
                                                     compute_node=compute_node,
                                                     cloud=cloud,
                                                     args=args,
                                                     entity_filepath=exp_entity_filepath,
                                                     data_filepaths=exp_data_filepaths,
                                                     compute_data_filepaths=exp_ll_data_filepaths)
        if 'parameter-sweeps' not in exp_i or len(exp_i['parameter-sweeps']) == 0:
            print("No parameters to sweep, just run once.")
            experiment.reset_prefix()
            run_parameterset_partial()
        else:
            for param_sweep in exp_i['parameter-sweeps']:  # array of sweep definitions
                counters = setup_parameter_sweepers(param_sweep)
                while True:
                    experiment.reset_prefix()
                    reset, sweep_param_vals = inc_parameter_set(experiment, compute_node, args,
                                                                exp_entity_filepath, counters)
                    if reset:
                        break
                    run_parameterset_partial(sweep_param_vals=sweep_param_vals)


def set_dataset(experiment, compute_node):
    """
    The dataset can be located in different locations on different machines. The location can be set in the
    experiments definition file (experiments.json). This method parses that file, finds the parameters to set
    relative to the AGI_DATA_HOME env variable, and sets the specified parameters.
    """

    print("\n....... Set Dataset")

    with open(experiment.experiment_def_file()) as data_exps_file:
        data = json.load(data_exps_file)

    for exp_i in data['experiments']:
        for param in exp_i['dataset-parameters']:  # array of sweep definitions
            entity_name = param['entity-name']
            param_path = param['parameter-path']
            data_filenames = param['value']

            data_filenames_arr = data_filenames.split(',')

            data_paths = ""
            for data_filename in data_filenames_arr:
                if data_paths != "":
                    # IMPORTANT: if space added here, additional characters ('+') get added, probably due to encoding
                    # issues on the request
                    data_paths += ","
                data_paths += experiment.datapath(data_filename)

            compute_node.set_parameter_db(experiment.entity_with_prefix(entity_name), param_path, data_paths)


def launch_compute_aws_ecs(compute_node, cloud, task_name):
    """
    Launch Compute on AWS ECS (elastic container service).
    Assumes that ECS is setup to have the necessary task, and container instances running.
    Hang till Compute is up and running. Return task arn.
    """

    print("launching Compute on AWS-ECS")

    if not task_name:
        raise ValueError("ERROR: you must specify a Task Name to run on aws-ecs")

    task_arn = cloud.ecs_run_task(task_name)
    compute_node.wait_up()
    return task_arn


def launch_compute_remote_docker(compute_node, cloud):
    """
    Launch Compute Node on AWS. Assumes there is a running ec2 instance running Docker
    Hang till Compute is up and running.
    """
    print("launching Compute on AWS (on ec2 using run-in-docker.sh)")
    cloud.remote_docker_launch_compute(compute_node.host_node)
    compute_node.wait_up()


def launch_compute_local(experiment, compute_node, args, main_class="", run_in_docker=True):
    """ Launch Compute locally. Hang till Compute is up and running.

    If main_class is specified, then use run-demo.sh,
    which builds entity graph and data from the relevant Demo project defined by the Main Class.
    WARNING: In this case, the properties file used is hardcoded to node.properties
    WARNING: and the prefix used is the global variable PREFIX
    """

    print("launching Compute locally")
    print("NOTE: generating run_stdout.log and run_stderr.log (in the current folder)")

    if run_in_docker:
        cmd = experiment.agi_binpath("/node_coordinator/run-in-docker.sh -d")
    else:
        cmd = experiment.agi_binpath("/node_coordinator/run.sh")

    if main_class is not "":
        cmd = experiment.agi_binpath("/node_coordinator/run-demo.sh")
        cmd = cmd + " node.properties " + main_class + " " + TEMPLATE_PREFIX

    if args.logging:
        print("Running: " + cmd)

    cmd += " > run_stdout.log 2> run_stderr.log "

    # we can't hold on to the stdout and stderr streams for logging, because it will hang on this line
    # instead, logging to a file
    subprocess.Popen(cmd,
                     shell=True,
                     executable="/bin/bash")

    compute_node.wait_up()


# TODO: is this ever called with use_ecs=True?
def launch_compute(experiment, compute_node, cloud, args, use_ecs=False):
    """ Launch Compute locally or remotely. Return task arn if on AWS ECS. """

    print("\n....... Launch Compute")

    task_arn = None

    if compute_node.remote():
        if use_ecs:
            task_arn = launch_compute_aws_ecs(compute_node, cloud, args.task_name)
        else:
            launch_compute_remote_docker(compute_node, cloud)
    else:
        launch_compute_local(experiment, compute_node, args, run_in_docker=args.no_docker)

    print("Running Compute version: " + compute_node.version())

    return task_arn


def shutdown_compute(compute_node, cloud, args, task_arn):
    """ Close compute: terminate and then if running on AWS, stop the task. """

    print("\n....... Shutdown System")

    compute_node.terminate()

    # note that task may be set up to terminate once compute has been terminated
    if args.remote_type == "aws" and (task_arn is not None):
        cloud.ecs_stop_task(task_arn)


def generate_input_files_locally(experiment, compute_node):
    entity_filepath, data_filepaths = experiment.inputfiles_for_generation()
    # write to the first listed data path name
    compute_node.export_subtree(root_entity=experiment.entity_with_prefix("experiment"),
                                entity_filepath=entity_filepath,
                                data_filepath=data_filepaths[0])


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

    # how to reach the Compute node
    parser.add_argument('--host', dest='host', required=False,
                        help='Host where the Compute node will be running (default=%(default)s). '
                             'THIS IS IGNORED IF RUNNING ON AWS (in which case the IP of the instance '
                             'specified by the Instance ID is used)')
    parser.add_argument('--port', dest='port', required=False,
                        help='Port where the Compute node will be running (default=%(default)s).')
    parser.add_argument('--user', dest='user', required=False,
                        help='If remote, the "user" on the remote Compute node (default=%(default)s).')
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
    parser.set_defaults(user="ec2-user")
    parser.set_defaults(remote_variables_file="/home/ec2-user/agief-project/variables/variables-ec2.sh")
    parser.set_defaults(pg_instance="localhost")
    parser.set_defaults(task_name="mnist-spatial-task:10")
    parser.set_defaults(ssh_keypath=utils.filepath_from_env_variable(".ssh/ecs-key.pem", "HOME"))
    parser.set_defaults(ami_ram='6')

    return parser.parse_args()


class LaunchMode(Enum):
    per_experiment = 1
    per_session = 2

    @classmethod
    def from_args(cls, args):
        return cls.per_session if args.launch_per_session else cls.per_experiment


def main():
    print("------------------------------------------")
    print("----          run-framework           ----")
    print("------------------------------------------")

    args = setup_arg_parsing()
    if args.logging:
        print("LOG: Arguments: ", args)

    if args.exps_file:
        experiment = Experiment(args.logging, TEMPLATE_PREFIX, PREFIX_DELIMITER, args.exps_file)
    else:
        # an instantiated object is still necessary for things such as getting paths to ENV variables defined in
        # variables file. This could be improved by making them static or breaking that out into another class.
        experiment = Experiment(args.logging, TEMPLATE_PREFIX, PREFIX_DELIMITER, "")

    # 1) Generate input files
    if args.main_class:
        compute_node = Compute(host_node=HostNode(), port=args.port, log=args.logging)
        compute_node.host = args.host
        # TODO: is not passing in run_in_docker a bug?
        launch_compute_local(experiment, compute_node, args, main_class=args.main_class)
        generate_input_files_locally(experiment, compute_node)
        compute_node.terminate()
        exit(1)

    # *) all other use cases (non Generate input files)

    cloud = Cloud(args.logging)

    if args.upload and not (args.export or args.export_compute):
        print("WARNING: Uploading experiment to S3 is enabled, but 'export experiment' is not, so the most "
              "important files (output entity.json and data.json) will be missing")

    if args.remote_type != "local":
        host_node = HostNode(args.host, args.user, args.ssh_keypath, args.remote_variables_file)
    else:
        host_node = HostNode(args.host, args.user)

    compute_node = Compute(host_node, args.port, args.logging)

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
            print("ERROR: the pg instance is set to an ec2 instance id, but you are not running AWS.")
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

        # 3.5) Sync data from S3 (typically used to download output files from a previous experiment to be used as
        #      input)
        if args.sync_s3_prefix:
            cloud.remote_download_output(args.sync_s3_prefix, compute_node.host_node)

        # 4) Launch Compute (remote or local) - *** IF Mode == 'Per Session' ***
        if (LaunchMode.from_args(args) is LaunchMode.per_session) and args.launch_compute:
            launch_compute(experiment, compute_node, cloud, args)

        # 5) Run experiments (includes per experiment 'export results' and 'upload results')
        if args.exps_file:
            run_sweeps(experiment, compute_node, cloud, args)
            experiment.persist_prefix_history()

    except Exception as e:
        failed = True
        print("ERROR: Something failed running sweeps generally. "
              "If the error occurred in a specific parameter set it should have been caught there. "
              "Attempt to shut down infrastructure if running, and exit.")
        print(e)

    # 6) Shutdown framework
    if args.shutdown:
        if LaunchMode.from_args(args) is LaunchMode.per_session:
            compute_node.terminate()

        # Shutdown infrastructure
        if args.remote_type == "aws":
            cloud.ec2_stop(instance_id)

            if is_pg_ec2:
                cloud.ec2_stop(args.pg_instance)

    if failed:
        exit(1)

if __name__ == '__main__':
    main()
