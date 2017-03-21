import json
import os
import subprocess
from enum import Enum

from agief_experiment import host_node
from agief_experiment import compute
from agief_experiment import cloud
from agief_experiment import experiment
from agief_experiment import utils
from agief_experiment import valueseries

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


def run_parameterset(entity_filepath, data_filepaths, compute_data_filepaths, sweep_param_vals):
    """
    Import input files
    Run Experiment and Export experiment
    The input files specified by params ('entity_file' and 'data_file')
    have parameters modified, which are described in parameters 'param_description'

    :param entity_filepath:
    :param data_filepaths:
    :param compute_data_filepaths:      data files on the compute machine, relative to run folder
    :param sweep_param_vals:
    :return:
    """

    print "........ Run parameter set."

    info = _experiment.info(sweep_param_vals)

    print info

    info_filepath = _experiment.outputfile("experiment-info.txt")
    utils.create_folder(info_filepath)
    with open(info_filepath, 'w') as data:
        data.write(info)

    is_valid = utils.check_validity([entity_filepath]) and utils.check_validity(data_filepaths)

    if not is_valid:
        print "ERROR: One of the input files are not valid:"
        print entity_filepath
        print json.dumps(data_filepaths)
        exit(1)

    if (launch_mode is LaunchMode.per_experiment) and args.launch_compute:
        task_arn = launch_compute()

    _compute_node.import_experiment(entity_filepath, data_filepaths)
    _compute_node.import_compute_experiment(compute_data_filepaths, is_data=True)

    set_dataset(_experiment.experiment_def_file())

    _compute_node.run_experiment(_experiment)

    if is_export:
        out_entity_file_path, out_data_file_path = _experiment.output_names_from_input_names(entity_filepath,
                                                                                             data_filepaths)
        _compute_node.export_experiment(_experiment.entity_with_prefix("experiment"),
                                        out_entity_file_path,
                                        out_data_file_path)

    if is_export_compute:
        _compute_node.export_experiment(_experiment.entity_with_prefix("experiment"),
                                        _experiment.outputfile_remote(),
                                        _experiment.outputfile_remote(),
                                        True)

    if (launch_mode is LaunchMode.per_experiment) and args.launch_compute:
        shutdown_compute(task_arn)

    if is_upload_results:
        _experiment.upload_results(_cloud, _compute_node, is_export_compute)


def setup_parameter_sweepers(param_sweep, val_sweepers):
    """
    For each 'param' in a set, get details and setup counter
    The result is an array of counters
    Each counter represents one parameter

    :param param_sweep:
    :param val_sweepers:
    :return:
    """

    param_i = 0
    for param in param_sweep['parameter-set']:  # set of params for one 'sweep'

        if False:
            print "LOG: Parameter sweep set part: " + str(param_i)
            print json.dumps(param, indent=4)
        param_i += 1

        entity_name = param['entity-name']
        param_path = param['parameter-path']

        if 'val-series' in param.keys():
            val_series = param['val-series']
            value_series = valueseries.ValueSeries(val_series)
        else:
            val_begin = param['val-begin']
            val_end = param['val-end']
            val_inc = param['val-inc']

            value_series = valueseries.ValueSeries.from_range(val_begin, val_end, val_inc)

        val_sweeper = {'value-series': value_series, 'entity-name': entity_name, 'param-path': param_path}
        val_sweepers.append(val_sweeper)


def inc_parameter_set(entity_filepath, val_sweepers):
    """
    Iterate through counters, incrementing each parameter in the set
    Set the new values in the input file, and then run the experiment
    First counter to reset, return False

    :param entity_filepath:
    :param val_sweepers:
    :return: reset (True if any counter has reached above max), description of parameters (string)
                            If reset is False, there MUST be a description of the parameters that have been set
    """

    if len(val_sweepers) == 0:
        print "WARNING: in_parameter_set: there are no counters to use to increment the parameter set."
        print "         Returning without any action. This may have undesirable consequences."
        return True, ""

    # inc all counters, and set parameter in entity file
    sweep_param_vals = []
    reset = False
    for val_sweeper in val_sweepers:
        val_series = val_sweeper['value-series']

        # check if it overflowed last time it was incremented
        overflowed = val_series.overflowed()

        if overflowed:
            if log:
                print "LOG: Sweeping has concluded for this sweep-set, due to the parameter: " + \
                      val_sweeper['entity-name'] + '.' + val_sweeper['param-path']
            reset = True
            break

        val = val_series.value()
        set_param = _compute_node.set_parameter_inputfile(entity_filepath,
                                                          _experiment.entity_with_prefix(val_sweeper['entity-name']),
                                                          val_sweeper['param-path'],
                                                          val)
        sweep_param_vals.append(set_param)
        val_series.next_val()

    if len(sweep_param_vals) == 0:
        print "WARNING: no parameters were changed."

    if log:
        if len(sweep_param_vals):
            print "LOG: Parameter sweep: ", sweep_param_vals

    if reset is False and len(sweep_param_vals) == 0:
        print "Error: inc_parameter_set() indeterminate state, reset is False, but parameter_description indicates " \
              "no parameters have been modified. If there is no sweep to conduct, reset should be True."
        exit(1)

    return reset, sweep_param_vals


def create_all_input_files(TEMPLATE_PREFIX, base_entity_filename, base_data_filenames):
    exp_entity_filepaths = _experiment.create_input_files(TEMPLATE_PREFIX, [base_entity_filename])
    exp_entity_filepath = exp_entity_filepaths[0]
    exp_data_filepaths = _experiment.create_input_files(TEMPLATE_PREFIX, base_data_filenames)
    return exp_entity_filepath, exp_data_filepaths


def run_sweeps():
    """ Perform parameter sweep steps, and run experiment for each step. """

    print "........ Run Sweeps"

    exps_filename = _experiment.experiment_def_file()

    if not os.path.exists(exps_filename):
        print "ERROR: Experiment file does not exist at: " + exps_filename
        print "Cannot continue."
        exit(1)

    with open(exps_filename) as exps_file:
        filedata = json.load(exps_file)

    for exp_i in filedata['experiments']:
        import_files = exp_i['import-files']  # import files dictionary

        if log:
            print "LOG: Import Files Dictionary = "
            print "LOG: ", json.dumps(import_files, indent=4)

        base_entity_filename = import_files['file-entities']
        base_data_filenames = import_files['file-data']

        exp_ll_data_filepaths = []
        if 'load-local-files' in exp_i.keys():
            load_local_files = exp_i['load-local-files']
            if 'file-data' in load_local_files.keys():
                base_ll_data_filenames = load_local_files['file-data']
                exp_ll_data_filepaths = map(_experiment.runpath, base_ll_data_filenames)

        if 'parameter-sweeps' not in exp_i or len(exp_i['parameter-sweeps']) == 0:
            print "No parameters to sweep, just run once."

            _experiment.reset_prefix()
            exp_entity_filepath, exp_data_filepaths = create_all_input_files(TEMPLATE_PREFIX,
                                                                             base_entity_filename,
                                                                             base_data_filenames)
            run_parameterset(exp_entity_filepath, exp_data_filepaths, exp_ll_data_filepaths, "")
        else:
            param_sweeps = exp_i['parameter-sweeps']
            for param_sweep in param_sweeps:  # array of sweep definitions

                counters = []
                setup_parameter_sweepers(param_sweep, counters)

                is_sweeping = True
                while is_sweeping:

                    _experiment.reset_prefix()

                    exp_entity_filepath, exp_data_filepaths = create_all_input_files(TEMPLATE_PREFIX,
                                                                                     base_entity_filename,
                                                                                     base_data_filenames)

                    reset, sweep_param_vals = inc_parameter_set(exp_entity_filepath, counters)
                    if reset:
                        is_sweeping = False
                    else:
                        run_parameterset(exp_entity_filepath, exp_data_filepaths, exp_ll_data_filepaths,
                                         sweep_param_vals)


def set_dataset(exps_file):
    """
    The dataset can be located in different locations on different machines. The location can be set in the
    experiments definition file (experiments.json). This method parses that file, finds the parameters to set
    relative to the AGI_DATA_HOME env variable, and sets the specified parameters.
    :param exps_file:
    :return:
    """

    print "....... Set Dataset"

    with open(exps_file) as data_exps_file:
        data = json.load(data_exps_file)

    for exp_i in data['experiments']:
        for param in exp_i['dataset-parameters']:  # array of sweep definitions
            entity_name = param['entity-name']
            param_path = param['parameter-path']
            data_filenames = param['value']

            data_filenames_arr = data_filenames.split(',')

            data_paths = ""
            for data_filename in data_filenames_arr:
                if data_paths is not "":
                    data_paths += ","  # IMPORTANT - if space added here, additional characters ('+') get added probably due to encoding issues on the request
                data_paths += _experiment.datapath(data_filename)

            _compute_node.set_parameter_db(_experiment.entity_with_prefix(entity_name), param_path, data_paths)


def launch_compute_aws_ecs(task_name):
    """
    Launch Compute on AWS ECS (elastic container service).
    Assumes that ECS is setup to have the necessary task, and container instances running.
    Hang till Compute is up and running. Return task arn.

    :param task_name:
    :return:
    """

    print "launching Compute on AWS-ECS"

    if task_name is None:
        print "ERROR: you must specify a Task Name to run on aws-ecs"
        exit(1)

    task_arn = _cloud.ecs_run_task(task_name)
    _compute_node.wait_up()
    return task_arn


def launch_compute_remote_docker():
    """
    Launch Compute Node on AWS. Assumes there is a running ec2 instance running Docker
    Hang till Compute is up and running.
    """

    print "launching Compute on AWS (on ec2 using run-in-docker.sh)"

    _cloud.remote_docker_launch_compute(_compute_node.host_node)
    _compute_node.wait_up()


def launch_compute_local(main_class="", run_in_docker=True):
    """ Launch Compute locally. Hang till Compute is up and running.

    If main_class is specified, then use run-demo.sh,
    which builds entity graph and data from the relevant Demo project defined by the Main Class.
    WARNING: In this case, the properties file used is hardcoded to node.properties
    WARNING: and the prefix used is the global variable PREFIX

    :param run_in_docker:
    :param main_class:
    :return:
    """

    print "launching Compute locally"
    print "NOTE: generating run_stdout.log and run_stderr.log (in the current folder)"

    if run_in_docker:
        cmd = _experiment.agi_binpath("/node_coordinator/run-in-docker.sh -d")
    else:
        cmd = _experiment.agi_binpath("/node_coordinator/run.sh")

    if main_class is not "":
        cmd = _experiment.agi_binpath("/node_coordinator/run-demo.sh")
        cmd = cmd + " node.properties " + main_class + " " + TEMPLATE_PREFIX

    if log:
        print "Running: " + cmd

    cmd += " > run_stdout.log 2> run_stderr.log "

    # we can't hold on to the stdout and stderr streams for logging, because it will hang on this line
    # instead, logging to a file
    subprocess.Popen(cmd,
                     shell=True,
                     executable="/bin/bash")

    _compute_node.wait_up()


def launch_compute(use_ecs=False):
    """ Launch Compute locally or remotely. Return task arn if on AWS ECS. """

    print "....... Launch Compute"

    task_arn = None

    if _compute_node.remote():
        if use_ecs:
            task_arn = launch_compute_aws_ecs(args.task_name)
        else:
            launch_compute_remote_docker()
    else:
        launch_compute_local(run_in_docker=is_local_docker)

    version = _compute_node.version()
    print "Running Compute version: " + version

    return task_arn


def shutdown_compute(task_arn):
    """ Close compute: terminate and then if running on AWS, stop the task. """

    print "....... Shutdown System"

    _compute_node.terminate()

    # note that task may be set up to terminate once compute has been terminated
    if is_aws and (task_arn is not None):
        _cloud.ecs_stop_task(task_arn)


def generate_input_files_locally():
    entity_file_path, data_file_paths = _experiment.inputfiles_for_generation()

    # write to the first listed data path name
    data_file_path = data_file_paths[0]

    root_entity = _experiment.entity_with_prefix("experiment")
    _compute_node.export_experiment(root_entity, entity_file_path, data_file_path)


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


if __name__ == '__main__':

    print "------------------------------------------"
    print "----          run-framework           ----"
    print "------------------------------------------"

    TEMPLATE_PREFIX = "SPAGHETTI"
    PREFIX_DELIMITER = "--"

    args = setup_arg_parsing()
    log = args.logging
    if log:
        print "LOG: Arguments: ", args

    if args.exps_file:
        _experiment = experiment.Experiment(log, TEMPLATE_PREFIX, PREFIX_DELIMITER, args.exps_file)

    # 1) Generate input files
    if args.main_class:
        host_node = host_node.HostNode()
        _compute_node = compute.Compute(host_node=host_node, port=args.port, log=log)
        _compute_node.host = args.host
        launch_compute_local(args.main_class)
        generate_input_files_locally()
        _compute_node.terminate()
        exit(1)

    # *) all other use cases (non Generate input files)

    _cloud = cloud.Cloud(log)

    is_export = args.export  # export from Compute node via API to local machine
    is_export_compute = args.export_compute  # export from Compute node to a file on Compute node
    is_upload_results = args.upload
    is_local_docker = args.no_docker
    sync_s3_prefix = args.sync_s3_prefix

    if is_upload_results and not (is_export or is_export_compute):
        print "WARNING: Uploading experiment to S3 is enabled, but 'export experiment' is not, so the most " \
              "important files (output entity.json and data.json) will be missing"

    if args.launch_per_session:
        launch_mode = LaunchMode.per_session
    else:
        launch_mode = LaunchMode.per_experiment

    is_aws = False
    if args.remote_type != "local":
        if args.remote_type == "aws":
            is_aws = True
        host_node = host_node.HostNode(args.host, args.user, args.ssh_keypath, args.remote_variables_file)
    else:
        host_node = host_node.HostNode(args.host, args.user)

    _compute_node = compute.Compute(host_node, args.port, log)

    if args.amiid and args.instanceid:
        print "ERROR: Both the AMI ID and EC2 Instance ID have been specified. Use just one to specify how to get " \
              "a running ec2 instance"
        exit(1)

    if not is_aws and (args.amiid or args.instanceid):
        print "ERROR: amiid or instanceid was specified, but AWS has not been set, so they have no effect."
        exit(1)

    if args.ssh_keypath and not _compute_node.remote():
        print "WARNING: a keypath has been set, but we're not running on a remote machine (arg: step_remote). " \
              "It will have no effect."

    if args.sync and not _compute_node.remote():
        print "ERROR: Syncing experiment is meaningless unless you're running on a " \
              "remote machine (use param --step_remote)"
        exit(1)

    if args.exps_file and not args.launch_compute:
        print "WARNING: You have elected to run experiment without launching a Compute node. For success, you'll" \
              "have to have one running already, or use param --step_compute)"

    # 2) Setup infrastructure (on AWS or nothing to do locally)
    ips = {'ip_public': args.host, 'ip_private': None}
    ips_pg = {'ip_public': None, 'ip_private': None}
    instance_id = None

    is_pg_ec2 = False
    if args.pg_instance:
        is_pg_ec2 = (args.pg_instance[:2] == 'i-')

    if is_aws:
        # start Compute ec2 either from instanceid or amiid
        if args.instanceid:
            ips = _cloud.ec2_start_from_instanceid(args.instanceid)
            instance_id = args.instanceid
        else:
            ips, instance_id = _cloud.ec2_start_from_ami('run-fwk auto', args.amiid, int(args.ami_ram))

        # start DB ec2, from instanceid
        if args.pg_instance and is_pg_ec2:
            ips_pg = _cloud.ec2_start_from_instanceid(args.pg_instance)
        else:
            ips_pg = {'ip_private': args.pg_instance}

    elif args.pg_instance:
        if is_pg_ec2:
            print "ERROR: the pg instance is set to an ec2 instance id, but you are not running AWS."
            exit(1)

        ips_pg = {'ip_public': args.pg_instance, 'ip_private': args.pg_instance}

    _compute_node.host_node.host = ips['ip_public']
    _compute_node.port = args.port

    # TEMPORARY HACK for ECS
    # Set the DB_HOST environment variable
    if args.pg_instance:
        os.putenv("DB_HOST", ips_pg['ip_private'])

    # 3) Sync code and run-home
    if args.sync:
        _cloud.sync_experiment(_compute_node.host_node)

    # 3.5) Sync data from S3 (typically used to download output files from a previous experiment to be used as input)
    if sync_s3_prefix:
        _cloud.remote_download_output(sync_s3_prefix, _compute_node.host_node)

    # 4) Launch Compute (remote or local) - *** IF Mode == 'Per Session' ***
    if (launch_mode is LaunchMode.per_session) and args.launch_compute:
        launch_compute()

    # 5) Run experiments (includes per experiment 'export results' and 'upload results')
    if args.exps_file:
        run_sweeps()

    # 6) Shutdown framework
    if args.shutdown:

        if launch_mode is LaunchMode.per_session:
            _compute_node.terminate()

        # Shutdown infrastructure
        if is_aws:
            _cloud.ec2_stop(instance_id)

            if is_pg_ec2:
                _cloud.ec2_stop(args.pg_instance)
