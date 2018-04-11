from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import json
import logging
import datetime
import traceback

from agief_experiment import utils
from agief_experiment.cloud import Cloud
from agief_experiment.compute import Compute
from agief_experiment.host_node import HostNode


def setup_arg_parsing():
    """
    Parse the commandline arguments
    """
    import argparse
    from argparse import RawTextHelpFormatter

    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)

    parser.add_argument('--step_phase', dest='phase',
                        help='Choose the specific experiment phase to run.'
                             'Options: train, eval or classify')
    parser.add_argument('--prefixes', dest='prefixes',
                        help='The prefixes to use for classify/eval.'
                             'Must be comma separated.')

    # main program flow
    parser.add_argument('--step_remote', dest='remote_type',
                        help='Run Compute on remote machine. This parameter '
                             'can specify "simple" or "aws". '
                             'If "AWS", then launch remote machine on AWS '
                             '(then --instanceid or --amiid needs to '
                             'be specified). Requires setting key path '
                             'with --ssh_keypath (default=%(default)s)')
    parser.add_argument('--exps_file', dest='exps_file', required=False,
                        help='Run experiments, defined in the file that is '
                             'set with this parameter. Filename is within '
                             'AGI_RUN_HOME that defines the experiments to '
                             'run (with parameter sweeps) in json format '
                             '(default=%(default)s).')
    parser.add_argument('--step_sync', dest='sync', action='store_true',
                        help='Sync the code and run folder. Copy from local '
                             'machine to remote. Requires setting '
                             '--step_remote and key path with --ssh_keypath')

    # how to reach the Compute node
    parser.add_argument('--host', dest='host', required=False,
                        help='Host where the Compute node will be running '
                             '(default=%(default)s). THIS IS IGNORED IF '
                             'RUNNING ON AWS (in which case the IP of the '
                             'instance specified by the Instance ID is used)')
    parser.add_argument('--user', dest='user', required=False,
                        help='If remote, the "user" on the remote '
                             'Compute node (default=%(default)s).')
    parser.add_argument('--ssh_port', dest='ssh_port', required=False,
                        help='Which port to use for ssh when communicating '
                             'with remote machine (default=%(default)s).')
    parser.add_argument('--remote_variables_file',
                        dest='remote_variables_file', required=False,
                        help='If remote, the path to the remote '
                             'VARIABLES_FILE to use on the remote '
                             'Compute node (default=%(default)s).')
    parser.add_argument('--ssh_keypath', dest='ssh_keypath', required=False,
                        help='Path to the private key for the remote machine, '
                             'used for comms over ssh (default=%(default)s).')

    parser.add_argument('--logging', dest='logging', required=False,
                        help='Logging level (default=%(default)s). '
                             'Options: debug, info, warning, error, critical')

    parser.set_defaults(prefixes=None)
    parser.set_defaults(remote_type='local')  # i.e. not remote
    parser.set_defaults(host='localhost')
    parser.set_defaults(ssh_port='22')
    parser.set_defaults(user='ubuntu')
    parser.set_defaults(remote_variables_file='/home/ubuntu/agief-python/'
                                              'agi-tensorflow/variables/'
                                              'variables-compute.sh')

    parser.set_defaults(logging='warning')

    return parser.parse_args()


def get_hparams_sweeps(sweeps):
    hparams_sweeps = []
    for i, sweep in enumerate(sweeps):
        hparams_override = ''
        for param, value in sweep.items():
            hparams_override += '{0}={1},'.format(param, value)
        hparams_override = hparams_override[:-1]
        hparams_sweeps.append(hparams_override)
    return hparams_sweeps


def classify_op(variables_file, summary_dir, dataset, model, last_step,
                model_dir):
    command = '''
        export VARIABLES_FILE={variables_file}
        source {variables_file}
        source activate tensorflow
        cd $TF_HOME/{model_dir}
        python classifier.py --model={model} --dataset={dataset} \
        --data_dir=$TF_SUMMARY/{summary_dir}/classify/output \
        --summary_dir=$TF_SUMMARY/{summary_dir}/classify \
        --last_step={last_step}
    '''.format(
        variables_file=variables_file,
        summary_dir=summary_dir,
        dataset=dataset,
        model_dir=model_dir,
        model=model,
        last_step=last_step
    )

    logging.info(command)

    return command


def eval_op(variables_file, exp_params, train_params, summary_dir,
            eval_sweep, hparams):
    command = '''
        export VARIABLES_FILE={variables_file}
        source {variables_file}
        source activate tensorflow
        cd $TF_HOME/{model_dir}
        python experiment.py --data_dir=$TF_DATA/{data_dir} --train=false \
        --checkpoint=$TF_SUMMARY/{summary_dir}/train/model.ckpt-{max_steps} \
        --summary_dir=$TF_SUMMARY/{summary_dir} --shift=0 --pad={pad} \
        --eval_set={eval_set} --eval_size={eval_size} --batch_size=100 \
        --eval_shard={eval_shard} --dataset={dataset} --num_gpus=1 \
        --hparams_override={hparams_override}
    '''.format(
        variables_file=variables_file,
        model_dir=exp_params['model'],
        max_steps=train_params['max_steps'],
        summary_dir=summary_dir,
        pad=eval_sweep['pad'],
        dataset=eval_sweep['dataset'],
        data_dir=train_params['dataset_path'],
        eval_set=eval_sweep['eval_set'],
        eval_shard=eval_sweep['eval_shard'],
        eval_size=eval_sweep['eval_size'],
        hparams_override=hparams
    )

    logging.info(command)

    return command


def train_op(vars_file, exp_params, train_params, summary_dir, hparams):
    command = '''
        export VARIABLES_FILE={variables_file}
        source {variables_file}
        source activate tensorflow
        cd $TF_HOME/{model_dir}
        python experiment.py --data_dir=$TF_DATA/{data_dir} \
        --summary_dir=$TF_SUMMARY/{summary_dir} --shift={shift} --pad={pad} \
        --batch_size={batch_size} --dataset={dataset} \
        --num_gpus={num_gpus} --max_steps={max_steps} \
        --hparams_override={hparams_override} \
        --summary_override=true
    '''.format(
        variables_file=vars_file,
        model_dir=exp_params['model'],
        num_gpus=exp_params['num_gpus'],
        max_steps=train_params['max_steps'],
        summary_dir=summary_dir,
        pad=train_params['pad'],
        shift=train_params['shift'],
        dataset=train_params['dataset'],
        batch_size=train_params['batch_size'],
        data_dir=train_params['dataset_path'],
        hparams_override=hparams
     )

    logging.info(command)

    return command


def main():
    """
    The main scope of the run-framework containing the high level code
    """

    print('------------------------------------------')
    print('----          run-framework           ----')
    print('------------------------------------------')

    # Record experiment start time
    exp_start_time = datetime.datetime.now()

    args = setup_arg_parsing()

    # setup logging
    log_format = ('[%(filename)s:%(lineno)s - %(funcName)s() ' +
                  '- %(levelname)s] %(message)s')
    logging.basicConfig(format=log_format,
                        level=utils.logger_level(args.logging))

    logging.debug('Python Version: ' + sys.version)
    logging.debug('Arguments: %s', args)

    exps_filepath = args.exps_file if args.exps_file else ''

    with open(exps_filepath) as config_file:
        config = json.load(config_file)
        hparams_sweeps = get_hparams_sweeps(config['train-sweeps'])

    cloud = Cloud()

    if args.remote_type != 'local':
        host_node = HostNode(args.host, args.user, args.ssh_keypath,
                             args.remote_variables_file, args.ssh_port)
    else:
        host_node = HostNode(args.host, args.user)

    compute_node = Compute(host_node)

    ips = {'ip_public': args.host, 'ip_private': None}
    ips_pg = {'ip_public': None, 'ip_private': None}
    instance_id = None
    # TODO: setup AWS/GCP infrastructure

    failed = False
    try:
        compute_node.host_node.host = ips['ip_public']

        # Sync experiment
        if args.sync:
            cloud.sync_tf_experiment(compute_node.host_node)

        # Run sweeps
        if args.phase == 'train':
            prefixes = []
            print('........ Training\n')
            for i, hparams in enumerate(hparams_sweeps):
                run_prefix = datetime.datetime.now().strftime('%y%m%d-%H%M')
                prefixes.append(run_prefix)

                summary_dir = os.path.join(
                    config['experiment-parameters']['summary_dir'],
                    run_prefix)

                # Start experiment
                utils.remote_run(
                    host_node,
                    train_op(host_node.remote_variables_file,
                             config['experiment-parameters'],
                             config['train-parameters'],
                             summary_dir, hparams))

            with open('prefixes.txt', 'w') as prefix_file:
                prefix_file.write(','.join(prefixes))

        if args.phase == 'eval' or args.phase == 'classify':
            if args.prefixes is None:
                raise Exception('No prefixes provided.')

            prefixes = [x.strip() for x in args.prefixes.split(',')]

            for i, prefix in enumerate(prefixes):
                summary_dir = os.path.join(
                    config['experiment-parameters']['summary_dir'],
                    prefix)

                if args.phase == 'eval':
                    # Export experiment for each prefix
                    print('........ Evaluating: {0}\n'.format(prefix))
                    for eval_sweep in config['eval-sweeps']:
                        utils.remote_run(
                            host_node,
                            eval_op(
                                host_node.remote_variables_file,
                                config['experiment-parameters'],
                                config['train-parameters'],
                                summary_dir, eval_sweep, hparams_sweeps[i]))

                if args.phase == 'classify':
                    # Classification
                    print('........ Classifying: {0}\n'.format(prefix))
                    for classify_sweep in config['classify-sweeps']:
                        for model in classify_sweep['model']:
                            utils.remote_run(
                                host_node,
                                classify_op(
                                    host_node.remote_variables_file,
                                    summary_dir,
                                    classify_sweep['dataset'],
                                    model,
                                    config['train-parameters']['max_steps'],
                                    config['experiment-parameters']['model']))

    except Exception as err:  # pylint: disable=W0703
        failed = True

        logging.error("Something failed running sweeps generally. If the "
                      "error occurred in a specific parameter set it should "
                      "have been caught there. Attempt to shut down "
                      "infrastructure if running, and exit.")
        logging.error(err)

        print('-'*60)
        traceback.print_exc(file=sys.stdout)
        print('-'*60)

    # TODO: Shutdown framework

    # Record experiment end time
    exp_end_time = datetime.datetime.now()

    # Log the experiment runtime in d:h:m:s:ms format
    exp_runtime = utils.format_timedelta(exp_end_time - exp_start_time)
    print("Experiment finished in %d days, %d hr, %d min, %d s" %
          tuple(exp_runtime))

    if failed:
        exit(1)


if __name__ == '__main__':
    main()
