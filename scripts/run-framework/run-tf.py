"""Experiment framework for TensorFlow."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import json
import logging
import datetime
import traceback

import googleapiclient.discovery

from agief_experiment import utils
from agief_experiment.compute import Compute
from agief_experiment.host_node import HostNode

from tf_experiment.memory_experiment import MemoryExperiment
from tf_experiment.sparsecaps_experiment import SparseCapsExperiment

EXPERIMENTS = {
    'memory': MemoryExperiment,
    'sparse_caps': SparseCapsExperiment
}

def setup_arg_parsing():
  '''
  Parse the commandline arguments
  '''
  import argparse
  from argparse import RawTextHelpFormatter

  parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)

  parser.add_argument('--step_exp', dest='exp_type',
                      help='Choose which experiment type to run.')
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
  parser.add_argument('--step_shutdown', dest='shutdown',
                      action='store_true',
                      help='Shutdown instances and Compute '
                           '(if --launch_per_session) after other stages.')

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

  # GCP details details
  parser.add_argument('--instanceid', dest='instanceid', required=False,
                      help='Instance ID of the GCP container instance - to '
                           'start a GCP instance')
  parser.add_argument('--zone', dest='zone', required=False,
                      help='The GCP instance region zone, e.g. us-east1-b.')
  parser.add_argument('--project', dest='project', required=False,
                      help='GCP project name.')

  parser.add_argument('--logging', dest='logging', required=False,
                      help='Logging level (default=%(default)s). '
                           'Options: debug, info, warning, error, critical')

  parser.set_defaults(prefixes=None)
  parser.set_defaults(exp_type='memory')
  parser.set_defaults(remote_type='local')  # i.e. not remote
  parser.set_defaults(host='localhost')
  parser.set_defaults(ssh_port='22')
  parser.set_defaults(user='ubuntu')
  parser.set_defaults(zone='us-east1-b')
  parser.set_defaults(remote_variables_file='/home/ubuntu/agief-python/'
                                            'agi-tensorflow/variables/'
                                            'variables-compute.sh')

  parser.set_defaults(logging='warning')

  return parser.parse_args()

def wait_for_operation(compute, project, zone, operation):
  """Waits for GCP-related operations to complete before proceeding."""
  print('Waiting for operation to finish...')
  while True:
    result = compute.zoneOperations().get(project=project, zone=zone, operation=operation).execute()

    if result['status'] == 'DONE':
      print('done.')
      if 'error' in result:
        raise Exception(result['error'])
      return result

    time.sleep(1)

def main():
  '''
  The main scope of the run-framework containing the high level code
  '''

  print('------------------------------------------')
  print('----          run-framework           ----')
  print('------------------------------------------')

  # Record experiment start time
  exp_start_time = datetime.datetime.now()

  args = setup_arg_parsing()

  # setup logging
  log_format = ('[%(filename)s:%(lineno)s - %(funcName)s() - %(levelname)s] %(message)s')
  logging.basicConfig(format=log_format, level=utils.logger_level(args.logging))

  logging.debug('Python Version: %s', sys.version)
  logging.debug('Arguments: %s', args)

  exps_filepath = args.exps_file if args.exps_file else ''
  with open(exps_filepath) as config_file:
    config = json.load(config_file)

  if args.remote_type != 'local':
    host_node = HostNode(args.host, args.user, args.ssh_keypath, args.remote_variables_file, args.ssh_port)
  else:
    host_node = HostNode(args.host, args.user)

  compute_node = Compute(host_node)
  gcp_compute = googleapiclient.discovery.build('compute', 'v1')

  # Setup Infrastructure
  ips = {'ip_public': args.host, 'ip_private': None}
  instance_id = None

  if args.remote_type == 'gcp':
    # Start GCP Compute using instanceid
    if args.instanceid:
      instance_id = args.instanceid
      operation = gcp_compute.instances().start(zone=args.zone, project=args.project, instance=instance_id).execute()
      wait_for_operation(gcp_compute, args.project, args.project, operation['name'])

  # Infrastructure has been started
  # Try to run experiment, and if fails with exception, still shut down infrastructure
  failed = False
  try:
    compute_node.host_node.host = ips['ip_public']

    # Create new experiment
    experiment = EXPERIMENTS[args.exp_type]()

    # Sync experiment
    if args.sync:
      experiment.sync_experiment(compute_node.host_node)

    # Run sweeps
    experiment.run_sweeps(config, args, host_node, config['parameter-sweeps'])

  except Exception as err:  # pylint: disable=W0703
    failed = True

    logging.error('Something failed running sweeps generally. If the '
                  'error occurred in a specific parameter set it should '
                  'have been caught there. Attempt to shut down '
                  'infrastructure if running, and exit.')
    logging.error(err)

    print('-'*60)
    traceback.print_exc(file=sys.stdout)
    print('-'*60)

  # Shutdown framework
  if args.shutdown:

    # Shutdown infrastructure
    if args.remote_type == 'gcp':
      shutdown_op = gcp_compute.instances().stop(zone=args.zone, project=args.project, instance=instance_id).execute()
      wait_for_operation(gcp_compute, args.project, args.project, shutdown_op['name'])

  # Record experiment end time
  exp_end_time = datetime.datetime.now()

  # Log the experiment runtime in d:h:m:s:ms format
  exp_runtime = utils.format_timedelta(exp_end_time - exp_start_time)
  print('Experiment finished in %d days, %d hr, %d min, %d s' % tuple(exp_runtime))

  if failed:
    exit(1)

if __name__ == '__main__':
  main()
