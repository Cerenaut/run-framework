"""Experiment base class."""

from agief_experiment import utils

class Experiment(object):
  """Base class for TensorFlow-based experiments."""

  def sync_experiment(self, remote):
    """
    Sync experiment from this machine to remote machine
    """
    print('\n....... Sync Experiment')

    cmd = '../remote/remote-sync-tf-experiment.sh ' + remote.host_key_user_variables()
    utils.run_bashscript_repeat(cmd, 15, 6)

  def run_sweeps(self, config, config_json, args, host_node):
    """Run the sweeps"""
    raise NotImplementedError('Not implemented')
