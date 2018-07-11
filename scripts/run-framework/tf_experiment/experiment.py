"""Experiment base class."""

class Experiment(object):
  """Base class for TensorFlow-based experiments."""

  def run_sweeps(self, config, args, host_node, hparams_sweeps):
    """Run the sweeps"""
    raise NotImplementedError('Not implemented')
