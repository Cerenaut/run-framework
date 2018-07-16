"""MemoryExperiment class."""

import logging
import datetime

from agief_experiment import utils
from tf_experiment.experiment import Experiment

class MemoryExperiment(Experiment):
  """Experiment class for the Memory project."""

  def run_sweeps(self, config, args, host_node, hparams_sweeps):
    """Run the sweeps"""

    experiment_id = self._create_experiment()

    for _, hparams in enumerate(hparams_sweeps):
      # Start experiment
      utils.remote_run(
          host_node,
          self._run_command(experiment_id, config, hparams))

  def _build_flags(self, exp_opts):
    flags = ''
    for key, value in exp_opts.items():
      flags += '--{0}={1} '.format(key, value)
    return flags

  def _create_experiment(self):
    """Creates new MLFlow experiment remotely."""
    experiment_prefix = datetime.datetime.now().strftime('%y%m%d-%H%M')

    command = '''
      source /media/data/anaconda3/bin/activate {anaenv}
      mlflow experiments create {prefix}
    '''.format(
        anaenv='tensorflow',
        prefix=experiment_prefix
    )

    output = utils.remote_run(host_node, command)
    experiment_id = int(output.split()[-1])

    return experiment_id

  def _run_command(self, experiment_id, config, hparams):
    """Start the training procedure via SSH."""

    exp_opts = config['experiment-options']
    exp_opts.update({
        'experiment_id': experiment_id
    })

    # Build command-line flags from the dict
    flags = self._build_flags(exp_opts)

    command = '''
        source /media/data/anaconda3/bin/activate {anaenv}

        export RUN_DIR=$HOME/agief-remote-run
        export SCRIPT=$RUN_DIR/memory/experiment.py

        pip install -r $RUN_DIR/memory/requirements.txt
        pip install -r $RUN_DIR/classifier_component/requirements.txt

        DIR=$(dirname "$SCRIPT")
        cd $DIR

        python -u $SCRIPT {flags} --hparams_override="{hparams}"
    '''.format(
        anaenv='tensorflow',
        flags=flags,
        hparams=str(hparams)
    )

    logging.info(command)

    return command
