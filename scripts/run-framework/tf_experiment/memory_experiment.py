# Copyright (C) 2018 Project AGI
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
# ==============================================================================

"""MemoryExperiment class."""

import os
import logging
import datetime
import itertools

from agief_experiment import utils
from tf_experiment.experiment import Experiment

class MemoryExperiment(Experiment):
  """Experiment class for the Memory project."""

  def run_sweeps(self, config, config_json, args, host_node):
    """Run the sweeps"""

    experiment_id, experiment_prefix = self._create_experiment(host_node)

    # Start experiment
    if 'parameter-sweeps' not in config or not config['parameter-sweeps']:
      utils.remote_run(
          host_node,
          self._run_command(host_node, experiment_id, experiment_prefix, config_json))
    else:
      for hparams, workflow_opts in itertools.zip_longest(
          config['parameter-sweeps']['hparams'],
          config['parameter-sweeps']['workflow-options']):
        utils.remote_run(host_node, self._run_command(
            host_node, experiment_id, experiment_prefix, config_json, param_sweeps={
                'hparams': hparams,
                'workflow_opts': workflow_opts
            }))

  def _build_flags(self, exp_opts):
    flags = ''
    for key, value in exp_opts.items():
      flags += '--{0}={1} '.format(key, value)
    return flags

  def _create_experiment(self, host_node):
    """Creates new MLFlow experiment remotely."""
    experiment_prefix = datetime.datetime.now().strftime('%y%m%d-%H%M')

    command = '''
      source {remote_env} {anaenv}

      export RUN_DIR=$HOME/agief-remote-run

      pip install -q -r $RUN_DIR/memory/requirements.txt
      pip install -q -r $RUN_DIR/classifier_component/requirements.txt

      cd $RUN_DIR/memory
      mlflow experiments create {prefix}
    '''.format(
        anaenv='tensorflow',
        remote_env=host_node.remote_env_path,
        prefix=experiment_prefix
    )

    remote_output = utils.remote_run(host_node, command)
    command_output = remote_output[1].strip().split(' ')
    experiment_id = int(command_output[-1])

    return experiment_id, experiment_prefix

  def _run_command(self, host_node, experiment_id, experiment_prefix, config_json, param_sweeps=None):
    """Start the training procedure via SSH."""

    # Build command-line flags from the dict
    now = datetime.datetime.now()
    summary_dir = 'summaries_' + now.strftime("%Y%m%d-%H%M%S") + '/'
    summary_path = os.path.join(experiment_prefix, summary_dir)

    hparams = ''
    workflow_opts = ''
    if param_sweeps is not None:
      hparams = str(param_sweeps['hparams'])
      workflow_opts = str(param_sweeps['workflow_opts'])

    command = '''
        source {remote_env} {anaenv}

        export RUN_DIR=$HOME/agief-remote-run
        export SCRIPT=$RUN_DIR/memory/experiment.py

        EXP_DEF="/tmp/experiment-definition.{prefix}.json"
        echo '{config_json}' > $EXP_DEF

        DIR=$(dirname "$SCRIPT")
        cd $DIR

        python -u $SCRIPT --experiment_def=$EXP_DEF --summary_dir=$DIR/run/{summary_path} \
        --experiment_id={experiment_id} --hparams_sweep="{hparams}" --workflow_opts_sweep="{workflow_opts}"
    '''.format(
        remote_env=host_node.remote_env_path,
        anaenv='tensorflow',
        prefix=experiment_prefix,
        config_json=config_json,
        summary_path=summary_path,
        experiment_id=experiment_id,
        hparams=hparams,
        workflow_opts=workflow_opts
    )

    logging.info(command)

    print("---------- Run Command -----------")
    print("-- PREFIX: " + experiment_prefix)
    print("-- Summary path: " + summary_path)
    print("----------------------------------")

    return command
