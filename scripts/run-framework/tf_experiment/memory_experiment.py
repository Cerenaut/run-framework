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

import logging
import datetime

from agief_experiment import utils
from tf_experiment.experiment import Experiment

class MemoryExperiment(Experiment):
  """Experiment class for the Memory project."""

  def run_sweeps(self, config, config_json, args, host_node):
    """Run the sweeps"""

    experiment_id, experiment_prefix = self._create_experiment(host_node)

    # Start experiment
    for _, hparams in enumerate(config['parameter-sweeps']):
      utils.remote_run(
          host_node,
          self._run_command(host_node, experiment_id, experiment_prefix, config, config_json, hparams))

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
    command_output = remote_output[0].strip().split(' ')
    experiment_id = int(command_output[-1])

    return experiment_id, experiment_prefix

  def _run_command(self, host_node, experiment_id, experiment_prefix, config, config_json, hparams):
    """Start the training procedure via SSH."""

    exp_opts = config['experiment-options']
    exp_opts.update({
        'experiment_id': experiment_id
    })

    # Build command-line flags from the dict
    flags = self._build_flags(exp_opts)

    command = '''
        source {remote_env} {anaenv}

        export RUN_DIR=$HOME/agief-remote-run
        export SCRIPT=$RUN_DIR/memory/experiment.py

        EXP_DEF="/tmp/experiment-definition.{prefix}.json"
        echo '{config_json}' > $EXP_DEF

        DIR=$(dirname "$SCRIPT")
        cd $DIR

        python -u $SCRIPT {flags} --experiment_def=$EXP_DEF --experiment_dir=$DIR/run/{prefix} \
        --hparams_override="{hparams}"
    '''.format(
        anaenv='tensorflow',
        remote_env=host_node.remote_env_path,
        flags=flags,
        prefix=experiment_prefix,
        config_json=config_json,
        hparams=str(hparams)
    )

    logging.info(command)

    return command
