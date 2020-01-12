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

"""PAGIExperiment class."""

import os
import logging
import datetime

from agief_experiment import utils
from tf_experiment.memory_experiment import MemoryExperiment

class PAGIExperiment(MemoryExperiment):
  """Experiment class for the PAGI-dependent projects."""

  def _create_experiment(self, host_node):
    """Creates new MLFlow experiment remotely."""
    experiment_prefix = datetime.datetime.now().strftime('%y%m%d-%H%M')

    if self.use_docker:
      # pylint: disable=anomalous-backslash-in-string
      command = '''
        docker exec -it {docker_id} bash -c "
          source activate {anaenv}

          export LC_ALL=C.UTF-8
          export LANG=C.UTF-8
          export RUN_DIR=$HOME/agief-remote-run

          pip install -e \$RUN_DIR/pagi --force-reinstall
          pip install -e \$RUN_DIR/{project} --force-reinstall

          cd \$RUN_DIR/{project}
          mlflow experiments create {prefix}
        "
      '''.format(
          anaenv='tensorflow',
          prefix=experiment_prefix,
          docker_id=self.docker_id,
          project=self.project
      )
    else:
      command = '''
        source {remote_env} {anaenv}

        export RUN_DIR=$HOME/agief-remote-run

        pip install -e $RUN_DIR/pagi --force-reinstall
        pip install -e $RUN_DIR/{project} --force-reinstall

        cd $RUN_DIR/{project}
        mlflow experiments create {prefix}
      '''.format(
          anaenv='tensorflow',
          remote_env=host_node.remote_env_path,
          prefix=experiment_prefix,
          project=self.project
      )

    remote_output = utils.remote_run(host_node, command)
    command_output = [s for s in remote_output if 'Created experiment' in s]
    command_output = command_output[0].strip().split(' ')
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
    experiment_opts = ''

    if param_sweeps is not None:
      if 'hparams' in param_sweeps and param_sweeps['hparams']:
        hparams = str(param_sweeps['hparams'])
      if 'workflow_opts' in param_sweeps and param_sweeps['workflow_opts']:
        workflow_opts = str(param_sweeps['workflow_opts'])
      if 'experiment_opts' in param_sweeps and param_sweeps['experiment_opts']:
        experiment_opts = str(param_sweeps['experiment_opts'])

    if self.use_docker:
      hparams = hparams.replace("'", '\\"')
      workflow_opts = workflow_opts.replace("'", '\\"')
      experiment_opts = experiment_opts.replace("'", '\\"')

      command = '''
        echo '{config_json}' > /tmp/experiment-definition.{prefix}.json

        docker exec -it {docker_id} bash -c '
          export DIR=$HOME/agief-remote-run/{project}
          export SCRIPT=$DIR/experiment.py
          export EXP_DEF=$DIR/experiment-definition.{prefix}.json

          cd $DIR
          source activate {anaenv}
          pagi run --experiment_def=$EXP_DEF --summary_dir=$DIR/run/{summary_path} \
          --experiment_id={experiment_id} --hparams_sweep="{hparams}" --workflow_opts_sweep="{workflow_opts}" \
          --experiment_opts_sweep="{experiment_opts}"
        '
      '''.format(
          anaenv='tensorflow',
          prefix=experiment_prefix,
          config_json=config_json,
          summary_path=summary_path,
          experiment_id=experiment_id,
          hparams=hparams,
          docker_id=self.docker_id,
          workflow_opts=workflow_opts,
          experiment_opts=experiment_opts,
          project=self.project
      )
    else:
      command = '''
        source {remote_env} {anaenv}

        export RUN_DIR=$HOME/agief-remote-run
        export DIR=$RUN_DIR/{project}

        EXP_DEF="/tmp/experiment-definition.{prefix}.json"
        echo '{config_json}' > $EXP_DEF

        cd $DIR

        pagi run --experiment_def=$EXP_DEF --summary_dir=$DIR/run/{summary_path} \
        --experiment_id={experiment_id} --hparams_sweep="{hparams}" --workflow_opts_sweep="{workflow_opts}" \
        --experiment_opts_sweep="{experiment_opts}"
      '''.format(
          remote_env=host_node.remote_env_path,
          anaenv='tensorflow',
          prefix=experiment_prefix,
          config_json=config_json,
          summary_path=summary_path,
          experiment_id=experiment_id,
          hparams=hparams,
          workflow_opts=workflow_opts,
          experiment_opts=experiment_opts,
          project=self.project
      )

    logging.info(command)

    print("---------- Run Command -----------")
    print("-- PREFIX: " + experiment_prefix)
    print("-- Summary path: " + summary_path)
    print("----------------------------------")

    return command
