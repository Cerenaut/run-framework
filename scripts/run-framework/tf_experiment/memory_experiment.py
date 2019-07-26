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

    # Launch container in the background
    if self.use_docker:
      print('Launching Docker...')
      self._launch_docker(host_node)

    experiment_id, experiment_prefix = self._create_experiment(host_node)

    # Start experiment
    if 'parameter-sweeps' not in config or not config['parameter-sweeps']:
      self._exec_experiment(host_node, experiment_id, experiment_prefix, config_json)
    else:
      hparams_sweeps = []
      workflow_opts_sweeps = []
      experiment_opts_sweeps = []

      if 'hparams' in config['parameter-sweeps'] and config['parameter-sweeps']['hparams']:
        hparams_sweeps = config['parameter-sweeps']['hparams']

      if 'workflow-options' in config['parameter-sweeps'] and config['parameter-sweeps']['workflow-options']:
        workflow_opts_sweeps = config['parameter-sweeps']['workflow-options']

      if 'experiment-options' in config['parameter-sweeps'] and config['parameter-sweeps']['experiment-options']:
        experiment_opts_sweeps = config['parameter-sweeps']['experiment-options']

      if hparams_sweeps or workflow_opts_sweeps or experiment_opts_sweeps:
        for hparams, workflow_opts, experiment_opts in itertools.zip_longest(hparams_sweeps, workflow_opts_sweeps, experiment_opts_sweeps):
          self._exec_experiment(host_node, experiment_id, experiment_prefix, config_json, param_sweeps={
              'hparams': hparams,
              'workflow_opts': workflow_opts,
              'experiment_opts': experiment_opts
          })
      else:
        self._exec_experiment(host_node, experiment_id, experiment_prefix, config_json)

  def _build_flags(self, exp_opts):
    flags = ''
    for key, value in exp_opts.items():
      flags += '--{0}={1} '.format(key, value)
    return flags

  def _exec_experiment(self, host_node, experiment_id, experiment_prefix,
                       config_json, param_sweeps=None):
    utils.remote_run(
        host_node,
        self._run_command(host_node, experiment_id, experiment_prefix,
                          config_json, param_sweeps))

    if self.export:
      utils.remote_run(
          host_node,
          self._upload_command(host_node, experiment_id, experiment_prefix))

  def _launch_docker(self, host_node):
    """Launch the Docker container on the remote machine."""
    assert self.docker_image is not None, 'Docker image not provided.'

    command = '''
      export RUN_DIR=$HOME/agief-remote-run

      docker pull {docker_image}
      docker run -d  -t --runtime=nvidia --mount type=bind,source=$RUN_DIR,target=$RUN_DIR \
        {docker_image} bash
    '''.format(
        docker_image=self.docker_image
    )

    remote_output = utils.remote_run(host_node, command)
    command_output = [s for s in remote_output]
    command_output = command_output[-1].strip()
    self.docker_id = command_output

    return command_output

  def _create_experiment(self, host_node):
    """Creates new MLFlow experiment remotely."""
    experiment_prefix = datetime.datetime.now().strftime('%y%m%d-%H%M')

    if self.use_docker:
      command = '''
        docker exec -it {docker_id} bash -c "
          source activate {anaenv}

          export RUN_DIR=$HOME/agief-remote-run
          export LC_ALL=C.UTF-8
          export LANG=C.UTF-8

          pip install -q -r \$RUN_DIR/memory/requirements.txt
          pip install -q -r \$RUN_DIR/classifier_component/requirements.txt

          cd \$RUN_DIR/memory
          mlflow experiments create {prefix}
        "
      '''.format(
          docker_id=self.docker_id,
          anaenv='tensorflow',
          prefix=experiment_prefix
      )
    else:
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
        echo '{config_json}' > $HOME/agief-remote-run/memory/experiment-definition.{prefix}.json

        docker exec -it {docker_id} bash -c '
          export DIR=$HOME/agief-remote-run/memory
          export SCRIPT=$DIR/experiment.py
          export EXP_DEF=$DIR/experiment-definition.{prefix}.json

          cd $DIR
          source activate {anaenv}
          python -u $SCRIPT --experiment_def=$EXP_DEF --summary_dir=$DIR/run/{summary_path} \
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
          experiment_opts=experiment_opts
      )
    else:
      command = '''
        source {remote_env} {anaenv}

        export RUN_DIR=$HOME/agief-remote-run
        export SCRIPT=$RUN_DIR/memory/experiment.py

        EXP_DEF="/tmp/experiment-definition.{prefix}.json"
        echo '{config_json}' > $EXP_DEF

        DIR=$(dirname "$SCRIPT")
        cd $DIR

        python -u $SCRIPT --experiment_def=$EXP_DEF --summary_dir=$DIR/run/{summary_path} \
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
          experiment_opts=experiment_opts
      )

    logging.info(command)

    print("---------- Run Command -----------")
    print("-- PREFIX: " + experiment_prefix)
    print("-- Summary path: " + summary_path)
    print("----------------------------------")

    return command

  def _upload_command(self, host_node, experiment_id, experiment_prefix):
    del host_node

    command = '''
      export DIR=$HOME/agief-remote-run/{project}

      gsutil cp -r $DIR/run/{prefix} gs://project-agi/experiments
      gsutil cp -r /tmp/experiment-definition.{prefix}.json gs://project-agi/experiments/{prefix}
      gsutil cp -r $DIR/mlruns/{experiment_id} gs://project-agi/experiments/{prefix}/mlflow-summary
    '''.format(
        prefix=experiment_prefix,
        experiment_id=experiment_id,
        project=self.project
    )

    logging.info(command)

    print('Uploading Experiment (prefix=' + experiment_prefix + ')...')

    return command
