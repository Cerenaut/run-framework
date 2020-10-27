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

"""CFSLExperiment class."""

import os
import logging
import datetime

from tf_experiment.memory_experiment import MemoryExperiment


class CFSLExperiment(MemoryExperiment):
  """Experiment class for the the CFSL project."""

  def _create_experiment(self, host_node):
    """Creates new MLFlow experiment remotely."""
    experiment_prefix = datetime.datetime.now().strftime('%y%m%d-%H%M')   # still useful
    experiment_id = 0   # only used for mlflow, so don't need it
    return experiment_id, experiment_prefix

  def _run_command(self, host_node, experiment_id, experiment_prefix, config_json, param_sweeps=None):
    """Start the training procedure via SSH."""

    # Build command-line flags from the dict
    now = datetime.datetime.now()
    summary_dir = 'summaries_' + now.strftime("%Y%m%d-%H%M%S") + '/'
    summary_path = os.path.join(experiment_prefix, summary_dir)

    hparams = ''
    workflow_opts = ''        # not used
    experiment_opts = ''      # not used

    if param_sweeps is not None:
      if 'hparams' in param_sweeps and param_sweeps['hparams']:
        hparams = str(param_sweeps['hparams'])

    if self.use_docker:
      hparams = hparams.replace("'", '\\"')
      workflow_opts = workflow_opts.replace("'", '\\"')
      experiment_opts = experiment_opts.replace("'", '\\"')

      command = '''
        echo '{config_json}' > $HOME/agief-remote-run/{project}/frameworks/cfsl/experiment-definition.{prefix}.json

        docker exec -it {docker_id} bash -c '
          export LC_ALL=C.UTF-8
          export LANG=C.UTF-8

          export DIR=$HOME/agief-remote-run/{project}/frameworks/cfsl
          export EXP_DEF=$DIR/experiment-definition.{prefix}.json

          export GPU_ID=0
          export CONTINUE_FROM_EPOCH=latest
          export DATASET_DIR="datasets/"
          export CUDA_VISIBLE_DEVICES=$GPU_ID

          cd $DIR
          source activate {anaenv}

          bash install.sh

          python train_continual_learning_few_shot_system.py \
            --name_of_args_json_file $EXP_DEF \
            --gpu_to_use $GPU_ID \
            --continue_from_epoch=$CONTINUE_FROM_EPOCH
        '
      '''.format(
          anaenv='pytorch',
          prefix=experiment_prefix,
          config_json=config_json,
          docker_id=self.docker_id,
          project=self.project
      )
    else:
      command = '''
        source {remote_env} {anaenv}

        export DIR=$HOME/agief-remote-run/{project}/frameworks/cfsl

        EXP_DEF=$DIR/experiment-definition.{prefix}.json
        echo '{config_json}' > $EXP_DEF

        export GPU_ID=0
        export CONTINUE_FROM_EPOCH=latest
        export DATASET_DIR="datasets/"
        export CUDA_VISIBLE_DEVICES=$GPU_ID

        cd $DIR

        bash install.sh

        python train_continual_learning_few_shot_system.py \
          --name_of_args_json_file $EXP_DEF \
          --gpu_to_use $GPU_ID \
          --continue_from_epoch=$CONTINUE_FROM_EPOCH
      '''.format(
          remote_env=host_node.remote_env_path,
          anaenv='pytorch',
          prefix=experiment_prefix,
          config_json=config_json,
          project=self.project
      )

    logging.info(command)

    print("---------- Run Command -----------")
    print("-- PREFIX: " + experiment_prefix)
    print("-- Summary path: " + summary_path)
    print("----------------------------------")

    return command

  def _upload_command(self, host_node, experiment_id, experiment_prefix):
    """Uploads definitions file, summaries and mlflow outputs."""
    del host_node, experiment_id

    command = '''
      export DIR=$HOME/agief-remote-run/{project}/frameworks/cfsl

      if [ -d "$DIR/runs/{prefix}" ]; then
        gsutil cp -r $DIR/run/{prefix} gs://project-agi/experiments
      fi

      if [ -f "$DIR/experiment-definition.{prefix}.json" ]; then
        gsutil cp -r $DIR/experiment-definition.{prefix}.json gs://project-agi/experiments/{prefix}
      fi
    '''.format(
        prefix=experiment_prefix,
        project=self.project
    )

    logging.info(command)

    print('Uploading Experiment (prefix=' + experiment_prefix + ')...')

    return command
