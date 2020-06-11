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

"""Self-organization project class."""

import os
import logging
import datetime

from agief_experiment import utils
from tf_experiment.memory_experiment import MemoryExperiment


class SelfOrgExperiment(MemoryExperiment):
  """Experiment class for the Self Organization projects."""

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
    workflow_opts = ''        # not used in self-org
    experiment_opts = ''      # not used in self-org

    if param_sweeps is not None:
      if 'hparams' in param_sweeps and param_sweeps['hparams']:
        hparams = str(param_sweeps['hparams'])

    if self.use_docker:
      hparams = hparams.replace("'", '\\"')
      workflow_opts = workflow_opts.replace("'", '\\"')
      experiment_opts = experiment_opts.replace("'", '\\"')

      command = '''
        echo '{config_json}' > $HOME/agief-remote-run/{project}/experiment-definition.{prefix}.json

        docker exec -it {docker_id} bash -c '
          export LC_ALL=C.UTF-8
          export LANG=C.UTF-8

          export DIR=$HOME/agief-remote-run/{project}/meta-learning
          export EXP_DEF=$DIR/experiment-definition.{prefix}.json

          cd $DIR
          source activate {anaenv}
          
          python run_nb.py --output_dir=$DIR/run/{summary_path} --hparams="{hparams}"
        '
      '''.format(
          anaenv='pytorch',
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
        export DIR=$RUN_DIR/{project}/meta-learning

        EXP_DEF="/tmp/experiment-definition.{prefix}.json"
        echo '{config_json}' > $EXP_DEF

        cd $DIR

        python run_nb.py --output_dir=$DIR/run/{summary_path} --hparams="{hparams}"
      '''.format(
          remote_env=host_node.remote_env_path,
          anaenv='pytorch',
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
