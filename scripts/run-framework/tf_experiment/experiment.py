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

"""Experiment base class."""

from agief_experiment import utils

class Experiment:
  """Base class for TensorFlow-based experiments."""

  def __init__(self, project=None, export=False, use_docker=False, docker_image=None):
    self.project = project
    self.export = export
    self.use_docker = use_docker
    self.docker_image = docker_image

  def sync_experiment(self, remote):
    """
    Sync experiment from this machine to remote machine
    """
    print('\n....... Sync Experiment')

    cmd = '../remote/remote-sync-tf-experiment.sh ' + remote.host_key_user_variables()
    print('sync cmd', cmd)
    utils.run_bashscript_repeat(cmd, 15, 6)

  def run_sweeps(self, config, config_json, args, host_node):
    """Run the sweeps"""
    raise NotImplementedError('Not implemented')
