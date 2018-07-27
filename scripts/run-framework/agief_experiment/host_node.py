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

"""HostNode class."""

class HostNode:
  """Details about the host node."""

  def __init__(self, host="localhost", user=None, keypath=None, remote_variables_file=None, ssh_port="22",
               remote_env_path="activate"):
    self.host = host
    self.user = user
    self.keypath = keypath
    self.remote_variables_file = remote_variables_file
    self.ssh_port = ssh_port
    self.remote_env_path = remote_env_path

  def host_key_user_variables(self):
    return (" " + self.host + " " + self.keypath +
            " " + self.user + " " + self.remote_variables_file +
            " " + self.ssh_port)

  def remote(self):
    """
    If remote, then no need for a keypath, so use this as a proxy to
    calculate whether remote or not.
    """
    return self.keypath is not None
