

class HostNode:
    """ Details about the host node. """

    def __init__(self,
                 host="localhost",
                 user=None,
                 keypath=None,
                 remote_variables_file=None):

        self.host = host
        self.user = user
        self.keypath = keypath
        self.remote_variables_file = remote_variables_file

    def host_key_user_variables(self):
        return " " + self.host + " " + self.keypath + " " + self.user + " " + self.remote_variables_file

    def remote(self):
        """ If remote, then no need for a keypath, so use this as a proxy to calculate whether remote or not. """
        if self.keypath is None:
            return False
        else:
            return True
