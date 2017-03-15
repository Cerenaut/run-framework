
class RemoteNode:
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
