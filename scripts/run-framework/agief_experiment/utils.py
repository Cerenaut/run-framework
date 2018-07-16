import json
import subprocess
import os
import errno
import zipfile
import fileinput
import sys
import time
import logging
import paramiko
import datetime


def restart_line():
    sys.stdout.write('\r')
    sys.stdout.flush()


def replace_in_file(src_string, dest_string, file_path):
    # NOTE: cannot use context manager (i.e. 'with') with FileInput in 2.7
    f = fileinput.FileInput(file_path, inplace=True)
    for line in f:
        line = line.replace(src_string, dest_string).rstrip()
        print(line)
    f.close()


def create_folder(filepath):
    if not os.path.exists(os.path.dirname(filepath)):
        try:
            os.makedirs(os.path.dirname(filepath))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


def append_before_ext(filename, text):
    file_split = os.path.splitext(filename)
    new_filename = file_split[0] + text + file_split[1]
    return new_filename


def getbaseurl(host, port):
    return 'http://' + host + ':' + port


def filepath_from_env_variable(filename, path_env):
    cmd = "echo $" + path_env
    output, error = subprocess.Popen(cmd,
                                     shell=True,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE,
                                     executable="/bin/bash").communicate()

    file_path = cleanpath(output, filename)
    return file_path


def cleanpath(path, filename):
    """
    Given a path and a filename, return the fully qualified filename with path
    """

    path_from_env = path.strip()
    filename = filename.strip()
    filename = filename.lstrip('/')
    file_path = os.path.join(path_from_env, filename)
    return file_path


def run_bashscript_repeat(cmd, max_repeats, wait_period):
    """
    Run a shell command repeatedly until exit status shows success.
    Run command 'cmd' a maximum of 'max_repeats' times and
    wait 'wait_period' between attempts.
    """

    logging.debug("running cmd = %s", str(cmd))

    success = False
    exit_status = 0
    for i in range(1, max_repeats + 1):
        child = subprocess.Popen(cmd,
                                 shell=True,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 executable="/bin/bash")

        # get the outputs.
        # NOTE: This will block until shell command returns.
        output, error = child.communicate()
        exit_status = child.returncode

        logging.debug("Exit status: %s", str(exit_status))
        logging.debug("Stdout: %s", str(output))

        if len(error):
            logging.error("Stderr: %s", str(error))

        if exit_status == 0:
            success = True
            break

        logging.warning("Run bash script was unsuccessful on attempt %s", str(i))
        logging.debug("Wait %ss, and try again.", str(wait_period))

        time.sleep(wait_period)

    if not success:
        msg = "ERROR: was not able run shell command: " + cmd + "\n"
        msg += " Exit status = " + str(exit_status)
        raise Exception(msg)


def check_validity(files):
    """
    Check validity of files, and exit if they do not exist or not specified
    """

    is_valid = True
    file_paths = []

    for f in files:
        # TODO for some reason exists() not working
        if os.path.isfile(f) and os.path.exists(f):
            file_paths.append(f)
        else:
            is_valid = False
            logging.error("this file is not valid: " + f)
            break

    return is_valid


def is_valid_filename(filepath):
    fileName, fileExtension = os.path.splitext(filepath)
    if fileExtension:
        return True
    return False


def compress_file(source_filepath):
    """
    Compress the specified file
    :param source_filepath: the path to the file to be compressed
    :return:
    """

    if os.path.isfile(source_filepath) and os.path.exists(source_filepath):
        zipf = zipfile.ZipFile(source_filepath + '.zip', 'w',
                               zipfile.ZIP_DEFLATED)
        zipf.write(source_filepath)
        zipf.close()
    else:
        logging.error("this file is not valid: " + source_filepath)


def compress_files(zipfilepath, source_filepaths):
    with zipfile.ZipFile(zipfilepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for filepath in source_filepaths:
            if os.path.isfile(filepath) and os.path.exists(filepath):
                zipf.write(filepath, os.path.basename(filepath))


def compress_folder_contents(source_path):
    """
    Compress all files in the specified folder
    :param source_path: the source folder where the contents will be compressed
    :return:
    """

    if os.path.isdir(source_path) and os.path.exists(source_path):
        for root, dirs, files in os.walk(source_path):
            for filename in files:
                filepath = os.path.join(root, filename)
                compress_file(filepath)
    else:
        logging.error("this folder is not valid: " + source_path)


def match_file_by_name(source_path, name):
    """
    Looks for a file with a matching name to the one provided inside the
    specified source directory. It then returns the filepath for that file.
    :param source_path: the source folder where the file is located
    :param name: name or partial name of the file to match
    :return:
    """

    if os.path.isdir(source_path) and os.path.exists(source_path):
        for root, dirs, files in os.walk(source_path):
            matching_files = [s for s in files if name in s]
            if matching_files:
                ret = os.path.abspath(root + "/" + matching_files[0])
                return ret
            else:
                logging.warning("no files matching '" + name +
                                "' found in: " + source_path)
    else:
        logging.warning("this folder is not valid: " + source_path)

    return None


def move_file(source_filepath, dest_path, create_dest=False):
    """
    Moves a file from the source path to the provided destination path.
    An optional parameter can be provided to create the destination folder.
    :param source_filepath: the path to the file that needs to be moved
    :param dest_path: the destination folder that the file will be moved to
    :param create_dest: (optional) if specified, the destination folder will
    be created
    :return:
    """
    if create_dest is True:
        create_folder(dest_path)

    if os.path.isfile(source_filepath) and os.path.exists(source_filepath):
        parsed_filepath = os.path.split(source_filepath)

        if os.path.isdir(dest_path) and os.path.exists(dest_path):
            # Move file from source to destination
            os.rename(source_filepath, dest_path + "/" + parsed_filepath[1])
        else:
            logging.error("the destination folder is not valid: " + dest_path)
    else:
        logging.error("the source file path is not valid: " + source_filepath)


def remove_file(source_filepath, silent=False):
    """
    Removes a file using the default os.remove method with the option for
    silent removal to avoid raising errors when the file is not found.
    :param source_filepath: the path to the file that needs to be removed
    :param silent: if True, no error will be raised if file not found
    :return:
    """
    try:
        os.remove(source_filepath)
    except OSError as e:
        # Raise error if not silent, or if error not file not found
        if not silent or e.errno != errno.ENOENT:
            raise


def get_entityfile_config(entity):
    """
    Get the config field straight out of an exported Entity,
    and turn it into valid JSON

    NOTE: Works with Import/Export API, which does not treat config
    string as a valid json string
    """

    config_str = entity["config"]

    logging.debug("Raw configStr   = " + config_str)

    # don't need this anymore, depends on python behaviour
    # configStr = configStr.replace("\\\"", "\"")
    config = json.loads(config_str)

    return config


def set_entityfile_config(entity, config):
    """
    Get a valid json config string, and put it back in the exported entity in
    a way that can be Imported i.e. with escape characters
    so that it is a dumb string

    NOTE: Works with Import/Export API, which does not treat config
    string as a valid json string
    """

    # put the escape characters back in the config str and write back to file
    config_str = json.dumps(config)

    # don't need this anymore, depends on python behaviour
    # configStr = configStr.replace("\"", "\\\"")

    logging.debug("Modified configStr   = " + config_str)

    entity["config"] = config_str


def format_timedelta(td):
    # Split td.seconds into minutes and seconds
    m = td.seconds / 60
    seconds = td.seconds % 60

    # Split m into hours and minutes.
    h = m / 60
    minutes = m % 60

    # Split h into days and hours
    days = h / 24
    hours = h % 24

    return days, hours, minutes, seconds


def format_runtime(runtime):
    td_runtime = datetime.timedelta(milliseconds=int(runtime))
    formatted_runtime = format_timedelta(td_runtime)
    return formatted_runtime


def docker_id():
    """
    Gets the ID of the last-run Docker container
    """
    try:
        output = subprocess.check_output(['docker', 'ps', '-l', '-q'])
        return output.rstrip()
    except subprocess.CalledProcessError:
        pass


def docker_stop(container_id=None):
    """
    Stops the last run Docker containter or a specific container by
    providing the container identifier.

    :param container_id: Docker container identifier
    """
    exit_status = 1
    try:
        if not container_id:
            container_id = docker_id()
        exit_status = subprocess.call(['docker', 'stop', container_id])
    except subprocess.CalledProcessError:
        pass
    return exit_status


def remote_run(host_node, cmd):
    """
    Runs a set of commands on a remote machine over SSH using paramiko.

    :param host_node: HostNode object
    :param commands: The commands to be executed
    :param verbose: Set to True to display the stdout
    """
    logging.debug("running cmd = " + cmd)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Connect to remote machine using HostNode details
    ssh.connect(host_node.host, username=host_node.user,
                key_filename=host_node.keypath, port=int(host_node.ssh_port))

    # Setup shell with input/output
    channel = ssh.invoke_shell()
    stdin = channel.makefile('wb')
    stdout = channel.makefile('rb')

    # The last command MUST be 'exit' to avoiding hanging
    cmd = '''
        {0}
        exit
    '''.format(cmd)

    def decode(s):
        try:
            return str(s, encoding='utf8')
        except:
            return s

    # Execute command and the capture output
    stdin.write(cmd)
    output = stdout.readlines()
    output = list(map(lambda x: decode(x), output))

    logging.debug("Stdout: " + ''.join(output))

    stdout.close()
    stdin.close()
    ssh.close()

    return output


def logger_level(level):
    """
    Map the specified level to the numerical value level for the logger

    :param level: Logging level from command argument
    """
    try:
        level = level.lower()
    except AttributeError:
        level = ""

    return {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }.get(level, logging.WARNING)
