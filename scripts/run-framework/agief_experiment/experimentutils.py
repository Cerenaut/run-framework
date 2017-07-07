import json
import utils
import os
import shutil
import subprocess
import logging


class ExperimentUtils:

    # environment variables
    agi_exp_home = "AGI_EXP_HOME"
    agi_home = "AGI_HOME"
    agi_run_home = "AGI_RUN_HOME"
    agi_data_run_home = "AGI_DATA_RUN_HOME"
    agi_data_exp_home = "AGI_EXP_HOME"
    variables_file = "VARIABLES_FILE"

    def __init__(self, experiments_def_filename):
        """" 
            Helper class for Experiments. 
            All things related to filenames and paths.
        """

        self.experiments_def_filename = experiments_def_filename

    def filepath_from_exp_variable(self, filename, path_env):

        variables_file = self.variables_filepath()

        if variables_file == "" or variables_file is None:
            logging.warning("unable to locate variables file.")

        logging.debug("experiment:filepath_from_env_variable: variables file = %s", variables_file)

        cmd = "source " + variables_file + " && echo $" + path_env
        output, error = subprocess.Popen(cmd,
                                         shell=True,
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE,
                                         executable="/bin/bash").communicate()

        file_path = utils.cleanpath(output, filename)
        return file_path

    def githash(self):
        """ return githash of experiment-definitions """

        folder = self.experiment_folder()
        cmd = "cd " + folder + " && git rev-parse --short HEAD"

        commit, error = subprocess.Popen(cmd,
                                         shell=True,
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE,
                                         executable="/bin/bash").communicate()

        return commit

    def inputfiles_for_generation(self):

        base_entity_filename, base_data_filenames = self.input_filenames_from_exp_definitions(False)

        """ Get the input files, with full path, to be generated """

        entity_filename = self.inputfile_base(base_entity_filename)

        data_filenames = []
        for base_data_filename in base_data_filenames:
            data_filename = self.inputfile_base(base_data_filename)
            data_filenames.append(data_filename)

        return entity_filename, data_filenames

    def input_filenames_from_exp_definitions(self, is_import_files):
        """ Get the input files as defined in the experiments definitions file.
        i.e. do not compute full path, do not add prefix etc.

        :param is_import_files: boolean to specify whether you want the input files for 'import' or 'generation'
        :return: entityfilename, datafilenames
        """

        exps_filename = self.experiment_def_file()

        with open(exps_filename) as exps_file:
            filedata = json.load(exps_file)

        for exp_i in filedata['experiments']:

            if is_import_files:
                key = 'import-files'
            else:
                key = 'gen-files'

            input_files = exp_i[key]  # import files dictionary

            logging.debug("Input Files Dictionary = %s", json.dumps(input_files, indent=4))

            # get experiment file-names, and expand to full path
            base_entity_filename = input_files['file-entities']
            base_data_filenames = input_files['file-data']

            return base_entity_filename, base_data_filenames

    def inputfile_base(self, filename):
        """
        Return the full path to the base inputfile specified by simple filename (AGI_EXP_HOME/input/filename)
        The base input file will be used to generate input files specific to the experiment
        (i.e. replace generic prefix with actual prefix)
        """
        return self.filepath_from_exp_variable("input/" + filename, self.agi_exp_home)

    def inputfile(self, prefix, filename):
        """
        Return the full path to the inputfile that is to be created by this experiment,
        specified by simple filename (AGI_EXP_HOME/input/prefix/filename).
        """
        return self.filepath_from_exp_variable("input/" + prefix + "/" + filename, self.agi_exp_home)

    def outputfile(self, prefix, filename=""):
        """
        Return the full path to the output file that is to be created by this experiment,
        specified by simple filename (AGI_EXP_HOME/output/prefix/filename)
        """
        return self.filepath_from_exp_variable("output/" + prefix + "/" + filename, self.agi_exp_home)

    def outputfile_remote(self, prefix, filename=""):
        """
        Return the full path to the output file if it was exported/saved on remote machine,
        that is to be created by this experiment,
        specified by simple filename (AGI_RUN_HOME/output/prefix/filename)
        """
        return self.filepath_from_exp_variable("output/" + prefix + "/" + filename, self.agi_run_home)

    def outputfile_base(self, filename):
        """ return the full path to the output file specified by simple filename (AGI_EXP_HOME/output/filename) """
        return self.filepath_from_exp_variable("output/" + filename, self.agi_exp_home)

    def runpath(self, path):
        """ return absolute path to a file or folder in the AGI_RUN_HOME/ folder """
        return self.filepath_from_exp_variable(path, self.agi_run_home)

    def datapath(self, path):
        """ return the file in the data folder, on the system where compute is running """
        return self.filepath_from_exp_variable(path, self.agi_data_run_home)

    def experiment_def_file(self):
        """ return the full path to the experiments definition file """
        return self.filepath_from_exp_variable(self.experiments_def_filename, self.agi_exp_home)

    def experiment_folder(self):
        """ return the full path to the experiments folder """
        return self.filepath_from_exp_variable("", self.agi_exp_home)

    def experiment_path(self, path):
        """ return the full path to a file in the folder AGI_EXP_HOME """
        return self.filepath_from_exp_variable(path, self.agi_exp_home)

    def agi_binpath(self, path):
        """ return absolute path to a file or folder in the AGI_BIN_HOME/ folder """
        return self.filepath_from_exp_variable(path, self.agi_home + "/bin/")

    def create_input_files(self, prefix, template_prefix, base_filenames):
        """
        Create 'experiment input files' from the 'base input files'.

        Duplicate input files appending prefix to name of new file,
        and change contents of entities to use the generated prefix.
        If they are in the /output subfolder, then do not modify.

        Base input files are located in:  'experiment-folder/input'
        experiment.py input files are located in subfolder:   'experiment-folder/input/prefix'

        :param prefix: 
        :param base_filenames: array of filenames (not full path) to be copied and prefix changed internally
        :param template_prefix:
        :return: array of modified filepaths (full path)
        """

        filenames = []
        for base_filename in base_filenames:

            base_filepath = self.inputfile_base(base_filename)

            if not os.path.isfile(base_filepath):
                logging.error("The file does not exist" + base_filepath + \
                      "\nCANNOT CONTINUE.")
                exit(1)

            # get the containing folder, and it's parent folder
            full_dirname = os.path.dirname(os.path.normpath(base_filepath))  # full dirname
            full_parentpath, dirname = os.path.split(full_dirname)  # take just the last part - next subfolder up
            parent_dirname = os.path.basename(full_parentpath)  # take just the last part - subfolder

            if dirname != "output" and parent_dirname != "output":
                filename = utils.append_before_ext(base_filename, "_" + prefix)
                filepath = self.inputfile(prefix, filename)
                utils.create_folder(filepath)  # create path if it doesn't exist
                shutil.copyfile(base_filepath, filepath)  # create new input files with prefix in the name
                utils.replace_in_file(template_prefix, prefix,
                                      filepath)  # search replace contents for PREFIX and replace with 'prefix'
                filenames.append(filepath)
            else:
                filenames.append(base_filepath)

        return filenames

    def variables_filepath(self):
        """ return full filename with path, of the file being used for the variables file """
        dir_path = os.path.dirname(os.path.realpath(__file__))
        variables_file = os.getenv(self.variables_file, dir_path + '/../../variables.sh')
        return variables_file

    def output_names_from_input_names(self, prefix, entity_filepath, data_filepaths):
        """ Create the filenames for export, from the input filenames """

        entity_filename = os.path.basename(entity_filepath)
        data_filename = os.path.basename(data_filepaths[0])

        new_entity_file = "exported_" + entity_filename
        new_data_file = "exported_" + data_filename

        out_entity_file_path = self.outputfile(prefix, new_entity_file)
        out_data_file_path = self.outputfile(prefix, new_data_file)

        return out_entity_file_path, out_data_file_path
