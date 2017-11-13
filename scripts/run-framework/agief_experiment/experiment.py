import datetime
import functools
import json
import os
import logging


import dpath

from agief_experiment.valueseries import ValueSeries
from agief_experiment.experimentutils import ExperimentUtils
from agief_experiment.launchmode import LaunchMode
from agief_experiment import utils


class Experiment:
    """ 
        An experiment consists of multiple runs (i.e. parameter sweep), each run is a test of one set of parameters.
        This class encapsulates functionality related to conducting a parameter sweep with behaviours related to
        starting and stopping the Compute, starting and stopping the framework and importing exporting data.
        It does _not_ relate to setting up the infrastructure.
    """

    TEMPLATE_PREFIX = "SPAGHETTI"
    PREFIX_DELIMITER = "--"

    def __init__(self, debug_no_run, launch_mode, exps_file, no_compress):
        self.exps_file = exps_file
        self.debug_no_run = debug_no_run
        self.launch_mode = launch_mode
        self.no_compress = no_compress

        self.experiment_utils = ExperimentUtils(exps_file)

        self.prefix_base = self.TEMPLATE_PREFIX
        self.prefixes_history = ""
        self.prefix_modifier = ""

    def reset_prefix(self):

        print("-------------- RESET_PREFIX -------------")

        use_prefix_file = False
        if use_prefix_file:
            prefix_filepath = self.experiment_utils.filepath_from_exp_variable('prefix.txt',
                                                                               ExperimentUtils.agi_exp_home)

            if not os.path.isfile(prefix_filepath) or not os.path.exists(prefix_filepath):
                logging.warning("no prefix.txt file could be found, using the default root entity name: "
                      "'experiment'")
                return None

            with open(prefix_filepath, 'r') as myfile:
                self.prefix_base = myfile.read()
        else:
            new_prefix = datetime.datetime.now().strftime("%y%m%d-%H%M")
            if new_prefix != self.prefix_base:
                self.prefix_base = new_prefix
                self.prefix_modifier = ""
            else:
                self.prefix_modifier += "i"

    def prefix(self):
        return self.prefix_base + self.prefix_modifier

    def remember_prefix(self):
        self.prefixes_history += self.prefix() + "\n"

    def persist_prefix_history(self, filename="prefixes.txt"):
        """ Save prefix history to a file """

        print("\n....... Save prefix history to " + filename)
        with open(filename, "w") as prefix_file:
            prefix_file.write(self.prefixes_history)

    def info(self, sweep_param_vals):

        message = ""
        message += "==============================================\n"
        message += "Experiment Information\n"
        message += "==============================================\n"

        message += "Datetime: " + datetime.datetime.now().strftime("%y %m %d - %H %M") + "\n"
        message += "Folder: " + self.experiment_utils.experiment_folder() + "\n"
        message += "Githash: " + self.experiment_utils.githash() + "\n"
        message += "Variables file: " + self.experiment_utils.variables_filepath() + "\n"
        message += "Prefix: " + self.prefix() + "\n"
        message += "==============================================\n"

        if sweep_param_vals:
            message += "\nSweep Parameters:"
            for param_def in sweep_param_vals:
                message += "\n" + param_def
            message += "\n"

        return message

    def log_results_config(self, compute_node):
        config_exp = compute_node.get_entity_config(self.entity_with_prefix("experiment"))

        reporting_name_key = "reportingEntityName"
        reporting_path_key = "reportingEntityConfigPath"
        if 'value' in config_exp and reporting_name_key in config_exp['value']:
            # get the reporting entity's config
            entity_name = config_exp['value'][reporting_name_key]
            config = compute_node.get_entity_config(entity_name)

            # get the relevant param, or if not there, show the whole config
            report = None
            param_path = None
            try:
                if reporting_path_key in config_exp['value']:
                    param_path = config_exp['value'][reporting_path_key]
                    report = dpath.util.get(config, 'value.' + param_path, '.')
                else:
                    logging.warning("No reporting entity config path found in experiment config.")
            except KeyError:
                logging.warning("KeyError Exception: trying to access path '" +
                param_path + "' at config.value, but it DOES NOT exist!")
            if report is None:
                print("\n================================================")
                print("Reporting Entity Config:")
                print(json.dumps(config, indent=4))
                print("================================================\n")
            else:
                print("\n================================================")
                print("Reporting Entity Config Path (" + entity_name + "-Config.value." + param_path + "):")
                print(report)
                print("================================================\n")
        else:
            logging.warning("No reportingEntityName has been specified in Experiment config.")

    def entity_with_prefix(self, entity_name):
        if self.prefix() is None or self.prefix() == "":
            return entity_name
        else:
            return self.prefix() + self.PREFIX_DELIMITER + entity_name

    def run_parameterset(self, compute_node, cloud, args, entity_filepath, data_filepaths, compute_data_filepaths,
                         sweep_param_vals=''):
        """
        Import input files
        Run Experiment and Export experiment
        The input files specified by params ('entity_file' and 'data_file')
        have parameters modified, which are described in parameters 'param_description'

        :param compute_node:
        :param cloud:
        :param args:
        :param entity_filepath:
        :param data_filepaths:
        :param compute_data_filepaths:      data files on the compute machine, relative to run folder
        :param sweep_param_vals:
        :return:
        """

        print("........ Run parameter set.")

        # print and save experiment info
        info = self.info(sweep_param_vals)
        print(info)

        info_filepath = self.experiment_utils.outputfile(self.prefix(), "experiment-info.txt")
        utils.create_folder(info_filepath)
        with open(info_filepath, 'w') as data:
            data.write(info)

        failed = False
        task_arn = None
        try:
            is_valid = utils.check_validity([entity_filepath]) and utils.check_validity(data_filepaths)

            if not is_valid:
                msg = "ERROR: One of the input files are not valid:\n"
                msg += entity_filepath + "\n"
                msg += json.dumps(data_filepaths)
                raise Exception(msg)

            if (self.launch_mode is LaunchMode.per_experiment) and args.launch_compute:
                task_arn = compute_node.launch(self, cloud=cloud, no_local_docker=args.no_docker)

            compute_node.import_experiment(entity_filepath, data_filepaths)
            compute_node.import_compute_experiment(compute_data_filepaths, is_data=True)

            self.set_dataset(compute_node)

            if not self.debug_no_run:
                compute_node.run_experiment(self.entity_with_prefix("experiment"))

            self.remember_prefix()

            # log results expressed in the appropriate entity config
            self.log_results_config(compute_node)

            # Get the path to labels and features CSV files
            out_labels_file_path = self.experiment_utils.datapath('labels.csv')
            out_features_file_path = self.experiment_utils.datapath('features.csv')

            if args.export:
                out_entity_file_path, out_data_file_path = self.experiment_utils.output_names_from_input_names(
                    self.prefix(),
                    entity_filepath,
                    data_filepaths)

                # Move labels/features files to the experiment output folder
                utils.move_file(out_features_file_path,
                                self.experiment_utils.outputfile(self.prefix()))
                utils.move_file(out_labels_file_path,
                                self.experiment_utils.outputfile(self.prefix()))

                compute_node.export_subtree(self.entity_with_prefix("experiment"),
                                            out_entity_file_path,
                                            out_data_file_path)

            if args.export_compute:
                # Move labels/features files to the experiment output folder
                utils.move_file(out_features_file_path,
                                self.experiment_utils.outputfile_remote(self.prefix()))
                utils.move_file(out_labels_file_path,
                                self.experiment_utils.outputfile_remote(self.prefix()))

                compute_node.export_subtree(self.entity_with_prefix("experiment"),
                                            self.experiment_utils.outputfile_remote(self.prefix()),
                                            self.experiment_utils.outputfile_remote(self.prefix()),
                                            True)
        except Exception as e:
            failed = True
            logging.error("Experiment failed for some reason, shut down Compute and continue.")
            logging.error(e)

        if (self.launch_mode is LaunchMode.per_experiment) and args.launch_compute:
            compute_node.shutdown_compute(cloud, args, task_arn)

        if not failed and args.upload:
            self.upload_results(cloud, compute_node, args.export_compute)

    @staticmethod
    def setup_parameter_sweepers(param_sweep):
        """
        For each 'param' in a set, get details and setup counter
        The result is an array of counters
        Each counter represents one parameter
        """
        val_sweepers = []
        for param in param_sweep['parameter-set']:  # set of params for one 'sweep'
            if 'val-series' in param:
                value_series = ValueSeries(param['val-series'])
            else:
                value_series = ValueSeries.from_range(minv=param['val-begin'],
                                                      maxv=param['val-end'],
                                                      deltav=param['val-inc'])
            val_sweepers.append({'value-series': value_series,
                                 'entity-name': param['entity-name'],
                                 'param-path': param['parameter-path']})
        return val_sweepers

    def inc_parameter_set(self, compute_node, args, entity_filepath, val_sweepers):
        """
        Iterate through counters, incrementing each parameter in the set
        Set the new values in the input file, and then run the experiment
        First counter to reset, return False

        :param compute_node:
        :param args:
        :param entity_filepath:
        :param val_sweepers:
        :return: reset (True if any counter has reached above max), description of parameters (string)
                                If reset is False, there MUST be a description of the parameters that have been set
        """

        if len(val_sweepers) == 0:
            logging.warning("there are no counters to use to increment the parameter set.")
            print("         Returning without any action. This may have undesirable consequences.")
            return True, ""

        # inc all counters, and set parameter in entity file
        sweep_param_vals = []
        reset = False
        for val_sweeper in val_sweepers:
            val_series = val_sweeper['value-series']

            # check if it overflowed last time it was incremented
            overflowed = val_series.overflowed()

            if overflowed:
                if args.logging:
                    logging.debug("Sweeping has concluded for this sweep-set, due to the parameter: " +
                          val_sweeper['entity-name'] + '.' + val_sweeper['param-path'])
                reset = True
                break

            set_param = compute_node.set_parameter_inputfile(entity_filepath,
                                                             self.entity_with_prefix(val_sweeper['entity-name']),
                                                             val_sweeper['param-path'],
                                                             val_series.value())
            sweep_param_vals.append(set_param)
            val_series.next_val()

        if len(sweep_param_vals) == 0:
            logging.warning("no parameters were changed.")

        if args.logging:
            if len(sweep_param_vals):
                logging.debug("Parameter sweep: " + str(sweep_param_vals))

        if reset is False and len(sweep_param_vals) == 0:
            logging.error("indeterminate state, reset is False, but parameter_description indicates "
                  "no parameters have been modified. If there is no sweep to conduct, reset should be True.")
            exit(1)

        return reset, sweep_param_vals

    def create_all_input_files(self, base_entity_filename, base_data_filenames):
        self.reset_prefix()
        return (self.experiment_utils.create_input_files(self.prefix(), self.TEMPLATE_PREFIX, [base_entity_filename])[0],
                self.experiment_utils.create_input_files(self.prefix(), self.TEMPLATE_PREFIX, base_data_filenames))

    def run_sweeps(self, compute_node, cloud, args):
        """ Perform parameter sweep steps, and run experiment for each step. """

        print("\n........ Run Sweeps")

        exps_filename = self.experiment_utils.experiment_def_file()

        if not os.path.exists(exps_filename):
            msg = "Experiment file does not exist at: " + exps_filename
            raise Exception(msg)

        with open(exps_filename) as exps_file:
            filedata = json.load(exps_file)

        for exp_i in filedata['experiments']:
            import_files = exp_i['import-files']  # import files dictionary

            logging.debug("Import Files Dictionary = \n" + json.dumps(import_files, indent=4))

            base_entity_filename = import_files['file-entities']
            base_data_filenames = import_files['file-data']

            exp_ll_data_filepaths = []
            if 'load-local-files' in exp_i:
                load_local_files = exp_i['load-local-files']
                if 'file-data' in load_local_files:
                    exp_ll_data_filepaths = list(map(self.experiment_utils.runpath, load_local_files['file-data']))

            run_parameterset_partial = functools.partial(self.run_parameterset,
                                                         compute_node=compute_node,
                                                         cloud=cloud,
                                                         args=args,
                                                         compute_data_filepaths=exp_ll_data_filepaths)

            if 'parameter-sweeps' not in exp_i or len(exp_i['parameter-sweeps']) == 0:
                print("No parameters to sweep, just run once.")
                exp_entity_filepath, exp_data_filepaths = self.create_all_input_files(base_entity_filename,
                                                                                      base_data_filenames)
                run_parameterset_partial(entity_filepath=exp_entity_filepath, data_filepaths=exp_data_filepaths)
            else:
                for param_sweep in exp_i['parameter-sweeps']:  # array of sweep definitions
                    counters = self.setup_parameter_sweepers(param_sweep)
                    while True:
                        exp_entity_filepath, exp_data_filepaths = self.create_all_input_files(base_entity_filename,
                                                                                              base_data_filenames)
                        reset, sweep_param_vals = self.inc_parameter_set(compute_node, args,
                                                                         exp_entity_filepath, counters)
                        if reset:
                            break
                        run_parameterset_partial(entity_filepath=exp_entity_filepath,
                                                 data_filepaths=exp_data_filepaths,
                                                 sweep_param_vals=sweep_param_vals)

    def set_dataset(self, compute_node):
        """
        The dataset can be located in different locations on different machines. The location can be set in the
        experiments definition file (experiments.json). This method parses that file, finds the parameters to set
        relative to the AGI_DATA_HOME env variable, and sets the specified parameters.
        """

        print("\n....... Set Dataset")

        with open(self.experiment_utils.experiment_def_file()) as data_exps_file:
            data = json.load(data_exps_file)

        for exp_i in data['experiments']:
            for param in exp_i['dataset-parameters']:  # array of sweep definitions
                entity_name = param['entity-name']
                param_path = param['parameter-path']
                data_filenames = param['value']

                data_filenames_arr = data_filenames.split(',')

                data_paths = ""
                for data_filename in data_filenames_arr:
                    if data_paths != "":
                        # IMPORTANT: if space added here, additional characters ('+') get added, probably due to encoding
                        # issues on the request
                        data_paths += ","
                    data_paths += self.experiment_utils.datapath(data_filename)

                compute_node.set_parameter_db(self.entity_with_prefix(entity_name), param_path, data_paths)

    def generate_input_files_locally(self, compute_node):
        entity_filepath, data_filepaths = self.experiment_utils.inputfiles_for_generation()
        # write to the first listed data path name
        compute_node.export_subtree(root_entity=self.entity_with_prefix("experiment"),
                                    entity_filepath=entity_filepath,
                                    data_filepath=data_filepaths[0])

    def upload_results(self, cloud, compute_node, export_compute):
        """ Upload the results of the experiment to the cloud storage (s3)

        :param compute_node: the compute node doing the compute
        :param export_compute: boolean, indicates if export is conducted on the compute node itself
        :type cloud: Cloud
        :type compute_node: Compute
        """

        print("\n...... Uploading results to S3")

        # upload /input folder (contains input files entity.json, data.json)
        folder_path = self.experiment_utils.inputfile(self.prefix(), "")
        self.upload_experiment_file(cloud,
                                    self.prefix(),
                                    "input",
                                    folder_path)

        # upload experiments definition file (if it exists)
        self.upload_experiment_file(cloud,
                                    self.prefix(),
                                    self.experiment_utils.experiments_def_filename,
                                    self.experiment_utils.experiment_def_file())

        # upload log4j configuration file that was used
        log_filename = "log4j2.log"

        if compute_node.remote():
            cloud.remote_upload_runfilename_s3(compute_node.host_node, self.prefix(), log_filename)
        else:
            log_filepath = self.experiment_utils.runpath(log_filename)
            self.upload_experiment_file(cloud,
                                        self.prefix(),
                                        log_filename,
                                        log_filepath)

        # upload /output files (entity.json, data.json and experiment-info.txt)

        folder_path = self.experiment_utils.outputfile(self.prefix(), "")

        # if data was saved on compute, upload data from there
        if compute_node.remote() and export_compute:
            print "\n --- Upload from exported file on remote machine."
            # remote upload of /output/[prefix] folder
            cloud.remote_upload_output_s3(compute_node.host_node, self.prefix(), self.no_compress)
        # otherwise, compress it here before upload if applicable
        elif self.no_compress is False:
            folder_path_big = self.experiment_utils.runpath("output-big/")

            # locate the output data file
            output_data_filepath = utils.match_file_by_name(folder_path, 'data')

            if output_data_filepath is None:
                logging.warning("No data file found. This should only happen if you are running remote via ssh, " \
                      "and exporting data by saving on compute.")
            else:
                # Compress data file
                utils.compress_file(output_data_filepath)

                # Move uncompressed data file to /output-big folder
                utils.move_file(output_data_filepath, folder_path_big)

        # for both, upload the output folder on this machine (where script is running)
        self.upload_experiment_file(cloud,
                                    self.prefix(),
                                    "output",
                                    folder_path)

    @staticmethod
    def upload_experiment_file(cloud, prefix, dest_name, source_path):
        """
        Upload experiment to s3.
        :param prefix: experiment prefix (used in the full name of uploaded bucket)
        :param dest_name: the name for the eventual uploaded s3 object (it can be file or folder)
        :param source_path: the file or folder to be uploaded
        :type cloud: Cloud
        :return:
        """

        print "  --- uploading exp file to S3: prefix = " + prefix + ", destination file/folder = " + dest_name \
              + ", source file/folder = " + source_path

        bucket_name = "agief-project"
        key = "experiment-output/" + prefix + "/" + dest_name

        if os.path.isfile(source_path):
            cloud.upload_file_s3(bucket_name, key, source_path)
        else:
            cloud.upload_folder_s3(bucket_name, key, source_path)
