from __future__ import print_function

import os
import re
import csv
import logging

CSV_HEADER = ['Prefix', 'Jenkins Job', 'Phase', 'Training Accuracy',
              'Test Accuracy', 'Experiment Information',
              'RESULTS - Confusion Matrix', 'RESULTS - F Score']

CSV_FILENAME = 'exported_diary_build-{0}.csv'

def setup_arg_parsing():
    """
    Parse the commandline arguments
    """
    import argparse
    from argparse import RawTextHelpFormatter

    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)

    parser.add_argument('--build_no', dest='build_no', required=True,
                        help='The build number on Jenkins')

    parser.add_argument('--input_path', dest='input_path', required=True,
                        help='Path to the Jenkins Console log')

    parser.add_argument('--output_path', dest='output_path', required=True,
                        help='Path to folder for saving CSV')

    parser.add_argument('--logging', dest='logging', required=False,
                        help='Logging level (default=%(default)s). '
                             'Options: debug, info, warning, error, critical')

    parser.set_defaults(grayscale=False)
    parser.set_defaults(logging='warning')

    return parser.parse_args()


def main():
    """
    The main scope of the parser containing the high level code
    """

    args = setup_arg_parsing()

    # Setup logging
    log_format = "[%(filename)s:%(lineno)s - %(funcName)s() - %(levelname)s] %(message)s"
    logging.basicConfig(format=log_format, level=logger_level(args.logging))

    # Parse and extract results
    results = parse_results(args.input_path)

    # Export results to CSV
    export_results(results, args.build_no, args.output_path)


def parse_results(input_filename):
    cm_index, f1_index = -1, -1
    is_ph1, is_ph2 = False, False
    results, exp_info_buffer = {}, []
    ph1_prefix, ph2_prefix = None, None
    is_info, is_cm, is_f1 = False, False, False

    with open(input_filename, 'r') as log:
        for line in log:
            # PHASE 1
            if not is_ph2:
                # Experiment Information
                if re.search('Experiment Information', line, re.IGNORECASE):
                    is_info = True
                    ph1_prefix = None
                if re.search('Launch Compute', line, re.IGNORECASE):
                    is_info = False

                # Check if reached Phase 2
                if re.search('Phase 2', line, re.IGNORECASE):
                    is_info, is_ph1, is_ph2 = False, False, True

                # Record Phase 1 Info
                if is_info and not is_ph2:
                    if re.search('Prefix:', line, re.IGNORECASE):
                        ph1_prefix = line[-12:].strip()
                        results[ph1_prefix] = {}
                        results[ph1_prefix]['ph1_info'] = exp_info_buffer
                        exp_info_buffer = []

                    if ph1_prefix:
                        results[ph1_prefix]['ph1_info'].append(line)
                    else:
                        exp_info_buffer.append(line)

            # PHASE 2
            else:
                # Capture Phase 1 prefix and initialise dictionary
                if re.search('Dataset from phase 1 experiment prefix', line, re.IGNORECASE):
                    ph1_prefix = line[-12:].strip()
                    if not results[ph1_prefix]:
                        results[ph1_prefix] = {}
                    results[ph1_prefix]['cm'] = {}
                    results[ph1_prefix]['f1'] = {}
                    results[ph1_prefix]['ph2_info'] = {}

                if re.search('RESET_PREFIX', line, re.IGNORECASE):
                    ph2_prefix = None

                # Experiment Information
                if re.search('Experiment Information', line, re.IGNORECASE):
                    is_info = True
                if re.search('Launch Compute', line, re.IGNORECASE):
                    is_info = False

                if is_info:
                    if re.search('Prefix:', line, re.IGNORECASE):
                        ph2_prefix = line[-12:].strip()
                        results[ph1_prefix]['cm'][ph2_prefix] = {}
                        results[ph1_prefix]['f1'][ph2_prefix] = {}
                        results[ph1_prefix]['ph2_info'][ph2_prefix] = exp_info_buffer
                        cm_index, f1_index = -1, -1
                        exp_info_buffer = []

                    if ph2_prefix:
                        results[ph1_prefix]['ph2_info'][ph2_prefix].append(line)
                    else:
                        exp_info_buffer.append(line)

                else:
                    # Confusion Matrix
                    if re.search('Errors:', line, re.IGNORECASE):
                        is_cm = True; is_f1 = False; cm_index += 1
                        results[ph1_prefix]['cm'][ph2_prefix][cm_index] = []

                    # F-Score
                    if re.search('F-Score:\n', line):
                        is_cm = False; is_f1 = True; f1_index += 1
                        results[ph1_prefix]['f1'][ph2_prefix][f1_index] = []

                    if is_cm:
                        results[ph1_prefix]['cm'][ph2_prefix][cm_index].append(line.lstrip())
                    if is_f1:
                        results[ph1_prefix]['f1'][ph2_prefix][f1_index].append(line.lstrip())
                    if re.search('Overall F-Score:', line):
                        is_f1 = False
    return results


def export_results(results, build_no, target_path):
    export_filename = CSV_FILENAME.format(build_no)
    export_filepath = os.path.join(target_path, export_filename)

    with open(export_filepath, 'w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',')

        # Header/Columns
        csv_writer.writerow(CSV_HEADER)

        for ph1_i in results:
            # Phase 1
            build = 'Build #{0}'.format(build_no)
            ph1_info = "".join(results[ph1_i]['ph1_info']).rstrip()
            csv_writer.writerow([ph1_i, build, 'Phase 1', 'N/A', 'N/A', ph1_info])

            # Phase 2
            for ph2_i in results[ph1_i]['ph2_info']:
                exp_info = "".join(results[ph1_i]['ph2_info'][ph2_i]).rstrip()
                cm_train = "".join(results[ph1_i]['cm'][ph2_i][0]).rstrip()
                cm_test  = "".join(results[ph1_i]['cm'][ph2_i][1]).rstrip()
                f1_test  = "".join(results[ph1_i]['f1'][ph2_i][1]).rstrip()

                train_acc = ''
                match_train_acc = re.search('=(.+?)% correct', cm_train)
                if match_train_acc:
                    train_acc = '{0}%'.format(match_train_acc.group(1).strip())
                else:
                    logging.warn('Failed to parse training accuracy from confusion matrix')
                    
                test_acc = ''
                match_test_acc = re.search('=(.+?)% correct', cm_test)
                if match_test_acc:
                    test_acc = '{0}%'.format(match_test_acc.group(1).strip())
                else:
                    logging.warn('Failed to parse test accuracy from confusion matrix')

                csv_writer.writerow([ph2_i, build, 'Phase 2', train_acc, test_acc, exp_info, cm_test, f1_test])
            csv_writer.writerow([])

    logging.info('Exported: %s' % export_filepath)


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


if __name__ == '__main__':
    main()