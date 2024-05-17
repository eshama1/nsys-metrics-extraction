import multiprocessing
import os
import time
from collections import OrderedDict

from absl import flags

from helper.communication import QUERY_COMMUNICATION, QUERY_COMMUNICATION_STATS, \
    COMM_REQUIRED_TABLES, create_specific_communication_stats, parallel_parse_communication_data
from helper.general import *
from helper.kernel import parallel_parse_kernel_data, QUERY_KERNEL, QUERY_KERNEL_STATS, KERNEL_REQUIRED_TABLES, \
    parallel_create_general_kernel_stats
from helper.export_statistics import generation_tables_and_figures
from helper.transfer import parallel_parse_transfer_data, QUERY_TRANSFERS, QUERY_TRANSFERS_STATS, \
    TRANSFER_REQUIRED_TABLES, create_specific_transfer_stats

# General Flags
flags.DEFINE_string('multi_data_label', None, "(REQUIRED for multi-files) Labels for each database/json file provided to distinguish in statistics ex:(1 GPU, 2 GPU, 3 GPU), commas used to split names and order must be same as provided files", short_name='mdl')

# Extraction Flags
flags.DEFINE_string('data_file', None, "Data Base file for extraction (sqlite)", short_name='df')
flags.DEFINE_string('json_file', None, "JSON file with extracted statistics", short_name='jf')
flags.DEFINE_boolean('no_kernel_metrics', False, "export kernel metrics", short_name='nkm')
flags.DEFINE_boolean('no_transfer_metrics', False, "export transfer metrics", short_name='ntm')
flags.DEFINE_boolean('no_communication_metrics', False, "export communication metrics", short_name='ncm')
flags.DEFINE_boolean('no_save_data', False, "Save metrics to JSON file", short_name='nsd')

# Graphics and Table Flags
flags.DEFINE_boolean('no_metrics_output', None, "disable metrics export after extraction", short_name='nmo')
flags.DEFINE_boolean('no_compare_metrics_output', False, "disable comparison metrics export (multi-file only)", short_name='ncmo')
flags.DEFINE_boolean('no_general_metrics_output', False, "disable general metrics export (Kernel, Transfer, Communication)", short_name='ngmo')
flags.DEFINE_boolean('no_specific_metrics_output', False, "disable specific metrics export (Duration, Size, Slack, Overhead, etc)", short_name='nsmo')
flags.DEFINE_boolean('no_individual_metrics_output', False, "disable individual metrics export (individual kernel, transfer, communication statistics)", short_name='nimo')
flags.DEFINE_integer('max_workers', None, "Number of threads to split work (Default to CPU count)", short_name='mw')
FLAGS = flags.FLAGS

KERNEL_STATS = 0
TRANSFER_STATS = 1
COMMUNICATION_STATS = 2


def generate_queries(qurey, id_list):
    queries = []

    for name in id_list:
        queries.append((qurey, name))

    return queries


def create_statistics(database_file, first_query, raw_data_query, metric_type, sort_metric='Time Total'):
    ids = []
    statistics = {}
    name_stats = ''

    if metric_type is KERNEL_STATS:
        name_stats = 'Kernel'
    elif metric_type is TRANSFER_STATS:
        name_stats = 'Transfer'
    elif metric_type is COMMUNICATION_STATS:
        name_stats = 'Communication'

    logging.info(f"Getting General {name_stats} Information")
    res = execute_query_in_thread((first_query, None), database_file)

    if metric_type is KERNEL_STATS:
        for id, time_percent, time_total, instance, name in res[1]:
            ids.append(id)
            statistics[id] = {'Name': name, 'Time Percent': time_percent, 'Time Total': time_total,
                              'Instance': instance}
    elif metric_type is TRANSFER_STATS:
        for type, time_percent, time_total, mem_total, instance in res[1]:
            ids.append(type)
            statistics[type] = {'Type': type, 'Time Percent': time_percent, 'Time Total': time_total,
                                'Memory Total': mem_total,
                                'Instance': instance}
    elif metric_type is COMMUNICATION_STATS:
        for name, time_percent, time_total, instance in res[1]:
            ids.append(name)
            statistics[name] = {'Name': name, 'Time Percent': time_percent, 'Time Total': time_total,
                                'Instance': instance}
    else:
        logging.error('Unknown metric type')

    if metric_type is KERNEL_STATS:
        logging.info(
            f"Getting RAW Data for each specific {name_stats} (RAW kernel extraction will take a while for large sqlite files, ~1h)")
    else:
        logging.info(f"Getting RAW Data for each specific {name_stats}")

    queries = generate_queries(raw_data_query, ids)
    queries_res = execute_queries_parallel(queries, database_file)

    logging.info(f"Parsing RAW Data and generating Statistics for {name_stats}")
    if metric_type is KERNEL_STATS:
        results = parallel_parse_kernel_data(queries_res)
    elif metric_type is TRANSFER_STATS:
        results = parallel_parse_transfer_data(queries_res)
    elif metric_type is COMMUNICATION_STATS:
        results = parallel_parse_communication_data(queries_res)

    for id, dict in results:
        statistics[id].update(dict)

    statistics = OrderedDict(
        sorted(statistics.items(), key=lambda item: item[1][sort_metric], reverse=True))

    return statistics


def create_statistics_from_file(database_file, output_dir):
    full_statistics = {}

    logging.info(f"Starting extraction and creation of statistics from {database_file}")

    if not FLAGS.no_kernel_metrics:
        logging.info("Starting Kernel Statistics")
        if mutiple_table_exists(database_file, KERNEL_REQUIRED_TABLES):
            kernel_statistics = create_statistics(database_file, QUERY_KERNEL, QUERY_KERNEL_STATS,
                                                  metric_type=KERNEL_STATS)
            full_statistics['Kernel Statistics'] = {'Individual Kernels': kernel_statistics}
            full_statistics['Kernel Statistics'].update(parallel_create_general_kernel_stats(kernel_statistics))

    if not FLAGS.no_transfer_metrics:
        logging.info("Starting Transfer Statistics")
        if mutiple_table_exists(database_file, TRANSFER_REQUIRED_TABLES):
            transfer_statistics = create_statistics(database_file, QUERY_TRANSFERS, QUERY_TRANSFERS_STATS,
                                                    metric_type=TRANSFER_STATS)
            full_statistics['Transfer Statistics'] = {'Individual Transfers': transfer_statistics}
            full_statistics['Transfer Statistics'].update(create_specific_transfer_stats(transfer_statistics))

    if not FLAGS.no_communication_metrics:
        logging.info("Starting Communication Statistics")
        if mutiple_table_exists(database_file, COMM_REQUIRED_TABLES):
            comm_statistics = create_statistics(database_file, QUERY_COMMUNICATION, QUERY_COMMUNICATION_STATS,
                                                metric_type=COMMUNICATION_STATS)
            full_statistics['Communication Statistics'] = {'Individual Communications': comm_statistics}
            full_statistics['Communication Statistics'].update(create_specific_communication_stats(comm_statistics))

    if mutiple_table_exists(database_file, DURATION_REQUIRED_TABLE):
        full_statistics['Total Duration'] = execute_query_in_thread((QUERY_TOTAL_DURATION, None), database_file)[1][0][0]

    if not FLAGS.no_save_data and full_statistics:
        database_file_JSON = output_dir + database_file.split('.')[0] + '_parsed_stats.json'
        logging.info(f"Saving Extracted Statistics of {database_file} to {database_file_JSON}")
        with open(database_file_JSON, 'w') as json_file:
            json.dump(full_statistics, json_file, indent=4)

    return full_statistics


def run(args):
    files, num_files, file_labels, output_data, extract_data = file_args_checking(args)
    output_dir = None

    if num_files > 1:
        temp = []
        for label in file_labels:
            dir = f"./output/{label}/"
            os.makedirs(dir, exist_ok=True)
            temp.append(dir)
        output_dir = temp
    else:
        output_dir = f"./output/" + files.split(".")[0] + "/"
        os.makedirs(output_dir, exist_ok=True)

    extracted_data = {}

    if extract_data:
        if num_files > 1:
            for i, file in enumerate(files):
                extracted_data[file_labels[i]] = create_statistics_from_file(file, output_dir[i])
        else:
            extracted_data.update(create_statistics_from_file(files,output_dir))
    else:
        if num_files > 1:
            for i, file in enumerate(files):
                extracted_data[file_labels[i]] = import_from_json(file)
        else:
            extracted_data.update(import_from_json(files))

    if output_data and extracted_data:
        no_compare = True if num_files < 2 and not args.no_compare_metrics_output else False
        generation_tables_and_figures(extracted_data, no_compare, args.no_general_metrics_output, args.no_specific_metrics_output, args.no_individual_metrics_output, num_files, output_dir)




def main(argv):
    args = FLAGS
    logging.set_verbosity(logging.INFO)
    if not args.data_file and not args.json_file:
        raise app.UsageError("Must provide path to data base file or already parsed json file")

    if not args.max_workers:
        max_workers = multiprocessing.cpu_count()
        MAX_WORKERS = max_workers
    else:
        MAX_WORKERS = args.max_workers
    logging.info(f"Using {MAX_WORKERS} threads")
    start_time = time.time()
    try:
        run(args)
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        exit(1)
    end_time = time.time()
    execution_time = end_time - start_time

    # Convert seconds to hours, minutes, and seconds
    hours, remainder = divmod(execution_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    # Format the time as HH:MM:SS
    formatted_time = "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds))
    logging.info("Script Execution Time: %s", formatted_time)


if __name__ == "__main__":
    app.run(main)
