import json
import matplotlib
import multiprocessing
from absl import app, flags, logging
from collections import OrderedDict

from helper.kernel import QUERY_KERNEL_NAMES, generate_kernel_queries, parallel_parse_kernel_data
from helper.transfer import parallel_parse_kernel_data, generate_transfer_queries, QUERY_TRANSFERS

matplotlib.use("pgf")
matplotlib.rcParams.update({
    "pgf.texsystem": "pdflatex",
    'font.family': 'serif',
    'font.size': 14,
    'text.usetex': True,
    'pgf.rcfonts': False,
})

from helper.general import *

flags.DEFINE_string('data_file', None, "Data Base file for extraction (sqlite)", short_name='df')
flags.DEFINE_boolean('kernel_metrics', False, "export kernel metrics", short_name='km')
flags.DEFINE_boolean('transfer_metrics', False, "export transfer metrics", short_name='tm')
flags.DEFINE_boolean('save_data', False, "Save metrics to JSON file", short_name='sd')
flags.DEFINE_integer('max_workers', None, "Number of threads to split work (Default to CPU count)", short_name='mw')
FLAGS = flags.FLAGS


def create_statistics_from_file():
    kernel_statistics = {}
    transfer_statistics = {}
    full_statistics = {}
    kernel_ids = []
    transfer_names = []

    database_file = FLAGS.data_file

    if not FLAGS.kernel_metrics:
        res = execute_query_in_thread((QUERY_KERNEL_NAMES, None), database_file)
        for id, time_percent, time_total, instance, name in res[1]:
            kernel_ids.append(id)
            kernel_statistics[id] = {'Name': name, 'Time Percent': time_percent, 'Time Total': time_total, 'Instance': instance}

        kernel_queries = generate_kernel_queries(kernel_ids)

        kernel_queries_res = execute_queries_parallel(kernel_queries, database_file)
        results = parallel_parse_kernel_data(kernel_queries_res)

        for id, dict in results:
            kernel_statistics[id].update(dict)

        kernel_statistics = OrderedDict(sorted(kernel_statistics.items(), key=lambda item: item[1]['Time Total'], reverse=True))

        full_statistics['Kernel Statistics'] = {'Individual Kernels': kernel_statistics}

    if FLAGS.transfer_metrics:
        res = execute_query_in_thread((QUERY_TRANSFERS, None), database_file)
        for type, time_percent, time_total, mem_total, instance in res[1]:
            transfer_names.append(type)
            transfer_statistics[type] = {'Type': type, 'Time Percent': time_percent, 'Time Total': time_total, 'Instance': instance}

        transfer_queries = generate_transfer_queries(transfer_names)
        transfer_query_res = execute_queries_parallel(transfer_queries, database_file)
        results = parallel_parse_kernel_data(transfer_query_res)

        for id, dict in results:
            transfer_statistics[id].update(dict)

        transfer_statistics = OrderedDict(sorted(transfer_statistics.items(), key=lambda item: item[1]['Time Total'], reverse=True))


    if FLAGS.save_data and full_statistics:
        database_file_JSON = database_file.split('.')[0] + '_parsed_stats.json'
        with open(database_file_JSON, 'w') as json_file:
            json.dump(full_statistics, json_file, indent=4)

    return full_statistics


def run(args):
    app_statistics = create_statistics_from_file()

    # with open('parsed_kernel_stats.json', 'r') as json_file:
    #     # Load the JSON data into a dictionary
    #     temp = json.load(json_file, parse_float=float)

    # with open('single_node_single_gpu_parsed_kernel_stats.json', 'r') as json_file:
    #     kernel_statistics = OrderedDict(json.load(json_file, parse_float=float))
    # with open('single_node_single_gpu_parsed_transfer_stats.json', 'r') as json_file:
    #     transfer_statistics = json.load(json_file, parse_float=float)

    print("kernel_statistics")


def main(argv):
    args = FLAGS
    logging.set_verbosity(logging.INFO)
    if not args.data_file:
        raise app.UsageError("Must provide path to data base file")

    if not args.max_workers:
        max_workers = multiprocessing.cpu_count()
        MAX_WORKERS = max_workers
    else:
        MAX_WORKERS = args.max_workers

    try:
        run(args)
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        exit(1)


if __name__ == "__main__":
    app.run(main)
