# This script extracts a few metrics about the GPU applications.
# Copyright (C) 2024 Ethan Shama
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#########################################################################

import os
import sys
import json
import matplotlib
import multiprocessing
from absl import app, flags, logging
from collections import OrderedDict

matplotlib.use("pgf")
matplotlib.rcParams.update({
    "pgf.texsystem": "pdflatex",
    'font.family': 'serif',
    'font.size': 14,
    'text.usetex': True,
    'pgf.rcfonts': False,
})
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator


from helper import *

flags.DEFINE_string('data_file', None, "Data Base file for extraction (sqlite)", short_name='df')
flags.DEFINE_boolean('kernel_metrics', False, "export kernel metrics", short_name='km')
flags.DEFINE_boolean('transfer_metrics', False, "export transfer metrics", short_name='tm')
flags.DEFINE_boolean('save_data', False, "Save metrics to JSON file", short_name='sd')
flags.DEFINE_integer('max_workers', None, "Number of threads to split work (Default to CPU count)", short_name='mw')
FLAGS = flags.FLAGS


def extract_and_save_data():
    kernel_statistics = {}
    transfer_statistics = {}
    kernel_names = []
    kernel_labels = []

    database_file = FLAGS.data_file

    # with open('parsed_kernel_stats.json', 'r') as json_file:
    #     # Load the JSON data into a dictionary
    #     temp = json.load(json_file, parse_float=float)

    if FLAGS.kernel_metrics:
        res = execute_query_in_thread((QUERY_KERNEL_NAMES, None), database_file)
        for item in res[1]:
            kernel_names.append(item)
            label_tmp = '{},{},{},{},{},{},{}'.format(*item[:7])
            kernel_labels.append(label_tmp)
            kernel_statistics[label_tmp] = {'Kernel Name': item[0],
                                            'Kernel Config': '{},{},{},{},{},{}'.format(*item[1:])}

        ker_exec_duration_query, ker_launch_overhead_query, ker_slack_query = generate_kernel_queries(kernel_names)

        ker_exec_duration_res = execute_queries_parallel(ker_exec_duration_query, database_file)
        ker_launch_overhead_res = execute_queries_parallel(ker_launch_overhead_query, database_file)
        ker_slack_res = execute_queries_parallel(ker_slack_query, database_file)

        # Parse duration results
        results = parallel_parse_kernel_data(ker_exec_duration_res, label="Execution Duration", duration=True)
        merge_duration_results(kernel_statistics, results)

        # Parse duration results
        results = parallel_parse_kernel_data(ker_launch_overhead_res, label="Launch Overhead", overhead=True)
        merge_results(kernel_statistics, results)

        # Parse slack results
        results = parallel_parse_kernel_data(ker_slack_res, label="Slack", slack=True)
        merge_results(kernel_statistics, results)

        kernel_statistics = OrderedDict(sorted(kernel_statistics.items(), key=lambda item: item[1]['Dominance'], reverse=True))

        if FLAGS.save_data:
            database_file_JSON = database_file.split('.')[0] + '_parsed_kernel_stats.json'
            with open(database_file_JSON, 'w') as json_file:
                json.dump(kernel_statistics, json_file)

    if FLAGS.transfer_metrics:
        transfer_query_res = execute_query_in_thread((QUERY_TRANSFERS, None), database_file)
        transfer_statistics = parallel_parse_transfer_data(transfer_query_res[1])

        if FLAGS.save_data:
            database_file_JSON = database_file.split('.')[0] + '_parsed_transfer_stats.json'
            with open(database_file_JSON, 'w') as json_file:
                json.dump(transfer_statistics, json_file)

    return kernel_statistics, transfer_statistics

def run(args):
    #kernel_statistics, transfer_statistics = extract_and_save_data()

    nvida = execute_query_in_thread((query_stub, None), FLAGS.data_file)


    with open('single_node_single_gpu_parsed_kernel_stats.json', 'r') as json_file:
        kernel_statistics = OrderedDict(json.load(json_file, parse_float=float))
    with open('single_node_single_gpu_parsed_transfer_stats.json', 'r') as json_file:
        transfer_statistics = json.load(json_file, parse_float=float)

    print(kernel_statistics)

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
