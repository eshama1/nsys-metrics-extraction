import os
import warnings
from concurrent.futures import ThreadPoolExecutor

from helper.figures import create_and_plot_k_mean_statistics, plot_bandwidth_distribution, plot_frequency_distribution
from helper.general import MAX_WORKERS
from helper.tables import export_single_general_stat_to_latex, export_single_general_stat_to_CSV, \
    export_summary_stat_to_latex, export_summary_stat_to_CSV, export_overall_summary_stat_to_latex, \
    export_summary_summary_stat_to_CSV

# Ignore Future warnings
warnings.filterwarnings ( 'ignore', category=FutureWarning )


def base_generate_tables_and_figures(data_dict, parent_dir, summary_combined_tables=False):
    if 'Individual Kernels' in parent_dir:
        title = data_dict['Name']
    else:
        title = parent_dir.split ( '/' )[-1]

    export_single_general_stat_to_CSV ( data_dict, parent_dir, title )
    export_single_general_stat_to_latex ( data_dict, parent_dir, title )

    if summary_combined_tables:
        stat_names = []
        individual_key = None

        for metric, stats in data_dict.items ():
            if isinstance ( stats, dict ) and 'Individual' not in metric:
                stat_names.append ( metric )
            elif isinstance ( stats, dict ) and 'Individual' in metric:
                individual_key = metric

        if individual_key:
            for stat in stat_names:
                individual_items = data_dict[individual_key]
                export_summary_stat_to_CSV ( individual_items, parent_dir, title, stat )
                export_summary_stat_to_latex ( individual_items, parent_dir, title, stat )

    for metric, stats in data_dict.items ():
        if metric == 'Bandwidth Distribution' and isinstance ( stats, dict ):
            temp_title = title + " " + metric
            plot_bandwidth_distribution ( stats, temp_title, parent_dir )
        elif isinstance ( stats, dict ) and 'Individual' not in metric:
            for sub_metric, sub_stats in stats.items ():
                temp_title = title + " " + metric + " " + sub_metric
                if sub_metric == 'Distribution' and isinstance ( sub_stats, dict ):
                    if 'Duration' in metric or 'Slack' in metric or 'Overhead' in metric:
                        units = ' (ns)'
                    else:
                        units = ''
                    xlabel = metric + units
                    plot_frequency_distribution ( sub_stats, temp_title, xlabel, parent_dir )
                elif 'k-mean' == sub_metric and isinstance ( sub_stats, dict ):
                    if sub_stats['Raw Data']:
                        create_and_plot_k_mean_statistics ( sub_stats, temp_title, parent_dir )

    return None


def generate_specific_tables_and_figures(data_dict, parent_dir):
    with ThreadPoolExecutor ( max_workers=MAX_WORKERS ) as executor:
        futures = []
        for sub_dir, sub_dict in data_dict.items ():
            temp_parent_dir = parent_dir + '/' + str ( sub_dir )
            os.makedirs ( temp_parent_dir, exist_ok=True )
            futures.append ( executor.submit ( base_generate_tables_and_figures, sub_dict, temp_parent_dir ) )

        # Wait for all tasks to complete
        for future in futures:
            future.result ()

    return None


def generate_general_tables_and_figures(data_dict, no_specific, no_individual, parent_dir):
    for sub_dir, sub_dict in data_dict.items ():
        if ('Individual' in sub_dir and not no_individual):
            temp_parent_dir = parent_dir + '/' + sub_dir
            os.makedirs ( temp_parent_dir, exist_ok=True )
            generate_specific_tables_and_figures ( sub_dict, temp_parent_dir )

    if not no_specific:
        base_generate_tables_and_figures ( data_dict, parent_dir, summary_combined_tables=True )

    return None


def export_overall_summary_tables(data_dict, parent_dir):
    summary_stats = {}
    total_time = 0

    for stats_names, sub_dict in data_dict.items ():
        summary_stats[stats_names] = {'Time Total': 0, 'Instance': 0}
        for metric, stats in sub_dict.items ():
            if 'Individual' in metric:
                for sub_metric, sub_stats in stats.items ():
                    if sub_stats['Time Total'] and sub_stats['Instance']:
                        summary_stats[stats_names]['Time Total'] += sub_stats['Time Total']
                        summary_stats[stats_names]['Instance'] += sub_stats['Instance']
                        total_time += sub_stats['Time Total']

    summary_stats['Time Total'] = total_time
    export_overall_summary_stat_to_latex ( summary_stats, parent_dir )
    export_summary_summary_stat_to_CSV ( summary_stats, parent_dir )


def extract_general_dict(data_dict, no_general, no_specific, no_individual, parent_dir):
    for sub_dir, sub_dict in data_dict.items ():
        temp_parent_dir = parent_dir + '/' + sub_dir
        os.makedirs ( temp_parent_dir, exist_ok=True )
        generate_general_tables_and_figures ( sub_dict, no_specific, no_individual, temp_parent_dir )

    if not no_general:
        export_overall_summary_tables ( data_dict, parent_dir )


def generation_tables_and_figures(data_dict, no_comparison, no_general, no_specific, no_individual, num_files):
    parent_dir = './output'

    if num_files < 2:
        extract_general_dict ( data_dict, no_general, no_specific, no_individual, parent_dir )
    else:
        for sub_dir, sub_dict in data_dict.items ():
            temp_parent_dir = parent_dir + '/' + sub_dir
            os.makedirs ( temp_parent_dir, exist_ok=True )
            extract_general_dict ( sub_dict, no_general, no_specific, no_individual, temp_parent_dir )

    if not no_comparison:
        # add comparison things
        print ( "add things" )

    return None
