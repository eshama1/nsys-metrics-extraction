import os
import warnings
from concurrent.futures import ThreadPoolExecutor

from helper.figures import create_and_plot_k_mean_statistics, plot_bandwidth_distribution, plot_frequency_distribution, \
    plot_combined_data
from helper.general import MAX_WORKERS
from helper.tables import export_single_general_stat_to_latex, export_single_general_stat_to_CSV, \
    export_summary_stat_to_latex, export_summary_stat_to_CSV, export_overall_summary_stat_to_latex, \
    export_summary_summary_stat_to_CSV, export_combined_summary_stat_to_CSV, export_combined_summary_stat_to_latex

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


def base_generate_combined_tables_and_figures(data_dict, parent_dir, combined_info, kernels=False):

    item_name = combined_info[0]
    labels = combined_info[1:]

    if kernels:
        keys = {label: next(key for key, value in data_dict[label].items() if value.get('Name') == item_name) for label in labels}

    item_dicts = {label: data_dict[label][keys[label]] if kernels else data_dict[label][item_name] for label in labels}
    for sub_metric, sub_dict in item_dicts[labels[0]].items():
        if isinstance(sub_dict, dict) and 'Individual' not in sub_metric:
            name = item_name
            plot_combined_data ( item_dicts, name, sub_metric, parent_dir)
            export_combined_summary_stat_to_CSV ( item_dicts, parent_dir, name, sub_metric)
            export_combined_summary_stat_to_latex ( item_dicts, parent_dir, name, sub_metric)



def find_common_keys_or_names(data_dict, kernels=False):
    # Collect kernel names/keys for each configuration
    kernel_sets = []
    for config, subdict in data_dict.items():
        if kernels:
            # Get kernel names
            items = {value.get("Name") for value in subdict.values()}
        else:
            # Get kernel keys
            items = set(subdict.keys())
        kernel_sets.append((config, items))

    # Find common kernels
    common_kernels = set.intersection(*[ks for _, ks in kernel_sets])

    # Filter configurations based on common kernels
    common_items = []
    for i in range ( len ( kernel_sets ) ):
        for j in range ( i + 1, len ( kernel_sets ) ):
            common_kernels_in_both = common_kernels.intersection ( kernel_sets[i][1], kernel_sets[j][1] )
            for kernel_name in common_kernels_in_both:
                common_configs = [config for config, kernels in kernel_sets if kernel_name in kernels]
                if len ( common_configs ) >= 2:
                    common_items.append ( (kernel_name, *common_configs) )

    return common_items

def generate_specific_tables_and_figures(data_dict, parent_dir, combined=False):
    with ThreadPoolExecutor ( max_workers=MAX_WORKERS ) as executor:
        futures = []
        if not combined:
            for sub_dir, sub_dict in data_dict.items ():
                temp_parent_dir = parent_dir + '/' + str ( sub_dir )
                os.makedirs ( temp_parent_dir, exist_ok=True )
                futures.append ( executor.submit ( base_generate_tables_and_figures, sub_dict, temp_parent_dir ) )
        else:
            kernels = True if 'Kernels' in parent_dir else False
            common_items = find_common_keys_or_names ( data_dict, kernels=kernels )
            for common_item in common_items:
                temp_parent_dir = parent_dir + '/' + str ( common_item[0] )
                os.makedirs ( temp_parent_dir, exist_ok=True )
                futures.append ( executor.submit ( base_generate_combined_tables_and_figures, data_dict, temp_parent_dir, common_item, kernels=kernels) )

        # Wait for all tasks to complete
        for future in futures:
            future.result ()

    return None


def generate_general_tables_and_figures(data_dict, parent_dir, no_specific=False, no_individual=False, combined=False):
    if not combined:
        for sub_dir, sub_dict in data_dict.items ():
            if ('Individual' in sub_dir and not no_individual):
                temp_parent_dir = parent_dir + '/' + sub_dir
                os.makedirs ( temp_parent_dir, exist_ok=True )
                generate_specific_tables_and_figures ( sub_dict, temp_parent_dir )
    else:
        configs = list(data_dict.keys ())
        stats = list(data_dict[configs[0]].keys())
        for stat in stats:
            if ('Individual' in stat):
                temp_dict = {config: data_dict[config][stat] for config in configs}
                temp_parent_dir = parent_dir + '/' + stat
                os.makedirs ( temp_parent_dir, exist_ok=True )
                generate_specific_tables_and_figures ( temp_dict, temp_parent_dir, combined=True )

    if not no_specific and not combined:
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


def extract_general_dict(data_dict, parent_dir, no_general=False, no_specific=False, no_individual=False, combined=False):

    if not combined:
        for sub_dir, sub_dict in data_dict.items ():
            temp_parent_dir = parent_dir + '/' + sub_dir
            os.makedirs ( temp_parent_dir, exist_ok=True )
            generate_general_tables_and_figures ( sub_dict, temp_parent_dir, no_specific, no_individual )
    else:
        configs = list(data_dict.keys ())
        stats = list(data_dict[configs[0]].keys())
        for stat in stats:
            temp_dict = {config: data_dict[config][stat] for config in configs}
            temp_parent_dir = parent_dir + '/' + stat
            os.makedirs ( temp_parent_dir, exist_ok=True )
            generate_general_tables_and_figures ( temp_dict, temp_parent_dir, combined=True)

    if not no_general and not combined:
        export_overall_summary_tables ( data_dict, parent_dir )


def generation_tables_and_figures(data_dict, no_comparison, no_general, no_specific, no_individual, num_files, output_dir):

    # if num_files < 2:
    #     extract_general_dict ( data_dict, output_dir, no_general, no_specific, no_individual)
    # else:
    #     for i, (sub_dir, sub_dict) in enumerate(data_dict.items ()):
    #         temp_parent_dir = output_dir[i] + '/' + sub_dir
    #         os.makedirs ( temp_parent_dir, exist_ok=True )
    #         extract_general_dict ( sub_dict, temp_parent_dir, no_general, no_specific, no_individual )

    if not no_comparison:
        temp_parent_dir = './output/combined_statistics'
        os.makedirs ( temp_parent_dir, exist_ok=True )
        extract_general_dict(data_dict, temp_parent_dir, combined=True)


    return None
