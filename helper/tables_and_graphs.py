import csv
import logging
import os
import re
import warnings
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import ticker
from sklearn.cluster import KMeans, DBSCAN
from sklearn.metrics import silhouette_score

from helper.general import MAX_WORKERS

#Ignore Future warnings
warnings.filterwarnings('ignore', category=FutureWarning)

""" 
TO DO

Finish fixing BW graph issues and change Y-axis labeks from 10^X to numbers
Do higher level figures and tables
do combined tables and figures
"""

def format_power_2_ticks(value, _):
    if value >= 2**50:
        return f'{value / 2**50:.0f}P'
    elif value >= 2**40:
        return f'{value / 2**40:.0f}T'
    elif value >= 2**30:
        return f'{value / 2**30:.0f}G'
    elif value >= 2**20:
        return f'{value / 2**20:.0f}M'
    elif value >= 2**10:
        return f'{value / 2**10:.0f}K'
    else:
        return str(value)


def format_power_10_ticks(value, _):
    if value >= 1e15:
        return f'{value / 1e15:.0f}P'
    elif value >= 1e12:
        return f'{value / 1e12:.0f}T'
    elif value >= 1e9:
        return f'{value / 1e9:.0f}G'
    elif value >= 1e6:
        return f'{value / 1e6:.0f}M'
    elif value >= 1e3:
        return f'{value / 1e3:.0f}K'
    else:
        return str(value)

def create_and_plot_k_mean_statistics(cluster_data, title, parent_dir):
    X = np.array(cluster_data['Raw Data'])

    silhouette_scores = []
    max_clusters = min(8, len(X))

    for i in range(3, max_clusters + 1):  # silhouette_score requires at least 2 clusters
        if len(X) > i:
            kmeans = KMeans(n_clusters=i, init='k-means++', max_iter=300, n_init=10, random_state=0)
            cluster_labels = kmeans.fit_predict(X)
            silhouette_avg = silhouette_score(X, cluster_labels)
            silhouette_scores.append(silhouette_avg)

    fig, ax = plt.subplots(figsize=(10, 10))
    ax.plot(range(3, max_clusters + 1), silhouette_scores)
    ax.set_title('Silhouette Method')
    ax.set_xlabel('Number of clusters')
    ax.set_ylabel('Silhouette Score')
    fig.tight_layout()
    fig.subplots_adjust(top=0.95)
    file = parent_dir + "/" + title.split(" ")[0].replace('-', '_') + '_silhouette_method.png'
    fig.savefig(file, bbox_inches='tight')
    plt.close(fig)

    n_clusters = silhouette_scores.index(max(silhouette_scores)) + 3
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    cluster_labels = kmeans.fit_predict(X)
    min_x = np.min(X[:, 0])
    min_y = np.min(X[:, 1])
    min_x_log2 = np.floor(np.log2(min_x))
    min_y_log2 = np.floor(np.log2(min_y))

    fig, ax = plt.subplots(1, figsize=(10, 10))
    ax.scatter(X[:, 0], X[:, 1], c=cluster_labels, cmap='tab10', s=50, alpha=0.5)
    ax.set_title('Execution Duration K-means Clustering')
    ax.set_xlabel('Mean Execution Duration (log2)')
    ax.set_ylabel('Median Execution Duration (log2)')
    ax.set_xscale('log', base=2)
    ax.set_yscale('log', base=2)
    ax.set_xlim(left=2 ** min_x_log2)
    ax.set_ylim(bottom=2 ** min_y_log2)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_power_2_ticks))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_power_2_ticks))
    fig.tight_layout()
    fig.subplots_adjust(top=0.95)
    file = parent_dir + "/" + title.split(" ")[0].replace('-', '_') + '_k_mean_cluster.png'
    fig.savefig(file, bbox_inches='tight')
    plt.close(fig)


def plot_bandwidth_distribution(histogram_data, title, parent_dir):
    array_lists = histogram_data['Histogram']
    labels = histogram_data['Bin Labels']

    x_values = np.arange(1, len(array_lists) + 1)
    fig, ax = plt.subplots(1, figsize=(10, 10))
    parts = ax.violinplot(array_lists, showmeans=True, showmedians=True)

    for pc in parts['bodies']:
        pc.set_facecolor('skyblue')
        pc.set_edgecolor('black')
        pc.set_alpha(0.7)

    parts['cmedians'].set_color('blue')
    parts['cmedians'].set_linewidth(2)
    parts['cmins'].set_color('red')
    parts['cmins'].set_linestyle('--')
    parts['cmaxes'].set_color('green')
    parts['cmaxes'].set_linestyle('--')
    parts['cbars'].set_color('black')

    ax.xaxis.set_ticks(x_values)
    ax.xaxis.set_ticklabels(labels)
    ax.tick_params(axis='x', rotation=45)
    min_value = min(min(sublist) for sublist in array_lists)
    ax.grid(axis='y', linestyle='--', linewidth=0.5, color='gray', alpha=0.5)
    ax.set_title(title)
    ax.set_xlabel("Transfer size range")
    plt.yscale('log', base=10)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_power_10_ticks))
    ax.set_ylabel("Bandwidth (MB/s)")

    if min_value > 0:
        min_value_power_of_ten =  10 ** int(np.floor(np.log10(min_value)))
        ax.set_ylim(bottom=min_value_power_of_ten)

    fig.tight_layout()
    fig.subplots_adjust(top=0.95)
    file = parent_dir + "/" + title.split(" ")[0].replace('-','_') + '_bandwidth_distribution.png'
    fig.savefig(file, bbox_inches='tight')
    plt.close(fig)


def plot_frequency_distribution(histogram_data, title, xlabel, parent_dir):
    bin_array = histogram_data['Histogram']
    labels = histogram_data['Bin Labels']

    fig, ax = plt.subplots(1, figsize=(10, 10))
    ax.bar(range(1, len(bin_array) + 1), bin_array, width=1, edgecolor='black')

    x_values = np.arange(1, len(bin_array) + 1)
    ax.xaxis.set_ticks(x_values)
    ax.xaxis.set_ticklabels(labels)
    ax.tick_params(axis='x', rotation=45)
    ax.grid(axis='y', linestyle='--', linewidth=0.5, color='gray', alpha=0.5)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Frequency")
    fig.tight_layout()
    fig.subplots_adjust(top=0.95)

    if 'Slack' in xlabel:
        file = parent_dir + "/" + title.split(" ")[0] + "_" + "_".join(xlabel.lower().split(" ")[0:1]) + '_frequency_distribution.png'
    else:
        file = parent_dir + "/" + title.split(" ")[0] + "_" + "_".join(xlabel.lower().split(" ")[0:2]) +'_frequency_distribution.png'

    fig.savefig(file, bbox_inches='tight')
    plt.close(fig)


def export_single_general_stat_to_CSV(data_dict, parent_dir, title):
    csv_filename = parent_dir + f'/{title}_general_statistics.csv'
    with open(csv_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([f"{title} General Statistics"])
        writer.writerow(['Metric', 'Mean', 'Median', 'Minimum', 'Maximum', 'Standard Deviation'])
        for metric_name, stats in data_dict.items():
            if isinstance(stats, dict) and 'Individual' not in metric_name and 'Bandwidth Distribution' not in metric_name:
                if 'Duration' in metric_name or 'Slack' in metric_name or 'Overhead' in metric_name:
                    units = ' (us)'
                else:
                    units = ' (B)'
                writer.writerow([metric_name + units] + [stats.get(stat, '') for stat in ['Mean', 'Median', 'Minimum', 'Maximum', 'Standard Deviation']])


def latex_safe_string(title):
    translation_table = str.maketrans({char: f'\{char}' for char in '\`*_{}[]()<>#+-.!$:;,/'})
    latex_safe_title = title.translate(translation_table)
    return latex_safe_title

def export_single_general_stat_to_latex(data_dict, parent_dir, title):

    latex_filename = parent_dir + f'/{title}_general_statistics.tex'
    safe_title = latex_safe_string(title)
    with open(latex_filename, 'w') as latexfile:
        latexfile.write("\\begin{table}[ht]\n")
        latexfile.write("\\centering\n")
        latexfile.write("\\caption{" + safe_title + " General Statistics}\n")
        latexfile.write("\\begin{tabular}{|c|c|c|c|c|c|}\n")
        latexfile.write("\\hline\n")
        latexfile.write("\\textbf{Metric} & \\textbf{Mean} & \\textbf{Median} & \\textbf{Minimum} & \\textbf{Maximum} & \\textbf{Standard Deviation} \\\\\n")
        latexfile.write("\\hline\n")
        for metric, stats in data_dict.items():
            if isinstance(stats, dict) and 'Individual' not in metric and 'Bandwidth Distribution' not in metric:
                if 'Duration' in metric or 'Slack' in metric or 'Overhead' in metric:
                    units = ' (us)'
                else:
                    units = ' (B)'

                metric = metric + units
                mean = stats.get('Mean', '')
                median = stats.get('Median', '')
                minimum = stats.get('Minimum', '')
                maximum = stats.get('Maximum', '')
                std_dev = stats.get('Standard Deviation', '')
                latexfile.write(f"{metric} & {mean} & {median} & {minimum} & {maximum} & {std_dev} \\\\\n")
                latexfile.write("\\hline\n")
        latexfile.write("\\end{tabular}\n")
        latexfile.write("\\label{tab:" + title + "_general_stats}\n")
        latexfile.write("\\end{table}\n")


def base_generate_tables_and_figures(data_dict, parent_dir):
    if 'Individual Kernels' in parent_dir:
        title = data_dict['Name']
    else:
        title = parent_dir.split('/')[-1]

    export_single_general_stat_to_CSV(data_dict, parent_dir, title)
    export_single_general_stat_to_latex(data_dict, parent_dir, title)

    for metric, stats in data_dict.items():
        if metric == 'Bandwidth Distribution' and isinstance(stats, dict):
            temp_title = title + " " + metric
            plot_bandwidth_distribution(stats, temp_title, parent_dir)
        elif isinstance(stats, dict) and 'Individual' not in metric:
            for sub_metric, sub_stats in stats.items():
                temp_title = title + " " + metric + " " + sub_metric
                if sub_metric == 'Distribution' and isinstance(sub_stats, dict):
                    if 'Duration' in metric or 'Slack' in metric or 'Overhead' in metric:
                        units = ' (us)'
                    else:
                        units = ''
                    xlabel = metric + units
                    plot_frequency_distribution(sub_stats, temp_title, xlabel, parent_dir)
                elif 'k-mean' == sub_metric and isinstance(sub_stats, dict):
                    if sub_stats['Raw Data']:
                        create_and_plot_k_mean_statistics(sub_stats, temp_title, parent_dir)

    return None

def generate_specific_tables_and_figures(data_dict, parent_dir):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for sub_dir, sub_dict in data_dict.items():
            temp_parent_dir = parent_dir + '/' + str(sub_dir)
            os.makedirs(temp_parent_dir, exist_ok=True)
            futures.append(executor.submit(base_generate_tables_and_figures, sub_dict, temp_parent_dir))

        # Wait for all tasks to complete
        for future in futures:
            future.result()

    return None


def generate_general_tables_and_figures(data_dict, no_specific, no_individual, parent_dir):
    for sub_dir, sub_dict in data_dict.items():
        if ('Individual' in sub_dir and not no_individual):
            temp_parent_dir = parent_dir + '/' + sub_dir
            os.makedirs(temp_parent_dir, exist_ok=True)
            generate_specific_tables_and_figures(sub_dict, temp_parent_dir)

    if not no_specific:
        base_generate_tables_and_figures(data_dict, parent_dir)

    return None


def extract_general_dict(data_dict, no_general, no_specific, no_individual, parent_dir):
    for sub_dir, sub_dict in data_dict.items():
        temp_parent_dir = parent_dir + '/' + sub_dir
        os.makedirs(temp_parent_dir, exist_ok=True)
        generate_general_tables_and_figures(sub_dict, no_specific, no_individual, temp_parent_dir)


def generation_tables_and_figures(data_dict, no_comparison, no_general, no_specific, no_individual, num_files):
    parent_dir = './output'

    if num_files < 2:
        extract_general_dict(data_dict, no_general, no_specific, no_individual, parent_dir)
    else:
        for sub_dir, sub_dict in data_dict.items():
            temp_parent_dir = parent_dir + '/' + sub_dir
            os.makedirs(temp_parent_dir, exist_ok=True)
            extract_general_dict(sub_dict, no_general, no_specific, no_individual, temp_parent_dir)

    if not no_comparison:
        #add comparison things
        print("add things")

    return None
