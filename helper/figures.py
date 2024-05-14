import os

import numpy as np
from absl import logging
from matplotlib import pyplot as plt, ticker
from matplotlib.ticker import ScalarFormatter
from sklearn.cluster import KMeans


def format_power_2_ticks(value, _):
    if value >= 2 ** 50:
        return f'{value / 2 ** 50:.2f}P'
    elif value >= 2 ** 40:
        return f'{value / 2 ** 40:.2f}T'
    elif value >= 2 ** 30:
        return f'{value / 2 ** 30:.2f}G'
    elif value >= 2 ** 20:
        return f'{value / 2 ** 20:.2f}M'
    elif value >= 2 ** 10:
        return f'{value / 2 ** 10:.2f}K'
    else:
        return str ( value )


def format_power_10_ticks(value, _):
    if value >= 1e15:
        return f'{value / 1e15:.2f}P'
    elif value >= 1e12:
        return f'{value / 1e12:.2f}T'
    elif value >= 1e9:
        return f'{value / 1e9:.2f}G'
    elif value >= 1e6:
        return f'{value / 1e6:.2f}M'
    elif value >= 1e3:
        return f'{value / 1e3:.2f}K'
    else:
        return str ( value )


def create_and_plot_k_mean_statistics(cluster_data, title, parent_dir):
    X = np.array ( cluster_data['Raw Data'] )

    wcss_values = []
    max_clusters = min ( 8, len ( X ) )
    for i in range ( 1, max_clusters + 1 ):
        kmeans = KMeans ( n_clusters=i, init='k-means++', max_iter=300, n_init=10, random_state=0 )
        kmeans.fit ( X )
        wcss_values.append ( kmeans.inertia_ )

    # Plot the WCSS values
    fig, ax = plt.subplots ( figsize=(10, 10) )
    ax.plot ( range ( 1, max_clusters + 1 ), wcss_values, marker='o' )
    ax.set_title ( 'Elbow Method for Optimal k' )
    ax.set_xlabel ( 'Number of clusters (k)' )
    ax.set_ylabel ( 'Within-Cluster Sum of Squares (WCSS)' )
    fig.tight_layout ()
    fig.subplots_adjust ( top=0.95 )
    file = parent_dir + "/" + title.split ( " " )[0].replace ( '-', '_' ) + '_elbow_method.png'
    fig.savefig ( file, bbox_inches='tight' )
    plt.close ( fig )

    cluster_dir = parent_dir + '/cluster_options'
    os.makedirs ( cluster_dir, exist_ok=True )

    for n_clusters in range ( 1, max_clusters + 1 ):
        kmeans = KMeans ( n_clusters=n_clusters, random_state=42 )
        cluster_labels = kmeans.fit_predict ( X )
        min_x = np.min ( X[:, 0] )
        min_y = np.min ( X[:, 1] )
        min_x_log2 = np.floor ( np.log2 ( min_x ) )
        min_y_log2 = np.floor ( np.log2 ( min_y ) )

        fig, ax = plt.subplots ( 1, figsize=(10, 10) )
        ax.scatter ( X[:, 0], X[:, 1], c=cluster_labels, cmap='tab10', s=50, alpha=0.5 )
        ax.set_title ( 'Execution Duration K-means Clustering' )
        ax.set_xlabel ( 'Mean Execution Duration (log2)' )
        ax.set_ylabel ( 'Median Execution Duration (log2)' )
        ax.set_xscale ( 'log', base=2 )
        ax.set_yscale ( 'log', base=2 )
        ax.set_xlim ( left=2 ** min_x_log2 )
        ax.set_ylim ( bottom=2 ** min_y_log2 )
        ax.xaxis.set_major_formatter ( ticker.FuncFormatter ( format_power_2_ticks ) )
        ax.yaxis.set_major_formatter ( ticker.FuncFormatter ( format_power_2_ticks ) )
        fig.tight_layout ()
        fig.subplots_adjust ( top=0.95 )
        file = cluster_dir + "/" + title.split ( " " )[0].replace ( '-', '_' ) + f'_k_{n_clusters}_mean_cluster.png'
        fig.savefig ( file, bbox_inches='tight' )
        plt.close ( fig )


def plot_combined_data(combined_data, title, metric, parent_dir, size=False):
    data = []
    labels = []

    for name, sub_dict in combined_data.items ():
        if sub_dict[metric]["Raw Data"]:
            labels.append ( name )
            data.append ( sub_dict[metric]["Raw Data"] )

    if len ( data ) < 2:
        logging.error ( f'Raw Data Missing for: {title} Combined {metric}' )

    fig, ax = plt.subplots ( 1, figsize=(10, 10) )
    parts = ax.violinplot ( data, showmeans=True, showmedians=True )

    for pc in parts['bodies']:
        pc.set_facecolor ( 'skyblue' )
        pc.set_edgecolor ( 'black' )
        pc.set_alpha ( 0.7 )

    parts['cmedians'].set_color ( 'blue' )
    parts['cmedians'].set_linewidth ( 2 )
    parts['cmins'].set_color ( 'red' )
    parts['cmins'].set_linestyle ( '--' )
    parts['cmaxes'].set_color ( 'green' )
    parts['cmaxes'].set_linestyle ( '--' )
    parts['cbars'].set_color ( 'black' )

    ax.xaxis.set_ticks ( range ( 1, len ( labels ) + 1 ) )
    ax.xaxis.set_ticklabels ( labels )
    ax.tick_params ( axis='x', rotation=45 )
    ax.set_xlabel ( "Configuration" )

    flat_data = [item for sublist in data for item in sublist]
    min_value = np.min ( flat_data )
    max_value = np.max ( flat_data )
    magnitude_diff = np.log10 ( max_value ) - np.log10 ( min_value )
    if magnitude_diff >= 1:
        plt.yscale ( 'log', base=10 )
    ax.grid ( axis='y', linestyle='--', linewidth=0.5, color='gray', alpha=0.5)
    ax.yaxis.set_major_formatter ( ticker.FuncFormatter ( format_power_10_ticks ) )
    if size:
        ax.set_ylabel ( "Size (B)" )
    else:
        ax.set_ylabel ( "Time (ns)" )

    ax.set_title ( f"{title} Combined {metric}" )

    fig.tight_layout ()
    fig.subplots_adjust ( top=0.95 )
    file = parent_dir + "/" + title.split ( " " )[0].replace ( '-', '_' ) + '_' + metric.replace ( ' ',
                                                                                                   '_' ) + '_combined_distribution.png'
    fig.savefig ( file, bbox_inches='tight' )
    plt.close ( fig )


def plot_bandwidth_distribution(histogram_data, title, parent_dir):
    array_lists = histogram_data['Histogram']
    labels = histogram_data['Bin Labels']

    x_values = np.arange ( 1, len ( array_lists ) + 1 )
    fig, ax = plt.subplots ( 1, figsize=(10, 10) )
    parts = ax.violinplot ( array_lists, showmeans=True, showmedians=True )

    for pc in parts['bodies']:
        pc.set_facecolor ( 'skyblue' )
        pc.set_edgecolor ( 'black' )
        pc.set_alpha ( 0.7 )

    parts['cmedians'].set_color ( 'blue' )
    parts['cmedians'].set_linewidth ( 2 )
    parts['cmins'].set_color ( 'red' )
    parts['cmins'].set_linestyle ( '--' )
    parts['cmaxes'].set_color ( 'green' )
    parts['cmaxes'].set_linestyle ( '--' )
    parts['cbars'].set_color ( 'black' )

    ax.xaxis.set_ticks ( x_values )
    ax.xaxis.set_ticklabels ( labels )
    ax.tick_params ( axis='x', rotation=45 )
    min_value = min ( min ( sublist ) for sublist in array_lists )
    ax.grid ( axis='y', linestyle='--', linewidth=0.5, color='gray', alpha=0.5 )
    ax.set_title ( title )
    ax.set_xlabel ( "Transfer size range" )
    plt.yscale ( 'log', base=10 )
    ax.yaxis.set_major_formatter ( ticker.FuncFormatter ( format_power_10_ticks ) )
    ax.set_ylabel ( "Bandwidth (B/s)" )

    if min_value > 0:
        min_value_power_of_ten = 10 ** int ( np.floor ( np.log10 ( min_value ) ) )
        ax.set_ylim ( bottom=min_value_power_of_ten )

    fig.tight_layout ()
    fig.subplots_adjust ( top=0.95 )
    file = parent_dir + "/" + title.split ( " " )[0].replace ( '-', '_' ) + '_bandwidth_distribution.png'
    fig.savefig ( file, bbox_inches='tight' )
    plt.close ( fig )


def plot_frequency_distribution(histogram_data, title, xlabel, parent_dir):
    bin_array = histogram_data['Histogram']
    labels = histogram_data['Bin Labels']

    fig, ax = plt.subplots ( 1, figsize=(10, 10) )
    ax.bar ( range ( 1, len ( bin_array ) + 1 ), bin_array, width=1, edgecolor='black' )

    x_values = np.arange ( 1, len ( bin_array ) + 1 )
    ax.xaxis.set_ticks ( x_values )
    ax.xaxis.set_ticklabels ( labels )
    ax.tick_params ( axis='x', rotation=45 )
    ax.grid ( axis='y', linestyle='--', linewidth=0.5, color='gray', alpha=0.5 )
    ax.set_title ( title )
    ax.set_xlabel ( xlabel )
    ax.set_ylabel ( "Frequency" )
    fig.tight_layout ()
    fig.subplots_adjust ( top=0.95 )

    if 'Slack' in xlabel:
        file = parent_dir + "/" + title.split ( " " )[0] + "_" + "_".join (
            xlabel.lower ().split ( " " )[0:1] ) + '_frequency_distribution.png'
    else:
        file = parent_dir + "/" + title.split ( " " )[0] + "_" + "_".join (
            xlabel.lower ().split ( " " )[0:2] ) + '_frequency_distribution.png'

    fig.savefig ( file, bbox_inches='tight' )
    plt.close ( fig )
