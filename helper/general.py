import sqlite3
from absl import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np

MAX_WORKERS = 12


def table_exists(database_file, table_name):
    try:
        with sqlite3.connect(database_file) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            result = cursor.fetchone()
            if result:
                return True
            else:
                logging.error(f"Statistics were requested but required table {table_name} does not exist")
                return False
    except sqlite3.Error as e:
        logging.error(f"Statistics were requested but required table {table_name} does not exist")
        return False


def mutiple_table_exists(database_file, table_name_list):
    for table_name in table_name_list:
        if not table_exists(database_file, table_name):
            return False
    return True

def execute_query(conn, query, params=None):
    cursor = conn.cursor()
    if params:
        key = params
        if not isinstance(params, tuple):
            params = (params,)
        cursor.execute(query, params)
    else:
        key = None
        cursor.execute(query)
    result = cursor.fetchall()
    return key, result


def execute_query_in_thread(query_params, database_file):
    conn = sqlite3.connect(database_file)  # Create a new connection object in each thread
    try:
        result = execute_query(conn, *query_params)
    except sqlite3.Error as error:
        print("Error reading data from SQLite table:", error)
    finally:
        conn.close()
    return result


def execute_queries_parallel(queries_with_params, database_file):
    results = []
    total_queries = len(queries_with_params)
    completed_queries = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for query_params in queries_with_params:
            future = executor.submit(execute_query_in_thread, query_params, database_file)
            futures.append(future)
        for future in as_completed(futures):
            results.append(future.result())
            completed_queries += 1
            # Check if 10% of total items are completed
            if int((completed_queries / total_queries) * 100) % 10 == 0:
                logging.info(f"Progress: {(completed_queries / total_queries) * 100:.1f}%")
    return results


def remove_outliers(data):
    # Calculate the first and third quartiles
    Q1 = np.percentile(data, 25)
    Q3 = np.percentile(data, 75)

    # Calculate the interquartile range (IQR)
    IQR = Q3 - Q1

    # Define the lower and upper bounds for outliers
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Remove outliers
    clean_data = [x for x in data if (x >= lower_bound) and (x <= upper_bound)]

    return clean_data


def generate_statistics(data, label):
    kernel_data = {}
    data = [float(x) for x in data]

    # Compute statistics
    mean_duration = np.mean(data)
    median_duration = np.median(data)
    min_duration = np.min(data)
    max_duration = np.max(data)
    std_deviation = np.std(data)

    # Round statistical results to 6 decimal places
    rounded_log_data = np.round(data, 6).tolist()
    rounded_mean_duration = round(mean_duration, 6)
    rounded_median_duration = round(median_duration, 6)
    rounded_min_duration = round(min_duration, 6)
    rounded_max_duration = round(max_duration, 6)
    rounded_std_deviation = round(std_deviation, 6)

    # Prepare kernel data dictionary
    kernel_data[label] = {
        'Raw Data': rounded_log_data,
        'Mean Duration': rounded_mean_duration,
        'Median Duration': rounded_median_duration,
        'Minimum Duration': rounded_min_duration,
        'Maximum Duration': rounded_max_duration,
        'Standard Deviation': rounded_std_deviation
    }

    return kernel_data
