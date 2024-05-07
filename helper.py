from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import sqlite3

QUERY_KERNEL_NAMES = """ 
    SELECT DISTINCT
        StringIds.value,
        cuda_gpu.gridX,
        cuda_gpu.gridY,
        cuda_gpu.gridZ,
        cuda_gpu.blockX,
        cuda_gpu.blockY,
        cuda_gpu.blockZ
    FROM StringIds 
    JOIN CUPTI_ACTIVITY_KIND_KERNEL AS cuda_gpu ON cuda_gpu.shortName = StringIds.id
    JOIN CUPTI_ACTIVITY_KIND_RUNTIME ON CUPTI_ACTIVITY_KIND_RUNTIME.correlationId = cuda_gpu.correlationId
    """


query_stub = """WITH
    summary AS (
        SELECT
            coalesce(shortname, demangledName) AS nameId,
            sum(end - start) AS total,
            count(*) AS num,
            avg(end - start) AS avg,
            median(end - start) AS med,
            min(end - start) AS min,
            max(end - start) AS max,
            stdev(end - start) AS stddev
        FROM
            CUPTI_ACTIVITY_KIND_KERNEL
        GROUP BY 1
    ),
    totals AS (
        SELECT sum(total) AS total
        FROM summary
    )
SELECT
    round(summary.total * 100.0 / (SELECT total FROM totals), 1) AS "Time:ratio_%",
    summary.total AS "Total Time:dur_ns",
    summary.num AS "Instances",
    round(summary.avg, 1) AS "Avg:dur_ns",
    round(summary.med, 1) AS "Med:dur_ns",
    summary.min AS "Min:dur_ns",
    summary.max AS "Max:dur_ns",
    round(summary.stddev, 1) AS "StdDev:dur_ns",
    ids.value AS "Name"
FROM
    summary
LEFT JOIN
    StringIds AS ids
    ON ids.id == summary.nameId
ORDER BY 2 DESC
;
"""



QUERY_EXEC_DUR = """
    SELECT KERNEL.end - KERNEL.start AS klo
    FROM CUPTI_ACTIVITY_KIND_RUNTIME AS RUNTIME
    JOIN CUPTI_ACTIVITY_KIND_KERNEL AS KERNEL
    ON RUNTIME.correlationId = KERNEL.correlationId
    JOIN StringIds AS StringIds
    ON KERNEL.shortName = StringIds.id
    WHERE StringIds.value = ?
    AND KERNEL.gridX = ?
    AND KERNEL.gridY = ?
    AND KERNEL.gridZ = ?
    AND KERNEL.blockX = ?
    AND KERNEL.blockY = ?
    AND KERNEL.blockZ = ?
    """

QUERY_LAUNCH_OVERHEAD = """ 
    SELECT RUNTIME.end - RUNTIME.start AS klo
    FROM CUPTI_ACTIVITY_KIND_RUNTIME AS RUNTIME
    JOIN CUPTI_ACTIVITY_KIND_KERNEL AS KERNEL
    ON RUNTIME.correlationId = KERNEL.correlationId
    JOIN StringIds AS StringIds
    ON KERNEL.shortName = StringIds.id
    WHERE StringIds.value = ?
    AND KERNEL.gridX = ?
    AND KERNEL.gridY = ?
    AND KERNEL.gridZ = ?
    AND KERNEL.blockX = ?
    AND KERNEL.blockY = ?
    AND KERNEL.blockZ = ?
    """

QUERY_SLACK = """ 
    SELECT KERNEL.start - RUNTIME.end AS time_difference
    FROM CUPTI_ACTIVITY_KIND_RUNTIME AS RUNTIME
    JOIN CUPTI_ACTIVITY_KIND_KERNEL AS KERNEL
    ON RUNTIME.correlationId = KERNEL.correlationId
    JOIN StringIds AS StringIds
    ON KERNEL.shortName = StringIds.id
    WHERE StringIds.value = ?
    AND KERNEL.gridX = ?
    AND KERNEL.gridY = ?
    AND KERNEL.gridZ = ?
    AND KERNEL.blockX = ?
    AND KERNEL.blockY = ?
    AND KERNEL.blockZ = ?
    """

QUERY_TRANSFERS = """
    SELECT 
        CASE 
            WHEN copyKind = 1 THEN 'DtH' 
            WHEN copyKind = 2 THEN 'HtD' 
            WHEN copyKind = 8 THEN 'DtD' 
            WHEN copyKind = 10 THEN 'PtP' 
            ELSE 'Unknown' 
        END AS transfer_type,
        bytes, start, end
    FROM 
        CUPTI_ACTIVITY_KIND_MEMCPY 
    WHERE 
        copyKind IN (1, 2, 8, 10)
"""

MAX_WORKERS = 12


def generate_kernel_queries(kernel_names):
    exec_duration = []
    launch_overhead = []
    slack = []

    for kernel_name in kernel_names:
        exec_duration.append((QUERY_EXEC_DUR, kernel_name))
        launch_overhead.append((QUERY_LAUNCH_OVERHEAD, kernel_name))
        slack.append((QUERY_SLACK, kernel_name))

    return exec_duration, launch_overhead, slack


def execute_query(conn, query, params=None):
    cursor = conn.cursor()
    key = None
    if params:
        cursor.execute(query, params)
        key = '{},{},{},{},{},{},{}'.format(*params[:7])
    else:
        cursor.execute(query)
    result = cursor.fetchall()
    return key, result


def execute_queries_parallel(queries_with_params, database_file):
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for query_params in queries_with_params:
            future = executor.submit(execute_query_in_thread, query_params, database_file)
            futures.append(future)
        for future in as_completed(futures):
            results.append(future.result())
    return results


def execute_query_in_thread(query_params, database_file):
    conn = sqlite3.connect(database_file)  # Create a new connection object in each thread
    try:
        result = execute_query(conn, *query_params)
    except sqlite3.Error as error:
        print("Error reading data from SQLite table:", error)
    finally:
        conn.close()
    return result


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


def generate_log10_statistics(data, label):
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


def parse_kernel_data(data, label, duration, overhead, slack):
    raw_data = [(item[0] / 100) if item[0] > 0 else 0 for item in data[1]]

    if overhead or slack:
        remove_outliers(raw_data)

    # Update kernels dictionary
    kernel_name = data[0]
    if slack and len(raw_data) == 1 and raw_data[0] == 0:
        kernel_data = {}
        kernel_data[label] = {
            'Raw Data': 0,
            'Mean Duration': 0,
            'Median Duration': 0,
            'Minimum Duration': 0,
            'Maximum Duration': 0,
            'Standard Deviation': 0
        }
        raw_data.append(0)
    else:
        kernel_data = generate_log10_statistics(raw_data, label)

    if duration:
        freq = len(raw_data)
        dom = np.median(raw_data) * freq
        return kernel_name, freq, dom, kernel_data
    else:
        return kernel_name, kernel_data


def merge_duration_results(kernels, results):
    for kernel_name, frequency, dominance, kernel_data in results:
        if kernel_name not in kernels:
            raise "Kernel not in Dict after importing all kernels"
        kernels[kernel_name]['Frequency'] = frequency
        kernels[kernel_name]['Dominance'] = dominance
        kernels[kernel_name].update(kernel_data)

def merge_results(kernels, results):
    for kernel_name, kernel_data in results:
        if kernel_name not in kernels:
            raise "Kernel not in Dict after importing all kernels"
        kernels[kernel_name].update(kernel_data)


def parallel_parse_kernel_data(data_list, label, duration=False, overhead=False, slack=False):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for data in data_list:
            future = executor.submit(parse_kernel_data, data, label, duration, overhead, slack)
            futures.append(future)
        results = [future.result() for future in futures]
    return results


def generate_transfer_stats(transfers, label):

    frequency_distro = np.zeros(10)
    bandwidth_distro = [[] for _ in range(10)]
    for size, duration in transfers:
        if (size <= 4096):
            frequency_distro[0] += 1
            bandwidth_distro[0].append((size * 953.674) / duration)
        elif (size <= 8192):
            frequency_distro[1] += 1
            bandwidth_distro[1].append((size * 953.674) / duration)
        elif (size <= 16384):
            frequency_distro[2] += 1
            bandwidth_distro[2].append((size * 953.674) / duration)
        elif (size <= 32768):
            frequency_distro[3] += 1
            bandwidth_distro[3].append((size * 953.674) / duration)
        elif (size <= 65536):
            frequency_distro[4] += 1
            bandwidth_distro[4].append((size * 953.674) / duration)
        elif (size <= 131072):
            frequency_distro[5] += 1
            bandwidth_distro[5].append((size * 953.674) / duration)
        elif (size <= 262144):
            frequency_distro[6] += 1
            bandwidth_distro[6].append((size * 953.674) / duration)
        elif (size <= 524288):
            frequency_distro[7] += 1
            bandwidth_distro[7].append((size * 953.674) / duration)
        elif (size <= 1048576):
            frequency_distro[8] += 1
            bandwidth_distro[8].append((size * 953.674) / duration)
        else:
            frequency_distro[9] += 1
            bandwidth_distro[9].append((size * 953.674) / duration)

    transfer_data = {
        'Raw Data': transfers,
        'Frequency Distribution': frequency_distro.tolist(),
        'Bandwidth Distribution': bandwidth_distro
    }

    return (label, transfer_data)


def parse_transfer_data(transfer_query_res):
    # Initialize empty lists for each transfer type
    DtH_transfers = []
    HtD_transfers = []
    DtD_transfers = []
    PtP_transfers = []

    # Iterate through the results and sort them into respective lists based on transfer type
    for transfer_type, bytes, start, end in transfer_query_res:
        if transfer_type == 'DtH':
            DtH_transfers.append((bytes, end - start))
        elif transfer_type == 'HtD':
            HtD_transfers.append((bytes, end - start))
        elif transfer_type == 'DtD':
            DtD_transfers.append((bytes, end - start))
        elif transfer_type == 'PtP':
            PtP_transfers.append((bytes, end - start))

    res = generate_transfer_stats(DtH_transfers)

    return DtH_transfers, HtD_transfers, DtD_transfers, PtP_transfers


def parallel_parse_transfer_data(transfer_query_res):

    DtH_transfers = []
    HtD_transfers = []
    DtD_transfers = []
    PtP_transfers = []
    for transfer_type, bytes, start, end in transfer_query_res:
        if transfer_type == 'DtH':
            DtH_transfers.append((bytes, end - start))
        elif transfer_type == 'HtD':
            HtD_transfers.append((bytes, end - start))
        elif transfer_type == 'DtD':
            DtD_transfers.append((bytes, end - start))
        elif transfer_type == 'PtP':
            PtP_transfers.append((bytes, end - start))

    def generate_stats(transfer_list, label):
        return generate_transfer_stats(transfer_list, label)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        # Submit tasks for each transfer type
        futures.append(executor.submit(generate_stats, DtH_transfers, 'DtH'))
        futures.append(executor.submit(generate_stats, HtD_transfers, 'HtD'))
        futures.append(executor.submit(generate_stats, DtD_transfers, 'DtD'))
        futures.append(executor.submit(generate_stats, PtP_transfers, 'PtP'))

        # Wait for all tasks to complete and get the results
        results = [future.result() for future in futures]

    transfer_stats = {}
    for transfer_type, result in results:
        transfer_stats[transfer_type] = result

    return transfer_stats