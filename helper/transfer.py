import numpy as np
from concurrent.futures import ThreadPoolExecutor
from helper.general import generate_statistics, MAX_WORKERS

QUERY_TRANSFERS = """
WITH
    MemcpyOperStrs AS (SELECT * FROM ENUM_CUDA_MEMCPY_OPER),
    memops AS (
        SELECT
            mos.label AS name,
            mcpy.end - mcpy.start AS duration,
            mcpy.bytes AS size
        FROM
            CUPTI_ACTIVITY_KIND_MEMCPY as mcpy
        INNER JOIN
            MemcpyOperStrs AS mos
            ON mos.id == mcpy.copyKind

        UNION ALL
        SELECT
            'Memset' AS name,
            end - start AS duration,
            bytes AS size
        FROM
            CUPTI_ACTIVITY_KIND_MEMSET
    ),
    summary AS (
        SELECT
            name AS name,
            sum(duration) AS time_total,
            sum(size) AS mem_total,
            count(*) AS num
        FROM
            memops
        GROUP BY 1
    ),
    totals AS (
        SELECT sum(time_total) AS time_total FROM summary
    )
SELECT
    summary.name AS "Operation",
    round(summary.time_total * 100.0 / (SELECT time_total FROM totals), 1) AS "Time:ratio_%",
    summary.time_total AS "Total Time:dur_ns",
    summary.mem_total AS "Total:mem_B",
    summary.num AS "Count"
FROM
    summary
ORDER BY 2 DESC
"""

QUERY_TRANSFERS_STATS = """
WITH
    MemcpyOperStrs AS (SELECT * FROM ENUM_CUDA_MEMCPY_OPER),
    transfers AS (
        SELECT
            mos.label AS name,
            mcpy.end - mcpy.start AS duration,
            mcpy.bytes AS size
        FROM
            CUPTI_ACTIVITY_KIND_MEMCPY as mcpy
        INNER JOIN
            MemcpyOperStrs AS mos
            ON mos.id == mcpy.copyKind
        UNION ALL
        SELECT
            'Memset' AS name,
            end - start AS duration,
            bytes AS size
        FROM
            CUPTI_ACTIVITY_KIND_MEMSET
    )
SELECT
    name AS "Name",
    duration AS "Duration",
    size AS "Size"
FROM
    transfers
WHERE
    name = ?
"""

def generate_transfer_queries(transfer_name):
    queries = []

    for name in transfer_name:
        queries.append((QUERY_TRANSFERS_STATS, name))

    return queries

def generate_transfer_stats(transfers):
    frequency_distro = np.zeros(10)
    bandwidth_distro = [[] for _ in range(10)]
    transfer_sizes = []
    transfer_durations = []

    for _, size, duration in transfers[1]:
        transfer_sizes.append(size)
        transfer_durations.append(duration)

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

    transfer_data = {}
    if transfer_sizes:
        transfer_data.update(generate_statistics(transfer_sizes, "Transfer Size"))
    else:
        transfer_data['Transfer Size'] = None

    if transfer_durations:
        transfer_data.update(generate_statistics(transfer_durations, "Transfer Durations"))
    else:
        transfer_data['Transfer Durations'] = None

    transfer_data['Frequency Distribution'] = frequency_distro.tolist()
    transfer_data['Bandwidth Distribution'] = bandwidth_distro

    return transfers[0], transfer_data


def parallel_parse_kernel_data(queries_res):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for data in queries_res:
            future = executor.submit(generate_transfer_stats, data)
            futures.append(future)
        results = [future.result() for future in futures]
    return results
