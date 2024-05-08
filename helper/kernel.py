import numpy as np
from concurrent.futures import ThreadPoolExecutor
from helper.general import remove_outliers, generate_statistics, MAX_WORKERS

QUERY_KERNEL_NAMES = """ 
WITH
    summary AS (
        SELECT
            coalesce(shortname, demangledName) AS nameId,
            shortname AS kernel_id,
            sum(end - start) AS total,
            count(*) AS num
        FROM
            CUPTI_ACTIVITY_KIND_KERNEL
        GROUP BY 1
    ),
    totals AS (
        SELECT sum(total) AS total
        FROM summary
    )
SELECT
    summary.kernel_id AS "ID",
    round(summary.total * 100.0 / (SELECT total FROM totals), 1) AS "Time:ratio_%",
    summary.total AS "Total Time:dur_ns",
    summary.num AS "Instances",
    ids.value AS "Name"
FROM
    summary
LEFT JOIN
    StringIds AS ids
    ON ids.id = summary.nameId
"""
QUERY_KERNEL_STATS = """
WITH
    kernel_summary AS (
        SELECT
            shortname AS kernel_id,
            KERNEL.end - KERNEL.start AS execution_time,
            RUNTIME.end - RUNTIME.start AS launch_overhead,
            KERNEL.start - RUNTIME.end AS slack
        FROM
            CUPTI_ACTIVITY_KIND_RUNTIME AS RUNTIME
        JOIN
            CUPTI_ACTIVITY_KIND_KERNEL AS KERNEL
        ON
            RUNTIME.correlationId = KERNEL.correlationId
        JOIN
            StringIds AS StringIds
        ON
            KERNEL.shortName = StringIds.id
    )
SELECT
    kernel_id AS "ID",
    execution_time AS "Execution time",
    launch_overhead AS "Launch overhead",
    slack AS "Slack"
FROM
    kernel_summary
WHERE
    kernel_id = ?
"""


def generate_kernel_queries(kernel_ids):
    queries = []

    for kernel_id in kernel_ids:
        queries.append((QUERY_KERNEL_STATS, kernel_id))

    return queries


def parse_kernel_data(data):
    raw_duration_data = []
    raw_overhead_data = []
    raw_slack_data = []

    for id, duration, overhead, slack in data[1]:
        raw_duration_data.append(duration) if duration > 0 else 0
        raw_overhead_data.append(overhead) if overhead > 0 else 0
        raw_slack_data.append(slack) if slack > 0 else 0

    if raw_overhead_data:
        remove_outliers(raw_overhead_data)
    if raw_slack_data:
        remove_outliers(raw_slack_data)

    results_dict = {}
    results_dict.update(generate_statistics(raw_duration_data, 'Execution Duration'))
    results_dict.update(generate_statistics(raw_overhead_data, 'Launch Overhead'))
    results_dict.update(generate_statistics(raw_slack_data, 'Slack'))

    if len(raw_slack_data) == 1 and raw_slack_data[0] == 0:
        results_dict.update( {
            'Raw Data': 0,
            'Mean Duration': 0,
            'Median Duration': 0,
            'Minimum Duration': 0,
            'Maximum Duration': 0,
            'Standard Deviation': 0
        })
        raw_slack_data.append(0)
    else:
        results_dict.update(generate_statistics(raw_slack_data, 'Slack'))

    if raw_duration_data:
        freq = len(raw_duration_data)
        dom = np.median(raw_duration_data) * freq
        results_dict['Frequency'] = freq
        results_dict['Dominance'] = dom

    return id, results_dict


def parallel_parse_kernel_data(queries_res):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for data in queries_res:
            future = executor.submit(parse_kernel_data, data)
            futures.append(future)
        results = [future.result() for future in futures]
    return results
