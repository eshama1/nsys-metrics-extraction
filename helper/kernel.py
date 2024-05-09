from absl import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from helper.general import remove_outliers, generate_statistics, MAX_WORKERS

QUERY_KERNEL = """ 
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
ORDER BY 2 DESC
"""
QUERY_KERNEL_STATS = """
WITH
    kernel_summary AS (
        SELECT
            KERNEL.shortname AS kernel_id,
            KERNEL.end - KERNEL.start AS execution_time,
            CASE
                WHEN RUNTIME.correlationId IS NOT NULL THEN RUNTIME.end - RUNTIME.start
                ELSE NULL 
            END AS launch_overhead,
            CASE
                WHEN RUNTIME.correlationId IS NOT NULL THEN KERNEL.start - RUNTIME.end
                ELSE NULL
            END AS slack
        FROM
            CUPTI_ACTIVITY_KIND_KERNEL AS KERNEL
        LEFT JOIN
            CUPTI_ACTIVITY_KIND_RUNTIME AS RUNTIME
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

KERNEL_REQUIRED_TABLES = ['CUPTI_ACTIVITY_KIND_KERNEL', 'CUPTI_ACTIVITY_KIND_RUNTIME', 'StringIds']

def generate_kernel_queries(kernel_ids):
    queries = []

    for kernel_id in kernel_ids:
        queries.append((QUERY_KERNEL_STATS, kernel_id))

    return queries


def parse_kernel_data(data):
    raw_duration_data = []
    raw_overhead_data = []
    raw_slack_data = []
    runtime_values = True

    for id, duration, overhead, slack in data[1]:
        raw_duration_data.append(duration) if duration > 0 else 0

        if overhead is None or slack is None:
            runtime_values = False
        else:
            raw_overhead_data.append(overhead) if overhead > 0 else 0
            raw_slack_data.append(slack) if slack > 0 else 0

    if runtime_values:
        if raw_overhead_data:
            remove_outliers(raw_overhead_data)
        if raw_slack_data:
            remove_outliers(raw_slack_data)

    results_dict = {}
    results_dict.update(generate_statistics(raw_duration_data, 'Execution Duration'))

    if runtime_values:
        results_dict.update(generate_statistics(raw_overhead_data, 'Launch Overhead'))
        results_dict.update(generate_statistics(raw_slack_data, 'Slack'))
    else:
        results_dict['Launch Overhead'] = None
        results_dict['Slack'] = None

    if raw_duration_data:
        freq = len(raw_duration_data)
        results_dict['Frequency'] = freq

    return id, results_dict


def parallel_parse_kernel_data(queries_res):
    total_tasks = len(queries_res)
    completed_tasks = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for data in queries_res:
            future = executor.submit(parse_kernel_data, data)
            futures.append(future)

        results = []
        for future in as_completed(futures):
            results.append(future.result())
            completed_tasks += 1

            if int((completed_tasks / total_tasks) * 100) % 10 == 0:
                logging.info(f"Progress: {(completed_tasks / total_tasks) * 100:.1f}%")

    return results


def create_general_duration_kernel_stats(kernel_stats):
    return None


def create_general_overhead_kernel_stats(kernel_stats):
    return None


def create_general_slack_kernel_stats(kernel_stats):
    return None


def create_general_kernel_stats(kernel_stats):
    return None
