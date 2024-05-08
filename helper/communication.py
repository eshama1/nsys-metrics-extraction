import numpy as np
from concurrent.futures import ThreadPoolExecutor
from helper.general import generate_statistics, MAX_WORKERS


QUERY_COMMUNICATION = """
WITH
    domains AS (
        SELECT
            min(start),
            domainId AS id,
            globalTid AS globalTid,
            text AS name
        FROM
            NVTX_EVENTS
        WHERE
            eventType == 75
        GROUP BY 2, 3
    ),
    maxts AS(
        SELECT max(max(start), max(end)) AS m
        FROM   NVTX_EVENTS
    ),
    nvtx AS (
        SELECT
            coalesce(ne.end, (SELECT m FROM maxts)) - ne.start AS duration,
            CASE
                WHEN d.name NOT NULL AND sid.value IS NOT NULL
                    THEN d.name || ':' || sid.value
                WHEN d.name NOT NULL AND sid.value IS NULL
                    THEN d.name || ':' || ne.text
                WHEN d.name IS NULL AND sid.value NOT NULL
                    THEN sid.value
                ELSE ne.text
            END AS tag,
            CASE ne.eventType
                WHEN 59
                    THEN 'PushPop'
                WHEN 60
                    THEN 'StartEnd'
                WHEN 70
                    THEN 'PushPop'
                WHEN 71
                    THEN 'StartEnd'
                ELSE 'Unknown'
            END AS style
        FROM
            NVTX_EVENTS AS ne
        LEFT OUTER JOIN
            domains AS d
            ON ne.domainId == d.id
                AND (ne.globalTid & 0x0000FFFFFF000000) == (d.globalTid & 0x0000FFFFFF000000)
        LEFT OUTER JOIN
            StringIds AS sid
            ON ne.textId == sid.id
        WHERE
            ne.eventType == 59
            OR
            ne.eventType == 60
            OR
            ne.eventType == 70
            OR
            ne.eventType == 71
    ),
    summary AS (
        SELECT
            tag AS name,
            style AS style,
            sum(duration) AS total,
            count(*) AS num
        FROM
            nvtx
        GROUP BY 1, 2
    ),
    totals AS (
        SELECT sum(total) AS total
        FROM summary
    )
    SELECT
        name AS "Name",
        round(total * 100.0 / (SELECT total FROM totals), 1) AS "Time:ratio_%",
        total AS "Total Time:dur_ns",
        num AS "Instances"
    FROM
        summary
    ORDER BY 2 DESC
"""

QUERY_COMMUNICATION_STATS = """ 
WITH
    domains AS (
        SELECT
            min(start),
            domainId AS id,
            globalTid AS globalTid,
            text AS name
        FROM
            NVTX_EVENTS
        WHERE
            eventType == 75
        GROUP BY 2, 3
    ),
    maxts AS(
        SELECT max(max(start), max(end)) AS m
        FROM   NVTX_EVENTS
    ),
    nvtx AS (
        SELECT
            coalesce(ne.end, (SELECT m FROM maxts)) - ne.start AS duration,
            CASE
                WHEN d.name NOT NULL AND sid.value IS NOT NULL
                    THEN d.name || ':' || sid.value
                WHEN d.name NOT NULL AND sid.value IS NULL
                    THEN d.name || ':' || ne.text
                WHEN d.name IS NULL AND sid.value NOT NULL
                    THEN sid.value
                ELSE ne.text
            END AS tag
        FROM
            NVTX_EVENTS AS ne
        LEFT OUTER JOIN
            domains AS d
            ON ne.domainId == d.id
                AND (ne.globalTid & 0x0000FFFFFF000000) == (d.globalTid & 0x0000FFFFFF000000)
        LEFT OUTER JOIN
            StringIds AS sid
            ON ne.textId == sid.id
        WHERE
            ne.eventType == 59
            OR
            ne.eventType == 60
            OR
            ne.eventType == 70
            OR
            ne.eventType == 71
    )
SELECT
    tag AS "Name",
    duration AS "Duration:dur_ns"
FROM
    nvtx
WHERE
    name = ?
"""

def generate_communicaiton_stats(comm):
    durations = [dur[1] for dur in comm[1]]
    label = comm[0]
    dict = generate_statistics(durations, label)
    return label, dict[label]

def parallel_parse_communication_data(queries_res):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for data in queries_res:
            future = executor.submit(generate_communicaiton_stats, data)
            futures.append(future)
        results = [future.result() for future in futures]
    return results