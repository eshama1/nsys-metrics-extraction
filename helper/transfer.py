import numpy as np
from concurrent.futures import ThreadPoolExecutor

from helper.general import generate_statistics, MAX_WORKERS

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


def generate_transfer_stats(transfers, label):
    frequency_distro = np.zeros(10)
    bandwidth_distro = [[] for _ in range(10)]
    transfer_sizes = []
    transfer_durations = []

    for size, duration in transfers:
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
