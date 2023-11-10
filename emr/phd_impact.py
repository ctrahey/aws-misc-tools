#! /usr/bin/env python3
import argparse
import csv
import sys
import time

PHD_COLUMN_FOR_ARN = 'resourceId'
EMR_COLUMN_FOR_ARN = 'details.clusterArn'
EMR_COLUMN_FOR_STATUS = 'State.value'
EMR_COLUMN_FOR_TERMINATED = 'details.status.timeline.endDateTime'
EMR_TIMESTAMP_DIVISOR = 1000

# phd and emr are lists of dicts
# in phd, take the value in field "resourceId" and extract the ARN as the string before the " | " sequence.
# For each of these ARNs, check if it is in the emr list in the field "details.clusterArn"
# if it is, check the field "State.value" and if it is anything other than "TERMINATED",
# add the row to the filtered_output list
def filter_for_impact(days, phd, emr):
    filtered_output = []
    now = int(time.time())
    num_checked = 0
    for row in phd:
        num_checked += 1
        arn = row[PHD_COLUMN_FOR_ARN].split(' | ')[0]
        for cluster in emr:
            if cluster[EMR_COLUMN_FOR_ARN] == arn:
                status = cluster[EMR_COLUMN_FOR_STATUS]
                if status != 'TERMINATED':
                    filtered_output.append((arn, status, 0))
                    break
                else:
                    terminated_timestamp = int(cluster[EMR_COLUMN_FOR_TERMINATED])/EMR_TIMESTAMP_DIVISOR
                    terminated_ago = (now - terminated_timestamp)/(60*60)
                    if terminated_ago < days * 24:
                        filtered_output.append((arn, status, terminated_ago))
    return num_checked, filtered_output


def process_files(days, phd, emr):
    if not phd.endswith('.csv') or not emr.endswith('.csv'):
        print('Error: Input files must be CSV files', file=sys.stderr)
        return
    with open(phd, 'r') as f:
        reader = csv.DictReader(f)
        phd_list = list(reader)
    with open(emr, 'r') as f:
        reader = csv.DictReader(f)
        emr_list = list(reader)
    num_checked, filtered_output = filter_for_impact(days, phd_list, emr_list)
    if len(filtered_output) == 0:
        print(f"Good news!", file=sys.stderr)
    else:
        print("arn,status,terminated_hours_ago")
        for row in filtered_output:
            print(f'{row[0]},{row[1]},{row[2]:.0f}')
    print(f'Of {num_checked} PHD rows, {len(filtered_output)} terminated more recently than {days} days ago',
          file=sys.stderr)
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--days', type=int, default=3,
                        help='Terminations more recently than this many days ago will be reported')
    parser.add_argument('phd', help='CSV file of PhD output listing impacted EMR clusters')
    parser.add_argument('emr', help='CSV file of EMR output listing all EMR clusters')
    args = parser.parse_args()
    process_files(args.days, args.phd, args.emr)
