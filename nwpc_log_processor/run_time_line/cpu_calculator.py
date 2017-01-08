# coding=utf-8
from __future__ import absolute_import
import argparse
import datetime
import csv
from pymongo import MongoClient

from nwpc_log_processor.run_time_line.conf.config import MONGODB_HOST, MONGODB_PORT

from nwpc_log_processor.run_time_line.time_line_chart_data_generator import load_schema

mongodb_client = MongoClient(MONGODB_HOST, MONGODB_PORT)
smslog_mongodb = mongodb_client.smslog
daily_tree_status_collection = smslog_mongodb.daily_tree_status_collection
daily_repo_time_line_collection = smslog_mongodb.daily_repo_time_line_collection

repo_time_line_collection = smslog_mongodb.daily_repo_time_line_collection


def calculate_cpu_for_repo(owner, repo, query_date):
    # get data
    time_line_key = {
        'owner': owner,
        'repo': repo,
        'query_date': query_date.strftime("%Y-%m-%d")
    }
    time_line_value = daily_repo_time_line_collection.find_one(time_line_key)

    if time_line_value is None:
        print("There is no record:", time_line_key)
        return None

    # time array
    cur_datetime = datetime.datetime.now()
    start_datetime = datetime.datetime.combine(cur_datetime.date(), datetime.time(0, 0))
    end_datetime = datetime.datetime.combine(cur_datetime.date(), datetime.time(23, 59))

    suite_rows = []
    suites = time_line_value["suites"]
    for a_suite in suites:
        suite_time_array = [0 for i in range(0, 60 * 24)]

        if 'times' not in a_suite:
            continue
        suite_run_times = a_suite['times']

        # find cpu count
        # NOTE: this is only a temporary solution.
        cpu_count = 0
        for a_run_time in suite_run_times:
            if 'label' in a_run_time and len(a_run_time['label']) > 0:
                cpu_count = int(a_run_time['label']) / 32
                break

        for a_run_time in suite_run_times:
            label = a_run_time['label']
            if len(label) > 0:
                continue

            record_start_time = a_run_time['start_time']
            record_end_time = a_run_time['end_time']

            record_start_datetime = datetime.datetime.strptime(record_start_time, "%H:%M:%S")
            record_end_datetime = datetime.datetime.strptime(record_end_time, "%H:%M:%S")

            record_start_num = record_start_datetime.hour * 60 \
                + record_start_datetime.minute
            record_end_num = record_end_datetime.hour * 60 \
                + record_end_datetime.minute

            for i in range(record_start_num, record_end_num + 1):
                suite_time_array[i] += cpu_count

        suite_row = [a_suite["name"]]
        suite_row.extend(suite_time_array)
        suite_rows.append(suite_row)

    return suite_rows


def repo_handler(args):
    owner_name = args.owner
    print('user name: {owner_name}'.format(owner_name=owner_name))

    repo_name = args.repo
    print('repo name: {repo_name}'.format(repo_name=repo_name))

    query_date = datetime.datetime.strptime(args.date, "%Y-%m-%d")
    print('query date: {query_date}'.format(query_date=query_date))

    suite_rows = calculate_cpu_for_repo(owner_name, repo_name, query_date)
    if suite_rows is None:
        print("Fatal Error: suite rows is None")
        return

    time_array = [(query_date + datetime.timedelta(minutes=i)).strftime("%H:%M") for i in range(0, 60 * 24)]
    title_row = ["suite"]
    title_row.extend(time_array)

    print(title_row)
    for a_suite in suite_rows:
        print(a_suite)

    with open('output/' + owner_name + '.' + repo_name + '.' + args.date + '.' + '.csv', 'w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(title_row)
        for a_row in suite_rows:
            csv_writer.writerow(a_row)
    return


def system_handler(args):
    schema_file_path = args.schema
    print('schema file path:', schema_file_path)
    query_date = datetime.datetime.strptime(args.date, "%Y-%m-%d")
    print('query date: {query_date}'.format(query_date=query_date))

    schema = load_schema(schema_file_path)
    if schema is None:
        print('Fatal error: load schema file failed')

    system_cpu_usage_rows = []

    for a_server in schema['series']:
        owner_name = a_server['owner']
        repo_name = a_server['repo']
        server_suite_cpu_usage = calculate_cpu_for_repo(owner_name, repo_name, query_date)
        system_cpu_usage_rows.extend(server_suite_cpu_usage)

    time_array = [(query_date + datetime.timedelta(minutes=i)).strftime("%H:%M") for i in range(0, 60 * 24)]
    title_row = ["suite"]
    title_row.extend(time_array)

    print(title_row)
    for a_suite in system_cpu_usage_rows:
        print(a_suite)

    if args.output_file:
        with open(args.output_file, 'w') as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(title_row)
            for a_row in system_cpu_usage_rows:
                csv_writer.writerow(a_row)


def main():
    start_time = datetime.datetime.now()

    # argument parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""\
DESCRIPTION
    Calculate cpu.""")

    sub_parsers = parser.add_subparsers(title="sub commands", dest="sub_command")

    suite_parser = sub_parsers.add_parser('suite', description="collect sms status from sms server.")
    suite_parser.add_argument("-o", "--owner", help="owner name", required=True)
    suite_parser.add_argument("-r", "--repo", help="repo name", required=True)
    suite_parser.add_argument("-d", "--date", help="query date (%Y-%m-%d)", required=True)
    suite_parser.add_argument("--output-file", help="output file path")
    suite_parser.add_argument("-p", "--print", dest='print_flag', help="print result to console.", action='store_true')

    server_parser = sub_parsers.add_parser('system', description="collect sms status from sms server.")
    server_parser.add_argument("-s", "--schema", help="schema file path", required=True)
    server_parser.add_argument("-d", "--date", help="query date (%Y-%m-%d)", required=True)
    server_parser.add_argument("--output-file", help="output file path")
    server_parser.add_argument("-p", "--print", dest='print_flag', help="print result to console.", action='store_true')

    args = parser.parse_args()

    if args.sub_command == "suite":
        repo_handler(args)
    elif args.sub_command == "system":
        system_handler(args)

    end_time = datetime.datetime.now()
    print(end_time - start_time)
    return


if __name__ == "__main__":
    main()
