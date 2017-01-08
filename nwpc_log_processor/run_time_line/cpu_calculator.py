# coding=utf-8
from __future__ import absolute_import
import argparse
import datetime
import csv
from pymongo import MongoClient

from nwpc_log_processor.config import MONGODB_HOST, MONGODB_PORT

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
        for a_run_time in suite_run_times:
            label = a_run_time['label']
            if len(label) == 0:
                cpu_count = 0
            else:
                cpu_count = int(label) / 32

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
    owner_name = 'nwp_xp'
    repo_name = 'nwp_cma20n03'
    query_date = datetime.datetime.strptime('2016-08-12', "%Y-%m-%d")

    if args.owner:
        owner_name = args.owner
        print('user name: {owner_name}'.format(owner_name=owner_name))
    else:
        print("Use default user: {owner_name}".format(owner_name=owner_name))

    if args.repo:
        repo_name = args.repo
        print('repo name: {repo_name}'.format(repo_name=repo_name))
    else:
        print("Use default repo name: {repo_name}".format(repo_name=repo_name))

    if args.date:
        query_date = datetime.datetime.strptime(args.date, "%Y-%m-%d")
        print('query date: {query_date}'.format(query_date=query_date))
    else:
        print("Use default query date: {query_date}".format(query_date=query_date))

    suite_rows = calculate_cpu_for_repo(owner_name, repo_name, query_date)
    if suite_rows is None:
        print("Fatal Error: suite rows is None")
        return

    time_array = [(query_date + datetime.timedelta(minutes=i)).strftime("%H:%M") for i in range(0, 60 * 24)]
    title_row = ["suite"]
    title_row.extend(time_array)

    print(time_array)
    for a_suite in suite_rows:
        print(a_suite)

    with open('output/' + owner_name + '_' + repo_name + '.csv', 'wb') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(title_row)
        for a_row in suite_rows:
            csv_writer.writerow(a_row)
    return


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
    suite_parser.add_argument("-o", "--owner", help="owner name")
    suite_parser.add_argument("-r", "--repo", help="repo name")
    suite_parser.add_argument("-d", "--date", help="query date (%Y-%m-%d)")
    suite_parser.add_argument("--output-file", help="output file path")
    suite_parser.add_argument("--save-to-db", help="save result to database", action='store_true')
    suite_parser.add_argument("-p", "--print", dest='print_flag', help="print result to console.", action='store_true')

    server_parser = sub_parsers.add_parser('server_parser', description="collect sms status from sms server.")
    server_parser.add_argument("-o", "--owner", help="owner name")
    server_parser.add_argument("-r", "--repo", help="repo name")
    server_parser.add_argument("-d", "--date", help="query date (%Y-%m-%d)")
    server_parser.add_argument("--output-file", help="output file path")
    server_parser.add_argument("--save-to-db", help="save result to database", action='store_true')
    server_parser.add_argument("-p", "--print", dest='print_flag', help="print result to console.", action='store_true')

    args = parser.parse_args()

    if args.sub_command == "repo":
        repo_handler(args)


    # 时间统计
    end_time = datetime.datetime.now()
    print(end_time - start_time)
    return


if __name__ == "__main__":
    main()
