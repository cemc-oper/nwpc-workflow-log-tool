# coding=utf-8
from __future__ import absolute_import

import argparse
import datetime
import json
import os

from pymongo import MongoClient

# settings
from nwpc_log_processor.run_time_line.conf.config import MONGODB_HOST, MONGODB_PORT

# mongodb collections
mongodb_client = MongoClient(MONGODB_HOST, MONGODB_PORT)
smslog_mongodb = mongodb_client.smslog
daily_repo_time_line_collection = smslog_mongodb.daily_repo_time_line_collection


def load_schema(config_file_path):
    if not os.path.exists(config_file_path):
        print("{config_file_path} doesn't exist".format(config_file_path=config_file_path))
        return None
    with open(config_file_path, 'r') as config_file:
        schema = json.load(config_file)
        return schema


def load_data(schema, query_date):
    series = schema['series']
    chart_data = []
    for a_record in series:
        owner = a_record['owner']
        repo = a_record['repo']
        key = {
            'owner': owner,
            'repo': repo,
            'query_date': query_date.strftime("%Y-%m-%d")
        }
        value = daily_repo_time_line_collection.find_one(key)
        if value is None:
            print("FATAL ERROR: data not found: ", owner, "/", repo)
        else:
            chart_data.extend(value['suites'])

    result = {
        'data': {
            'chart_data': chart_data
        }
    }
    return result


def main():
    start_time = datetime.datetime.now()

    # argument parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""\
DESCRIPTION
    Generate time line chart data.""")

    parser.add_argument("-d", "--date", help="query date (%Y-%m-%d)", required=True)
    parser.add_argument("-c", "--config", help="config file path")
    parser.add_argument("--output-file", help="output file path")
    parser.add_argument("-p", "--print", dest='print_flag', help="print result to console.", action='store_true')

    args = parser.parse_args()

    query_date = datetime.datetime.strptime(args.date, "%Y-%m-%d")
    print('query date: {query_date}'.format(query_date=query_date))

    config_file_path = args.config

    schema = load_schema(config_file_path)
    if schema is None:
        print("Fatal Error: load schema failed.")
        return

    result = load_data(schema, query_date)

    # if args.print_flag:
    #     print(json.dumps(result, indent=4))

    # save to file
    if args.output_file:
        output_file_path = args.output_file
        print("Write results to output file: {output_file_path}".format(output_file_path=output_file_path))
        output_file_content = json.dumps(result['data']['chart_data'], indent=2)
        with open(output_file_path, 'w') as output_file:
            output_file.write(output_file_content)

    # 时间统计
    end_time = datetime.datetime.now()
    print(end_time - start_time)
    return


if __name__ == "__main__":
    main()
