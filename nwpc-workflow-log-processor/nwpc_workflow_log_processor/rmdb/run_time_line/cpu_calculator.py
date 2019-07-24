# coding=utf-8
import datetime
import csv
import pathlib

import click
import yaml
from pymongo import MongoClient

from nwpc_workflow_log_processor.rmdb.run_time_line.config import load_processor_config


@click.group()
def cli():
    """
    Calculate cpu
    """
    pass


@cli.command('suite')
@click.option("-c", "--config", "config_file_path", help="config file path", required=True)
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("-d", "--date", help="query date (%Y-%m-%d)", required=True)
@click.option("--output-file", help="output file path")
@click.option("-p", "--print", "print_flag", is_flag=True, help="print result to console.")
def suite(config_file_path, owner, repo, date, output_file, print_flag):
    """
    collect sms status from sms server.
    """
    print('user name: {owner}'.format(owner=owner))
    print('repo name: {repo}'.format(repo=repo))

    query_date = datetime.datetime.strptime(date, "%Y-%m-%d")
    print('query date: {query_date}'.format(query_date=query_date))

    config = load_processor_config(config_file_path)

    suite_rows = calculate_cpu_for_repo(config, owner, repo, query_date)
    if suite_rows is None:
        print("Fatal Error: suite rows is None")
        return

    time_array = [(query_date + datetime.timedelta(minutes=i)).strftime("%H:%M") for i in range(0, 60 * 24)]
    title_row = ["suite"]
    title_row.extend(time_array)

    print(title_row)
    for a_suite in suite_rows:
        print(a_suite)

    with open('output/' + owner + '.' + repo + '.' + date + '.' + '.csv', 'w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(title_row)
        for a_row in suite_rows:
            csv_writer.writerow(a_row)
    return


@cli.command('system')
@click.option("-c", "--config", "config_file_path", help="config file path", required=True)
@click.option("-s", "--schema", "schema_file_path", help="schema file path", required=True)
@click.option("-d", "--date", help="query date (%Y-%m-%d)", required=True)
@click.option("--output-file", help="output file path")
@click.option("-p", "--print", "print_flag", is_flag=True)
def system_handler(config_file_path, schema_file_path, date, output_file, print_flag):
    print('schema file path:', schema_file_path)
    query_date = datetime.datetime.strptime(date, "%Y-%m-%d")
    print('query date: {query_date}'.format(query_date=query_date))

    config = load_processor_config(config_file_path)

    schema = load_schema(schema_file_path)
    if schema is None:
        print('Fatal error: load schema file failed')

    system_cpu_usage_rows = []

    for a_server in schema['series']:
        owner_name = a_server['owner']
        repo_name = a_server['repo']
        server_suite_cpu_usage = calculate_cpu_for_repo(config, owner_name, repo_name, query_date)
        system_cpu_usage_rows.extend(server_suite_cpu_usage)

    time_array = [(query_date + datetime.timedelta(minutes=i)).strftime("%H:%M") for i in range(0, 60 * 24)]
    title_row = ["suite"]
    title_row.extend(time_array)

    print(title_row)
    for a_suite in system_cpu_usage_rows:
        print(a_suite)

    if output_file:
        with open(output_file, 'w') as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(title_row)
            for a_row in system_cpu_usage_rows:
                csv_writer.writerow(a_row)


def calculate_cpu_for_repo(config, owner, repo, query_date):
    statistic_dbms_config = config['datastore']['statistic_dbms']
    mongodb_client = MongoClient(statistic_dbms_config['host'], statistic_dbms_config['port'])
    mongodb_database = mongodb_client[statistic_dbms_config['database']]

    daily_repo_time_line_collection = mongodb_database.daily_repo_time_line_collection

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


def load_schema(schema_path):
    with open(schema_path, 'r') as config_file:
        schema = yaml.safe_load(config_file)
        return schema


if __name__ == "__main__":
    cli()
