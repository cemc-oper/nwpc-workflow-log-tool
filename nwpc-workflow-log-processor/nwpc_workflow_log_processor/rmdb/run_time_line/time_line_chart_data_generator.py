# coding=utf-8
import datetime
import json
import pathlib

import click
import yaml
from pymongo import MongoClient

from nwpc_workflow_log_processor.rmdb.run_time_line.config import load_processor_config


def load_schema(config):
    schema_path = config['processor']['chart_schema']['path']
    if schema_path.startswith('.'):
        schema_path = pathlib.Path(pathlib.Path(config['_file_path']).parent, schema_path)

    if not schema_path.exists():
        print("{config_file_path} doesn't exist".format(config_file_path=schema_path))
        return None
    with open(schema_path, 'r') as config_file:
        schema = yaml.load(config_file)
        return schema


def load_data(config, schema, query_date):
    statistic_dbms_config = config['datastore']['statistic_dbms']
    mongodb_client = MongoClient(statistic_dbms_config['host'], statistic_dbms_config['port'])
    mongodb_database = mongodb_client[statistic_dbms_config['database']]

    daily_repo_time_line_collection = mongodb_database.daily_repo_time_line_collection

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


def generate_chart_data(config, query_date, output_file, save_to_db, print_flag):
    print('query date: {query_date}'.format(query_date=query_date))
    schema = load_schema(config)
    if schema is None:
        print("Fatal Error: load schema failed.")
        return

    result = load_data(config, schema, query_date)

    if print_flag:
        print(json.dumps(result, indent=4))

    # save to file
    if output_file:
        output_file_path = output_file
        print("Write results to output file: {output_file_path}".format(output_file_path=output_file_path))
        output_file_content = json.dumps(result['data']['chart_data'], indent=2)
        with open(output_file_path, 'w') as output_file:
            output_file.write(output_file_content)


@click.command()
@click.option("-c", "--config", "config_file_path", help="config file path", required=True)
@click.option("-d", "--date", help="query date (%Y-%m-%d)", required=True)
@click.option("--output-file", help="output file path")
@click.option("--save-to-db", is_flag=True, default=False, help="save result to database")
@click.option("-p", "--print", "print_flag", is_flag=True, default=False, help="print result to console.")
def cli(config_file_path, date, output_file, save_to_db, print_flag):
    """\
DESCRIPTION
    Generate time line chart data."""
    start_time = datetime.datetime.now()

    query_date = datetime.datetime.strptime(date, "%Y-%m-%d")

    config = load_processor_config(config_file_path)

    generate_chart_data(config, query_date, output_file, save_to_db, print_flag)

    # 时间统计
    end_time = datetime.datetime.now()
    print(end_time - start_time)
    return


if __name__ == "__main__":
    cli()
