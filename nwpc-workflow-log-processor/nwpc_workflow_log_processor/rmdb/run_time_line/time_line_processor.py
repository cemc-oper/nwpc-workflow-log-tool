# coding=utf-8
import datetime
import json
import pathlib

import click
import yaml
from mongoengine import connect
from pymongo import MongoClient

from nwpc_workflow_log_model.rmdb.util.bunch_util import BunchUtil

from nwpc_workflow_log_processor.rmdb.run_time_line.config import load_processor_config
from nwpc_workflow_log_processor.common.util import get_record_class
from nwpc_workflow_log_processor.rmdb.run_time_line.calculator.time_line import calculate_time_line


def get_bunch(owner_name, repo_name, repo_type, query_date, rmdb_session, mongodb_client):
    """
    从 MySQL 中生成某个项目的树形结构，保存到 MongoDB 中。

    :param owner_name:
    :param repo_name:
    :param query_date:
    :param rmdb_session:
    :param mongodb_client:
    :return: Bunch
    """

    record_class = get_record_class(repo_type)

    # generate tree
    tree = BunchUtil.generate_repo_tree_from_session(rmdb_session, owner_name, repo_name, query_date, record_class)

    from nwpc_workflow_log_model.mongodb.node_tree import save_bunch
    save_bunch(owner_name, repo_name, query_date, tree, update_type='insert')

    return tree


def load_schema(config: dict, owner: str, repo: str):
    schema_dir = config['processor']['system_schema']['dir']
    if schema_dir.startswith('.'):
        schema_dir = pathlib.Path(pathlib.Path(config['_file_path']).parent, schema_dir)

    config_file_name = "{owner}.{repo}.schema.yml".format(
        owner=owner, repo=repo
    )

    config_file_path = pathlib.Path(schema_dir, config_file_name)
    if not config_file_path.exists():
        click.echo("{config_file_path} doesn't exist".format(config_file_path=config_file_path))
        return None
    with open(config_file_path, 'r') as config_file:
        schema = yaml.load(config_file)
        return schema


def process_time_line(config, owner_name, repo_name, repo_type, query_date):
    from nwpc_workflow_log_model.rmdb.util.session import get_session

    log_dbms_config = config['datastore']['log_dbms']
    mysql_session = get_session(log_dbms_config['database_uri'])

    mongodb_database = config['datastore']['statistic_dbms']['database']
    mongodb_host = config['datastore']['statistic_dbms']['host']
    mongodb_port = config['datastore']['statistic_dbms']['port']
    mongodb_client = connect(mongodb_database, host=mongodb_host, port=mongodb_port)

    bunch = get_bunch(owner_name, repo_name, repo_type, query_date, mysql_session, mongodb_client)

    schema = load_schema(config, owner_name, repo_name)
    if schema is None:
        click.echo("Fatal Error: load schema failed.")
        return None

    record_class = get_record_class(repo_type)

    result = calculate_time_line(owner_name, repo_name, record_class, query_date, schema, bunch, mysql_session)
    return result


def save_time_line_to_db(config, time_line_result):
    key = {
        'owner': time_line_result['owner'],
        'repo': time_line_result['repo'],
        'query_date': time_line_result['query_date']
    }
    value = time_line_result

    statistic_dbms_config = config['datastore']['statistic_dbms']
    mongodb_client = MongoClient(statistic_dbms_config['host'], statistic_dbms_config['port'])
    mongodb_database = mongodb_client[statistic_dbms_config['database']]

    daily_repo_time_line_collection = mongodb_database.daily_repo_time_line_collection
    daily_repo_time_line_collection.replace_one(key, value, upsert=True)


def save_time_line_to_file(config, time_line_result, output_file_path):
    output_file_content = json.dumps(time_line_result, indent=2)
    with open(output_file_path, 'w') as output_file:
        output_file.write(output_file_content)


def time_line_processor(config, owner, repo, repo_type, query_date, output_file, save_to_db, print_flag):
    click.echo('owner name: {owner}'.format(owner=owner))
    click.echo('repo name: {repo}'.format(repo=repo))
    click.echo('query date: {query_date}'.format(query_date=query_date))

    result = process_time_line(config, owner, repo, repo_type, query_date)
    if result is None:
        return

    # print result
    if print_flag:
        click.echo(json.dumps(result, indent=4))

    # store data to MongoDB
    if save_to_db:
        click.echo("Save results to database")
        save_time_line_to_db(config, result)

    # output to file
    if output_file:
        click.echo("Write results to output file: {output_file_path}".format(output_file_path=output_file))
        save_time_line_to_file(config, result, output_file)
    return


@click.command()
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("--repo-type", type=click.Choice(["sms", "ecflow"]), help="repo type", required=True)
@click.option("-d", "--date", help="query date (%Y-%m-%d)", required=True)
@click.option("--output-file", help="output file path")
@click.option("--save-to-db", is_flag=True, default=False, help="save result to database")
@click.option("-p", "--print", "print_flag", is_flag=True, default=False, help="print result to console.")
@click.option("-c", "--config", "config_file_path", help="config file path", required=True)
def cli(owner, repo, repo_type, date, output_file, save_to_db, print_flag, config_file_path):
    """\
DESCRIPTION
    Calculate time line for owner/repo on query date according to config file."""

    start_time = datetime.datetime.now()

    query_date = datetime.datetime.strptime(date, "%Y-%m-%d")
    config = load_processor_config(config_file_path)

    time_line_processor(config, owner, repo, repo_type, query_date, output_file, save_to_db, print_flag)

    end_time = datetime.datetime.now()
    click.echo(end_time - start_time)
    return


if __name__ == "__main__":
    cli()
