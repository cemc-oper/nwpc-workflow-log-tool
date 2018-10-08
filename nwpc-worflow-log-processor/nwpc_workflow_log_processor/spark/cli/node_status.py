# coding=utf-8
"""
Get record list for each node in some day.
"""
import datetime

import click
import yaml

from nwpc_workflow_log_processor.spark.engine.session import create_mysql_session, create_local_file_session
from nwpc_workflow_log_processor.spark.data_source.file import get_from_file
from nwpc_workflow_log_processor.spark.data_source.rmdb import get_from_mysql
from nwpc_workflow_log_processor.spark.node_status_calculator import calculate_node_status
# from nwpc_workflow_log_processor.spark.data_store.kafka import save_to_kafka
from nwpc_workflow_log_processor.spark.data_store.mongodb import save_to_mongodb


def load_config(config_file):
    with open(config_file) as f:
        config = yaml.load(f)
        return config


def generate_node_status(config, owner, repo, begin_date, end_date, log_file):
    spark = create_local_file_session(config)
    spark.sparkContext.setLogLevel('INFO')

    record_rdd = get_from_file(config, owner, repo, begin_date, end_date, log_file, spark)

    bunch_map, data_node_status_list = calculate_node_status(owner, repo, begin_date, end_date, record_rdd, spark)

    spark.stop()

    # 保存 bunch_map 和 data_node_status_list
    save_to_mongodb(config, owner, repo, bunch_map, data_node_status_list, begin_date, end_date)
    # save_to_kafka(user_name, repo_name, bunch_map, date_node_status_list, start_date, end_date)


def generate_node_status_from_database(config, owner, repo, begin_date, end_date):
    spark = create_mysql_session(config)
    spark.sparkContext.setLogLevel('INFO')

    record_rdd = get_from_mysql(config, owner, repo, begin_date, end_date, spark)

    bunch_map, data_node_status_list = calculate_node_status(owner, repo, begin_date, end_date, record_rdd, spark)

    spark.stop()

    # 保存 bunch_map 和 data_node_status_list
    save_to_mongodb(config, owner, repo, bunch_map, data_node_status_list, begin_date, end_date)
    # save_to_kafka(user_name, repo_name, bunch_map, date_node_status_list, start_date, end_date)


@click.group()
def cli():
    pass


@cli.command('file')
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("--begin-date", help="begin date, YYYY-MM-DD, [begin_date, end_date)", required=True)
@click.option("--end-date", help="end date, YYYY-MM-DD, [begin_date, end_date)", required=True)
@click.option("-l", "--log", "log_file", help="log file path", required=True)
@click.option("-c", "--config", "config_file", help="config file path")
def local_file(owner, repo, begin_date, end_date, log_file, config_file):
    """\
DESCRIPTION
    Calculate node status from file using Spark."""

    config = load_config(config_file)
    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    generate_node_status(config, owner, repo, begin_date, end_date, log_file)


@cli.command('database')
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("--begin-date", help="begin date, YYYY-MM-DD, [begin_date, end_date)", required=True)
@click.option("--end-date", help="end date, YYYY-MM-DD, [begin_date, end_date)", required=True)
@click.option("-c", "--config", "config_file", help="config file path")
def database(owner, repo, begin_date, end_date, config_file):
    """\
DESCRIPTION
    Calculate node status from database using Spark."""

    config = load_config(config_file)
    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    generate_node_status_from_database(config, owner, repo, begin_date, end_date)


if __name__ == "__main__":
    cli()
