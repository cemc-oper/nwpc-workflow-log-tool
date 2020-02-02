# coding=utf-8
"""
Get record list for each node in date range.
"""
import datetime

import click

from nwpc_workflow_log_processor.common.util import load_config
from nwpc_workflow_log_processor.spark.calculator.node_status_calculator import calculate_node_status
from nwpc_workflow_log_processor.spark.data_source.file import load_records_from_file
from nwpc_workflow_log_processor.spark.data_source.rmdb import load_records_from_rmdb
# from nwpc_workflow_log_processor.spark.data_store.kafka import save_to_kafka
from nwpc_workflow_log_processor.spark.data_store.mongodb import save_to_mongodb
from nwpc_workflow_log_processor.spark.engine.session import create_mysql_session, create_local_file_session


@click.group()
def cli():
    pass


@cli.command('file')
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("--repo-type", type=click.Choice(["sms", "ecflow"]), help="repo type", required=True)
@click.option("--begin-date", help="begin date, YYYY-MM-DD, [begin_date, end_date)", required=True)
@click.option("--end-date", help="end date, YYYY-MM-DD, [begin_date, end_date)", required=True)
@click.option("-l", "--log", "log_file", help="log file path", required=True)
@click.option("-c", "--config", "config_file", help="config file path")
def local_file(owner, repo, repo_type, begin_date, end_date, log_file, config_file):
    """\
DESCRIPTION
    Calculate node status from file using Spark."""

    config = load_config(config_file)

    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    generate_node_status(
        config,
        log_file,
        owner=owner,
        repo=repo,
        begin_date=begin_date,
        end_date=end_date,
        repo_type=repo_type,
    )


@cli.command('database')
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("--repo-type", type=click.Choice(["sms", "ecflow"]), help="repo type", required=True)
@click.option("--begin-date", help="begin date, YYYY-MM-DD, [begin_date, end_date)", required=True)
@click.option("--end-date", help="end date, YYYY-MM-DD, [begin_date, end_date)", required=True)
@click.option("-c", "--config", "config_file", help="config file path")
def database(owner, repo, repo_type, begin_date, end_date, config_file):
    """\
DESCRIPTION
    Calculate node status from database using Spark."""

    config = load_config(config_file)

    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    generate_node_status_from_database(
        config,
        owner=owner,
        repo=repo,
        begin_date=begin_date,
        end_date=end_date,
        repo_type=repo_type,
    )


def generate_node_status(
        config: dict,
        log_file: str,
        owner: str = None,
        repo: str = None,
        begin_date: datetime.datetime = None,
        end_date: datetime.datetime = None,
        repo_type: str = "ecflow"
):
    """Generate node status from local file.

    Parameters
    ----------
    config: dict
        config object.
    log_file: str
        local log file path
    owner: str, optional
        owner name
    repo: str, optional
        repo name
    begin_date: datetime.datetime, optional
        begin date, [begin_date, end_date)
    end_date: datetime.datetime, optional
        end date, [begin_date, end_date)
    repo_type: ["ecflow", "sms"], optional
        workflow type, ecflow or sms, currently we are focusing on ecFlow.
    """
    spark = create_local_file_session(config)

    record_rdd = load_records_from_file(
        config,
        log_file,
        spark=spark,
        owner=owner,
        repo=repo,
        begin_date=begin_date,
        end_date=end_date,
        repo_type=repo_type
    )

    bunch_map, data_node_status_list = calculate_node_status(
        owner, repo, repo_type, begin_date, end_date, record_rdd, spark)

    spark.stop()

    # 保存 bunch_map 和 data_node_status_list
    save_to_mongodb(config, owner, repo, bunch_map, data_node_status_list, begin_date, end_date)
    # save_to_kafka(user_name, repo_name, bunch_map, date_node_status_list, start_date, end_date)


def generate_node_status_from_database(
        config: dict,
        owner: str = None,
        repo: str = None,
        begin_date: datetime.datetime = None,
        end_date: datetime.datetime = None,
        repo_type: str = "ecflow"
):
    spark = create_mysql_session(config)

    record_rdd = load_records_from_rmdb(
        config,
        spark,
        owner=owner,
        repo=repo,
        begin_date=begin_date,
        end_date=end_date,
        repo_type=repo_type
    )

    bunch_map, data_node_status_list = calculate_node_status(
        owner, repo, repo_type, begin_date, end_date, record_rdd, spark)

    spark.stop()

    # 保存 bunch_map 和 data_node_status_list
    save_to_mongodb(config, owner, repo, bunch_map, data_node_status_list, begin_date, end_date)
    # save_to_kafka(user_name, repo_name, bunch_map, date_node_status_list, start_date, end_date)


if __name__ == "__main__":
    cli()
