# coding=utf-8
import datetime

import click
import yaml
from loguru import logger

from nwpc_workflow_log_processor.spark.engine.session import create_mysql_session, create_local_file_session
from nwpc_workflow_log_processor.spark.calculator.node_tree_calculator import calculate_node_tree
from nwpc_workflow_log_processor.spark.data_source.rmdb import get_from_mysql
from nwpc_workflow_log_processor.spark.data_source.file import get_from_file

from nwpc_workflow_model.visitor import pre_order_travel, SimplePrintVisitor


def load_config(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)
        return config


def generate_node_tree_from_database(
        config: dict,
        owner: str,
        repo: str,
        begin_date: datetime.datetime,
        end_date: datetime.datetime,
        repo_type: str = "ecflow"
):
    run_start_time = datetime.datetime.now()

    spark = create_mysql_session(config)
    spark.sparkContext.setLogLevel('INFO')

    record_rdd = get_from_mysql(config, owner, repo, repo_type, begin_date, end_date, spark)

    bunch_map = calculate_node_tree(config, repo_type, record_rdd, spark)

    run_end_time = datetime.datetime.now()
    logger.info(f"step finished, using: {run_end_time - run_start_time}")

    spark.stop()

    for date, bunch in bunch_map.items():
        pre_order_travel(bunch, SimplePrintVisitor())


def generate_node_tree_from_file(
        config: dict,
        log_file: str,
        owner: str,
        repo: str,
        begin_date: datetime.datetime,
        end_date: datetime.datetime,
        repo_type: str = "ecflow"
):
    run_start_time = datetime.datetime.now()

    spark = create_local_file_session(config)
    spark.sparkContext.setLogLevel("INFO")

    record_rdd = get_from_file(
        config,
        log_file,
        spark=spark,
        owner=owner,
        repo=repo,
        begin_date=begin_date,
        end_date=end_date,
        repo_type=repo_type
    )

    bunch_map = calculate_node_tree(config, repo_type, record_rdd, spark)

    run_end_time = datetime.datetime.now()
    logger.info(f"step finished, using: {run_end_time - run_start_time}")

    spark.stop()

    for date, bunch in bunch_map.items():
        pre_order_travel(bunch, SimplePrintVisitor())


@click.group()
def cli():
    pass


@cli.command('database')
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("--repo-type", type=click.Choice(["ecflow", "sms"]), help="repo type", required=True)
@click.option("--begin-date", help="begin date, YYYY-MM-DD, [begin_date, end_date)")
@click.option("--end-date", help="end date, YYYY-MM-DD, [begin_date, end_date)")
@click.option("-c", "--config", "config_file", help="config file path", required=True)
def database(owner, repo, repo_type, begin_date, end_date, config_file):
    if begin_date:
        begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    if end_date:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    config = load_config(config_file)

    generate_node_tree_from_database(
        config,
        owner, repo,
        begin_date, end_date,
        repo_type=repo_type
    )


@cli.command('file')
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("--repo-type", type=click.Choice(["ecflow", "sms"]), help="repo type", required=True)
@click.option("--begin-date", help="begin date, YYYY-MM-DD, [begin_date, end_date)")
@click.option("--end-date", help="end date, YYYY-MM-DD, [begin_date, end_date)")
@click.option("-l", "--log", "log_file", help="log file path", required=True)
@click.option("-c", "--config", "config_file", help="config file path", required=True)
def cli_file(owner, repo, repo_type, begin_date, end_date, log_file, config_file):
    if begin_date:
        begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    if end_date:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    config = load_config(config_file)

    generate_node_tree_from_file(
        config,
        log_file,
        owner, repo,
        begin_date, end_date,
        repo_type=repo_type
    )


if __name__ == "__main__":
    cli()
