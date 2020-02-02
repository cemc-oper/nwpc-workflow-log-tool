# coding=utf-8
import datetime
import json

import click
import yaml
from loguru import logger

from nwpc_workflow_log_processor.spark.engine.session import create_mysql_session, create_local_file_session
from nwpc_workflow_log_processor.spark.calculator.node_tree_calculator import calculate_node_tree
from nwpc_workflow_log_processor.spark.data_source.rmdb import load_records_from_rmdb
from nwpc_workflow_log_processor.spark.data_source.file import load_records_from_file
from nwpc_workflow_log_processor.common.util import load_config

from nwpc_workflow_model.visitor import pre_order_travel, SimplePrintVisitor


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
@click.option("--output-type", type=click.Choice(["print", "file"]), help="output type", default="print")
@click.option("--output-file", help="output file path", type=str, default=None)
def database(owner, repo, repo_type, begin_date, end_date, config_file, output_type, output_file):
    """Generate node tree from records in database.
    """
    if begin_date:
        begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    if end_date:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    config = load_config(config_file)

    bunch_map = generate_node_tree_from_database(
        config,
        owner=owner,
        repo=repo,
        begin_date=begin_date,
        end_date=end_date,
        repo_type=repo_type
    )

    _sink_result(
        output_type, bunch_map,
        output_file=output_file)


@cli.command('file')
@click.option("-c", "--config", "config_file", help="config file path", required=True)
@click.option("-l", "--log", "log_file", help="log file path", required=True)
@click.option("--repo-type", type=click.Choice(["ecflow", "sms"]), help="repo type", required=True)
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("--begin-date", help="begin date, YYYY-MM-DD, [begin_date, end_date)")
@click.option("--end-date", help="end date, YYYY-MM-DD, [begin_date, end_date)")
@click.option("--output-type", type=click.Choice(["print", "file"]), help="output type", default="print")
@click.option("--output-file", help="output file path", type=str, default=None)
def cli_file(
        config_file,
        log_file,
        repo_type,
        owner,
        repo,
        begin_date,
        end_date,
        output_type,
        output_file,
):
    """Generate node tree from log file.
    """
    if begin_date:
        begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    if end_date:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    if output_type == "file":
        if output_file is None or output_file == "":
            raise click.BadArgumentUsage("output_file must be set when output_type is file")

    config = load_config(config_file)

    bunch_map = generate_node_tree_from_file(
        config,
        log_file,
        owner=owner,
        repo=repo,
        begin_date=begin_date,
        end_date=end_date,
        repo_type=repo_type
    )

    _sink_result(
        output_type, bunch_map,
        output_file=output_file)


def _sink_result(output_type, bunch_map, **kwargs):
    if output_type == "print":
        for date, bunch in bunch_map.items():
            pre_order_travel(bunch, SimplePrintVisitor())
    elif output_type == "file":
        output_file = kwargs["output_file"]
        with open(output_file, "w") as f:
            result = {}
            for date, bunch in bunch_map.items():
                date_string = date.strftime("%Y-%m-%d")
                result[date_string] = bunch.to_dict()
            json.dump(result, f)


def generate_node_tree_from_database(
        config: dict,
        owner: str = None,
        repo: str = None,
        begin_date: datetime.datetime = None,
        end_date: datetime.datetime = None,
        repo_type: str = "ecflow"
) -> dict:
    run_start_time = datetime.datetime.now()

    spark = create_mysql_session(config)

    record_rdd = load_records_from_rmdb(
        config,
        spark,
        owner=owner,
        repo=repo,
        begin_date=begin_date,
        end_date=end_date,
        repo_type=repo_type,
    )

    bunch_map = calculate_node_tree(
        config,
        record_rdd,
        spark,
        repo_type=repo_type
    )

    spark.stop()

    run_end_time = datetime.datetime.now()
    logger.info(f"step finished, using: {run_end_time - run_start_time}")
    return bunch_map


def generate_node_tree_from_file(
        config: dict,
        log_file: str,
        owner: str = None,
        repo: str = None,
        begin_date: datetime.datetime = None,
        end_date: datetime.datetime = None,
        repo_type: str = "ecflow"
) -> dict:
    run_start_time = datetime.datetime.now()

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

    bunch_map = calculate_node_tree(
        config,
        record_rdd,
        spark,
        repo_type=repo_type
    )

    spark.stop()

    run_end_time = datetime.datetime.now()
    logger.info(f"step finished, using: {run_end_time - run_start_time}")
    return bunch_map


if __name__ == "__main__":
    cli()
