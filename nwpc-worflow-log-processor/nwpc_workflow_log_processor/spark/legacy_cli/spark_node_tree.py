# coding=utf-8
import datetime

import click
from pyspark import SparkContext, SparkConf


from nwpc_workflow_log_processor.spark.node_tree_calculator import calculate_node_tree
from nwpc_workflow_log_processor.spark.io.file import get_from_file
from nwpc_work_flow_model.sms.visitor import SimplePrintVisitor, pre_order_travel


def generate_node_tree(config, owner, repo, begin_date, end_date, log_file):

    conf = SparkConf().setAppName("Simple App").setMaster("local")
    sc = SparkContext(conf=conf)

    record_rdd = get_from_file(config, owner, repo, begin_date, end_date, log_file, sc)

    bunch_map = calculate_node_tree(None, record_rdd, sc)

    for date, bunch in bunch_map.items():
        pre_order_travel(bunch, SimplePrintVisitor())


@click.command()
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("--begin-date", help="begin date, YYYY-MM-DD, [begin_date, end_date)")
@click.option("--end-date", help="end date, YYYY-MM-DD, [begin_date, end_date)")
@click.option("--log-file", help="log file path", required=True)
@click.option("-c", "--config", help="config file path")
def cli(owner, repo, begin_date, end_date, log_file, config):
    """\
DESCRIPTION
    Generate node tree by Spark from SMS log file"""
    if begin_date:
        begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    if end_date:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    generate_node_tree(config, owner, repo, begin_date, end_date, log_file)


if __name__ == "__main__":
    cli()
