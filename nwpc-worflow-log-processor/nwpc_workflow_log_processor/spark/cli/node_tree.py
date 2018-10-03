# coding=utf-8
import datetime

import click
import yaml
import findspark
from pyspark.sql import SparkSession
from pyspark import SparkConf


from nwpc_workflow_log_processor.spark.node_tree_calculator import calculate_node_tree
from nwpc_workflow_log_processor.spark.io.rmdb import get_from_mysql
from nwpc_work_flow_model.sms.visitor import SimplePrintVisitor, pre_order_travel


def load_config(config_file):
    with open(config_file) as f:
        config = yaml.load(f)
        return config


def generate_node_tree(config, owner, repo, begin_date, end_date):
    findspark.init(config['engine']['spark']['base'])

    # conf = SparkConf().setAppName("sms.spark.nwpc-workflow-log-processor").setMaster("local")
    spark = SparkSession \
        .builder \
        .appName("sms.spark.nwpc-workflow-log-processor") \
        .config("spark.driver.extraClassPath", config['datastore']['mysql']['driver']) \
        .config("spark.executor.extraClassPath", config['datastore']['mysql']['driver']) \
        .config("spark.executor.memory", '4g') \
        .config("spark.driver.memory", '4g') \
        .getOrCreate()
    # sc = SparkContext(conf=conf)

    record_rdd = get_from_mysql(config, owner, repo, begin_date, end_date, spark)

    bunch_map = calculate_node_tree(None, record_rdd, spark)

    for date, bunch in bunch_map.items():
        pre_order_travel(bunch, SimplePrintVisitor())


@click.command()
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("--begin-date", help="begin date, YYYY-MM-DD, [begin_date, end_date)")
@click.option("--end-date", help="end date, YYYY-MM-DD, [begin_date, end_date)")
@click.option("-c", "--config", "config_file", help="config file path", required=True)
def cli(owner, repo, begin_date, end_date, config_file):
    """\
DESCRIPTION
    Generate node tree by Spark from SMS log file"""
    if begin_date:
        begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    if end_date:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    config = load_config(config_file)

    generate_node_tree(config, owner, repo, begin_date, end_date)


if __name__ == "__main__":
    cli()
