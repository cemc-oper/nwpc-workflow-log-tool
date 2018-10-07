# coding=utf-8
import datetime

import click
import yaml
import findspark
from pyspark.sql import SparkSession


from nwpc_workflow_log_processor.spark.node_tree_calculator import calculate_node_tree
from nwpc_workflow_log_processor.spark.data_source.rmdb import get_from_mysql
from nwpc_workflow_log_processor.spark.data_source.file import get_from_file
from nwpc_work_flow_model.sms.visitor import SimplePrintVisitor, pre_order_travel


def load_config(config_file):
    with open(config_file) as f:
        config = yaml.load(f)
        return config


def create_mysql_session(config):
    findspark.init(config['engine']['spark']['base'])
    spark = SparkSession \
        .builder \
        .appName("sms.spark.nwpc-workflow-log-processor") \
        .master("local[4]") \
        .config("spark.driver.extraClassPath", config['datastore']['mysql']['driver']) \
        .config("spark.executor.extraClassPath", config['datastore']['mysql']['driver']) \
        .config("spark.executor.memory", '4g') \
        .config("spark.driver.memory", '4g') \
        .getOrCreate()
    return spark


def create_session(config):
    findspark.init(config['engine']['spark']['base'])
    spark = SparkSession \
        .builder \
        .appName("sms.spark.nwpc-workflow-log-processor") \
        .master("local[4]") \
        .config("spark.executor.memory", '4g') \
        .getOrCreate()
    return spark


def generate_node_tree_from_rmdb(config, owner, repo, begin_date, end_date):
    t1 = datetime.datetime.now()

    spark = create_mysql_session(config)
    spark.sparkContext.setLogLevel('INFO')

    record_rdd = get_from_mysql(config, owner, repo, begin_date, end_date, spark)

    bunch_map = calculate_node_tree(config, record_rdd, spark)

    t2 = datetime.datetime.now()
    print(t2 - t1)

    spark.stop()

    # for date, bunch in bunch_map.items():
    #     pre_order_travel(bunch, SimplePrintVisitor())


def generate_node_tree_from_file(config, owner, repo, begin_date, end_date, log_file):
    t1 = datetime.datetime.now()

    spark = create_mysql_session(config)
    spark.sparkContext.setLogLevel('INFO')

    record_rdd = get_from_file(config, owner, repo, begin_date, end_date, log_file, spark)

    bunch_map = calculate_node_tree(None, record_rdd, spark)

    t2 = datetime.datetime.now()
    print(t2 - t1)

    spark.stop()

    # for date, bunch in bunch_map.items():
    #     pre_order_travel(bunch, SimplePrintVisitor())


@click.group()
def cli():
    pass


@cli.command('rmdb')
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("--begin-date", help="begin date, YYYY-MM-DD, [begin_date, end_date)")
@click.option("--end-date", help="end date, YYYY-MM-DD, [begin_date, end_date)")
@click.option("-c", "--config", "config_file", help="config file path", required=True)
def cli_rmdb(owner, repo, begin_date, end_date, config_file):
    if begin_date:
        begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    if end_date:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    config = load_config(config_file)

    generate_node_tree_from_rmdb(config, owner, repo, begin_date, end_date)


@cli.command('file')
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("--begin-date", help="begin date, YYYY-MM-DD, [begin_date, end_date)")
@click.option("--end-date", help="end date, YYYY-MM-DD, [begin_date, end_date)")
@click.option("-l", "--log", "log_file", help="log file path", required=True)
@click.option("-c", "--config", "config_file", help="config file path", required=True)
def cli_file(owner, repo, begin_date, end_date, log_file, config_file):
    if begin_date:
        begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    if end_date:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    config = load_config(config_file)

    generate_node_tree_from_file(config, owner, repo, begin_date, end_date, log_file)


if __name__ == "__main__":
    cli()
