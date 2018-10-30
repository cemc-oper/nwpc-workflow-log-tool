#!/usr/bin/env python
# coding=utf-8
"""
Get record list for each node in some day.

Before running this file, we should update sms_log.zip in project root directory:
    cd {PROJECT_ROOT}
    zip -r sms_log.zip sms_log
And run this script using Spark like this:
    $SPARK_HOME/bin/spark-submit --master local[4] --py-files /vagrant/nwpc_work_flow_model.zip spark_node_status.py
"""
import datetime

import click
from pyspark.sql import SparkSession

from nwpc_workflow_log_processor.spark.data_source.hive import get_from_hive
from nwpc_workflow_log_processor.spark.calculator.node_status_calculator import calculate_node_status


def generate_node_status(config, owner, repo, begin_date, end_date):
    spark = SparkSession \
        .builder \
        .appName("NWPC Log Processor") \
        .enableHiveSupport() \
        .getOrCreate()

    record_rdd = get_from_hive(config, owner, repo, begin_date, end_date, spark)

    bunch_map, data_node_status_list = calculate_node_status(spark, record_rdd, owner, repo, begin_date, end_date)

    ##########
    # 存储结果
    ##########

    # 保存 bunch_map 和 data_node_status_list
    # save_to_kafka(user_name, repo_name, bunch_map, date_node_status_list, start_date, end_date)
    # save_to_mongodb(user_name, repo_name, bunch_map, date_node_status_list, start_date, end_date)

    spark.stop()


@click.command()
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("--begin-date", help="begin date, YYYY-MM-DD", required=True)
@click.option("--end-date", help="end date, YYYY-MM-DD", required=True)
@click.option("-c", "--config", help="config file path")
def node_status_tool(owner, repo, begin_date, end_date, config):
    """\
DESCRIPTION
    Calculate node status using Spark."""

    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")

    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    generate_node_status(config, owner, repo, begin_date, end_date)


if __name__ == "__main__":
    t1 = datetime.datetime.now()
    node_status_tool()
    t2 = datetime.datetime.now()
    print(t2-t1)
