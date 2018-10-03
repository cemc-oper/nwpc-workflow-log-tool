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
from pyspark import SparkContext, SparkConf

from nwpc_workflow_log_processor.spark.node_status_io import get_from_file, save_to_mongodb, save_to_kafka
from nwpc_workflow_log_processor.spark.node_status_calculator import calculate_node_status


def generate_node_status(config, owner, repo, begin_date, end_date, log_file):
    # 日期范围 [ start_date - 1, end_date ]，这是日志条目收集的范围
    query_date_list = []
    i = begin_date - datetime.timedelta(days=1)
    while i <= end_date:
        query_date_list.append(i.date())
        i = i + datetime.timedelta(days=1)

    conf = SparkConf().setAppName("Simple App").setMaster("local")
    spark = SparkContext(conf=conf)

    record_rdd = get_from_file(None, owner, repo, begin_date, end_date, log_file, spark)
    bunch_map, data_node_status_list = calculate_node_status(spark, record_rdd, owner, repo, begin_date, end_date)

    spark.stop()

    ##########
    # 存储结果
    ##########

    # 保存 bunch_map 和 data_node_status_list
    # save_to_kafka(user_name, repo_name, bunch_map, date_node_status_list, start_date, end_date)
    save_to_mongodb(owner, repo, bunch_map, data_node_status_list, begin_date, end_date)


@click.command()
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("--begin-date", help="begin date, YYYY-MM-DD, [begin_date, end_date)", required=True)
@click.option("--end-date", help="end date, YYYY-MM-DD, [begin_date, end_date)", required=True)
@click.option("--log-file", help="log file path", required=True)
@click.option("-c", "--config", help="config file path")
def cli(owner, repo, begin_date, end_date, log_file, config):
    """\
DESCRIPTION
    Calculate node status from file using Spark."""
    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    generate_node_status(config, owner, repo, begin_date, end_date, log_file)


if __name__ == "__main__":
    t1 = datetime.datetime.now()
    start_day = datetime.datetime(2015, 6, 1)
    end_day = datetime.datetime(2015, 6, 6)
    cli()
    t2 = datetime.datetime.now()
    print(t2-t1)
