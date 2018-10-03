# coding=utf-8
"""
根据日志条目计算指定时间段内的节点状态，并保存到数据库中。

日志数据来自MySQL。

Before running this file, we should update sms_log.zip in project root directory:
    cd {PROJECT_ROOT}
    zip -r sms_log.zip sms_log
And run this script using Spark like this:
    $SPARK_HOME/bin/spark-submit --master local[4] --py-files /vagrant/nwpc_work_flow_model.zip spark_node_status.py
"""
import datetime

import click
import yaml
from pyspark.sql import SparkSession

from nwpc_workflow_log_processor.spark.node_status_io import get_from_mysql, save_to_mongodb, save_to_kafka
from nwpc_workflow_log_processor.spark.node_status_calculator import calculate_node_status


def generate_node_status(config, owner, repo, begin_date, end_date):
    """
    get node status in some date range，such as [begin_date, end_date).
    :param config: config file path
    :param owner: user name
    :param repo: repo name
    :param begin_date: datetime
    :param end_date: datetime
    :return:
    """

    # spark start
    spark = SparkSession \
        .builder \
        .appName("sms.spark.nwpc-workflow-log-processor") \
        .getOrCreate()

    record_rdd = get_from_mysql(config, owner, repo, begin_date, end_date, spark)

    bunch_map, data_node_status_list = calculate_node_status(spark, record_rdd, owner, repo, begin_date, end_date)

    spark.stop()

    return


@click.command()
@click.option("-o", "--owner", help="owner name", required=True)
@click.option("-r", "--repo", help="repo name", required=True)
@click.option("--begin-date", help="begin date, YYYY-MM-DD, [begin_date, end_date)", required=True)
@click.option("--end-date", help="end date, YYYY-MM-DD, [begin_date, end_date)", required=True)
@click.option("-c", "--config", help="config file path")
def cli(owner, repo, begin_date, end_date, config):
    """\
DESCRIPTION
    Calculate node status using Spark."""
    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    generate_node_status(config, owner, repo, begin_date, end_date)


if __name__ == "__main__":
    t1 = datetime.datetime.now()
    start_day = datetime.datetime(2015, 6, 1)
    end_day = datetime.datetime(2015, 6, 6)
    cli()
    t2 = datetime.datetime.now()
    print(t2-t1)
