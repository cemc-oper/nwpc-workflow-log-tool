# coding: utf-8
import datetime

from pyspark.sql import SparkSession
from pyspark import RDD

from nwpc_workflow_log_processor.common.util import get_record_class


def load_records_from_rmdb(
        config: dict,
        spark: SparkSession,
        owner: str = None,
        repo: str = None,
        begin_date: datetime.datetime = None,
        end_date: datetime.datetime = None,
        repo_type: str = "ecflow",
) -> RDD:
    record_class = get_record_class(repo_type)

    record_class.prepare(owner, repo)
    table_name = record_class.__table__.name

    spark_config = config['engine']['spark']
    mysql_config = config['datastore']['mysql']

    query_datetime = begin_date
    query_date = begin_date.date()

    # date range [begin_date - 1, end_date]
    #   This is the date range for log records to be collected
    query_start_date = begin_date - datetime.timedelta(days=1)
    query_end_date = end_date

    df = spark.read.format('jdbc').option(
        "url", "jdbc:mysql://{host}:{port}/{db}?serverTimezone=GMT%2B8".format(
            host=mysql_config['host'],
            port=mysql_config['port'],
            db=mysql_config['database'])
    ).option(
        "dbtable", f"`{mysql_config['database']}`.`{table_name}`"
    ).option(
        "user", mysql_config['user']
    ).option(
        "password", mysql_config['password']
    ).load()

    df.registerTempTable("record")

    sql = ("SELECT repo_id, version_id, line_no, date, time, log_record "
           "FROM record "
           "WHERE date>='{start_date}' AND date<='{end_date}' ").format(
        start_date=query_start_date.strftime("%Y-%m-%d"),
        end_date=query_end_date.strftime("%Y-%m-%d")
    )

    log_data = spark.sql(sql)

    # test
    # records = log_data.collect()
    # for i in records[0:10]:
    #     print(i)
    # return

    ##############
    # parse log records. Generate Record object.
    ##############
    # record row => record object
    def parse_log_line(row):
        record = record_class()
        record.version_id = row[0]
        record.line_no = row[1]
        record.parse(row[5])
        return record

    record_rdd = log_data.rdd.map(parse_log_line)

    return record_rdd
