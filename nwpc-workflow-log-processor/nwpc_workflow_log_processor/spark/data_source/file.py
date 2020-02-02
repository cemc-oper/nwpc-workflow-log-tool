# coding: utf-8
import datetime

from pyspark.sql import SparkSession
from pyspark import RDD

from nwpc_workflow_log_processor.common.util import get_record_class


def load_records_from_file(
        config: dict,
        log_file: str,
        spark: SparkSession,
        owner: str = "default_owner",
        repo: str = "default_repo",
        begin_date: datetime.datetime = None,
        end_date: datetime.datetime = None,
        repo_type: str = "ecflow"
) -> RDD:
    """Load records RDD from workflow's log file.

    Records are parsed using ``nwpc_workflow_model`` package,
    and filtered by date range [begin_date, end_date) if not None.
    Support ecFlow and SMS. Currently we only use ecFlow.

    Parameters
    ----------
    config: dict
        config dict of processor.
    log_file: str
        log file path
    spark: SparkSession
        SparkSession of app.
    owner: str, optional
        owner name, used by RMDBs
    repo: str, optional
        repo name, used by RMDBs
    begin_date: datetime.datetime, optional
        begin date, [begin_date, end_date)
    end_date: datetime.datetime, optional
        end date, [begin_date, end_date)
    repo_type: {"ecflow", "sms"}
        repo type. Currently we are focusing on ecFlow.

    Returns
    -------
    RDD
        RDD of log records
    """
    record_class = get_record_class(repo_type)

    record_class.prepare(owner, repo)

    log_data = spark.read.text(log_file).rdd

    # record line => record object
    def parse_log_line(line):
        record = record_class()
        record.parse(line.value)
        return record

    log_data = log_data.map(parse_log_line)

    # record object => record object
    #   date range: [begin_date - 1, end_date]
    #   This is the date range for log records to be collected
    if begin_date is not None:
        start_date = begin_date - datetime.timedelta(days=1)

    def filter_node(record):
        if begin_date is not None:
            if record.date < start_date.date():
                return False

        if end_date is not None:
            if record.date >= end_date.date():
                return False

        return True

    record_rdd = log_data.filter(filter_node)

    record_class.init()

    return record_rdd
