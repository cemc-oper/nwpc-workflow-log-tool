# coding: utf-8
import datetime

from nwpc_workflow_log_model.rmdb.sms.record import SmsRecord
from nwpc_workflow_log_model.rmdb.ecflow.record import EcflowRecord


def get_from_mysql(config: dict, owner: str, repo: str, repo_type: str, begin_date, end_date, spark):
    if repo_type == "sms":
        record_class = SmsRecord
    elif repo_type == "ecflow":
        record_class = EcflowRecord
    else:
        raise ValueError("repo type is not supported: " + repo_type)

    record_class.prepare(owner, repo)
    table_name = record_class.__table__.name

    spark_config = config['engine']['spark']
    mysql_config = config['datastore']['mysql']

    query_datetime = begin_date
    query_date = begin_date.date()

    # date range [ start_date - 1, end_date ]，这是日志条目收集的范围
    query_date_list = []
    i = begin_date - datetime.timedelta(days=1)
    while i <= end_date:
        query_date_list.append(i.date())
        i = i + datetime.timedelta(days=1)

    df = spark.read.format('jdbc').option(
        "url", "jdbc:mysql://{host}:{port}/{db}?serverTimezone=GMT%2B8".format(
            host=mysql_config['host'],
            port=mysql_config['port'],
            db=mysql_config['database'])
    ).option("dbtable", "`{db}`.`{table_name}`".format(
        db=mysql_config['database'],
        table_name=table_name
    )).option("user", mysql_config['user']).option("password", mysql_config['password']).load()

    df.registerTempTable("record")

    sql = ("SELECT repo_id, version_id, line_no, date, time, log_record "
           "FROM record "
           "WHERE date>='{start_date}' AND date<='{end_date}' ").format(
        start_date=query_date_list[0].strftime("%Y-%m-%d"),
        end_date=query_date_list[-1].strftime("%Y-%m-%d")
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
    def parse_sms_log(row):
        record = record_class()
        record.version_id = row[0]
        record.line_no = row[1]
        record.parse(row[5])
        return record

    record_rdd = log_data.rdd.map(parse_sms_log)

    return record_rdd
