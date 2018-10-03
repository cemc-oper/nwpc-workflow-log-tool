import datetime

import yaml

from nwpc_log_model import Record


def get_from_mysql(config: dict, owner: str, repo: str, begin_date, end_date, spark):
    Record.prepare(owner, repo)
    table_name = Record.__table__.name

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
    )).option("user", "windroc").option("password", "shenyang").load()

    df.registerTempTable("record")

    sql = ("SELECT repo_id, version_id, line_no, record_date, record_time, record_string "
           "FROM record "
           "WHERE record_date>='{start_date}' AND record_date<='{end_date}' ").format(
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
        record = Record()
        record.version_id = row[0]
        record.line_no = row[1]
        record.parse(row[5])
        return record

    record_rdd = log_data.rdd.map(parse_sms_log)

    return record_rdd
