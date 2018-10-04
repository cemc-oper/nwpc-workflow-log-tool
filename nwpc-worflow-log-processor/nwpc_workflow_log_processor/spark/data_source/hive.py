import datetime

from nwpc_log_model import Record


def get_from_hive(config, owner, repo, begin_date, end_date, spark):
    Record.prepare(owner, repo)
    table_name = Record.__table__.name
    hive_table_name = table_name.replace('.', '_')

    """
    获取某段日期内的节点状态，[start_date, end_date)
    """

    query_datetime = begin_date
    query_date = begin_date.date()

    # 日期范围 [ start_date - 1, end_date ]，这是日志条目收集的范围
    query_date_list = []
    i = begin_date - datetime.timedelta(days=1)
    while i <= end_date:
        query_date_list.append(i.date())
        i = i + datetime.timedelta(days=1)

    #################
    # 从Hive中获取日志条目
    #################

    # We need to fetch version_id.
    # DESCRIBE record_nwp_cma20n03:
    # repo_id               int
    # version_id          	int
    # line_no             	int
    # record_date         	string
    # record_time         	string
    # record_string       	string
    sql = ("FROM {hive_table_name} SELECT version_id, line_no, record_string "
           "WHERE record_date >='{start_date}' AND record_date<='{end_date}'").format(
        hive_table_name=hive_table_name,
        start_date=query_date_list[0].strftime("%Y-%m-%d"),
        end_date=query_date_list[-1].strftime("%Y-%m-%d"))

    log_data = spark.sql(sql)
    log_data = log_data.distinct()

    # log_file = "/vagrant_data/sample-data/sample.sms.log"
    # log_file = "/vagrant_data/sample-data/nwp_cma20n03.sms.log"
    # log_data = sc.textFile(log_file).cache()

    ##############
    # 解析日志文本，生成Record对象
    ##############
    # record row => record object

    def parse_sms_log(row):
        record = Record()
        record.version_id = row[0]
        record.line_no = row[1]
        record.parse(row[2])
        return record

    record_rdd = log_data.rdd.map(parse_sms_log)

    return record_rdd