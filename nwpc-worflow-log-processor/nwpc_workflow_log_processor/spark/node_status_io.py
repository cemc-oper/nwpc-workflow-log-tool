# coding=utf-8
import json
import datetime
import logging

# from kafka import KafkaClient, SimpleProducer
from pymongo import MongoClient

from nwpc_log_model import Record

# mongodb
MONGODB_HOST = '192.168.99.100'
MONGODB_PORT = 32808

# kafka
KAFKA_HOST = '10.28.32.175'
KAFKA_PORT = '59092'


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


def get_from_file(config, owner, repo, begin_date, end_date, log_file, sc):
    Record.prepare(owner, repo)

    log_data = sc.textFile(log_file)

    # record line => record object
    def parse_sms_log(line):
        record = Record()
        record.parse(line)
        return record

    log_data = log_data.map(parse_sms_log)

    # record object => record object
    # 日期范围 [ begin_date - 1, end_date ]，这是日志条目收集的范围
    if begin_date is not None:
        start_date = begin_date - datetime.timedelta(days=1)

    def filter_node(record):
        if begin_date is not None:
            if record.record_date < start_date.date():
                return False

        if end_date is not None:
            if record.record_date >= end_date.date():
                return False

        return True

    record_rdd = log_data.filter(filter_node)

    Record.init()

    return record_rdd


def json_default(obj):
    if isinstance(obj, datetime.datetime):
        return obj.strftime('%Y-%m-%dT%H:%M:%S')  # obj.strftime('%Y-%m-%dT%H:%M:%S')
    elif isinstance(obj, datetime.date):
        return obj.strftime('%Y-%m-%d')
    elif isinstance(obj, datetime.time):
        return obj.strftime('%H:%M:%S')
    elif isinstance(obj, datetime.timedelta):
        return {'day': obj.days, 'seconds': obj.seconds}
    else:
        raise TypeError('%r is not JSON serializable' % obj)


def save_to_mongodb(user_name, repo_name, bunch_map, date_node_status_list, start_date, end_date):
    """
    数据流程：Spark Driver => MongoDB
    :param user_name:
    :param repo_name:
    :param bunch_map:
    :param date_node_status_list:
    :return:
    """
    update_type = "insert"  # "update" or "insert"

    mongodb_client = MongoClient(MONGODB_HOST, MONGODB_PORT)
    smslog_mongodb = mongodb_client.smslog
    daily_tree_status_collection = smslog_mongodb.daily_tree_status_collection
    daily_node_status_collection = smslog_mongodb.daily_node_status_collection

    # saving results to mongodb
    if update_type == "insert":
        # delete previous data
        daily_tree_status_collection.remove({
            'owner': user_name,
            'repo': repo_name,
            'date': {'$gte': start_date - datetime.timedelta(days=1), '$lte': end_date}
        })

    total_count = len(bunch_map)
    logging.info("Adding {total_count} tree status to mongodb".format(total_count=total_count))
    cur_count = 0
    cur_percent = 0
    for cur_query_date in bunch_map:
        print(cur_query_date)
        cur_count += 1
        percent = int(cur_count * 100.0 / total_count)
        if percent > cur_percent:
            print("[{percent}%]".format(percent=percent))
            cur_percent = percent
        cur_query_datetime = datetime.datetime.combine(cur_query_date, datetime.time())
        cur_bunch = bunch_map[cur_query_date]
        tree_status_key = {
            'owner': user_name,
            'repo': repo_name,
            'date': cur_query_datetime
        }
        tree_status = {
            'owner': user_name,
            'repo': repo_name,
            'date': cur_query_datetime,
            'tree': cur_bunch.to_dict()
        }
        print(tree_status['owner'], tree_status['repo'], tree_status['date'])
        if update_type == "upsert":
            daily_tree_status_collection.update(tree_status_key, tree_status, upsert=True)
        else:
            daily_tree_status_collection.insert(tree_status)

    total_count = len(date_node_status_list)
    logging.info("Add {total_count} node status to mongodb...".format(total_count=total_count))

    if update_type == "insert":
        # delete previous data
        daily_node_status_collection.remove({
            'owner': user_name,
            'repo': repo_name,
            'date': {'$gte': start_date, '$lt': end_date}
        })

    cur_count = 0
    cur_percent = 0
    for status in date_node_status_list:
        cur_count += 1
        percent = int(cur_count * 100.0 / total_count)
        if percent > cur_percent:
            print("[{percent}%]".format(percent=percent))
            cur_percent = percent
        if status is None:
            continue
        a_node_status_key, a_node_status = status
        if a_node_status['type'] == 'family' or a_node_status['type'] == 'task':
            if update_type == "upsert":
                daily_node_status_collection.update(a_node_status_key, a_node_status, upsert=True)
            else:
                daily_node_status_collection.insert(a_node_status)
    logging.info("Saving to mongodb is done.")


def save_to_kafka(user_name, repo_name, bunch_map, date_node_status_list, start_date, end_date):
    """
    将 bunch_map 和 data_node_status_list 发送到 Kafka 中，由 Kafka 消费者存储到 MongoDB 中，加快 spark 程序运行速度，
    但这样似乎会增加存储到 mongodb 的时间（可能因为需要从网络中接收数据，并需要多次序列化和反序列化）

    数据流程：Spark Driver => Kafka => Consumer => MongoDB

    :param user_name:
    :param repo_name:
    :param bunch_map:
    :param date_node_status_list:
    :return:
    """
    kafka_topic = "processor"

    kafka_client = KafkaClient("{kafka_host}:{kafka_port}".format(kafka_host=KAFKA_HOST, kafka_port=KAFKA_PORT))
    processor_producer = SimpleProducer(kafka_client)
    logging.info("Sending data to kafka")

    # tree status
    logging.info("Start to send tree statuses to kafka")
    tree_status_t1 = datetime.datetime.now()
    for cur_query_date in bunch_map:
        cur_query_datetime = datetime.datetime.combine(cur_query_date, datetime.time())
        cur_bunch = bunch_map[cur_query_date]
        tree_status_key = {
            'owner': user_name,
            'repo': repo_name,
            'date': cur_query_datetime
        }
        tree_status = {
            'owner': user_name,
            'repo': repo_name,
            'date': cur_query_datetime,
            'tree': cur_bunch.to_dict()
        }
        a_tree_status = (tree_status_key, tree_status)
        message = {
            'app': 'nwpc_log_processor',
            'type': 'mongodb_status',
            'timestamp': datetime.datetime.utcnow(),
            'data': {
                'version': '0.1',
                'type': 'tree_status',
                'status': a_tree_status
            }
        }
        message_string = json.dumps(message, default=json_default)
        response = processor_producer.send_messages(kafka_topic, message_string.encode('utf-8'))
    tree_status_t2 = datetime.datetime.now()
    logging.info("[1/2] Finish to send tree statuses to kafka in {t}".format(t=tree_status_t2 - tree_status_t1))

    # node status
    logging.info("Start to send node status to kafka")
    node_status_t1 = datetime.datetime.now()
    for a_node_status in date_node_status_list:
        message = {
            'app': 'nwpc_log_processor',
            'type': 'mongodb_status',
            'timestamp': datetime.datetime.utcnow(),
            'data': {
                'version': '0.1',
                'type': 'node_status',
                'status': a_node_status
            }
        }
        message_string = json.dumps(message, default=json_default)
        response = processor_producer.send_messages(kafka_topic, message_string.encode('utf-8'))
    node_status_t2 = datetime.datetime.now()
    logging.info("[2/2] Finish to send node statues to kafka in {t}".format(t=node_status_t2 - node_status_t1))
    logging.info("Finish send to mongodb.")
