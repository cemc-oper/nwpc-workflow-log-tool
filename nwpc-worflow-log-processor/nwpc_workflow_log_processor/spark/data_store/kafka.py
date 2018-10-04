import datetime
import json
import logging

KAFKA_HOST = '10.28.32.175'
KAFKA_PORT = '59092'


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