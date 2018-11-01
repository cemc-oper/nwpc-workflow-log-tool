# coding=utf-8
import datetime
from operator import attrgetter

from nwpc_workflow_log_model.rmdb.util.node_situation_util import NodeSituationUtil
from nwpc_work_flow_model.sms import Bunch


def calculate_node_status(owner: str, repo: str, repo_type: str, begin_date, end_date, record_rdd, spark):
    query_date_list = []
    i = begin_date - datetime.timedelta(days=1)
    while i <= end_date:
        query_date_list.append(i.date())
        i = i + datetime.timedelta(days=1)

    ###############
    # STEP: check record_date, not necessary for mysql.
    ###############
    # record object => record object
    def filter_node(record):
        if record.date in query_date_list:
            return True
        else:
            return False

    some_date_record_rdd = record_rdd.filter(filter_node).cache()

    #####################
    # STEP: map to (date, node path)
    ######################
    # record object => (date, node_path)  distinct
    def node_path_map(record):
        return record.date, record.node_path

    date_node_path_rdd = some_date_record_rdd.map(node_path_map).distinct()

    ##########################
    # STEP: group by date
    #########################
    # (record_date, list of node_path)
    date_node_path_list_rdd = date_node_path_rdd.groupByKey()
    date_with_node_path_list = date_node_path_list_rdd.collect()

    print("Generating bunch...",)
    bunch_map = {}
    for i in date_with_node_path_list:
        day = i[0]
        node_path_list = i[1]
        bunch = Bunch()
        for node_path in node_path_list:
            if node_path is not None:
                bunch.add_node(node_path)
        print("Done")
        bunch_map[day] = bunch

    print("Begin to generate node status...")

    ##############
    # STEP: Record to (node path, record list)
    ##############
    # record object => (node_path, record) => (node_path, list of record)
    # NODE:
    #       When there are duplicate records in Hive, we need to distinct (node_path, record) or
    #       do something like it somewhere.
    def get_node_records(record):
        return record.node_path, record

    node_record_rdd = some_date_record_rdd.map(get_node_records).groupByKey()

    ##############
    # STEP: date, node_path, record list
    ##############
    # (node_path, list of record) => [(date, node_path, list of record), ...]
    def date_path_list_map(pair):
        start_date_object = begin_date.date()
        end_date_object = end_date.date()
        node_path = pair[0]
        record_list = pair[1]

        date_map = {}
        cur_date = start_date_object
        while cur_date < end_date_object:
            date_map[cur_date] = {}
            cur_date = cur_date + datetime.timedelta(days=1)

        for record in record_list:
            if node_path is None:
                continue
            cur_date = record.date
            prev_date = cur_date - datetime.timedelta(days=1)
            next_date = cur_date + datetime.timedelta(days=1)
            if start_date_object <= prev_date < end_date_object:
                if record.node_path in date_map[prev_date]:
                    date_map[prev_date][record.node_path].append(record)
                else:
                    date_map[prev_date][record.node_path] = [record]
            if start_date_object <= cur_date < end_date_object:
                if record.node_path in date_map[cur_date]:
                    date_map[cur_date][record.node_path].append(record)
                else:
                    date_map[cur_date][record.node_path] = [record]
            if start_date_object <= next_date < end_date_object:
                if record.node_path in date_map[next_date]:
                    date_map[next_date][record.node_path].append(record)
                else:
                    date_map[next_date][record.node_path] = [record]

        result_list = []

        for date in date_map:
            for path in date_map[date]:
                result_list.append((date, path, date_map[date][path]))

        return result_list

    date_node_record_rdd = node_record_rdd.flatMap(date_path_list_map)

    # test for [(date, node_path, list of record), ...] rdd
    # date_node_record = date_node_record_rdd.collect()
    # for a_date_node_record in date_node_record:
    #     print a_date_node_record[0], a_date_node_record[1]
    #     for i in a_date_node_record[2]:
    #         print "\t", i.record_string

    ##############################
    # STEP: generate node status
    #############################
    # (date, node_path, list of record) => (node_status_key, node_status)
    def get_node_status(o):
        local_date = o[0]
        local_datetime = datetime.datetime.combine(local_date, datetime.time())
        local_node_path = o[1]
        local_node_list = o[2]
        local_bunch = bunch_map[local_date]
        record_list = []
        for record in local_node_list:
            record_list.append(record)
        record_list = sorted(record_list, key=attrgetter('version_id', 'line_no'))

        if local_node_path is None:
            return
        node = local_bunch.find_node(local_node_path)
        if node is None:
            return
        node_type = node.get_node_type_string()

        node_status_key = {
            'owner': owner,
            'repo': repo,
            'node': local_node_path,
            'date': local_datetime
        }

        node_status = {
            'owner': owner,
            'repo': repo,
            'node': local_node_path,
            'date': local_datetime,
            'type': node_type,
            'time_point': {},
            'time_period': {}
        }

        if node_type == "task":
            node_status = NodeSituationUtil.get_task_node_situation_from_record_list(
                node_status, local_node_path, local_date, record_list)
        elif node_type == "family":
            node_status = NodeSituationUtil.get_family_node_situation_from_record_list(
                node_status, local_node_path, local_date, record_list)

        return node_status_key, node_status

    date_node_status_rdd = date_node_record_rdd.map(get_node_status)

    # test for each partition
    # 方案二：每个分区分别发送消息到 Kafka
    # def send_to_kafka_for_each_partition(iterator):
    #     # node status
    #     kafka = KafkaClient("{kafka_host}:{kafka_port}".format(kafka_host=KAFKA_HOST,
    #                                                        kafka_port=KAFKA_PORT))
    #     producer = SimpleProducer(kafka)
    #     for a_node_status in iterator:
    #         message = a_node_status
    #         message_string = json.dumps(message, default=json_default)
    #         response = producer.send_messages("test", message_string.encode('utf-8'))
    #
    # date_node_status_rdd.foreachPartition(send_to_kafka_for_each_partition)
    # return

    date_node_status_list = date_node_status_rdd.collect()

    return bunch_map, date_node_status_list
