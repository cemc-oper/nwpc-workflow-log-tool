import datetime
import logging

from pymongo import MongoClient

MONGODB_HOST = '192.168.99.100'
MONGODB_PORT = 32808


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
