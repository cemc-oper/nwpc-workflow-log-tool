# coding: utf-8
import datetime
import logging

from mongoengine import connect

from nwpc_workflow_log_model.mongodb.node_tree import NodeTreeBlobData, NodeTreeBlob
from nwpc_workflow_log_model.mongodb.node_status import NodeStatusBlobData, NodeStatusBlob


MAX_INSERT_COUNT = 100


def save_to_mongodb(config: dict, owner: str, repo: str,
                    bunch_map: dict, date_node_status_list: dict,
                    start_date, end_date):
    """
    数据流程：Spark Driver => MongoDB
    :param config:
    :param owner:
    :param repo:
    :param bunch_map:
    :param date_node_status_list:
    :param start_date:
    :param end_date:
    :return:
    """
    update_type = "insert"  # "update" or "insert"

    mongodb_database = config['datastore']['mongodb']['database']
    mongodb_host = config['datastore']['mongodb']['host']
    mongodb_port = config['datastore']['mongodb']['port']

    mongodb_client = connect(mongodb_database, host=mongodb_host, port=mongodb_port)

    from nwpc_workflow_log_model.mongodb.node_tree import save_bunch_map
    save_bunch_map(owner, repo, start_date, end_date, bunch_map, update_type)

    from nwpc_workflow_log_model.mongodb.node_status import save_node_status
    save_node_status(owner, repo, start_date, end_date, date_node_status_list, update_type)

    logging.info("Saving to mongodb is done.")
