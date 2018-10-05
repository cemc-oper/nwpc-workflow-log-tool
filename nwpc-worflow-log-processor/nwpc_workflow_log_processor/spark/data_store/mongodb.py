import datetime
import logging

from mongoengine import connect

from nwpc_workflow_log_model.mongodb.node_tree import NodeTreeBlobData, NodeTreeBlob
from nwpc_workflow_log_model.mongodb.node_status import NodeStatusBlobData, NodeStatusBlob


def save_to_mongodb(config: dict, owner: str, repo: str, bunch_map: dict, date_node_status_list: dict,
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

    # saving results to mongodb
    if update_type == "insert":
        # delete previous data
        results = NodeTreeBlob.objects(
            owner=owner,
            repo=repo,
            data__date__gte=start_date - datetime.timedelta(days=1),
            data__date__lte=end_date
        )
        for a_node_tree_blob in results:
            a_node_tree_blob.delete()

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

        node_tree = NodeTreeBlob(
            owner=owner,
            repo=repo,
            data=NodeTreeBlobData(
                date=cur_query_datetime,
                tree=cur_bunch.to_dict()
            )
        )

        print(node_tree.owner, node_tree.repo, node_tree.data.date)
        if update_type == "upsert":
            NodeTreeBlob.objects(owner=owner, repo=repo, data__date=cur_query_datetime) \
                .update_one(set__tree=cur_bunch.to_dict(), upsert=True)
        else:
            node_tree.save()

    total_count = len(date_node_status_list)
    logging.info("Add {total_count} node status to mongodb...".format(total_count=total_count))

    if update_type == "insert":
        # delete previous data
        results = NodeStatusBlob.objects(
            owner=owner,
            repo=repo,
            data__date__gte=start_date,
            data__date__lte=end_date
        )
        for a_node_status_blob in results:
            a_node_status_blob.delete()

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
                NodeStatusBlob.objects(
                    owner=owner,
                    repo=repo,
                    data__date=a_node_status['date'],
                    data__node=a_node_status['node']
                ).update_one(
                    set__type=a_node_status['type'],
                    set__time_point=a_node_status['time_point'],
                    set__time_period=a_node_status['time_period'],
                    upsert=True)
            else:
                node_status = NodeStatusBlob(
                    owner=owner,
                    repo=repo,
                    data=NodeStatusBlobData(
                        node=a_node_status['node'],
                        date=a_node_status['date'],
                        type=a_node_status['type'],
                        time_point=a_node_status['time_point'],
                        time_period=a_node_status['time_period']
                    )
                )
                node_status.save()
    logging.info("Saving to mongodb is done.")
